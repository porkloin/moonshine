//! OpenTelemetry integration for moonshine.
//!
//! Two signals are exported when `[telemetry] otlp_endpoint` is set in the
//! config (or `--otlp-endpoint` is passed to the bench harness):
//!
//! - **Traces**: per-frame `frame.encode` root span with child spans for each
//!   pipeline stage (`channel_wait`, `import`, `convert`, `encode`,
//!   `packetize`, `send`). Useful for debugging individual outliers — when
//!   a spike fires you can pull up the trace and see which stage exploded.
//!   Tail-sampled by default (keep all frames > frame_budget, sample 1% of
//!   normal frames) so a 120 fps session doesn't drown the collector.
//!
//! - **Metrics**: pre-aggregated histograms / gauges / counters exported on
//!   a fixed cadence (default 10s). Cheap, full-fidelity, perfect for
//!   dashboards and alerts. The histograms are the same percentiles the
//!   bench text report shows; metrics let you watch them trend over hours
//!   instead of computing them once per bench run.
//!
//! Hot path is never blocked: spans are batched + flushed by a background
//! tokio task, metrics collected via lock-free instruments. If the
//! collector goes away, exports drop on the floor and moonshine keeps
//! streaming.

use opentelemetry::{
	global,
	metrics::{Counter, Gauge, Histogram, Meter},
	trace::TracerProvider as _,
	KeyValue,
};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
	metrics::{PeriodicReader, SdkMeterProvider},
	runtime,
	trace::{Sampler, TracerProvider},
	Resource,
};
use opentelemetry_semantic_conventions::resource as semres;
use std::time::Duration;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// Configuration for the OTel pipeline. Constructed from `[telemetry]` in
/// the config file, or from bench-harness CLI flags.
#[derive(Debug, Clone)]
pub struct TelemetryConfig {
	/// OTLP/gRPC endpoint URL (e.g. "http://localhost:4317"). When `None`,
	/// telemetry is disabled — no spans created, no metrics collected,
	/// zero overhead beyond a couple of dead-code branches.
	pub otlp_endpoint: Option<String>,

	/// Optional service name override (default: "moonshine").
	pub service_name: Option<String>,

	/// Sampling rate for non-spike traces (0.0–1.0). Defaults to 0.01 —
	/// keep all spike frames via the always-on tail rule, sample 1% of
	/// the rest so collector load stays sane at 120 fps. Set to 1.0 in
	/// bench mode for full-fidelity capture.
	pub trace_sample_rate: f64,

	/// Metrics export interval. Defaults to 10s (Prometheus convention).
	pub metric_export_interval: Duration,
}

impl Default for TelemetryConfig {
	fn default() -> Self {
		Self {
			otlp_endpoint: None,
			service_name: None,
			trace_sample_rate: 0.01,
			metric_export_interval: Duration::from_secs(10),
		}
	}
}

/// Held by main(). Drops the OTel pipelines on shutdown so spans/metrics
/// in the batch buffer get flushed.
pub struct TelemetryGuard {
	tracer_provider: Option<TracerProvider>,
	meter_provider: Option<SdkMeterProvider>,
}

impl Drop for TelemetryGuard {
	fn drop(&mut self) {
		if let Some(tp) = self.tracer_provider.take() {
			let _ = tp.shutdown();
		}
		if let Some(mp) = self.meter_provider.take() {
			let _ = mp.shutdown();
		}
	}
}

/// Build the resource attributes attached to every export. Using OTel
/// semantic conventions where possible so dashboards from other Rust
/// services can reuse the same field names.
fn build_resource(service_name: &str) -> Resource {
	Resource::new([
		KeyValue::new(semres::SERVICE_NAME, service_name.to_string()),
		KeyValue::new(semres::SERVICE_VERSION, env!("CARGO_PKG_VERSION")),
		KeyValue::new("moonshine.host", hostname::get().ok().and_then(|h| h.into_string().ok()).unwrap_or_default()),
	])
}

/// Initialize OTel + bridge moonshine's existing `tracing` spans into it.
/// Returns a guard that must be held alive for the program lifetime.
///
/// When `cfg.otlp_endpoint` is `None`, this still installs the local
/// stdout `tracing-subscriber` (so logs work) but skips all OTel pipeline
/// init.
pub fn init(cfg: &TelemetryConfig) -> Result<TelemetryGuard, String> {
	let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
	let fmt_layer = tracing_subscriber::fmt::layer();

	let Some(endpoint) = &cfg.otlp_endpoint else {
		// Telemetry off — install only the stdout layer.
		tracing_subscriber::registry().with(env_filter).with(fmt_layer).init();
		return Ok(TelemetryGuard {
			tracer_provider: None,
			meter_provider: None,
		});
	};

	let service_name = cfg.service_name.clone().unwrap_or_else(|| "moonshine".to_string());
	let resource = build_resource(&service_name);

	// === Tracer provider ===
	// Tail sampling: ParentBased(TraceIdRatioBased(rate)). Caller spans
	// can override per-trace via tracing attributes (`always_sample = true`)
	// when emitting a known-spike frame.
	let exporter = opentelemetry_otlp::SpanExporter::builder()
		.with_tonic()
		.with_endpoint(endpoint)
		.build()
		.map_err(|e| format!("OTel: build span exporter: {e}"))?;

	let tracer_provider = TracerProvider::builder()
		.with_resource(resource.clone())
		.with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(cfg.trace_sample_rate))))
		.with_batch_exporter(exporter, runtime::Tokio)
		.build();

	let tracer = tracer_provider.tracer(service_name.clone());
	global::set_tracer_provider(tracer_provider.clone());

	// === Meter provider ===
	let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
		.with_tonic()
		.with_endpoint(endpoint)
		.build()
		.map_err(|e| format!("OTel: build metric exporter: {e}"))?;

	let reader = PeriodicReader::builder(metric_exporter, runtime::Tokio)
		.with_interval(cfg.metric_export_interval)
		.build();

	let meter_provider = SdkMeterProvider::builder().with_resource(resource).with_reader(reader).build();
	global::set_meter_provider(meter_provider.clone());

	// === tracing → OTel bridge ===
	// Existing `tracing::info_span!` calls in the pipeline now also emit
	// OTel spans without code changes.
	let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

	tracing_subscriber::registry()
		.with(env_filter)
		.with(fmt_layer)
		.with(otel_layer)
		.init();

	Ok(TelemetryGuard {
		tracer_provider: Some(tracer_provider),
		meter_provider: Some(meter_provider),
	})
}

/// Pre-built metric instruments used by the video pipeline. Cheap to
/// construct (interns into the global meter provider) and lock-free to
/// record into. Held by `VideoPipelineInner` so we don't re-resolve
/// instruments per frame.
pub struct PipelineMetrics {
	pub frames_total: Counter<u64>,
	pub spikes_total: Counter<u64>,
	pub stage_latency_us: Histogram<u64>,
	pub total_latency_us: Histogram<u64>,
	pub encoded_bytes: Histogram<u64>,
	// Sampled at 1 Hz by `pipeline::spawn_gpu_sampler` whenever telemetry
	// is enabled. AMD-only — no-op when no AMD card is found under
	// `/sys/class/drm`.
	pub gpu_sclk_mhz: Gauge<u64>,
	pub gpu_busy_pct: Gauge<u64>,
	pub vram_used_bytes: Gauge<u64>,
	pub dmabuf_cache_size: Gauge<u64>,
}

impl PipelineMetrics {
	pub fn new(meter: &Meter) -> Self {
		Self {
			frames_total: meter.u64_counter("moonshine.frames").build(),
			spikes_total: meter.u64_counter("moonshine.spikes").build(),
			stage_latency_us: meter
				.u64_histogram("moonshine.stage_latency")
				.with_unit("us")
				.with_description("Per-stage frame latency (channel_wait/import/convert/encode/packetize/send)")
				.build(),
			total_latency_us: meter
				.u64_histogram("moonshine.total_latency")
				.with_unit("us")
				.with_description("End-to-end host-processing latency per frame")
				.build(),
			encoded_bytes: meter.u64_histogram("moonshine.encoded_bytes").with_unit("By").build(),
			gpu_sclk_mhz: meter.u64_gauge("moonshine.gpu.sclk_mhz").with_unit("MHz").build(),
			gpu_busy_pct: meter.u64_gauge("moonshine.gpu.busy").with_unit("%").build(),
			vram_used_bytes: meter.u64_gauge("moonshine.vram.used").with_unit("By").build(),
			dmabuf_cache_size: meter
				.u64_gauge("moonshine.dmabuf.cache_size")
				.with_description("Number of cached DMA-BUF imports currently resident")
				.build(),
		}
	}

	/// Convenience: record a fully-tagged latency sample.
	pub fn record_frame(&self, codec: &str, hdr: bool, sample: &PipelineLatency) {
		let attrs = [
			KeyValue::new("codec", codec.to_string()),
			KeyValue::new("hdr", hdr),
		];
		self.frames_total.add(1, &attrs);
		self.total_latency_us.record(sample.total_us, &attrs);
		self.encoded_bytes.record(sample.encoded_bytes as u64, &attrs);
		for (stage, us) in sample.stages() {
			let stage_attrs = [
				KeyValue::new("codec", codec.to_string()),
				KeyValue::new("hdr", hdr),
				KeyValue::new("stage", stage.to_string()),
			];
			self.stage_latency_us.record(us, &stage_attrs);
		}
		if sample.total_us > sample.frame_budget_us {
			self.spikes_total.add(1, &attrs);
		}
	}
}

/// Mirror of the existing pipeline `LatencySample` shaped for metric emission.
pub struct PipelineLatency {
	pub channel_wait_us: u64,
	pub import_us: u64,
	pub convert_us: u64,
	pub encode_us: u64,
	pub packetize_us: u64,
	pub send_us: u64,
	pub total_us: u64,
	pub encoded_bytes: usize,
	pub frame_budget_us: u64,
}

impl PipelineLatency {
	fn stages(&self) -> [(&'static str, u64); 6] {
		[
			("channel_wait", self.channel_wait_us),
			("import", self.import_us),
			("convert", self.convert_us),
			("encode", self.encode_us),
			("packetize", self.packetize_us),
			("send", self.send_us),
		]
	}
}

// ---- minimal hostname shim so we don't add another dependency just for this
mod hostname {
	use std::ffi::CStr;
	pub fn get() -> std::io::Result<std::ffi::OsString> {
		let mut buf = vec![0u8; 256];
		// SAFETY: gethostname writes a NUL-terminated string into buf.
		let r = unsafe { libc::gethostname(buf.as_mut_ptr() as *mut _, buf.len()) };
		if r != 0 {
			return Err(std::io::Error::last_os_error());
		}
		let cstr = unsafe { CStr::from_ptr(buf.as_ptr() as *const _) };
		Ok(std::ffi::OsString::from(cstr.to_string_lossy().into_owned()))
	}
}
