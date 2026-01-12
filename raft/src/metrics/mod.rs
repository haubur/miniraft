//! Simple, Prometheus-style metrics.

use std::collections::HashMap;
use std::fmt::Display;
use std::sync::{LazyLock, Mutex};

pub(crate) use inflight_proxy_requests::{InflightProxyRequests, InflightProxyRequestsLabels};

use crate::NodeID;

/// Simple HTTP/1.1 implementation for serving metrics.
pub mod http;

/// [Type of metric](https://prometheus.io/docs/concepts/metric_types/).
#[derive(Debug, Clone, Copy)]
enum Type {
    Gauge,
}

impl Display for Type {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Type::Gauge => write!(f, "gauge"),
        }
    }
}

/// A metric while can export itself to Prometheus.
pub(crate) trait PrometheusMetric: Send {
    fn name(&self) -> &'static str;
    /// Serialize self into [acceptable wire
    /// format](https://prometheus.io/docs/instrumenting/exposition_formats/).
    fn to_prometheus(&self) -> String;
}

/// A [Gauge metric](https://prometheus.io/docs/concepts/metric_types/#gauge).
pub(crate) trait Gauge: PrometheusMetric {
    type Value;
    type Labels;

    fn set(val: Self::Value, labels: Self::Labels);
}

/// Metrics registered for export.
static REGISTERED_METRICS: LazyLock<Mutex<Vec<Box<dyn PrometheusMetric>>>> =
    LazyLock::new(|| Mutex::new(Vec::new()));

mod inflight_proxy_requests {
    #[allow(clippy::wildcard_imports)]
    use super::*;

    #[derive(Debug, Default)]
    pub(crate) struct InflightProxyRequests;

    #[derive(Debug, PartialEq, Eq, Hash)]
    pub(crate) struct InflightProxyRequestsLabels {
        pub(crate) node: NodeID,
    }

    static INFLIGHT_PROXY_REQUESTS_BACKING: LazyLock<
        Mutex<HashMap<InflightProxyRequestsLabels, usize>>,
    > = LazyLock::new(|| {
        REGISTERED_METRICS
            .lock()
            .expect("no poison")
            .push(Box::new(InflightProxyRequests));
        eprintln!("metrics: registered inflight proxy requests metric");

        Mutex::new(HashMap::new())
    });

    impl PrometheusMetric for InflightProxyRequests {
        fn name(&self) -> &'static str {
            "inflight_proxy_requests"
        }

        fn to_prometheus(&self) -> String {
            use std::fmt::Write;
            let mut s = String::with_capacity(256);
            let name = self.name();

            writeln!(s, "# TYPE {} {}", name, Type::Gauge).expect("infallible for String");
            for (labels, value) in INFLIGHT_PROXY_REQUESTS_BACKING
                .lock()
                .expect("no poison")
                .iter()
            {
                writeln!(s, "{name}{{node=\"{}\"}} {}", labels.node, *value as f64)
                    .expect("infallible for String");
            }

            s
        }
    }

    impl Gauge for InflightProxyRequests {
        type Value = usize;
        type Labels = InflightProxyRequestsLabels;

        fn set(val: Self::Value, labels: Self::Labels) {
            INFLIGHT_PROXY_REQUESTS_BACKING
                .lock()
                .expect("no poison")
                .insert(labels, val);
        }
    }
}
