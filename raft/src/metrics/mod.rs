//! Simple, Prometheus-style metrics.

use std::collections::HashMap;
use std::fmt::Display;
use std::sync::{LazyLock, Mutex};

pub(crate) use inflight_proxy_requests::{InflightProxyRequests, InflightProxyRequestsLabels};
pub(crate) use state::{StateMetrics, StateMetricsLabels};

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

/// Metrics for overall internal Raft state.
mod state {
    use std::collections::HashMap;
    use std::sync::{LazyLock, Mutex};

    use crate::NodeID;
    use crate::metrics::{Gauge, PrometheusMetric, REGISTERED_METRICS, Type};
    use crate::state::stats::{Role, Stats};

    #[derive(Debug, Default)]
    pub(crate) struct StateMetrics;

    #[derive(Debug, PartialEq, Eq, Hash)]
    pub(crate) struct StateMetricsLabels {
        pub(crate) node: NodeID,
    }

    static STATE_METRICS_BACKING: LazyLock<Mutex<HashMap<StateMetricsLabels, Stats>>> =
        LazyLock::new(|| {
            REGISTERED_METRICS
                .lock()
                .expect("no poison")
                .push(Box::new(StateMetrics));
            eprintln!("metrics: registered state metric");

            Mutex::new(HashMap::new())
        });

    /// A bit weird, treating the entry thing as one gauge... works because [`Stats`]
    /// can be replaced atomically, all its contents are in fact gauges.
    impl Gauge for StateMetrics {
        type Value = Stats;
        type Labels = StateMetricsLabels;

        fn set(val: Self::Value, labels: Self::Labels) {
            STATE_METRICS_BACKING
                .lock()
                .expect("no poison")
                .insert(labels, val);
        }
    }

    impl PrometheusMetric for StateMetrics {
        fn name(&self) -> &'static str {
            unimplemented!("has several names, Prometheus conversion is custom");
        }

        fn to_prometheus(&self) -> String {
            use std::fmt::Write;
            let mut s = String::with_capacity(256);

            for (labels, value) in STATE_METRICS_BACKING.lock().expect("no poison").iter() {
                let node_pair = format!("\"node\"=\"{}\"", labels.node);

                // Role
                {
                    writeln!(s, "# TYPE role {}", Type::Gauge).expect("infallible for String");
                    writeln!(s, "role{{{node_pair}, role=\"{}\"}} 1", value.role)
                        .expect("infallible for String");

                    // Set to 0 explicitly so these count as exported. One of these checks
                    // will be false.
                    let mut n = 0;
                    if value.role != Role::Follower {
                        n += 1;
                        writeln!(s, "role{{{node_pair}, role=\"{}\"}} 0", Role::Follower)
                            .expect("infallible for String");
                    }
                    if value.role != Role::Candidate {
                        n += 1;
                        writeln!(s, "role{{{node_pair}, role=\"{}\"}} 0", Role::Candidate)
                            .expect("infallible for String");
                    }
                    if value.role != Role::Leader {
                        n += 1;
                        writeln!(s, "role{{{node_pair}, role=\"{}\"}} 0", Role::Leader)
                            .expect("infallible for String");
                    }
                    assert_eq!(n, 2);
                }

                // Term
                {
                    writeln!(s, "# TYPE term {}", Type::Gauge).expect("infallible for String");
                    writeln!(s, "term{{{node_pair}}} {}", value.term)
                        .expect("infallible for String");
                }

                // Voted for
                writeln!(s, "# TYPE voted_for {}", Type::Gauge).expect("infallible for String");
                if let Some(peer) = &value.voted_for {
                    writeln!(s, "voted_for{{{node_pair}, peer=\"{}\"}} 1", peer)
                        .expect("infallible for String");
                } else {
                    writeln!(s, "voted_for{{{node_pair}}} 0").expect("infallible for String");
                }

                // Log size
                {
                    writeln!(s, "# TYPE log_size {}", Type::Gauge).expect("infallible for String");
                    writeln!(
                        s,
                        "log_size{{{node_pair}}} {}",
                        value.log_size.map(|idx| idx.0.get()).unwrap_or_default()
                    )
                    .expect("infallible for String");
                }

                // Commit index
                {
                    writeln!(s, "# TYPE commit_index {}", Type::Gauge)
                        .expect("infallible for String");
                    writeln!(
                        s,
                        "commit_index{{{node_pair}}} {}",
                        value
                            .commit_index
                            .map(|idx| idx.0.get())
                            .unwrap_or_default()
                    )
                    .expect("infallible for String");
                }

                // Last applied
                {
                    writeln!(s, "# TYPE last_applied {}", Type::Gauge)
                        .expect("infallible for String");
                    writeln!(
                        s,
                        "last_applied{{{node_pair}}} {}",
                        value
                            .last_applied
                            .map(|idx| idx.0.get())
                            .unwrap_or_default()
                    )
                    .expect("infallible for String");
                }
            }

            s
        }
    }
}

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

#[cfg(test)]
mod tests {
    use std::num::NonZero;

    use crate::metrics::state::{StateMetrics, StateMetricsLabels};
    use crate::metrics::{Gauge, PrometheusMetric};
    use crate::state::stats::{Role, Stats};
    use crate::state::{LogIndex, Term};

    #[test]
    fn test_state_metrics() {
        StateMetrics::set(
            Stats {
                role: Role::Candidate,
                term: Term(3),
                voted_for: Some("server02".into()),
                log_size: Some(LogIndex(NonZero::new(4).unwrap())),
                commit_index: Some(LogIndex(NonZero::new(3).unwrap())),
                last_applied: None,
            },
            StateMetricsLabels {
                node: "server01".into(),
            },
        );

        // Implicit global state...
        let sm = StateMetrics;

        assert_eq!(
            sm.to_prometheus(),
            r#"# TYPE role gauge
role{"node"="server01", role="candidate"} 1
role{"node"="server01", role="follower"} 0
role{"node"="server01", role="leader"} 0
# TYPE term gauge
term{"node"="server01"} 3
# TYPE voted_for gauge
voted_for{"node"="server01", peer="server02"} 1
# TYPE log_size gauge
log_size{"node"="server01"} 4
# TYPE commit_index gauge
commit_index{"node"="server01"} 3
# TYPE last_applied gauge
last_applied{"node"="server01"} 0
"#
        );
    }
}
