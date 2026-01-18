//! A very simple JSON diffing implementation. Does not produce minimal diffs.

use std::collections::HashSet;
use std::fmt::Display;

use crate::Value;

/// A diff between two JSON documents.
#[derive(Debug)]
pub struct Diff<'doc> {
    findings: Vec<Finding<'doc>>,
}

/// Pretty-print.
impl Display for Diff<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for finding in &self.findings {
            writeln!(
                f,
                "{}: {}:",
                finding.loc,
                match finding.verdict {
                    Verdict::OnlyLeft(_) => "only in left document",
                    Verdict::OnlyRight(_) => "only in right document",
                    Verdict::Differ(_, _) => "values differ",
                }
            )?;

            match finding.verdict {
                Verdict::OnlyLeft(v) | Verdict::OnlyRight(v) => writeln!(f, "\t{v}\n")?,
                Verdict::Differ(l, r) => writeln!(f, "\tleft:\n\t\t{l}\n\tright:\n\t\t{r}\n")?,
            }
        }

        Ok(())
    }
}

#[derive(Debug)]
struct Finding<'doc> {
    loc: Loc,
    verdict: Verdict<'doc>,
}

/// A location in a JSON document (given by keys in objects and indexes in arrays).
#[derive(Debug, Clone)]
struct Loc(Vec<String>);

impl Loc {
    fn with(&self, tail: String) -> Self {
        let mut loc = self.clone();
        loc.0.push(tail);
        loc
    }
}

/// Pretty-print to a JSON pointer-like representation.
impl Display for Loc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "$.{}", self.0.join("."))
    }
}

#[derive(Debug)]
enum Verdict<'doc> {
    OnlyLeft(&'doc Value),
    OnlyRight(&'doc Value),
    Differ(&'doc Value, &'doc Value),
}

/// Produce a diff between two JSON values.
pub fn diff<'doc>(left: &'doc Value, right: &'doc Value) -> Diff<'doc> {
    let mut findings = diff_rec(Loc(vec![]), left, right);

    // Make diff reporting stable
    findings.sort_by_key(|f| f.loc.to_string());

    Diff { findings }
}

/// Recursive backing implementation.
fn diff_rec<'doc>(location: Loc, left: &'doc Value, right: &'doc Value) -> Vec<Finding<'doc>> {
    // Findings for this recursion level only.
    let mut findings = vec![];

    if location.0.len() > 256 {
        // Location length is recursion depth
        eprintln!("depth too large, quitting diff");
        return findings;
    }

    match (left, right) {
        (Value::Object(l), Value::Object(r)) => {
            let l_keys: HashSet<&String> = l.keys().collect();
            let r_keys: HashSet<&String> = r.keys().collect();

            for (k, v) in l.iter().filter(|(k, _)| !r_keys.contains(k)) {
                findings.push(Finding {
                    loc: location.with(k.clone()),
                    verdict: Verdict::OnlyLeft(v),
                });
            }

            for (k, v) in r.iter().filter(|(k, _)| !l_keys.contains(k)) {
                findings.push(Finding {
                    loc: location.with(k.clone()),
                    verdict: Verdict::OnlyRight(v),
                });
            }

            for &key in l_keys.intersection(&r_keys) {
                findings.extend(diff_rec(
                    location.with(key.clone()),
                    l.get(key).expect("checked for key"),
                    r.get(key).expect("checked for key"),
                ));
            }
        }
        (Value::Array(l), Value::Array(r)) => {
            let mut l = l.iter();
            let mut r = r.iter();

            for i in 0.. {
                let mut loc = location.clone();

                // If this is not top-level, attach array indexing notation to current
                // location.
                if let Some(tail) = loc.0.last_mut() {
                    *tail = format!("{tail}[{i}]");
                } else {
                    loc.0 = vec![format!("[{i}]")]
                }

                match (l.next(), r.next()) {
                    (Some(l), Some(r)) => {
                        findings.extend(diff_rec(loc, l, r));
                    }
                    (Some(v), None) => {
                        findings.push(Finding {
                            loc,
                            verdict: Verdict::OnlyLeft(v),
                        });
                    }
                    (None, Some(v)) => {
                        findings.push(Finding {
                            loc,
                            verdict: Verdict::OnlyRight(v),
                        });
                    }
                    (None, None) => break,
                }
            }
        }
        (Value::Number(l), Value::Number(r)) => {
            if l != r {
                findings.push(Finding {
                    loc: location.clone(),
                    verdict: Verdict::Differ(left, right),
                });
            }
        }
        (Value::String(l), Value::String(r)) => {
            if l != r {
                findings.push(Finding {
                    loc: location.clone(),
                    verdict: Verdict::Differ(left, right),
                });
            }
        }
        (Value::Bool(l), Value::Bool(r)) => {
            if l != r {
                findings.push(Finding {
                    loc: location.clone(),
                    verdict: Verdict::Differ(left, right),
                });
            }
        }
        (Value::Null, Value::Null) => { /* always identical */ }
        _ => findings.push(Finding {
            // Different types: never equal
            loc: location.clone(),
            verdict: Verdict::Differ(left, right),
        }),
    }

    findings
}
