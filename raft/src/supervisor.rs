use std::os::unix;
use std::process::{Command, Stdio};
use std::time::Duration;
use std::{env, io, process, thread};

use crate::maelstrom::infra::read_and_handle_init;

/// Mark spawned children to break process tree recursion.
const CHILD_MARKER_ENV_VAR: &str = "RAFT_RUNNER";
const CHILD_MARKER_ENV_VAR_VALUE: &str = "6f5b5003-35b9-4a1a-8fab-93bf344af0e0";

/// Threshold from 0 to 1, that if passed by dice roll will cause time bomb to trigger.
const TIME_BOMB_THRESHOLD: f64 = 0.97;

/// Interval at which to run time bomb dice rolls.
const TIME_BOMB_INTERVAL: Duration = Duration::from_millis(1_000);

/// A gate to run at process launch: the parent process supervisor never passes, instead
/// loops forever spawning one child (clone of itself -- a bit like `fork` without
/// `exec` but full memory wipe).
///
/// The child passes this same gate and runs as a normal process. On crash, it is
/// revived by the supervisor.
pub fn gate(mut env: env::VarsOs) -> Result<(String, Vec<String>), io::Error> {
    if env.any(|(name, value)| name == CHILD_MARKER_ENV_VAR && value == CHILD_MARKER_ENV_VAR_VALUE)
    {
        eprintln!(
            "process: runner: pid {}, parent {}",
            process::id(),
            unix::process::parent_id()
        );

        thread::Builder::new()
            .name("process-time-bomb-loop".into())
            .spawn(|| {
                eprintln!("process: runner: launching time bomb thread");
                loop {
                    eprintln!("process: runner: time bomb loop");

                    if rand::rand() >= TIME_BOMB_THRESHOLD {
                        eprintln!("process: runner: triggering time bomb");

                        // No mercy: no destructors, no unwinding, no nothing. Pull the
                        // plug! This wipes all state, and forces the node to reboot
                        // from persistent state. That exercises persist + restore
                        // routines (see Raft paper for what gets wiped and what gets
                        // restored, and what we're thus exercising).
                        process::abort();
                    }

                    thread::sleep(TIME_BOMB_INTERVAL);
                }
            })
            .expect("named thread creation should always succeed");

        let this_node = env::var("NODE").expect("child should be passed own node name");
        let peers = env::var("PEERS")
            .expect("child should be passed peer node names")
            .split(',')
            .map(|s| s.to_string())
            .collect();

        return Ok((this_node, peers));
    }

    eprintln!(
        "process: supervisor: pid {}, parent {}",
        process::id(),
        unix::process::parent_id()
    );

    let (this_node, peers) = {
        let mut buf = String::with_capacity(64);
        let (this_node, mut peers) = read_and_handle_init(&mut buf)?;
        peers.retain(|id| *id != this_node);
        (this_node, peers) // de-mut
    };

    // Get ourselves. Note, this can be insecure:
    // https://doc.rust-lang.org/std/env/fn.current_exe.html#security.
    let exe = env::current_exe()?;
    eprintln!("process: supervisor: current exe: {exe:?}");

    loop {
        eprintln!("process: supervisor: spawning runner child");

        let mut handle = Command::new(&exe)
            .env(CHILD_MARKER_ENV_VAR, CHILD_MARKER_ENV_VAR_VALUE)
            .env("NODE", this_node.clone())
            .env("PEERS", peers.join(","))
            // For `spawn`, `inherit` is default, but we rely on this so be explicit.
            .stdin(Stdio::inherit())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()?;

        eprintln!("process: supervisor: awaiting runner child");
        handle.wait()?;
        eprintln!("process: supervisor: awaited runner child, rebooting it");

        // Simulate node reboot time.
        thread::sleep(Duration::from_millis(500));
    }
}
