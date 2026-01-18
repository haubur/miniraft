use std::env;
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    let args: Vec<&str> = args
        .iter()
        .skip(/* ignore exec path */ 1)
        .map(|s| s.as_str())
        .collect();

    match args.as_slice() {
        ["jdiff", left, right] => {
            let left = std::fs::read_to_string(left)?;
            let right = std::fs::read_to_string(right)?;

            let left = json::parse(left.as_bytes())?;
            let right = json::parse(right.as_bytes())?;

            let diff = json::diff::diff(&left, &right).to_string();
            if diff.trim().is_empty() {
                println!("no difference");
                Ok(())
            } else {
                println!("{diff}");
                Err("values differ".into())
            }
        }
        other => Err(format!("unknown command invocation: {other:?}").into()),
    }
}
