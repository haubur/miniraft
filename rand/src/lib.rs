use std::fs::File;
use std::io::Read;

const RANDOM_FILE: &str = "/dev/urandom";

/// Generate a random float between 0 and 1.0.
///
/// Do not use for anything serious.
#[cfg(unix)]
pub fn rand() -> f64 {
    let mut file = File::open(RANDOM_FILE).expect("unix system should have random file");
    let mut buf = [0u8; 8];

    file.read_exact(&mut buf)
        .expect("failed to read bytes from random file");

    // u64 has no invalid bit patterns.
    let val = u64::from_ne_bytes(buf);
    (val as f64) / (u64::MAX as f64)
}

#[cfg(test)]
mod tests {
    use crate::rand;

    #[test]
    fn test_rand() {
        let n = 1_000;

        let mut seen = Vec::with_capacity(n);

        for _ in 0..=n {
            let v = rand();
            assert!((0.0..=1.0).contains(&v));

            seen.push(v);
        }

        // Some extremely basic assertions...
        let avg: f64 = seen.iter().sum::<f64>() / (seen.len() as f64);
        assert!((0.3..=0.7).contains(&avg));
    }
}
