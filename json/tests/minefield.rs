//! Tests from <https://seriot.ch/software/parsing_json.html>, aka
//! <https://github.com/nst/JSONTestSuite>.

use std::error::Error;
use std::fmt::Display;
use std::fs;
use std::io::Read;
use std::path::PathBuf;

#[derive(PartialEq, PartialOrd, Eq, Ord)]
enum Expect {
    Success,
    Failure,
    Either,
}

#[derive(Debug)]
struct Errors(Vec<Box<dyn Error>>);

impl Display for Errors {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let errs: Vec<_> = self.0.iter().map(|e| e.to_string()).collect();
        write!(f, "got error(s): {:?}", errs)
    }
}

impl Error for Errors {}

#[test]
fn test_json_minefield() -> Result<(), Box<dyn Error>> {
    let mut errors = Errors(vec![]);

    let mut corpus = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    corpus.push("tests/corpus/minefield");
    eprintln!("using minefield corpus at {corpus:?}");

    let mut files = vec![];
    for entry in fs::read_dir(corpus)? {
        let entry = entry?;
        let path = entry.path();
        assert!(path.is_file());

        let name = path
            .file_name()
            .expect("should all have names")
            .to_str()
            .ok_or_else(|| format!("non-utf8 filename: {:?}", path.file_name()))?;

        if name == "LICENSE" {
            continue;
        }

        assert_eq!(path.extension().and_then(|ext| ext.to_str()), Some("json"));

        let expect = match name.chars().take(1).next().unwrap() {
            'y' => Expect::Success,
            'n' => Expect::Failure,
            'i' => Expect::Either,
            c => panic!("unknown expectation: {}", c),
        };

        files.push((path, expect));
    }

    assert_eq!(files.len(), 318, "did not find expected number of tests");
    files.sort();

    for (file, expect) in files {
        let mut buf = Vec::with_capacity(32);
        let _ = fs::File::open(&file)?.read_to_end(&mut buf)?;

        let name = &file.file_name().unwrap();
        eprintln!("parsing {name:?}");

        match (expect, json::parse(&buf)) {
            (Expect::Success, Err(e)) => errors
                .0
                .push(format!("in {name:?}: want success, got failure: {e}").into()),
            (Expect::Failure, Ok(val)) => errors
                .0
                .push(format!("in {name:?}: want failure, got success value: {val}").into()),
            (Expect::Success, Ok(_))
            | (Expect::Failure, Err(_))
            | (Expect::Either, Ok(_))
            | (Expect::Either, Err(_)) => { /*OK */ }
        }
    }

    if errors.0.is_empty() {
        Ok(())
    } else {
        Err(errors.into())
    }
}
