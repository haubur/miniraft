use std::error::Error;
use std::fs;
use std::path::PathBuf;

#[test]
fn test_diff() -> Result<(), Box<dyn Error>> {
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

    let left = base.join("tests/corpus/diff/left.json");
    let right = base.join("tests/corpus/diff/right.json");

    let left = json::parse(fs::read_to_string(left)?.as_bytes())?;
    let right = json::parse(fs::read_to_string(right)?.as_bytes())?;

    let diff = json::diff::diff(&left, &right);

    assert_eq!(
        diff.to_string(),
        r#"$.a: only in left document:
	1

$.b: only in right document:
	1

$.differing_items[2]: values differ:
	left:
		100
	right:
		0

$.differing_items[3]: only in left document:
	101

$.foo: values differ:
	left:
		"bar"
	right:
		["baz"]

$.nested.a: only in left document:
	{"deeper": 1}

$.nested.b: only in right document:
	1

"#
    );

    Ok(())
}
