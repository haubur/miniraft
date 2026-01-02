#![feature(test)]

extern crate test;
use json::parse;
use test::Bencher;

const JSON_FRAGMENT: &str = r#"
{
    "id": 12345,
    "title": "Complex \u2764 Data",
    "subtitle": "Native UTF8, different widths: José (2), ⌣ (3), 🔥 (4)",
    "description": "Surrogate Unicode pair: \ud834\udd1e !",
    "is_active": true,
    "deleted": false,
    "meta": {
        "created_at": "2023-10-27T10:00:00Z",
        "counts": [ 1, 2, 3 ],
        "empty_map": {}
    },
    "values": [
        null,
        -0.5,
        1.23e+4,
        "escaped\nline"
    ],
    "empty_list": []
}
"#;

#[bench]
fn bench_parse_throughput(b: &mut Bencher) {
    let repeat_count = 1_000;
    let mut json = String::with_capacity(JSON_FRAGMENT.len() * repeat_count + 2);

    // Build array
    json.push('[');
    for i in 0..repeat_count {
        if i > 0 {
            json.push(',');
        }
        json.push_str(JSON_FRAGMENT);
    }
    json.push(']');

    let data = json.as_bytes();
    b.bytes = data.len() as u64;
    b.iter(|| parse(data));
}
