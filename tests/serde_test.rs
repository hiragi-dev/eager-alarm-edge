use chrono::Weekday;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
struct TestStruct {
    days: Vec<Weekday>,
}

#[test]
fn test_weekday_serde() {
    let s = r#"{"days": ["Mon", "Wed", "Sun"]}"#;
    let decoded: TestStruct = serde_json::from_str(s).unwrap();
    println!("{:?}", decoded);
}
