use std::ops::Index;

use serde::{Serialize, Deserialize};

use combee;

#[derive(Clone, Deserialize, Serialize, Debug, PartialEq)]
struct Data {
    name: String,
    age: u32
}

#[test]
fn test_read_basic_csv_len() {
    let df = match combee::read_csv::<Data>(String::from("tests/fixtures/basic.csv")) {
        Ok(df) => df,
        Err(e) => panic!("{}", e.message)
    };

    assert_eq!(df.len(), 3);
}

#[test]
fn test_read_basic_csv_head_len() {
    let df = match combee::read_csv::<Data>(String::from("tests/fixtures/basic.csv")) {
        Ok(df) => df,
        Err(e) => panic!("{}", e.message)
    };
    assert_eq!(df.len(), 3);

    let result = df.take(2);

    assert_eq!(result.len(), 2);
}

#[test]
fn test_read_basic_csv_data() {
    let df = match combee::read_csv::<Data>(String::from("tests/fixtures/basic.csv")) {
        Ok(df) => df,
        Err(e) => panic!("{}", e.message)
    };

    assert_eq!(df.len(), 3);

    let result = df.take(10);
    assert_eq!(result.len(), 3);

    assert_eq!(*result.index(0), Data {name: String::from("Daniel"), age: 26});
    assert_eq!(*result.index(1), Data {name: String::from("Sergio"), age: 30});
    assert_eq!(*result.index(2), Data {name: String::from("Leticia"), age: 22});
}
