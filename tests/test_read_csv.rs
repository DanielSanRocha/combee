use serde::{Deserialize, Serialize};
use std::ops::Index;

use combee;

#[derive(Clone, Deserialize, Serialize, PartialEq, Debug)]
struct Data {
    name: String,
    age: u32,
}

#[derive(Clone, Deserialize, Serialize, PartialEq, Debug)]
struct InvalidData {
    name: u32,
    age: u32,
}

#[test]
fn test_read_basic_csv_len() {
    let df = match combee::read_csv::<Data>(String::from("tests/fixtures/basic.csv")) {
        Ok(df) => df,
        Err(e) => panic!("{}", e.message),
    };

    assert_eq!(df.len(), 3);
}

#[test]
fn test_read_basic_csv_head_len() {
    let df = match combee::read_csv::<Data>(String::from("tests/fixtures/basic.csv")) {
        Ok(df) => df,
        Err(e) => panic!("{}", e.message),
    };
    assert_eq!(df.len(), 3);

    let result = df.take(2);

    assert_eq!(result.len(), 2);
}

#[test]
fn test_read_basic_csv_data() {
    let df = match combee::read_csv::<Data>(String::from("tests/fixtures/basic.csv")) {
        Ok(df) => df,
        Err(e) => panic!("{}", e.message),
    };

    assert_eq!(df.len(), 3);

    let result = df.take(10);
    assert_eq!(result.len(), 3);

    assert_eq!(
        *result.index(0),
        Data {
            name: String::from("Daniel"),
            age: 26
        }
    );
    assert_eq!(
        *result.index(1),
        Data {
            name: String::from("Sergio"),
            age: 30
        }
    );
    assert_eq!(
        *result.index(2),
        Data {
            name: String::from("Leticia"),
            age: 22
        }
    );
}

#[test]
fn test_read_basic_csv_with_invalid_structure() {
    match combee::read_csv::<InvalidData>(String::from("tests/fixtures/basic.csv")) {
        Ok(_) => panic!("read_csv should return an error!"),
        Err(e) => assert!(e.message.contains("string")),
    };
}

#[test]
fn test_read_invalid_csv() {
    match combee::read_csv::<Data>(String::from("tests/fixtures/invalid.csv")) {
        Ok(_) => panic!("read_csv should return an error!"),
        Err(e) => assert!(e.message.contains("string")),
    }
}

#[test]
fn test_read_csv_unwrap() {
    let df = combee::read_csv::<Data>(String::from("tests/fixtures/basic.csv")).unwrap();
    assert_eq!(df.len(), 3);
}

#[test]
fn test_read_csv_expects() {
    let df = combee::read_csv::<Data>(String::from("tests/fixtures/basic.csv")).expect("msg");
    assert_eq!(df.len(), 3);
}
