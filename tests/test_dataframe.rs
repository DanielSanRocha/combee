use serde::{Deserialize, Serialize};
use std::ops::Index;

use combee;

#[derive(Clone, Deserialize, Serialize, PartialEq, Debug)]
struct Data {
    name: String,
    age: u32,
}

#[derive(Clone, Deserialize, Serialize, PartialEq, Debug)]
struct Message {
    message: String,
}

#[test]
fn test_basic_csv_head_2() {
    let df = match combee::read_csv::<Data>(String::from("tests/fixtures/basic.csv")) {
        Ok(df) => df,
        Err(e) => panic!("{}", e.message),
    };

    let df_head = df.head(2);
    assert_eq!(df_head.len(), 2);

    let result = df_head.take(2);
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
}

#[test]
fn test_basic_csv_head_20() {
    let df = match combee::read_csv::<Data>(String::from("tests/fixtures/basic.csv")) {
        Ok(df) => df,
        Err(e) => panic!("{}", e.message),
    };

    let df_head = df.head(20);
    assert_eq!(df_head.len(), 3);

    let result = df_head.take(3);
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
fn test_basic_csv_apply() {
    let df = match combee::read_csv::<Data>(String::from("tests/fixtures/basic.csv")) {
        Ok(df) => df,
        Err(e) => panic!("{}", e.message),
    };

    let df_message = df.apply(|row| Message {
        message: format!("Hello {} with {} years!", row.name, row.age),
    });

    assert_eq!(df_message.len(), 3);

    let messages = df_message.take(3);
    assert_eq!(
        *messages.index(0),
        Message {
            message: String::from("Hello Daniel with 26 years!")
        }
    );
    assert_eq!(
        *messages.index(1),
        Message {
            message: String::from("Hello Sergio with 30 years!")
        }
    );
    assert_eq!(
        *messages.index(2),
        Message {
            message: String::from("Hello Leticia with 22 years!")
        }
    );
}

#[test]
fn test_basic_csv_filter() {
    let df = match combee::read_csv::<Data>(String::from("tests/fixtures/basic.csv")) {
        Ok(df) => df,
        Err(e) => panic!("{}", e.message),
    };

    let df_filtered = df.filter(|row| row.age < 27);
    assert_eq!(df_filtered.len(), 2);

    let result = df_filtered.take(3);
    assert_eq!(result.len(), 2);
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
            name: String::from("Leticia"),
            age: 22
        }
    );
}
