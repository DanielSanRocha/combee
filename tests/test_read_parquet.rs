use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use combee;

#[derive(Clone, Deserialize, Serialize, PartialEq, Debug)]
struct Data {
    name: String,
    age: u32,
}

#[derive(Clone, Deserialize, Serialize, PartialEq, Debug)]
struct Complex {
    index: i32,
    sequence: Vec<i32>,
    map: HashMap<String, Vec<i32>>,
}

#[test]
fn test_read_basic_parquet_len() {
    let df = match combee::read_parquet::<Data>(String::from("tests/fixtures/basic.parquet")) {
        Ok(df) => df,
        Err(e) => panic!("{}", e.message),
    };

    assert_eq!(df.len(), 3);
}

#[test]
fn test_read_basic_parquet() {
    let df = match combee::read_parquet::<Data>(String::from("tests/fixtures/basic.parquet")) {
        Ok(df) => df,
        Err(e) => panic!("{}", e.message),
    };
    assert_eq!(df.len(), 3);

    let rowd = df.find(|x| x.name == "Daniel").unwrap();
    assert_eq!(
        *rowd,
        Data {
            name: String::from("Daniel"),
            age: 26
        }
    );

    let rows = df.find(|x| x.name == "Sergio").unwrap();
    assert_eq!(
        *rows,
        Data {
            name: String::from("Sergio"),
            age: 30
        }
    );

    let rowl = df.find(|x| x.name == "Leticia").unwrap();
    assert_eq!(
        *rowl,
        Data {
            name: String::from("Leticia"),
            age: 22
        }
    );
}

#[test]
fn test_read_complex_parquet() {
    let df =
        combee::read_parquet::<Complex>(String::from("tests/fixtures/complex.parquet")).unwrap();

    assert_eq!(df.len(), 3);

    let row1 = df.find(|x| x.index == 1).unwrap();
    let mut map1 = HashMap::new();
    map1.insert(String::from("x"), Vec::<i32>::new());
    map1.insert(String::from("y"), Vec::<i32>::new());
    assert_eq!(
        *row1,
        Complex {
            index: 1,
            sequence: Vec::new(),
            map: map1
        }
    );

    let row2 = df.find(|x| x.index == 2).unwrap();
    let mut map2 = HashMap::new();
    map2.insert(String::from("x"), [2, 42].to_vec());
    map2.insert(String::from("y"), Vec::new());
    assert_eq!(
        *row2,
        Complex {
            index: 2,
            sequence: [2, 42].to_vec(),
            map: map2
        }
    );

    let row3 = df.find(|x| x.index == 3).unwrap();
    let mut map3 = HashMap::new();
    map3.insert(String::from("x"), [1].to_vec());
    map3.insert(String::from("y"), [2].to_vec());
    assert_eq!(
        *row3,
        Complex {
            index: 3,
            sequence: [4, 12].to_vec(),
            map: map3
        }
    );
}
