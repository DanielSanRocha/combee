use std::ops::Index;
use serde::{Serialize, Deserialize};

use combee;

#[derive(Clone, Deserialize, Serialize, PartialEq, Debug)]
struct Data {
    name: String,
    age: u32
}

#[test]
fn test_basic_csv_sort_by_age() {
    let df = combee::read_csv::<Data>(String::from("tests/fixtures/basic.csv")).unwrap();
    let df_sorted = df.sort(|x1,x2| x1.age > x2.age);

    let result = df_sorted.take(20);
    assert_eq!(result.len(), 3);

    assert_eq!(*result.index(0), Data {name: String::from("Leticia"), age: 22});
    assert_eq!(*result.index(1), Data {name: String::from("Daniel"), age: 26});
    assert_eq!(*result.index(2), Data {name: String::from("Sergio"), age: 30});
}

#[test]
fn test_unsorted_csv_sort_by_age() {
    let df = combee::read_csv::<Data>(String::from("tests/fixtures/unsorted.csv")).unwrap();
    let df_sorted = df.sort(|x1,x2| x1.age <= x2.age);

    assert_eq!(df.len(), 6);
    assert_eq!(df_sorted.len(), 6);

    let result = df_sorted.take(30);
    assert_eq!(result.len(), 6);

    assert_eq!(*result.index(0), Data {name: String::from("Mateus"), age: 31});
    assert_eq!(*result.index(1), Data {name: String::from("Sergio"), age: 30});
    assert_eq!(*result.index(2), Data {name: String::from("Daniel"), age: 26});
    assert_eq!(*result.index(3), Data {name: String::from("Lucas"), age: 26});
    assert_eq!(*result.index(4), Data {name: String::from("Leticia"), age: 22});
    assert_eq!(*result.index(5), Data {name: String::from("Francisco"), age: 22});
}
