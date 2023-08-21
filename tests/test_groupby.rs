use std::fmt::Debug;
use serde::{Serialize, Deserialize};

use combee;
use combee::functions::{count, mean, sum};

#[derive(Clone, Deserialize, Serialize, PartialEq, Debug)]
struct Data {
    name: String,
    age: u32
}

#[derive(Clone, Deserialize, Serialize, PartialEq, Debug)]
struct Stats<V: Clone + Serialize + PartialEq + Debug> {
    index: u32,
    value: V
}

#[test]
fn test_unsorted_groupby_count() {
    let df = combee::read_csv::<Data>(String::from("tests/fixtures/unsorted.csv")).unwrap();
    let df_grouped = df.groupby(|x| x.age).agg(|index, g| Stats { index: *index, value: count(g)});

    assert_eq!(df_grouped.len(), 4);

    let row_26 = df_grouped.find(|x| x.index == 26).unwrap();
    assert_eq!(row_26.value, 2);

    let row_30 = df_grouped.find(|x| x.index == 30).unwrap();
    assert_eq!(row_30.value, 1);

    let row_22 = df_grouped.find(|x| x.index == 22).unwrap();
    assert_eq!(row_22.value, 2);

    let row_31 = df_grouped.find(|x| x.index == 31).unwrap();
    assert_eq!(row_31.value, 1);
}

#[test]
fn test_unsorted_groupby_sum() {
    let df = combee::read_csv::<Data>(String::from("tests/fixtures/unsorted.csv")).unwrap();
    let df_grouped = df.groupby(|_| 1).agg(|index, g| Stats { index: *index, value: sum(g, |x| x.age)});

    assert_eq!(df_grouped.len(), 1);

    let row = df_grouped.find(|_| true).unwrap();

    assert_eq!(row.index, 1);
    assert_eq!(row.value, 157);
}

#[test]
fn test_unsorted_groupby_mean() {
    let df = combee::read_csv::<Data>(String::from("tests/fixtures/unsorted.csv")).unwrap();
    let df_grouped = df.groupby(|_| 1).agg(|index, g| Stats { index: *index, value: mean(g, |x| x.age as f64)});

    assert_eq!(df_grouped.len(), 1);

    let row = df_grouped.find(|_| true).unwrap();

    assert_eq!(row.index, 1);
    assert_eq!(row.value, 157.0/6.0);
}

#[test]
fn test_groupby_groupby_mean() {
    let df = combee::read_csv::<Stats<f64>>(String::from("tests/fixtures/groupby.csv")).unwrap();

    let df_grouped = df
        .groupby(|x| x.index)
        .agg(|index, group| Stats { index: *index, value: mean(group, |x| x.value) });

    assert_eq!(df_grouped.len(), 3);

    let row_1 = df_grouped.find(|x| x.index == 1).unwrap();
    assert_eq!(row_1.index, 1);
    assert!(row_1.value - 0.21 < 0.0001);
    assert!(0.21 - row_1.value < 0.001);

    let row_2 = df_grouped.find(|x| x.index == 2).unwrap();
    assert_eq!(row_2.index, 2);
    assert!(row_2.value - 0.45 < 0.0001);
    assert!(0.45 - row_2.value < 0.0001);

    let row_3 = df_grouped.find(|x| x.index == 3).unwrap();
    assert_eq!(row_3.index, 3);
    assert!(row_3.value - 0.6 < 0.0001);
    assert!(0.6 - row_3.value < 0.0001);
}

#[test]
fn test_groupby_groupby_count() {
    let df = combee::read_csv::<Stats<f64>>(String::from("tests/fixtures/groupby.csv")).unwrap();

    let df_grouped = df
        .groupby(|x| x.index)
        .agg(|index, group| Stats { index: *index, value: count(group) });

    assert_eq!(df_grouped.len(), 3);

    let row_1 = df_grouped.find(|x| x.index == 1).unwrap();
    assert_eq!(row_1.index, 1);
    assert_eq!(row_1.value, 2);

    let row_2 = df_grouped.find(|x| x.index == 2).unwrap();
    assert_eq!(row_2.index, 2);
    assert_eq!(row_2.value, 2);

    let row_3 = df_grouped.find(|x| x.index == 3).unwrap();
    assert_eq!(row_3.index, 3);
    assert_eq!(row_3.value, 2);
}

#[test]
fn test_groupby_groupby_sum() {
    let df = combee::read_csv::<Stats<f64>>(String::from("tests/fixtures/groupby.csv")).unwrap();

    let df_grouped = df
        .groupby(|x| x.index)
        .agg(|index, group| Stats { index: *index, value: sum(group, |x| x.value) });

    assert_eq!(df_grouped.len(), 3);

    let row_1 = df_grouped.find(|x| x.index == 1).unwrap();
    assert_eq!(row_1.index, 1);
    assert!(row_1.value - 0.42 < 0.0001);
    assert!(0.42 - row_1.value < 0.001);

    let row_2 = df_grouped.find(|x| x.index == 2).unwrap();
    assert_eq!(row_2.index, 2);
    assert!(row_2.value - 0.9 < 0.0001);
    assert!(0.9 - row_2.value < 0.0001);

    let row_3 = df_grouped.find(|x| x.index == 3).unwrap();
    assert_eq!(row_3.index, 3);
    assert!(row_3.value - 1.2 < 0.0001);
    assert!(1.2 - row_3.value < 0.0001);
}
