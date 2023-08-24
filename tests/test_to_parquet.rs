use serde::{Serialize, Deserialize};

use combee::{dataframe::DataFrame, read_parquet};


#[derive(Clone, Serialize, Deserialize)]
struct A {
    b: bool,
    a: u32,
    x: f64,
    y: String
}

#[derive(Clone, Serialize, Deserialize)]
struct C {
    b: bool,
    x: f32,
    y: Vec<f32>
}

#[derive(Clone, Serialize, Deserialize)]
struct D {
    index: String,
    c: C
}

#[derive(Clone, Serialize, Deserialize)]
struct X {
    index: String,
    childrens: Vec<C>
}

#[test]
fn test_to_parquet_super_basic() {
    let path = String::from("tmp/super_basic.parquet");

    let df = DataFrame::new(vec![A { b: false, x: 1.1, y: "xpto".to_string(), a: 321 }, A { b: true, x: -0.1, y: "jujuba".to_string(), a: 123 }]);
    df.to_parquet(path.clone()).unwrap();

    let new_df = read_parquet::<A>(path).unwrap();

    assert_eq!(new_df.len(), 2);

    let row_xpto = new_df.find(|d| d.y == "xpto").unwrap();
    assert_eq!(row_xpto.x, 1.1);
    assert_eq!(row_xpto.a, 321);
    assert!(!row_xpto.b);

    let row_jujuba = new_df.find(|d| d.y == "jujuba").unwrap();
    assert_eq!(row_jujuba.x, -0.1);
    assert_eq!(row_jujuba.a, 123);
    assert!(row_jujuba.b);
}

#[test]
fn test_to_parquet_with_vector() {
    let path = String::from("tmp/with_vector.parquet");

    let df = DataFrame::new(vec![
        C {b: false, x: 32.0, y: vec![] },
        C {b: false, x: 12.0, y: vec![42.0] },
        C {b: true, x: 0.0, y: vec![]}
    ]);

    df.to_parquet(path.clone()).unwrap();

    let new_df = read_parquet::<C>(path.clone()).unwrap();

    assert_eq!(new_df.len(), 3);

    let row1 = new_df.find(|d| d.x == 32.0).unwrap();
    assert_eq!(row1.y.len(), 0);
    assert!(!row1.b);

    let row2 = new_df.find(|d| d.x == 12.0).unwrap();
    assert_eq!(row2.y, vec![42.0]);
    assert!(!row2.b);

    let row3 = new_df.find(|d| d.x == 0.0).unwrap();
    assert_eq!(row3.y.len(), 0);
    assert!(row3.b);
}

#[test]
fn test_to_parquet_basic() {
    let path = String::from("tmp/basic.parquet");

    let df = DataFrame::new(vec![
        D {index: String::from("xpto"), c: C { b: false, x: 1.2, y: vec![-0.3] }},
        D {index: String::from("jujuba"), c: C { b: true, x: -0.1, y: vec![] }}
    ]);

    df.to_parquet(path.clone()).unwrap();

    let new_df = read_parquet::<D>(path.clone()).unwrap();

    assert_eq!(new_df.len(), 2);

    let row_xpto = new_df.find(|d| d.index == "xpto").unwrap();
    assert_eq!(row_xpto.c.x, 1.2);
    assert_eq!(row_xpto.c.y, vec![-0.3]);
    assert!(!row_xpto.c.b);

    let row_jujuba = new_df.find(|d| d.index == "jujuba").unwrap();
    assert_eq!(row_jujuba.c.x, -0.1);
    assert_eq!(row_jujuba.c.y.len(), 0);
    assert!(row_jujuba.c.b);
}

#[test]
fn test_to_parquet_basic_slice() {
    let path = String::from("tmp/slice.parquet");

    let df = DataFrame::new(vec![
        D {index: String::from("xpto"), c: C { x: 1.2, y: vec![-0.3], b: false }},
        D {index: String::from("jujuba"), c: C { x: -0.1, y: vec![], b: true }}
    ]);

    let df_head = df.head(1);
    df_head.to_parquet(path.clone()).unwrap();

    let new_df = read_parquet::<D>(path.clone()).unwrap();

    assert_eq!(new_df.len(), 1);

    let row_xpto = new_df.find(|d| d.index == "xpto").unwrap();
    assert_eq!(row_xpto.c.x, 1.2);
    assert_eq!(row_xpto.c.y, vec![-0.3]);
}

#[test]
fn test_to_parquet_complex() {
    let path = String::from("tmp/complex.parquet");

    let df = DataFrame::new(vec![
        X { index: "xpto".to_string(), childrens: vec![C {b: false, x: 1.2, y: vec![0.1]}]},
        X { index: "abcd".to_string(), childrens: vec![]},
        X { index: "xyz".to_string(), childrens: vec![]},
        X { index: "www".to_string(), childrens: vec![C {b: true, x: 0.6, y: vec![0.2]}, C {b: false, x: 0.1, y: vec![0.1]}]}
    ]);

    df.to_parquet(path.clone()).unwrap();

    let new_df = read_parquet::<X>(path.clone()).unwrap();

    assert_eq!(new_df.len(), 4);
}