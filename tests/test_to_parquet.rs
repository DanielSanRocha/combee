use serde::{Serialize, Deserialize};

use combee::{dataframe::DataFrame, read_csv, read_parquet};

#[derive(Clone, Serialize, Deserialize)]
struct C {
    x: f32,
    y: Vec<f32>
}

#[derive(Clone, Serialize, Deserialize)]
struct D {
    index: String,
    c: C
}

#[test]
fn test_to_parquet_basic() {
    let path = String::from("tmp/basic.parquet");

    let df = DataFrame::new(vec![
        D {index: String::from("xpto"), c: C { x: 1.2, y: vec![-0.3] }},
        D {index: String::from("jujuba"), c: C { x: -0.1, y: vec![] }}
    ]);

    df.to_parquet(path.clone()).unwrap();

    let new_df = read_parquet::<D>(path.clone()).unwrap();

    assert_eq!(new_df.len(), 2);

    let row_xpto = new_df.find(|d| d.index == "xpto").unwrap();
    assert_eq!(row_xpto.c.x, 1.2);
    assert_eq!(row_xpto.c.y, vec![-0.3]);

    let row_jujuba = new_df.find(|d| d.index == "jujuba").unwrap();
    assert_eq!(row_jujuba.c.x, -0.1);
    assert_eq!(row_jujuba.c.y, vec![]);
}

#[test]
fn test_to_parquet_basic_slice() {
    let path = String::from("tmp/slice.parquet");

    let df = DataFrame::new(vec![
        D {index: String::from("xpto"), c: C { x: 1.2, y: vec![-0.3] }},
        D {index: String::from("jujuba"), c: C { x: -0.1, y: vec![] }}
    ]);

    let df_head = df.head(1);
    df_head.to_parquet(path.clone()).unwrap();

    let new_df = read_parquet::<D>(path.clone()).unwrap();

    assert_eq!(new_df.len(), 1);

    let row_xpto = new_df.find(|d| d.index == "xpto").unwrap();
    assert_eq!(row_xpto.c.x, 1.2);
    assert_eq!(row_xpto.c.y, vec![-0.3]);
}
