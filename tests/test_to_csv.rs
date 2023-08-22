use serde::{Serialize, Deserialize};

use combee::{dataframe::DataFrame, read_csv};

#[derive(Clone, Serialize, Deserialize)]
struct D {
    index: String,
    x: f32,
    y: f32
}

#[test]
fn test_to_csv_basic() {
    let path = String::from("tmp/basic.csv");

    let df = DataFrame::new(vec![
        D {index: String::from("xpto"), x: 1.2, y: -0.3},
        D {index: String::from("jujuba"), x: -0.1, y: 42.0}
    ]);

    df.to_csv(path.clone()).unwrap();

    let new_df = read_csv::<D>(path.clone()).unwrap();

    assert_eq!(new_df.len(), 2);

    let row_xpto = new_df.find(|d| d.index == "xpto").unwrap();
    assert_eq!(row_xpto.x, 1.2);
    assert_eq!(row_xpto.y, -0.3);

    let row_jujuba = new_df.find(|d| d.index == "jujuba").unwrap();
    assert_eq!(row_jujuba.x, -0.1);
    assert_eq!(row_jujuba.y, 42.0);
}

#[test]
fn test_to_csv_basic_slice() {
    let path = String::from("tmp/slice.csv");

    let df = DataFrame::new(vec![
        D {index: String::from("xpto"), x: 1.2, y: -0.3},
        D {index: String::from("jujuba"), x: -0.1, y: 42.0}
    ]);

    let df_head = df.head(1);

    df_head.to_csv(path.clone()).unwrap();

    let new_df = read_csv::<D>(path.clone()).unwrap();

    assert_eq!(new_df.len(), 1);

    let row_xpto = new_df.find(|d| d.index == "xpto").unwrap();
    assert_eq!(row_xpto.x, 1.2);
    assert_eq!(row_xpto.y, -0.3);
}

