use combee::{
    dataframe::DataFrame,
    functions::{all, max, min, vec},
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
struct D {
    value: u32,
}

#[test]
fn test_functions_max_min() {
    let df = DataFrame::new(vec![D { value: 9 }, D { value: 2 }, D { value: 13 }]);

    let result = df
        .groupby(all)
        .agg(|_, g| ((max(g, |x| x.value)), min(g, |x| x.value)))
        .take(10);

    assert_eq!(result.len(), 1);
    assert_eq!(result.first().unwrap(), &(13, 2));
}

#[test]
fn test_functions_vec() {
    let df = DataFrame::new(vec![D { value: 9 }, D { value: 2 }, D { value: 13 }]);
    let result = df.groupby(all).agg(|_, g| vec(g, |x| x.value)).take(10);

    assert_eq!(result.len(), 1);
    let vec = result.first().unwrap();
    assert_eq!(vec.len(), 3);
    assert_eq!(vec[0], 9);
    assert_eq!(vec[1], 2);
    assert_eq!(vec[2], 13);
}
