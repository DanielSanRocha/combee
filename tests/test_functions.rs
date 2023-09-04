use combee::{
    dataframe::DataFrame,
    functions::{all, max, min},
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
