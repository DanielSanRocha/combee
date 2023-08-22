use std::ops::{Add, Div};
use serde::{Serialize, de::DeserializeOwned};

use crate::dataframe::Group;

/// Aggregator function that sums the result from the function property.
pub fn sum<D: Clone + Serialize + DeserializeOwned, N: Add<Output=N> + Default, F>(group: &Group<D>, property: F) -> N where F: Fn(&D) -> N {
    let mut value: N = N::default();
    for &x in group.data.clone().into_iter() {
        value = value + property(x);
    }
    value
}

/// Aggregator function that calculate the mean of the values of the function property.
pub fn mean<D: Clone + Serialize + DeserializeOwned, N: Div<f64, Output=N> + Add<Output=N> + Default, F>(group: &Group<D>, property: F) -> N where F: Fn(&D) -> N {
    let mut value: N = N::default();
    let mut count: f64 = 0.0;
    for x in group.data.clone() {
        value = value + property(*x);
        count += 1.0;
    }

    value/count
}

/// Aggregator function that returns the number of elements in a given group.
pub fn count<D: Clone + Serialize + DeserializeOwned>(group: &Group<D>) -> usize {
    let mut count: usize = 0;
    for _ in group.data.clone() {
        count += 1;
    }
    count
}

/// Groupby function that group all rows together.
pub fn all<D: Clone + Serialize + DeserializeOwned>(_: &D) -> usize {
    1
}
