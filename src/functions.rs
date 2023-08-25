use std::ops::{Add, Div};
use serde::{Serialize, de::DeserializeOwned};

use crate::dataframe::Group;

/// Aggregator function that sums the result from the function property.
/// Example:
/// ```
/// use combee::{dataframe::DataFrame, functions::{sum, all}};
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Clone, Serialize, Deserialize)]
/// struct D {
///     name: String,
///     age: u32
/// }
///
/// let df = DataFrame::new(vec![D { name: "jujuba".to_string(), age: 26}, D { name: "xpto".to_string(), age: 40}, D { name: "rainbow".to_string(), age: 30}]);
/// println!("{:?}", df.groupby(all).agg(|_, g| sum(g, |x| x.age)).head(1));
/// ```
pub fn sum<D: Clone + Serialize + DeserializeOwned, N: Add<Output=N> + Default, F>(group: &Group<D>, property: F) -> N where F: Fn(&D) -> N {
    let mut value: N = N::default();
    for &x in group.data.clone().into_iter() {
        value = value + property(x);
    }
    value
}

/// Aggregator function that calculate the mean of the values of the function property.
/// Example:
/// ```
/// use combee::{dataframe::DataFrame, functions::{mean, all}};
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Clone, Serialize, Deserialize)]
/// struct D {
///     name: String,
///     age: u32
/// }
///
/// let df = DataFrame::new(vec![D { name: "jujuba".to_string(), age: 26}, D { name: "xpto".to_string(), age: 40}, D { name: "rainbow".to_string(), age: 30}]);
/// println!("{:?}", df.groupby(all).agg(|_, g| mean(g, |x| x.age as f64)).head(1));
/// ```
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
/// Example:
/// ```
/// use combee::{dataframe::DataFrame, functions::{count, all}};
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Clone, Serialize, Deserialize)]
/// struct D {
///     name: String,
///     age: u32
/// }
///
/// let df = DataFrame::new(vec![D { name: "jujuba".to_string(), age: 26}, D { name: "xpto".to_string(), age: 40}, D { name: "rainbow".to_string(), age: 30}]);
/// println!("{:?}", df.groupby(all).agg(|_, g| count(g)).head(1));
/// ```
pub fn count<D: Clone + Serialize + DeserializeOwned>(group: &Group<D>) -> usize {
    let mut count: usize = 0;
    for _ in group.data.clone() {
        count += 1;
    }
    count
}

/// Groupby function that group all rows together.
/// Example:
/// ```
/// use combee::{dataframe::DataFrame, functions::{sum, mean, count, all}};
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Clone, Serialize, Deserialize)]
/// struct D {
///     name: String,
///     age: u32
/// }
///
/// let df = DataFrame::new(vec![D { name: "jujuba".to_string(), age: 26}, D { name: "xpto".to_string(), age: 40}, D { name: "rainbow".to_string(), age: 30}]);
/// println!("{:?}", df.groupby(all).agg(|_, g|
///     (count(g), mean(g, |x| x.age as f64), sum(g, |x| x.age))
/// ).head(1));
/// ```
pub fn all<D: Clone + Serialize + DeserializeOwned>(_: &D) -> usize {
    1
}

/// Calculate the maximum value of a closure applied to a group of rows.
/// Example:
/// ```
/// use combee::{dataframe::DataFrame, functions::{max, all}};
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Clone, Serialize, Deserialize)]
/// struct D {
///     name: String,
///     age: u32
/// }
///
/// let df = DataFrame::new(vec![D { name: "jujuba".to_string(), age: 26}, D { name: "xpto".to_string(), age: 40}, D { name: "rainbow".to_string(), age: 30}]);
/// println!("{:?}", df.groupby(all).agg(|_, g|
///     max(g, |x| x.age)
/// ).head(1));
/// ```
pub fn max<D: Clone + Serialize + DeserializeOwned, N: Ord, F>(group: &Group<D>, f: F) -> N where F: Fn(&D) -> N {
    if group.data.len() == 0 {
        panic!("Trying to calculate maximum value of empty DataFrame!")
    }

    let mut value = f(group.data.clone().find(|_| true).unwrap());

    for row in group.data.clone().into_iter() {
        let nvalue = f(row);

        if nvalue > value {
            value = nvalue;
        }
    };
    value
}

/// Calculate theminimum value of a closure applied to a group of rows.
/// Example:
/// ```
/// use combee::{dataframe::DataFrame, functions::{min, all}};
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Clone, Serialize, Deserialize)]
/// struct D {
///     name: String,
///     age: u32
/// }
///
/// let df = DataFrame::new(vec![D { name: "jujuba".to_string(), age: 26}, D { name: "xpto".to_string(), age: 40}, D { name: "rainbow".to_string(), age: 30}]);
/// println!("{:?}", df.groupby(all).agg(|_, g|
///     min(g, |x| x.age)
/// ).head(1));
/// ```
pub fn min<D: Clone + Serialize + DeserializeOwned, N: Ord, F>(group: &Group<D>, f: F) -> N where F: Fn(&D) -> N {
    if group.data.len() == 0 {
        panic!("Trying to calculate maximum value of empty DataFrame!")
    }

    let mut value = f(group.data.clone().find(|_| true).unwrap());

    for row in group.data.clone().into_iter() {
        let nvalue = f(row);

        if nvalue < value {
            value = nvalue;
        }
    };
    value
}