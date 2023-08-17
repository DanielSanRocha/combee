use log;
use serde::{Serialize, de::DeserializeOwned};

/// A DataFrame is the main data structure of combee
pub struct DataFrame<D: Clone + DeserializeOwned + Serialize> {
    pub(crate) data: Vec<D>
}

impl<D: Clone + DeserializeOwned + Serialize> DataFrame<D> {
    /// Instatiante a new DataFrame
    pub(crate) fn new(data: Vec<D>) -> Self {
        log::trace!("Creating new DataFrame...");
        DataFrame { data: data }
    }

    /// Returns a vector with the first 'num' rows of this DataFrame.
    pub fn take(&self, num: usize) -> Vec<D> {
        let min_num = std::cmp::min(num, self.data.len());
        self.data[0..min_num].to_vec().clone()
    }

    /// Returns a new DataFrame with the first 'num' rows of this DataFrame.
    pub fn head(&self, num: usize) -> DataFrame<D> {
        let min_num = std::cmp::min(num, self.len());

        let head_data = self.data[0..min_num].to_vec();

        DataFrame { data: head_data }
    }

    /// Return the number of rows of the DataFrame.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Apply a function for each row of a DataFrame and returns a new DataFrame.
    pub fn apply<S: Clone + DeserializeOwned + Serialize, F>(&self, func: F) -> DataFrame<S>
        where F: Fn(&D) -> S {
        let mut new_data = Vec::<S>::new();

        for row in self.data.iter() {
            new_data.push(func(row))
        }

        DataFrame::new(new_data)
    }

    /// Filter the DataFrame with the condition given by the closure parameter.
    pub fn filter<F>(&self, func: F) -> Self
        where F: Fn(&D) -> bool {
        let mut new_data = Vec::<D>::new();

        for row in self.data.iter() {
            if func(row) {
                new_data.push(row.clone())
            }
        }

        DataFrame::new(new_data)
    }
}
