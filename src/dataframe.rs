use serde::{Serialize, de::DeserializeOwned};

/// A DataFrame is the main data structure of combee
pub struct DataFrame<D: Clone + DeserializeOwned + Serialize> {
    pub(crate) data: Vec<D>
}

impl<'a, D: Clone + DeserializeOwned + Serialize> DataFrame<D> {
    /// Instatiante a new DataFrame
    pub(crate) fn new(data: Vec<D>) -> Self {
        DataFrame { data: data }
    }

    /// Returns a vector with the first 'num' elements of this dataframe.
    pub fn take(&self, num: usize) -> Vec<D> {
        let min_num = std::cmp::min(num, self.data.len());
        self.data[0..min_num].to_vec()
    }

    /// Return the number of rows of the DataFrame.
    pub fn len(&self) -> usize {
        self.data.len()
    }
}
