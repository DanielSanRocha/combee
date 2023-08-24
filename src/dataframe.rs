use std::{cmp::{Ordering, Eq}, fmt::{self, Debug}, slice, collections::HashMap, hash::Hash};
extern crate alloc;
use log;
use serde::{Serialize, de::DeserializeOwned};
use csv;
use serde_arrow::{schema::TracingOptions, arrow2::{serialize_into_arrays, serialize_into_fields}};
use arrow2::{chunk::Chunk, datatypes::Schema, array::Array, self, io::parquet::write::{transverse, CompressionOptions, Encoding, FileWriter, RowGroupIterator, Version, WriteOptions}};

use std::fs::File;


use crate::errors;

/// A DataFrame is the main data structure of combee.
pub struct DataFrame<D: Clone + DeserializeOwned + Serialize> {
    data: Vec<D>
}

/// A Slice of a DataFrame.
pub struct SliceDataFrame<'a, D: Clone + DeserializeOwned + Serialize> {
    dataframe: &'a DataFrame<D>,
    start: usize,
    end: usize
}

/// A group of rows of a DataFrame.
pub struct Group<'a, D: Clone + DeserializeOwned + Serialize> {
    pub(crate) data: slice::Iter::<'a, &'a D>
}

/// A Grouped DataFrame, you can use a aggregator function to get a DataFrame.
pub struct GroupedDataFrame<'a, D: Clone + DeserializeOwned + Serialize, I: Eq + Hash + Clone, F> where F: Fn(&D) -> I {
    dataframe: &'a DataFrame<D>,
    index: F
}

impl<D: Clone + DeserializeOwned + Serialize> DataFrame<D> {
    /// Instatiante a new DataFrame.
    pub fn new(data: Vec<D>) -> Self {
        log::trace!("Creating new DataFrame...");
        DataFrame { data: data }
    }

    /// Returns a vector with the first 'num' rows of this DataFrame.
    pub fn take(&self, num: usize) -> Vec<D> {
        let min_num = std::cmp::min(num, self.data.len());
        self.data[0..min_num].to_vec().clone()
    }

    /// Returns a new DataFrame with the first 'num' rows of this DataFrame.
    pub fn head(&self, num: usize) -> SliceDataFrame<D> {
        let min_num = std::cmp::min(num, self.len());
        SliceDataFrame::new(self, 0 , min_num)
    }

    /// Return the number of rows of the DataFrame.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Apply a function for each row of a DataFrame and returns a new DataFrame.
    pub fn apply<S: Clone + DeserializeOwned + Serialize, F>(&self, func: F) -> DataFrame<S> where F: Fn(&D) -> S {
        let mut new_data = Vec::<S>::new();

        for row in self.data.iter() {
            new_data.push(func(row))
        }

        DataFrame::new(new_data)
    }

    /// Filter the DataFrame with the condition given by the closure parameter.
    pub fn filter<F>(&self, func: F) -> Self where F: Fn(&D) -> bool {
        let mut new_data = Vec::<D>::new();

        for row in self.data.iter() {
            if func(row) {
                new_data.push(row.clone())
            }
        }

        DataFrame::new(new_data)
    }

    /// Sort the DataFrame by a comparison function and returns a new DataFrame sorted.
    /// This sorting algorithm is not stable, i.e, does not preserve the order of equal elements.
    pub fn sort<F>(&self, comp: F) -> Self where F: Fn(&D,&D) -> bool {
        let mut new_data = Vec::<D>::new();

        log::trace!("Cloning DataFrame data...");
        for row in self.data.iter() {
            new_data.push(row.clone())
        }

        log::trace!("Sorting DataFrame data...");
        new_data.sort_unstable_by(|x1,x2| {
            if comp(x1,x2) {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        });

        DataFrame::new(new_data)
    }

    /// Group DataFrame by index function.
    pub fn groupby<F,I: Eq + Hash + Clone>(&self, index: F) -> GroupedDataFrame<'_, D, I, F> where F: Fn(&D) -> I {
        GroupedDataFrame::new(self, index)
    }

    /// Find a row in the dataframe given a condition.
    pub fn find<F>(&self, condition: F) -> Option<&D> where F: Fn(&D) -> bool {
        for row in self.data[0..self.len()].into_iter() {
            if condition(row) {
                return Some(row);
            }
        }
        return None;
    }

    /// Save a DataFrame as a CSV file.
    pub fn to_csv(&self, path: String) -> Result<(),errors::Error> {
        log::debug!("Saving DataFrame to CSV file at path: {}", path);

        let mut writer = match csv::Writer::from_path(path) {
            Ok(w) => w,
            Err(e) => return Err(errors::Error { message: e.to_string() })
        };

        for row in self.data[0..self.len()].into_iter() {
            match writer.serialize(row) {
                Ok(_) => (),
                Err(e) => return Err(errors::Error {message: e.to_string() })
            };
        }

        match writer.flush() {
            Ok(_) => Ok(()),
            Err(e) => Err(errors::Error { message: e.to_string() })
        }
    }

    /// Save a DataFrame as a Parquet file.
    pub fn to_parquet(&self, path: String) -> Result<(), errors::Error> {
        log::debug!("Saving DataFrame to Parquet file at path: {}", path);

        let schema = match serialize_into_fields(&self.data, TracingOptions::default()) {
            Ok(s) => s,
            Err(e) => return Err(errors::Error { message: e.to_string() })
        };

        let arrays = match serialize_into_arrays(&schema, &self.data) {
            Ok(a) => a,
            Err(e) => return Err(errors::Error { message: e.to_string() })
        };

        match write_chunk_parquet(&path, Schema::from(schema), Chunk::new(arrays)) {
            Ok(_) => Ok(()),
            Err(_) => Err(errors::Error { message: "Error saving parquet!".to_string() })
        }
    }
}


fn write_chunk_parquet(path: &str, schema: Schema, chunk: Chunk<Box<dyn Array>>) -> arrow2::error::Result<()> {
    let options = WriteOptions {
        write_statistics: false,
        compression: CompressionOptions::Uncompressed,
        version: Version::V2,
        data_pagesize_limit: None
    };

    let iter = vec![Ok(chunk)];

    let encodings = schema
        .fields
        .iter()
        .map(|f| transverse(&f.data_type, |_| Encoding::Plain))
        .collect();

    let row_groups = RowGroupIterator::try_new(iter.into_iter(), &schema, options, encodings)?;

    // Create a new empty file
    let file = File::create(path)?;

    let mut writer = FileWriter::try_new(file, schema, options)?;

    for group in row_groups {
        writer.write(group?)?;
    }
    let _size = writer.end(None)?;
    Ok(())
}


impl<'a, D: Clone + DeserializeOwned + Serialize> SliceDataFrame<'a, D> {
    fn new(dataframe: &'a DataFrame<D>, start: usize, end: usize) -> Self {
        log::trace!("Creating a new SliceDataFrame with start: {} and end: {}", start, end);
        SliceDataFrame { dataframe: dataframe, start: start, end: end }
    }

    /// Clone the slice into a new DataFrame.
    pub fn clone(&self) -> DataFrame<D> {
        log::trace!("Cloning slice of a DataFrame into a new DataFrame...");
        let data = self.dataframe.data[self.start..self.end].iter().map(|x| x.clone()).collect();
        DataFrame::new(data)
    }

    /// Return the size of a SliceDataFrame.
    pub fn len(&self) -> usize {
        self.end - self.start
    }

    /// Take num rows from SliceDataFrame.
    pub fn take(&self, num: usize) -> Vec<D> {
        let min_num = std::cmp::min(num, self.len());
        let data = self.dataframe.data[self.start..(self.start + min_num)].iter().map(|x| x.clone()).collect();
        data
    }

    /// Find a row in the slice of a dataframe given a condition.
    pub fn find<F>(&self, condition: F) -> Option<&D> where F: Fn(&D) -> bool {
        for row in self.dataframe.data[self.start..self.end].into_iter() {
            if condition(row) {
                return Some(row);
            }
        }
        return None;
    }

    /// Save a SliceDataFrame to a CSV file.
    pub fn to_csv(&self, path: String) -> Result<(),errors::Error> {
        let mut writer = match csv::Writer::from_path(path) {
            Ok(w) => w,
            Err(e) => return Err(errors::Error { message: e.to_string() })
        };

        for row in self.dataframe.data[self.start..self.end].into_iter() {
            match writer.serialize(row) {
                Ok(_) => (),
                Err(e) => return Err(errors::Error {message: e.to_string() })
            };
        }

        match writer.flush() {
            Ok(_) => Ok(()),
            Err(e) => Err(errors::Error { message: e.to_string() })
        }
    }

    /// Save a SliceDataFrame as a Parquet file.
    pub fn to_parquet(&self, path: String) -> Result<(), errors::Error> {
        self.clone().to_parquet(path)
    }
}

impl<'a, D: Clone + DeserializeOwned + Serialize + Debug> fmt::Debug for SliceDataFrame<'a, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for row in self.dataframe.data[self.start..self.end].iter() {
            match write!(f, "{:?}\n", row) {
                Ok(_) => (),
                Err(e) => return Err(e)
            }
        }

        return Ok(())
    }
}

impl<'a,D: Clone + DeserializeOwned + Serialize, I: Eq + Clone + Hash, F> GroupedDataFrame<'a, D, I, F> where F: Fn(&D) -> I {
    fn new(df: &'a DataFrame<D>, index: F) -> Self {
        GroupedDataFrame { dataframe: df, index: index }
    }

    /// Aggregates a GroupedDataFrame in a new DataFrame using a aggregator function.
    pub fn agg<S: Clone + DeserializeOwned + Serialize, G>(&self, aggregator: G) -> DataFrame<S> where G: Fn(&I, &Group<D>) -> S {
        let mut hashmap: HashMap<I, Vec<&'a D>> = HashMap::new();

        for row in self.dataframe.data.iter() {
            let ind = (self.index)(&row);
            match hashmap.get_mut(&ind) {
                Some(group) => group.push(row),
                None => {
                    let mut v = Vec::new();
                    v.push(row);
                    hashmap.insert(ind, v);
                },
            }
        };

        let data = hashmap.iter().map(|(ind, group)| aggregator(ind, &Group { data: group.iter() })).collect();
        DataFrame::new(data)
    }
}
