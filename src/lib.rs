//! This crate contains combee: a strong typed data analysis library implemented in pure Rust.
//!
//! # Getting Started
//! Start with some examples:
//!
//! 1. [function@read_csv] for reading csv files.
//!
//! 2. [function@read_parquet] for reading parquet files.

use std::{path::Path, fs::File};
use parquet::{file::serialized_reader::SerializedFileReader};
use serde::{Serialize, de::DeserializeOwned};
use log;
use csv;

use crate::{dataframe::DataFrame, parquet_deserializer::from_row};

/// DataFrame module, contains all the basic functions (groupby, agg, find...).
pub mod dataframe;

/// Contains useful functions (aggregation functions, groupby functions...).
pub mod functions;

/// Error module.
pub mod errors;

mod parquet_deserializer;

/// Read a CSV file, the data parameter D must be compatible with the columns of the csv.
/// The first row of the CSV must be the header.
/// # Examples
/// 1) Load a CSV file.
/// Suppose we have a CSV file like this:
/// ```csv
/// name,age
/// Daniel,26
/// Sergio,30
/// Leticia,22
/// ```
/// We can load this file using combee:
/// ```
/// use combee;
///
///  struct D {
///     name: String,
///     age: usize
/// }
///
/// let df = combee::read_csv::<D>("dataset.csv");
/// ```
pub fn read_csv<D: Clone + DeserializeOwned + Serialize>(path: String) -> Result<dataframe::DataFrame<D>, errors::Error> {
    log::debug!("Reading CSV at path '{}'", path);
    match csv::Reader::from_path(path) {
        Ok(mut reader) => {
            let mut data = Vec::new();

            for result in reader.deserialize::<D>() {

                let row = match result {
                    Ok(row) => row,
                    Err(e) => return Err(errors::Error {message: e.to_string()})
                };

                log::trace!("Read one row of CSV, loading into the array...");
                data.push(row);
            }

            Ok(dataframe::DataFrame::new(data))
        },
        Err(e) => Err(errors::Error {message: e.to_string()})
    }
}

/// Read an Apache Parquet file, the data parameter D must be compatible with the columns of the parquet.
pub fn read_parquet<D: Clone + DeserializeOwned + Serialize>(path: String) -> Result<dataframe::DataFrame<D>, errors::Error> {
    log::debug!("Reading Parquet at path '{}'", path);
    let p: &Path = Path::new(&path);

    if let Ok(file) = File::open(&p) {
        let reader = match SerializedFileReader::new(file) {
            Ok(r) => r,
            Err(e) => return Err(errors::Error { message: e.to_string() })
        };

        let mut data = Vec::new();
        for r in reader.into_iter() {
            match r {
                Ok(row) => {
                    match from_row(&row) {
                        Ok(d) => data.push(d),
                        Err(e) => return Err(e)
                    }
                },
                Err(e) => return Err(errors::Error { message: e.to_string() })
            }
        }

        return Ok(DataFrame::new(data));
    } else {
        return Err(errors::Error { message: format!("Could not open file {}", path) })
    }
}

/// Returns a list of string with the columns of a given CSV.
pub fn read_csv_schema(path: String) -> Result<Vec<String>, errors::Error> {
    todo!();
}
