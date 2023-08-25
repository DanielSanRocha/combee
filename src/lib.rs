//! This crate contains combee: a strong typed data analysis library implemented in pure Rust.
//! Check the github page for notebooks (using evcxr_jupyter) using combee: <https://github.com/DanielSanRocha/combee>.
//!
//! # Getting Started
//! Start with some examples:
//!
//! 1. [function@read_csv] for reading csv files.
//!
//! 2. [function@read_parquet] for reading parquet files.
//!
//! 3. [dataframe::DataFrame::groupby] for grouping rows together.
//!
//! 4. [function@functions::mean] for calculating average value of group of rows.
//!
use std::{path::Path, fs::File, io::{BufRead, BufReader}};
use parquet::{file::serialized_reader::SerializedFileReader, arrow::arrow_reader::ParquetRecordBatchReaderBuilder};
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
/// use combee::{read_csv, read_csv_schema};
/// use serde::{Serialize, Deserialize};
///
/// let columns = read_csv_schema("dataset.csv".to_string()).unwrap();
/// println!("{}", columns.join(",")); // Print the columns names to facilitate the creation of the struct.
///
///  #[derive(Clone, Serialize, Deserialize)]
///  struct D {
///     name: String,
///     age: usize
/// }
///
/// let df = read_csv::<D>("dataset.csv".to_string()).unwrap();
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
/// ```
/// use combee::{read_parquet_schema, read_parquet};
/// use serde::{Serialize, Deserialize};
///
/// let schema = read_parquet_schema("complex.parquet".to_string()).unwrap();
/// println!("{}", schema); // Print the schema so you can construct the struct to hold the data
///
/// #[derive(Clone, Serialize, Deserialize)]
/// struct C {
///     b: bool,
///     x: f32,
///     y: Vec<f32>
/// }
///
/// #[derive(Clone, Serialize, Deserialize)]
/// struct D {
///     index: String,
///     childrens: Vec<C>
/// }
///
/// let df = read_parquet::<D>("complex.parquet".to_string()).unwrap();
/// ```
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
/// Example:
/// ```
/// use combee::read_csv_schema;
///
/// let columns = read_csv_schema("dataset.csv".to_string()).unwrap();
/// println!("{}", columns.join(","));
/// ```
pub fn read_csv_schema(path: String) -> Result<Vec<String>, errors::Error> {
    let file = match File::open(&path) {
        Ok(f) => f,
        Err(_) => return Err(errors::Error { message: format!("Could not open CSV file at path {}!", path) })
    };

    let mut buffer = BufReader::new(file);
    let mut first_line = String::new();

    match buffer.read_line(&mut first_line) {
        Ok(_) => (),
        Err(_) => return Err(errors::Error { message: format!("Could not read first line of CSV file at path {}", path) })
    };

    Ok(first_line.trim().split(",").map(|x| String::from(x)).collect())
}

/// Returns the schema of a parquet as a string.
/// Example:
/// ```
/// use combee::read_parquet_schema;
///
/// println!("{}", read_parquet_schema("complex.parquet".to_string()).unwrap());
/// ```
pub fn read_parquet_schema(path: String) -> Result<String, errors::Error> {
    let file = match File::open(&path) {
        Ok(f) => f,
        Err(e) => return Err(errors::Error { message: e.to_string() })
    };

    let builder = match ParquetRecordBatchReaderBuilder::try_new(file) {
        Ok(b) => b,
        Err(e) => return Err(errors::Error { message: e.to_string() })
    };

    Ok(format!("{}", builder.schema()))
}