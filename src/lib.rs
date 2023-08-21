use std::{path::Path, fs::File};
use parquet::{file::serialized_reader::SerializedFileReader};
use serde::{Serialize, de::DeserializeOwned};
use log;
use csv;

use crate::{dataframe::DataFrame, parquet_deserializer::from_row};

pub mod dataframe;
pub mod functions;
pub mod errors;
mod parquet_deserializer;

/// Read a CSV file, the data parameter D must be compatible with the columns of the csv.
/// The first row of the CSV must be the header.
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