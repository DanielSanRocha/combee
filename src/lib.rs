use serde::{Serialize, de::DeserializeOwned};
use log;
use csv;

pub mod dataframe;
pub mod errors;

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
