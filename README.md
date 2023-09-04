# Combee

<img src="assets/combee.jpg" height="300" width="600"/>

Combee is a strong typed data analysis and neural netowkr library written in pure Rust inspired by pandas and keras (python).

## Installation

Run in a Rust project directory:

```bash
cargo add combee
```

## Examples

1) Check the notebook using evcxr_jupyter on [notebooks/analysis.ipynb](notebooks/analysis.ipynb) to an example of analysis of dataset.

2) Below an example of loading a CSV file, filtering the dataset, and applying a function to each row:

(dataset.csv)
```csv
name,age
Daniel,26
Sergio,30
Leticia,22
```

(main.rs)
```rust
use serde::{Serialize, Deserialize};
use combee::{read_csv, dataframe::DataFrame};

#[derive(Clone, Deserialize, Serialize)]
struct Data {
    name: String,
    age: u32
}

let df = read_csv::<Data>(String::from("../tests/fixtures/basic.csv")).unwrap();
let df_filtered: DataFrame<Data> = df.filter(|row| row.age < 27);
let df_message: DataFrame<String> = df_filtered.apply(|row| format!("Hello {} with {} years!", row.name, row.age));
let messages = df_message.take(2);

println!("{}", messages[0]);
println!("{}", messages[1]);
```

2) An example of groupby with aggregation

(main.rs)
```rust
use serde::{Serialize, Deserialize};
use combee::{read_csv, functions::{avg, sum, count, all}};

#[derive(Clone, Deserialize, Serialize)]
struct Data {
    name: String,
    age: u32
}

fn main() {
    let df = read_csv::<Data>(String::from("dataset.csv")).unwrap();

    let stats = df.groupby(all).agg(|_, g|
        (count(g), avg(g, |x| x.age), sum(g, |x| x.age))
    ).head(1);

    println("{:?}", stats);
}
```

## Acknowledgments

Daniel Santana: Made with Love 💗.\
ali5h: Code to deserialize parquet row [link](https://github.com/ali5h/serde-parquet).
