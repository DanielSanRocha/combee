# Combee

<img src="assets/combee.jpg" height="300" width="600"/>

Combee is a flexible data analysis library written in pure Rust inspired by pandas (python).

## Installation

Run in a Rust project directory:

```bash
cargo add combee
```

## Examples

1) Below an example of loading a CSV file, filtering the dataset, and applying a function to each row:

(dataset.csv)
```csv
name,age
Daniel,26
Sergio,30
Leticia,22
```

(main.rs)
```rust
use std::fmt::Display;
use serde::{Serialize, Deserialize};

use combee;

#[derive(Clone, Deserialize, Serialize)]
struct Data {
    name: String,
    age: u32
}

#[derive(Clone, Deserialize, Serialize)]
struct Message {
    message: String
}

impl Display for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

fn main() {
    let df = combee::read_csv::<Data>(String::from("dataset.csv")).unwrap();
    let df_filtered = df.filter(|row| row.age < 27);
    let df_message = df_filtered.apply(|row| Message { message: format!("Hello {} with {} years!", row.name, row.age)});
    let messages = df_message.take(2);

    println!("{}", messages[0]);
    println!("{}", messages[1]);
}
```

2) An example of groupby with aggregation

(main.rs)
```rust
use serde::{Serialize, Deserialize};

use combee;
use combee::functions::{mean, sum, count};

#[derive(Clone, Deserialize, Serialize)]
struct Data {
    name: String,
    age: u32
}

fn main() {
    let df = combee::read_csv::<Data>(String::from("dataset.csv")).unwrap();

    let stats = df.groupby(|_| 1).agg(|_, g|
        (count(g), mean(g, |x| x.age), sum(g, |x| x.age))
    ).head(1);

    println("{:?}", stats);
}
```

## Acknowledgments

Daniel Santana: Made with Love.\
ali5h: Code to deserialize parquet row [link](https://github.com/ali5h/serde-parquet).
