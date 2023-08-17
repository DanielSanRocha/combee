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

```rust
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

fn main() {
    let df = combee::read_csv::<Data>(String::from("tests/fixtures/basic.csv")).unwrap();
    let df_filtered = df.filter(|row| row.age < 27);
    let df_message = df.apply(|row| Message { message: format!("Hello {} with {} years!", row.name, row.age)});
    let messages = df_message.take(2);
}
```

## Acknowledgments

Made with Love by Daniel Santana
