use crate::errors;

/// dense layer module.
pub mod dense_layer;

/// Basic Layer trait.
pub trait Layer {
    fn shape(&self) -> Vec<usize>;
}

/// The main structure of this module.
pub struct NeuralNetwork {
    layers: Vec<Box<dyn Layer>>,
}

impl NeuralNetwork {
    /// Create a new Neural Network from a vector of layers.
    pub fn new(layers: Vec<Box<dyn Layer>>) -> Result<Self, errors::Error> {
        if layers.len() == 0 {
            Err(errors::Error {
                message: String::from("Neural Network must have at least one layer!"),
            })
        } else {
            Ok(NeuralNetwork { layers: layers })
        }
    }

    /// Returns the input_shape of this neural network.
    pub fn input_shape(&self) -> Vec<usize> {
        self.layers.first().unwrap().shape()
    }

    /// Returns the output shape of this neural network.
    pub fn output_shape(&self) -> Vec<usize> {
        self.layers.last().unwrap().shape()
    }
}
