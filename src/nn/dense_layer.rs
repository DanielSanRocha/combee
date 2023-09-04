use crate::nn::Layer;

/// Dense Layer, instatiante using DenseLayer::new
pub struct DenseLayer {
    shape: Vec<usize>,
    weights: Vec<f64>,
}

impl DenseLayer {
    /// Create a new DenseLayer with given shape.
    pub fn new(shape: Vec<usize>) -> Self {
        let total = shape.iter().fold(1, |x, y| x * y);

        let mut weights = vec![];
        for _ in 0..total {
            weights.push(0.0 as f64);
        }

        DenseLayer {
            shape: shape,
            weights: weights,
        }
    }
}

impl Layer for DenseLayer {
    fn shape(&self) -> Vec<usize> {
        self.shape.clone()
    }
}
