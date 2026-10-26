//! Vector distance kernels shared by the SQL distance functions, the exact
//! top-k path and the `vss` extension.
//!
//! Every kernel is a flat loop with four independent accumulators and a small
//! tail; the shape is chosen so LLVM auto-vectorises it without `unsafe` SIMD.
//! Summation order is fixed (four lanes then the tail), so a result is
//! reproducible across builds and platforms.

/// One of the three HNSW metrics, named as DuckDB's vss names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Metric {
    /// Squared Euclidean distance (`l2sq`). Ordering is the same as `L2`;
    /// squaring only avoids a square root per candidate.
    L2Sq,
    /// `1 - cosine similarity` (`cosine`).
    Cosine,
    /// Negated inner product (`ip`), so smaller is closer like the others.
    InnerProduct,
}

impl Metric {
    pub fn parse(s: &str) -> Option<Metric> {
        match s.to_ascii_lowercase().as_str() {
            "l2sq" => Some(Metric::L2Sq),
            "cosine" => Some(Metric::Cosine),
            "ip" => Some(Metric::InnerProduct),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Metric::L2Sq => "l2sq",
            Metric::Cosine => "cosine",
            Metric::InnerProduct => "ip",
        }
    }
}

/// Squared Euclidean distance.
pub fn l2sq_f32(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    let n = a.len() & !3;
    let (mut s0, mut s1, mut s2, mut s3) = (0.0f32, 0.0, 0.0, 0.0);
    for i in (0..n).step_by(4) {
        let d0 = a[i] - b[i];
        let d1 = a[i + 1] - b[i + 1];
        let d2 = a[i + 2] - b[i + 2];
        let d3 = a[i + 3] - b[i + 3];
        s0 += d0 * d0;
        s1 += d1 * d1;
        s2 += d2 * d2;
        s3 += d3 * d3;
    }
    let mut sum = (s0 + s1) + (s2 + s3);
    for i in n..a.len() {
        let d = a[i] - b[i];
        sum += d * d;
    }
    sum
}

pub fn l2sq_f64(a: &[f64], b: &[f64]) -> f64 {
    debug_assert_eq!(a.len(), b.len());
    let n = a.len() & !3;
    let (mut s0, mut s1, mut s2, mut s3) = (0.0f64, 0.0, 0.0, 0.0);
    for i in (0..n).step_by(4) {
        let d0 = a[i] - b[i];
        let d1 = a[i + 1] - b[i + 1];
        let d2 = a[i + 2] - b[i + 2];
        let d3 = a[i + 3] - b[i + 3];
        s0 += d0 * d0;
        s1 += d1 * d1;
        s2 += d2 * d2;
        s3 += d3 * d3;
    }
    let mut sum = (s0 + s1) + (s2 + s3);
    for i in n..a.len() {
        let d = a[i] - b[i];
        sum += d * d;
    }
    sum
}

pub fn dot_f32(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    let n = a.len() & !3;
    let (mut s0, mut s1, mut s2, mut s3) = (0.0f32, 0.0, 0.0, 0.0);
    for i in (0..n).step_by(4) {
        s0 += a[i] * b[i];
        s1 += a[i + 1] * b[i + 1];
        s2 += a[i + 2] * b[i + 2];
        s3 += a[i + 3] * b[i + 3];
    }
    let mut sum = (s0 + s1) + (s2 + s3);
    for i in n..a.len() {
        sum += a[i] * b[i];
    }
    sum
}

pub fn dot_f64(a: &[f64], b: &[f64]) -> f64 {
    debug_assert_eq!(a.len(), b.len());
    let n = a.len() & !3;
    let (mut s0, mut s1, mut s2, mut s3) = (0.0f64, 0.0, 0.0, 0.0);
    for i in (0..n).step_by(4) {
        s0 += a[i] * b[i];
        s1 += a[i + 1] * b[i + 1];
        s2 += a[i + 2] * b[i + 2];
        s3 += a[i + 3] * b[i + 3];
    }
    let mut sum = (s0 + s1) + (s2 + s3);
    for i in n..a.len() {
        sum += a[i] * b[i];
    }
    sum
}

/// `1 - dot / (|a| |b|)`; a zero vector gives NaN, like DuckDB.
pub fn cosine_distance_f32(a: &[f32], b: &[f32]) -> f32 {
    1.0 - cosine_similarity_f32(a, b)
}

pub fn cosine_similarity_f32(a: &[f32], b: &[f32]) -> f32 {
    let dot = dot_f32(a, b);
    let na = dot_f32(a, a).sqrt();
    let nb = dot_f32(b, b).sqrt();
    dot / (na * nb)
}

pub fn cosine_distance_f64(a: &[f64], b: &[f64]) -> f64 {
    1.0 - cosine_similarity_f64(a, b)
}

pub fn cosine_similarity_f64(a: &[f64], b: &[f64]) -> f64 {
    let dot = dot_f64(a, b);
    let na = dot_f64(a, a).sqrt();
    let nb = dot_f64(b, b).sqrt();
    dot / (na * nb)
}

/// The three metrics in the shape the index and the exact heap consume.
pub fn metric_f32(metric: Metric, a: &[f32], b: &[f32]) -> f32 {
    match metric {
        Metric::L2Sq => l2sq_f32(a, b),
        Metric::Cosine => cosine_distance_f32(a, b),
        Metric::InnerProduct => -dot_f32(a, b),
    }
}

pub fn metric_f64(metric: Metric, a: &[f64], b: &[f64]) -> f64 {
    match metric {
        Metric::L2Sq => l2sq_f64(a, b),
        Metric::Cosine => cosine_distance_f64(a, b),
        Metric::InnerProduct => -dot_f64(a, b),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kernels_match_a_naive_loop() {
        let a: Vec<f32> = (0..37).map(|i| (i as f32 * 0.37).sin()).collect();
        let b: Vec<f32> = (0..37).map(|i| (i as f32 * 0.11).cos()).collect();
        let l2: f32 = a
            .iter()
            .zip(&b)
            .map(|(x, y)| (x - y) * (x - y))
            .sum::<f32>();
        let dot: f32 = a.iter().zip(&b).map(|(x, y)| x * y).sum();
        assert!((l2sq_f32(&a, &b) - l2).abs() <= l2.abs() * 1e-6 + 1e-6);
        assert!((dot_f32(&a, &b) - dot).abs() <= dot.abs() * 1e-6 + 1e-6);
    }

    #[test]
    fn cosine_of_identical_vectors_is_zero_and_zero_vector_is_nan() {
        let a = [1.0f32, 2.0, 3.0];
        assert!(cosine_distance_f32(&a, &a).abs() < 1e-6);
        assert!(cosine_distance_f32(&[0.0f32, 0.0, 0.0], &a).is_nan());
    }

    #[test]
    fn metric_names_round_trip() {
        for m in [Metric::L2Sq, Metric::Cosine, Metric::InnerProduct] {
            assert_eq!(Metric::parse(m.name()), Some(m));
        }
        assert_eq!(Metric::parse("L2SQ"), Some(Metric::L2Sq));
        assert_eq!(Metric::parse("euclid"), None);
    }
}
