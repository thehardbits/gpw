use std::io;

#[derive(Debug)]
pub enum GpwError {
    Io(io::Error),
    /// Generic parsing error
    Parse(&'static str, Option<Box<dyn std::fmt::Debug>>),
}

impl From<io::Error> for GpwError {
    fn from(e: io::Error) -> Self {
        GpwError::Io(e)
    }
}

impl<E: std::fmt::Debug + 'static> From<(&'static str, E)> for GpwError {
    fn from((field, e): (&'static str, E)) -> Self {
        GpwError::Parse(field, Some(Box::new(e)))
    }
}
