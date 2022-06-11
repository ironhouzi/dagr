use std::io;

#[derive(Debug)]
pub struct RuntimeError {
    message: String,
}

impl RuntimeError {
    pub fn new(message: &str) -> Self {
        Self { message: message.to_string() }
    }
}

impl std::fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", &self.message)
    }
}

#[derive(Debug)]
pub enum DagrError {
    Internal(RuntimeError),
    Io(io::Error),
    Container(bollard::errors::Error),
    Template(minijinja::Error),
}

impl std::fmt::Display for DagrError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            DagrError::Internal(ref err) => write!(f, "dagr internal error: {}", err),
            DagrError::Io(ref err) => write!(f, "dagr io error: {}", err),
            DagrError::Container(ref err) => write!(f, "dagr container error: {}", err),
            DagrError::Template(ref err) => write!(f, "CMD interpolation error: {}", err),
        }
    }
}

impl From<RuntimeError> for DagrError {
    fn from(err: RuntimeError) -> DagrError {
        DagrError::Internal(err)
    }
}

impl From<io::Error> for DagrError {
    fn from(err: io::Error) -> DagrError {
        DagrError::Io(err)
    }
}

impl From<bollard::errors::Error> for DagrError {
    fn from(err: bollard::errors::Error) -> DagrError {
        DagrError::Container(err)
    }
}

impl From<minijinja::Error> for DagrError {
    fn from(err: minijinja::Error) -> DagrError {
        DagrError::Template(err)
    }
}
