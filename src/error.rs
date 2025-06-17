use fbtree::{access_method::AccessMethodError, prelude::TxnStorageStatus};

#[derive(Debug)]
pub enum ExecError {
    FieldOp(String),
    Conversion(String),
    Catalog(String),
    Storage(String),
    Pipeline(String),
    Other(String),
}

impl From<TxnStorageStatus> for ExecError {
    fn from(status: TxnStorageStatus) -> ExecError {
        ExecError::Storage(status.into())
    }
}

impl From<AccessMethodError> for ExecError {
    fn from(err: AccessMethodError) -> ExecError {
        ExecError::Storage(format!("{:?}", err))
    }
}
