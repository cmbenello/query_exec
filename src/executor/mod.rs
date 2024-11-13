use std::sync::Arc;

use fbtree::prelude::TxnStorageTrait;

use crate::{error::ExecError, tuple::Tuple};

mod bytecode_expr;
mod inmem_pipeline;
mod ondisk_pipeline;
mod volcano;

pub mod prelude {
    pub use super::inmem_pipeline::InMemPipelineGraph;
    pub use super::ondisk_pipeline::{MemoryPolicy, OnDiskPipelineGraph};
    pub use super::volcano::VolcanoIterator;
    pub use super::{Executor, TupleBuffer, TupleBufferIter};
}

pub trait Executor<T: TxnStorageTrait> {
    type Buffer: TupleBuffer;
    fn to_pretty_string(&self) -> String;
    fn execute(self, txn: &T::TxnHandle) -> Result<Arc<Self::Buffer>, ExecError>;
}

pub trait TupleBuffer {
    type Iter: TupleBufferIter;
    fn num_tuples(&self) -> usize;
    fn iter(self: &Arc<Self>) -> Self::Iter;
}

pub trait TupleBufferIter {
    fn next(&mut self) -> Result<Option<Tuple>, ExecError>;
}
