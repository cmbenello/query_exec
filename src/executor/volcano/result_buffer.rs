use std::{
    cell::UnsafeCell,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use crate::{
    error::ExecError,
    executor::{TupleBuffer, TupleBufferIter},
    rwlatch::RwLatch,
    tuple::Tuple,
};

pub struct ResultBuffer {
    latch: RwLatch,
    buffer: UnsafeCell<Vec<Tuple>>,
}

impl Default for ResultBuffer {
    fn default() -> Self {
        Self::new()
    }
}

impl ResultBuffer {
    pub fn new() -> Self {
        Self {
            latch: RwLatch::default(),
            buffer: UnsafeCell::new(Vec::new()),
        }
    }

    pub fn push(&self, tuple: Tuple) {
        self.latch.exclusive();
        unsafe {
            (*self.buffer.get()).push(tuple);
        }
        self.latch.release_exclusive();
    }

    pub fn pop(&self) -> Option<Tuple> {
        self.latch.exclusive();
        let tuple = unsafe { (*self.buffer.get()).pop() };
        self.latch.release_exclusive();
        tuple
    }

    pub fn len(&self) -> usize {
        self.latch.shared();
        let len = unsafe { (*self.buffer.get()).len() };
        self.latch.release_shared();
        len
    }

    pub fn is_empty(&self) -> bool {
        self.latch.shared();
        let is_empty = unsafe { (*self.buffer.get()).is_empty() };
        self.latch.release_shared();
        is_empty
    }
}

pub struct ResultBufferIter {
    buffer: Arc<ResultBuffer>,
    index: AtomicUsize,
}

impl TupleBuffer for ResultBuffer {
    type Iter = ResultBufferIter;

    fn num_tuples(&self) -> usize {
        self.len()
    }

    fn iter(self: &Arc<ResultBuffer>) -> Self::Iter {
        self.latch.shared();
        ResultBufferIter {
            buffer: self.clone(),
            index: AtomicUsize::new(0),
        }
    }
}

impl TupleBufferIter for ResultBufferIter {
    fn next(&mut self) -> Result<Option<Tuple>, ExecError> {
        let index = self.index.fetch_add(1, Ordering::AcqRel);
        if index < self.buffer.len() {
            Ok(Some(unsafe { (*self.buffer.buffer.get())[index].copy() }))
        } else {
            Ok(None)
        }
    }
}

impl Drop for ResultBufferIter {
    fn drop(&mut self) {
        self.buffer.latch.release_shared();
    }
}
