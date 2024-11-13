mod disk_buffer;
mod hash_table;
mod sort;
use std::marker::PhantomData;

use core::num;
use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    sync::Arc,
};

use fbtree::{
    access_method::{
        fbt::FosterBtreeAppendOnlyRangeScanner,
        append_only_store::AppendOnlyStoreRangeIter,
    },
    bp::{ContainerId, ContainerKey, DatabaseId, MemPool},
    prelude::{AppendOnlyStore, TxnStorageTrait},
};
use hash_table::{OnDiskHashAggregation, OnDiskHashTableCreation};
use sort::OnDiskSort;

use crate::{
    error::ExecError,
    expression::{AggOp, Expression, JoinType},
    log_debug, log_info,
    optimizer::PhysicalRelExpr,
    prelude::{CatalogRef, ColumnDef, DataType, Schema, SchemaRef},
    tuple::{FromBool, Tuple},
    ColumnId, Field,
};

use super::{bytecode_expr::ByteCodeExpr, Executor};
use super::{TupleBuffer, TupleBufferIter};
use disk_buffer::{OnDiskBuffer, OnDiskBufferIter};

// Pipeline iterators are non-blocking.
pub enum NonBlockingOp<T: TxnStorageTrait, M: MemPool> {
    Scan(PScanIter<T, M>),
    RangeScan(PRangeScanIter<T, M>),
    Filter(PFilterIter<T, M>),
    Project(PProjectIter<T, M>),
    Map(PMapIter<T, M>),
    HashJoin(PHashJoinIter<T, M>),
    NestedLoopJoin(PNestedLoopJoinIter<T, M>),
}

impl<T: TxnStorageTrait, M: MemPool> NonBlockingOp<T, M> {
    pub fn schema(&self) -> &SchemaRef {
        match self {
            NonBlockingOp::Scan(iter) => iter.schema(),
            NonBlockingOp::RangeScan(iter) => iter.schema(),
            NonBlockingOp::Filter(iter) => iter.schema(),
            NonBlockingOp::Project(iter) => iter.schema(),
            NonBlockingOp::Map(iter) => iter.schema(),
            NonBlockingOp::HashJoin(iter) => iter.schema(),
            NonBlockingOp::NestedLoopJoin(iter) => iter.schema(),
        }
    }

    pub fn rewind(&mut self) {
        match self {
            NonBlockingOp::Scan(iter) => iter.rewind(),
            NonBlockingOp::RangeScan(iter) => iter.rewind(),
            NonBlockingOp::Filter(iter) => iter.rewind(),
            NonBlockingOp::Project(iter) => iter.rewind(),
            NonBlockingOp::Map(iter) => iter.rewind(),
            NonBlockingOp::HashJoin(iter) => iter.rewind(),
            NonBlockingOp::NestedLoopJoin(iter) => iter.rewind(),
        }
    }

    pub fn deps(&self) -> HashSet<PipelineID> {
        match self {
            NonBlockingOp::Scan(iter) => iter.deps(),
            NonBlockingOp::RangeScan(iter) => iter.deps(),
            NonBlockingOp::Filter(iter) => iter.deps(),
            NonBlockingOp::Project(iter) => iter.deps(),
            NonBlockingOp::Map(iter) => iter.deps(),
            NonBlockingOp::HashJoin(iter) => iter.deps(),
            NonBlockingOp::NestedLoopJoin(iter) => iter.deps(),
        }
    }

    pub fn print_inner(&self, indent: usize, out: &mut String) {
        match self {
            NonBlockingOp::Scan(iter) => iter.print_inner(indent, out),
            NonBlockingOp::RangeScan(iter) => iter.print_inner(indent, out),
            NonBlockingOp::Filter(iter) => iter.print_inner(indent, out),
            NonBlockingOp::Project(iter) => iter.print_inner(indent, out),
            NonBlockingOp::Map(iter) => iter.print_inner(indent, out),
            NonBlockingOp::HashJoin(iter) => iter.print_inner(indent, out),
            NonBlockingOp::NestedLoopJoin(iter) => iter.print_inner(indent, out),
        }
    }

    pub fn next(
        &mut self,
        context: &HashMap<PipelineID, Arc<OnDiskBuffer<T, M>>>,
    ) -> Result<Option<Tuple>, ExecError> {
        match self {
            NonBlockingOp::Scan(iter) => iter.next(context),
            NonBlockingOp::RangeScan(iter) => iter.next(context),
            NonBlockingOp::Filter(iter) => iter.next(context),
            NonBlockingOp::Project(iter) => iter.next(context),
            NonBlockingOp::Map(iter) => iter.next(context),
            NonBlockingOp::HashJoin(iter) => iter.next(context),
            NonBlockingOp::NestedLoopJoin(iter) => iter.next(context),
        }
    }

    pub fn estimate_num_tuples(
        &self,
        context: &HashMap<PipelineID, Arc<OnDiskBuffer<T, M>>>,
    ) -> usize {
        match self {
            NonBlockingOp::Scan(iter) => iter.estimate_num_tuples(context),
            NonBlockingOp::RangeScan(iter) => unimplemented!("estimate_num_tuples for RangeScan"),
            NonBlockingOp::Filter(iter) => iter.estimate_num_tuples(context),
            NonBlockingOp::Project(iter) => iter.estimate_num_tuples(context),
            NonBlockingOp::Map(iter) => iter.estimate_num_tuples(context),
            NonBlockingOp::HashJoin(iter) => {
                unimplemented!("estimate_num_tuples for HashJoin")
                // iter.estimate_num_tuples(),
            }
            NonBlockingOp::NestedLoopJoin(iter) => {
                unimplemented!("estimate_num_tuples for NestedLoopJoin")
                // iter.estimate_num_tuples(),
            }
        }
    }

    pub fn clone_with_range(&self, start_index: usize, end_index: usize) -> Self {
        match self {
            NonBlockingOp::Scan(iter) => NonBlockingOp::RangeScan(PRangeScanIter::<T, M>::new(
                iter.schema().clone(),
                iter.id,
                iter.column_indices.clone(),
                start_index,
                end_index,
            )),
            NonBlockingOp::Filter(iter) => NonBlockingOp::Filter(PFilterIter {
                schema: iter.schema.clone(),
                input: Box::new(iter.input.clone_with_range(start_index, end_index)),
                expr: iter.expr.clone(),
                num_tuples_scanned: 0,
                num_tuples_filtered: 0,
            }),
            NonBlockingOp::Project(iter) => NonBlockingOp::Project(PProjectIter {
                schema: iter.schema.clone(),
                input: Box::new(iter.input.clone_with_range(start_index, end_index)),
                column_indices: iter.column_indices.clone(),
            }),
            NonBlockingOp::Map(iter) => NonBlockingOp::Map(PMapIter {
                schema: iter.schema.clone(),
                input: Box::new(iter.input.clone_with_range(start_index, end_index)),
                exprs: iter.exprs.clone(),
            }),
            other => {
                panic!("clone_with_range not implemented for other methods");
            }
        }
    }
}

pub struct PScanIter<T: TxnStorageTrait, M: MemPool> {
    schema: SchemaRef, // Output schema
    id: PipelineID,
    column_indices: Vec<ColumnId>,
    iter: Option<OnDiskBufferIter<T, M>>,
}

impl<T: TxnStorageTrait, M: MemPool> PScanIter<T, M> {
    pub fn new(
        schema: SchemaRef, // Output schema
        id: PipelineID,
        column_indices: Vec<ColumnId>,
    ) -> Self {
        Self {
            schema,
            id,
            column_indices,
            iter: None,
        }
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn rewind(&mut self) {
        self.iter = None;
    }

    pub fn deps(&self) -> HashSet<PipelineID> {
        let mut deps = HashSet::new();
        deps.insert(self.id);
        deps
    }

    pub fn print_inner(&self, indent: usize, out: &mut String) {
        out.push_str(&format!("{}->scan(p_id({}), ", " ".repeat(indent), self.id));
        let mut split = "";
        out.push('[');
        for col_id in &self.column_indices {
            out.push_str(split);
            out.push_str(&format!("{}", col_id));
            split = ", ";
        }
        out.push_str("])\n");
    }

    pub fn next(
        &mut self,
        context: &HashMap<PipelineID, Arc<OnDiskBuffer<T, M>>>,
    ) -> Result<Option<Tuple>, ExecError> {
        log_debug!("ScanIter::next");
        if let Some(iter) = &mut self.iter {
            let next = iter.next();
            next.map(|res| res.map(|tuple| tuple.project(&self.column_indices)))
        } else {
            self.iter = context.get(&self.id).map(|buf| buf.iter());
            if let Some(iter) = &mut self.iter {
                let next = iter.next();
                next.map(|res| res.map(|tuple| tuple.project(&self.column_indices)))
            } else {
                Ok(None)
            }
        }
    }

    pub fn estimate_num_tuples(
        &self,
        context: &HashMap<PipelineID, Arc<OnDiskBuffer<T, M>>>,
    ) -> usize {
        context
            .get(&self.id)
            .map(|buf| buf.num_tuples())
            .unwrap_or(0)
    }
}

pub struct PRangeScanIter<T: TxnStorageTrait, M: MemPool> {
    schema: SchemaRef,
    id: PipelineID,
    column_indices: Vec<ColumnId>,
    iter: Option<AppendOnlyStoreRangeIter<M>>, //xtx forcine append only for now
    start_index: usize,
    end_index: usize,
    current_index: usize,
    _marker: PhantomData<T>,
}

impl<T: TxnStorageTrait, M: MemPool> PRangeScanIter<T, M> {
    pub fn new(
        schema: SchemaRef,
        id: PipelineID,
        column_indices: Vec<ColumnId>,
        start_index: usize,
        end_index: usize,
    ) -> Self {
        Self {
            schema,
            id,
            column_indices,
            iter: None,
            start_index,
            end_index,
            current_index: 0,
        }
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn rewind(&mut self) {
        self.iter = None;
        self.current_index = 0;
    }

    pub fn deps(&self) -> HashSet<PipelineID> {
        let mut deps = HashSet::new();
        deps.insert(self.id);
        deps
    }

    pub fn print_inner(&self, indent: usize, out: &mut String) {
        out.push_str(&format!(
            "{}->range_scan(p_id({}), start: {}, end: {}, ",
            " ".repeat(indent),
            self.id,
            self.start_index,
            self.end_index,
        ));
        let mut split = "";
        out.push('[');
        for col_id in &self.column_indices {
            out.push_str(split);
            out.push_str(&format!("{}", col_id));
            split = ", ";
        }
        out.push_str("])\n");
    }


    fn next(
        &mut self, 
        context: &HashMap<PipelineID, Arc<OnDiskBuffer<T, M>>>
    ) -> Result<Option<Tuple>, ExecError> {
        log_debug!("RangeScanIter::next");

        // Initialize the iterator if it's not already initialized
        if self.iter.is_none() {
            if let Some(buf) = context.get(&self.id) {
                if let OnDiskBuffer::AppendOnlyStore(store) = buf.as_ref() {
                    let range_iter = store.range_scan(self.start_index, self.end_index);
                    self.iter = Some(range_iter);
                } else {
                    // return Err(ExecError::Storage("RangeScanIter: Unsupported buffer type"));
                    unimplemented!()
                }
            } else {
                // return Err(ExecError::new("RangeScanIter: Buffer not found in context"));
                unimplemented!()
            }
        }

        // Use the iterator if it's initialized
        if let Some(iter) = &mut self.iter {
            if let Some((key, value)) = iter.next() {
                // Project if necessary
                let projected_tuple = Tuple::from_fields(vec![
                    Field::from_bytes(&key),
                    Field::from_bytes(&value),
                ]).project(&self.column_indices);
                Ok(Some(projected_tuple))
            } else {
                // Iterator exhausted
                Ok(None)
            }
        } else {
            // Iterator could not be initialized
            Ok(None)
        }
    }
}

// // XTx idk what is going on here/when to use here or in the impl for Prangescaniter
// impl<T: TxnStorageTrait, M: MemPool> TupleBufferIter for PRangeScanIter<T, M> {
//     fn next(&mut self, context: &HashMap<PipelineID, Arc<OnDiskBuffer<T, M>>>) -> Result<Option<Tuple>, ExecError> {
//         log_debug!("RangeScanIter::next");

//         // Initialize the iterator if it's not already initialized
//         if self.iter.is_none() {
//             if let Some(buf) = context.get(&self.id) {
//                 if let OnDiskBuffer::AppendOnlyStore(store) = buf.as_ref() {
//                     let range_iter = store.range_scan(self.start_index, self.end_index);
//                     self.iter = Some(range_iter);
//                 } else {
//                     return Err(ExecError::new("RangeScanIter: Unsupported buffer type"));
//                 }
//             } else {
//                 return Err(ExecError::new("RangeScanIter: Buffer not found in context"));
//             }
//         }

//         // Use the iterator if it's initialized
//         if let Some(iter) = &mut self.iter {
//             if let Some((key, value)) = iter.next() {
//                 // Project if necessary
//                 let projected_tuple = Tuple::new(key, value).project(&self.column_indices);
//                 Ok(Some(projected_tuple))
//             } else {
//                 // Iterator exhausted
//                 Ok(None)
//             }
//         } else {
//             // Iterator could not be initialized
//             Ok(None)
//         }
//     }
// }

impl<T: TxnStorageTrait, M: MemPool> Clone for PRangeScanIter<T, M> {
    fn clone(&self) -> Self {
        Self {
            schema: self.schema.clone(),
            id: self.id,
            column_indices: self.column_indices.clone(),
            iter: None,
            start_index: self.start_index,
            end_index: self.end_index,
            current_index: 0,
        }
    }
}
pub struct PFilterIter<T: TxnStorageTrait, M: MemPool> {
    schema: SchemaRef, // Output schema
    input: Box<NonBlockingOp<T, M>>,
    expr: ByteCodeExpr,
    // Counts to estimate selectivity.
    num_tuples_scanned: usize,
    num_tuples_filtered: usize,
}

impl<T: TxnStorageTrait, M: MemPool> PFilterIter<T, M> {
    pub fn new(schema: SchemaRef, input: Box<NonBlockingOp<T, M>>, expr: ByteCodeExpr) -> Self {
        Self {
            schema,
            input,
            expr,
            num_tuples_scanned: 0,
            num_tuples_filtered: 0,
        }
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn rewind(&mut self) {
        self.input.rewind();
    }

    pub fn deps(&self) -> HashSet<PipelineID> {
        self.input.deps()
    }

    pub fn print_inner(&self, indent: usize, out: &mut String) {
        out.push_str(&format!("{}->filter({})", " ".repeat(indent), self.expr));
        out.push('\n');
        self.input.print_inner(indent + 2, out);
    }

    pub fn next(
        &mut self,
        context: &HashMap<PipelineID, Arc<OnDiskBuffer<T, M>>>,
    ) -> Result<Option<Tuple>, ExecError> {
        log_debug!("FilterIter::next");
        while let Some(tuple) = self.input.next(context)? {
            self.num_tuples_scanned += 1;
            if self.expr.eval(&tuple)? == Field::from_bool(true) {
                return Ok(Some(tuple));
            }
            self.num_tuples_filtered += 1;
        }
        Ok(None)
    }

    pub fn estimate_num_tuples(
        &self,
        context: &HashMap<PipelineID, Arc<OnDiskBuffer<T, M>>>,
    ) -> usize {
        if self.num_tuples_scanned == 0 {
            self.input.estimate_num_tuples(context)
        } else {
            (self.input.estimate_num_tuples(context) as f64
                * (1.0 - self.num_tuples_filtered as f64 / self.num_tuples_scanned as f64))
                as usize
        }
    }
}

pub struct PProjectIter<T: TxnStorageTrait, M: MemPool> {
    schema: SchemaRef, // Output schema
    input: Box<NonBlockingOp<T, M>>,
    column_indices: Vec<ColumnId>,
}

impl<T: TxnStorageTrait, M: MemPool> PProjectIter<T, M> {
    pub fn new(
        schema: SchemaRef,
        input: Box<NonBlockingOp<T, M>>,
        column_indices: Vec<ColumnId>,
    ) -> Self {
        Self {
            schema,
            input,
            column_indices,
        }
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn rewind(&mut self) {
        self.input.rewind();
    }

    pub fn deps(&self) -> HashSet<PipelineID> {
        self.input.deps()
    }

    pub fn print_inner(&self, indent: usize, out: &mut String) {
        out.push_str(&format!("{}->project(", " ".repeat(indent)));
        let mut split = "";
        out.push('[');
        for col_id in &self.column_indices {
            out.push_str(split);
            out.push_str(&format!("{}", col_id));
            split = ", ";
        }
        out.push_str("])\n");
        self.input.print_inner(indent + 2, out);
    }

    pub fn next(
        &mut self,
        context: &HashMap<PipelineID, Arc<OnDiskBuffer<T, M>>>,
    ) -> Result<Option<Tuple>, ExecError> {
        log_debug!("ProjectIter::next");
        if let Some(tuple) = self.input.next(context)? {
            let new_tuple = tuple.project(&self.column_indices);
            Ok(Some(new_tuple))
        } else {
            Ok(None)
        }
    }

    pub fn estimate_num_tuples(
        &self,
        context: &HashMap<PipelineID, Arc<OnDiskBuffer<T, M>>>,
    ) -> usize {
        self.input.estimate_num_tuples(context)
    }
}

pub struct PMapIter<T: TxnStorageTrait, M: MemPool> {
    schema: SchemaRef, // Output schema
    input: Box<NonBlockingOp<T, M>>,
    exprs: Vec<ByteCodeExpr>,
}

impl<T: TxnStorageTrait, M: MemPool> PMapIter<T, M> {
    pub fn new(
        schema: SchemaRef,
        input: Box<NonBlockingOp<T, M>>,
        exprs: Vec<ByteCodeExpr>,
    ) -> Self {
        Self {
            schema,
            input,
            exprs,
        }
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn rewind(&mut self) {
        self.input.rewind();
    }

    pub fn deps(&self) -> HashSet<PipelineID> {
        self.input.deps()
    }

    pub fn print_inner(&self, indent: usize, out: &mut String) {
        out.push_str(&format!("{}->map(", " ".repeat(indent)));
        let mut split = "";
        out.push('[');
        for expr in &self.exprs {
            out.push_str(split);
            out.push_str(&format!("{}", expr));
            split = ", ";
        }
        out.push_str("])\n");
        self.input.print_inner(indent + 2, out);
    }

    pub fn next(
        &mut self,
        context: &HashMap<PipelineID, Arc<OnDiskBuffer<T, M>>>,
    ) -> Result<Option<Tuple>, ExecError> {
        log_debug!("MapIter::next");
        if let Some(mut tuple) = self.input.next(context)? {
            for expr in &self.exprs {
                tuple.push(expr.eval(&tuple)?);
            }
            Ok(Some(tuple))
        } else {
            Ok(None)
        }
    }

    pub fn estimate_num_tuples(
        &self,
        context: &HashMap<PipelineID, Arc<OnDiskBuffer<T, M>>>,
    ) -> usize {
        self.input.estimate_num_tuples(context)
    }
}

pub enum PHashJoinIter<T: TxnStorageTrait, M: MemPool> {
    Inner(PHashJoinInnerIter<T, M>),
    RightOuter(PHashJoinRightOuterIter<T, M>), // Probe side is the right
    RightSemi(PHashJoinRightSemiIter<T, M>),   // Probe side is the right
    RightAnti(PHashJoinRightAntiIter<T, M>),   // Probe side is the right
    RightMark(PHashJoinRightMarkIter<T, M>),   // Probe side is the right
}

impl<T: TxnStorageTrait, M: MemPool> PHashJoinIter<T, M> {
    pub fn schema(&self) -> &SchemaRef {
        match self {
            PHashJoinIter::Inner(iter) => iter.schema(),
            PHashJoinIter::RightOuter(iter) => iter.schema(),
            PHashJoinIter::RightSemi(iter) => iter.schema(),
            PHashJoinIter::RightAnti(iter) => iter.schema(),
            PHashJoinIter::RightMark(iter) => iter.schema(),
        }
    }

    pub fn rewind(&mut self) {
        match self {
            PHashJoinIter::Inner(iter) => iter.rewind(),
            PHashJoinIter::RightOuter(iter) => iter.rewind(),
            PHashJoinIter::RightSemi(iter) => iter.rewind(),
            PHashJoinIter::RightAnti(iter) => iter.rewind(),
            PHashJoinIter::RightMark(iter) => iter.rewind(),
        }
    }

    pub fn deps(&self) -> HashSet<PipelineID> {
        match self {
            PHashJoinIter::Inner(iter) => iter.deps(),
            PHashJoinIter::RightOuter(iter) => iter.deps(),
            PHashJoinIter::RightSemi(iter) => iter.deps(),
            PHashJoinIter::RightAnti(iter) => iter.deps(),
            PHashJoinIter::RightMark(iter) => iter.deps(),
        }
    }

    pub fn print_inner(&self, indent: usize, out: &mut String) {
        match self {
            PHashJoinIter::Inner(iter) => iter.print_inner(indent, out),
            PHashJoinIter::RightOuter(iter) => iter.print_inner(indent, out),
            PHashJoinIter::RightSemi(iter) => iter.print_inner(indent, out),
            PHashJoinIter::RightAnti(iter) => iter.print_inner(indent, out),
            PHashJoinIter::RightMark(iter) => iter.print_inner(indent, out),
        }
    }

    pub fn next(
        &mut self,
        context: &HashMap<PipelineID, Arc<OnDiskBuffer<T, M>>>,
    ) -> Result<Option<Tuple>, ExecError> {
        match self {
            PHashJoinIter::Inner(iter) => iter.next(context),
            PHashJoinIter::RightOuter(iter) => iter.next(context),
            PHashJoinIter::RightSemi(iter) => iter.next(context),
            PHashJoinIter::RightAnti(iter) => iter.next(context),
            PHashJoinIter::RightMark(iter) => iter.next(context),
        }
    }
}

pub struct PHashJoinInnerIter<T: TxnStorageTrait, M: MemPool> {
    schema: SchemaRef, // Output schema
    probe_side: Box<NonBlockingOp<T, M>>,
    build_side: PipelineID,
    exprs: Vec<ByteCodeExpr>,
    current: Option<(Tuple, FosterBtreeAppendOnlyRangeScanner<M>)>,
}

impl<T: TxnStorageTrait, M: MemPool> PHashJoinInnerIter<T, M> {
    pub fn new(
        schema: SchemaRef, // Output schema
        probe_side: Box<NonBlockingOp<T, M>>,
        build_side: PipelineID,
        exprs: Vec<ByteCodeExpr>,
    ) -> Self {
        Self {
            schema,
            probe_side,
            build_side,
            exprs,
            current: None,
        }
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn rewind(&mut self) {
        self.probe_side.rewind();
        self.current = None;
    }

    pub fn deps(&self) -> HashSet<PipelineID> {
        let mut deps = self.probe_side.deps();
        deps.insert(self.build_side);
        deps
    }

    pub fn print_inner(&self, indent: usize, out: &mut String) {
        out.push_str(&format!(
            "{}-> hash_join_inner(build_p_id({}), ",
            " ".repeat(indent),
            self.build_side
        ));
        let mut split = "";
        out.push('[');
        for expr in &self.exprs {
            out.push_str(split);
            out.push_str(&format!("{}", expr));
            split = ", ";
        }
        out.push_str("])\n");
        self.probe_side.print_inner(indent + 2, out);
    }

    pub fn next(
        &mut self,
        context: &HashMap<PipelineID, Arc<OnDiskBuffer<T, M>>>,
    ) -> Result<Option<Tuple>, ExecError> {
        log_debug!("HashJoinInnerIter::next");
        if let Some((probe, build_iter)) = &mut self.current {
            if let Some((_, build)) = build_iter.next() {
                let build = Tuple::from_bytes(&build);
                let result = build.merge_mut(probe);
                return Ok(Some(result));
            }
        }
        let build_side =
            if let OnDiskBuffer::HashTable(tb) = context.get(&self.build_side).unwrap().as_ref() {
                tb
            } else {
                unreachable!("The build side is not a hash table");
            };
        // Reset the current tuple and build iterator.
        loop {
            // Iterate the probe side until a match is found.
            // If match is found
            if let Some(probe) = self.probe_side.next(context)? {
                let key = self
                    .exprs
                    .iter()
                    .map(|expr| expr.eval(&probe))
                    .collect::<Result<Vec<_>, _>>()?;
                let mut build_iter = build_side.iter_key(&key);
                // There should be at least one tuple in the build side iterator.
                if let Some((_, build)) = build_iter.next() {
                    let build = Tuple::from_bytes(&build);
                    let result = build.merge_mut(&probe);
                    self.current = Some((probe, build_iter));
                    return Ok(Some(result));
                } else {
                    // No match found. Continue to the next probe tuple.
                }
            } else {
                return Ok(None);
            }
        }
    }
}

pub struct PHashJoinRightOuterIter<T: TxnStorageTrait, M: MemPool> {
    schema: SchemaRef,                    // Output schema
    probe_side: Box<NonBlockingOp<T, M>>, // Probe side is the right. All tuples in the probe side will be preserved.
    build_side: PipelineID,
    exprs: Vec<ByteCodeExpr>,
    current: Option<(Tuple, FosterBtreeAppendOnlyRangeScanner<M>)>,
    nulls: Tuple,
}

impl<T: TxnStorageTrait, M: MemPool> PHashJoinRightOuterIter<T, M> {
    pub fn new(
        schema: SchemaRef, // Output schema
        probe_side: Box<NonBlockingOp<T, M>>,
        build_side: PipelineID,
        exprs: Vec<ByteCodeExpr>,
        nulls: Tuple,
    ) -> Self {
        Self {
            schema,
            probe_side,
            build_side,
            exprs,
            current: None,
            nulls,
        }
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn rewind(&mut self) {
        self.probe_side.rewind();
        self.current = None;
    }

    pub fn deps(&self) -> HashSet<PipelineID> {
        let mut deps = self.probe_side.deps();
        deps.insert(self.build_side);
        deps
    }

    pub fn print_inner(&self, indent: usize, out: &mut String) {
        out.push_str(&format!(
            "{}-> hash_join_right_outer(build_p_id({}), ",
            " ".repeat(indent),
            self.build_side
        ));
        let mut split = "";
        out.push('[');
        for expr in &self.exprs {
            out.push_str(split);
            out.push_str(&format!("{}", expr));
            split = ", ";
        }
        out.push_str("])\n");
        self.probe_side.print_inner(indent + 2, out);
    }

    pub fn next(
        &mut self,
        context: &HashMap<PipelineID, Arc<OnDiskBuffer<T, M>>>,
    ) -> Result<Option<Tuple>, ExecError> {
        log_debug!("HashJoinRightOuterIter::next");
        if let Some((probe, build_iter)) = &mut self.current {
            if let Some((_, build)) = build_iter.next() {
                let build = Tuple::from_bytes(&build);
                let result = build.merge_mut(probe);
                return Ok(Some(result));
            }
        }

        let build_side =
            if let OnDiskBuffer::HashTable(tb) = context.get(&self.build_side).unwrap().as_ref() {
                tb
            } else {
                unreachable!("The build side is not a hash table");
            };

        // Reset the current tuple and build iterator.
        if let Some(probe) = self.probe_side.next(context)? {
            let key = self
                .exprs
                .iter()
                .map(|expr| expr.eval(&probe))
                .collect::<Result<Vec<_>, _>>()?;

            let mut build_iter = build_side.iter_key(&key);
            let result = if let Some((_, build)) = build_iter.next() {
                // Try to iterate the build side once to check if there is any match.
                let build = Tuple::from_bytes(&build);
                let result = build.merge_mut(&probe);
                self.current = Some((probe, build_iter));
                result
            } else {
                self.current = None;
                // No match found. Output the probe tuple with nulls for the build side.
                self.nulls.merge(&probe)
            };
            Ok(Some(result))
        } else {
            Ok(None)
        }
    }
}

pub struct PHashJoinRightSemiIter<T: TxnStorageTrait, M: MemPool> {
    schema: SchemaRef,                    // Output schema
    probe_side: Box<NonBlockingOp<T, M>>, // Probe side is the right. All tuples in the probe side will be preserved.
    build_side: PipelineID,
    exprs: Vec<ByteCodeExpr>,
}

impl<T: TxnStorageTrait, M: MemPool> PHashJoinRightSemiIter<T, M> {
    pub fn new(
        schema: SchemaRef, // Output schema
        probe_side: Box<NonBlockingOp<T, M>>,
        build_side: PipelineID,
        exprs: Vec<ByteCodeExpr>,
    ) -> Self {
        Self {
            schema,
            probe_side,
            build_side,
            exprs,
        }
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn rewind(&mut self) {
        self.probe_side.rewind();
    }

    pub fn deps(&self) -> HashSet<PipelineID> {
        let mut deps = self.probe_side.deps();
        deps.insert(self.build_side);
        deps
    }

    pub fn print_inner(&self, indent: usize, out: &mut String) {
        out.push_str(&format!(
            "{}-> hash_join_right_semi(build_p_id({}), ",
            " ".repeat(indent),
            self.build_side
        ));
        let mut split = "";
        out.push('[');
        for expr in &self.exprs {
            out.push_str(split);
            out.push_str(&format!("{}", expr));
            split = ", ";
        }
        out.push_str("])\n");
        self.probe_side.print_inner(indent + 2, out);
    }

    pub fn next(
        &mut self,
        context: &HashMap<PipelineID, Arc<OnDiskBuffer<T, M>>>,
    ) -> Result<Option<Tuple>, ExecError> {
        log_debug!("HashJoinRightSemiIter::next");
        let build_side =
            if let OnDiskBuffer::HashTable(tb) = context.get(&self.build_side).unwrap().as_ref() {
                tb
            } else {
                unreachable!("The build side is not a hash table");
            };
        // If there is a match in the build side, output the probe tuple.
        // Otherwise go to the next probe tuple
        loop {
            if let Some(probe) = self.probe_side.next(context)? {
                let key = self
                    .exprs
                    .iter()
                    .map(|expr| expr.eval(&probe))
                    .collect::<Result<Vec<_>, _>>()?;
                let mut build_iter = build_side.iter_key(&key);
                if build_iter.next().is_some() {
                    return Ok(Some(probe));
                } else {
                    // No match found. Continue to the next probe tuple.
                }
            } else {
                return Ok(None);
            }
        }
    }
}

pub struct PHashJoinRightAntiIter<T: TxnStorageTrait, M: MemPool> {
    schema: SchemaRef,                    // Output schema
    probe_side: Box<NonBlockingOp<T, M>>, // Probe side is the right. All tuples in the probe side will be preserved.
    build_side: PipelineID,
    exprs: Vec<ByteCodeExpr>,
}

impl<T: TxnStorageTrait, M: MemPool> PHashJoinRightAntiIter<T, M> {
    pub fn new(
        schema: SchemaRef, // Output schema
        probe_side: Box<NonBlockingOp<T, M>>,
        build_side: PipelineID,
        exprs: Vec<ByteCodeExpr>,
    ) -> Self {
        Self {
            schema,
            probe_side,
            build_side,
            exprs,
        }
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn rewind(&mut self) {
        self.probe_side.rewind();
    }

    pub fn deps(&self) -> HashSet<PipelineID> {
        let mut deps = self.probe_side.deps();
        deps.insert(self.build_side);
        deps
    }

    pub fn print_inner(&self, indent: usize, out: &mut String) {
        out.push_str(&format!(
            "{}-> hash_join_right_anti(build_p_id({}), ",
            " ".repeat(indent),
            self.build_side
        ));
        let mut split = "";
        out.push('[');
        for expr in &self.exprs {
            out.push_str(split);
            out.push_str(&format!("{}", expr));
            split = ", ";
        }
        out.push_str("])\n");
        self.probe_side.print_inner(indent + 2, out);
    }

    pub fn next(
        &mut self,
        context: &HashMap<PipelineID, Arc<OnDiskBuffer<T, M>>>,
    ) -> Result<Option<Tuple>, ExecError> {
        log_debug!("HashJoinRightAntiIter::next");
        let build_side =
            if let OnDiskBuffer::HashTable(tb) = context.get(&self.build_side).unwrap().as_ref() {
                tb
            } else {
                unreachable!("The build side is not a hash table");
            };
        // If there is no match in the build side, output the probe tuple.
        // Otherwise go to the next probe tuple
        loop {
            if let Some(probe) = self.probe_side.next(context)? {
                let key = self
                    .exprs
                    .iter()
                    .map(|expr| expr.eval(&probe))
                    .collect::<Result<Vec<_>, _>>()?;
                let mut build_iter = build_side.iter_key(&key);
                if let Some((_, _)) = build_iter.next() {
                    // Match found. Continue to the next probe tuple.
                } else {
                    return Ok(Some(probe));
                }
                // No match found. Continue to the next probe tuple.
            } else {
                return Ok(None);
            }
        }
    }
}

// Mark join is similar to semi/anti join but returns all the probe tuples.
// The tuples are marked with a boolean value indicating if there is a match in the build side.
// If there is a matching tuple, the mark is true.
// If there is no matching tuple, if the build side has nulls, the mark is null.
// Otherwise, the mark is false.
pub struct PHashJoinRightMarkIter<T: TxnStorageTrait, M: MemPool> {
    schema: SchemaRef,                    // Output schema
    probe_side: Box<NonBlockingOp<T, M>>, // Probe side is the right. All tuples in the probe side will be preserved.
    build_side: PipelineID,
    exprs: Vec<ByteCodeExpr>,
}

impl<T: TxnStorageTrait, M: MemPool> PHashJoinRightMarkIter<T, M> {
    pub fn new(
        schema: SchemaRef, // Output schema
        probe_side: Box<NonBlockingOp<T, M>>,
        build_side: PipelineID,
        exprs: Vec<ByteCodeExpr>,
    ) -> Self {
        Self {
            schema,
            probe_side,
            build_side,
            exprs,
        }
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn rewind(&mut self) {
        self.probe_side.rewind();
    }

    pub fn deps(&self) -> HashSet<PipelineID> {
        let mut deps = self.probe_side.deps();
        deps.insert(self.build_side);
        deps
    }

    pub fn print_inner(&self, indent: usize, out: &mut String) {
        out.push_str(&format!(
            "{}-> hash_join_right_mark(build_p_id({}), ",
            " ".repeat(indent),
            self.build_side
        ));
        let mut split = "";
        out.push('[');
        for expr in &self.exprs {
            out.push_str(split);
            out.push_str(&format!("{}", expr));
            split = ", ";
        }
        out.push_str("])\n");
        self.probe_side.print_inner(indent + 2, out);
    }

    pub fn next(
        &mut self,
        context: &HashMap<PipelineID, Arc<OnDiskBuffer<T, M>>>,
    ) -> Result<Option<Tuple>, ExecError> {
        log_debug!("HashJoinRightMarkIter::next");
        // If there is a match in the build side, output the probe tuple.
        // Otherwise go to the next probe tuple
        let build_side =
            if let OnDiskBuffer::HashTable(tb) = context.get(&self.build_side).unwrap().as_ref() {
                tb
            } else {
                unreachable!("The build side is not a hash table");
            };

        loop {
            if let Some(mut probe) = self.probe_side.next(context)? {
                let key = self
                    .exprs
                    .iter()
                    .map(|expr| expr.eval(&probe))
                    .collect::<Result<Vec<_>, _>>()?;
                let mut build_iter = build_side.iter_key(&key);
                let mark = if build_iter.next().is_some() {
                    Field::from_bool(true)
                } else if build_side.has_null() {
                    Field::null(&DataType::Boolean)
                } else {
                    Field::from_bool(false)
                };
                probe.push(mark);
                return Ok(Some(probe));
            } else {
                return Ok(None);
            }
        }
    }
}

pub struct PNestedLoopJoinIter<T: TxnStorageTrait, M: MemPool> {
    schema: SchemaRef,               // Output schema
    outer: Box<NonBlockingOp<T, M>>, // Outer loop of NLJ
    inner: Box<NonBlockingOp<T, M>>, // Inner loop of NLJ
    current_outer: Option<Tuple>,
    current_inner: Option<Tuple>,
}

impl<T: TxnStorageTrait, M: MemPool> PNestedLoopJoinIter<T, M> {
    pub fn new(
        schema: SchemaRef,
        outer: Box<NonBlockingOp<T, M>>,
        inner: Box<NonBlockingOp<T, M>>,
    ) -> Self {
        Self {
            schema,
            outer,
            inner,
            current_outer: None,
            current_inner: None,
        }
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn rewind(&mut self) {
        self.outer.rewind();
        self.inner.rewind();
        self.current_outer = None;
        self.current_inner = None;
    }

    pub fn deps(&self) -> HashSet<PipelineID> {
        let mut deps = self.outer.deps();
        deps.extend(self.inner.deps());
        deps
    }

    pub fn print_inner(&self, indent: usize, out: &mut String) {
        out.push_str(&format!("{}-> nested_loop_join\n", " ".repeat(indent)));
        self.outer.print_inner(indent + 2, out);
        self.inner.print_inner(indent + 2, out);
    }

    pub fn next(
        &mut self,
        context: &HashMap<PipelineID, Arc<OnDiskBuffer<T, M>>>,
    ) -> Result<Option<Tuple>, ExecError> {
        loop {
            // Set the outer
            if self.current_outer.is_none() {
                match self.outer.next(context)? {
                    Some(tuple) => {
                        self.current_outer = Some(tuple);
                        self.inner.rewind();
                    }
                    None => return Ok(None),
                }
            }
            // Set the inner
            if self.current_inner.is_none() {
                match self.inner.next(context)? {
                    Some(tuple) => {
                        self.current_inner = Some(tuple);
                    }
                    None => {
                        self.current_outer = None;
                        continue;
                    }
                }
            }

            // Outer and inner are set. Merge the tuples.
            let new_tuple = self
                .current_outer
                .as_ref()
                .unwrap()
                .merge(self.current_inner.as_ref().unwrap());
            self.current_inner = None;
            return Ok(Some(new_tuple));
        }
    }
}

#[derive(Debug)]
pub enum MemoryPolicy {
    FixedSizeLimit(usize), // Fixed number of frames
    Proportional(f64),     // Proportional to the data size
    Unbounded,
}

pub enum BlockingOp<T: TxnStorageTrait, M: MemPool> {
    Dummy(NonBlockingOp<T, M>),
    OnDiskSort(OnDiskSort<T, M>),
    OnDiskHashTableCreation(OnDiskHashTableCreation<T, M>),
    OnDiskHashAggregate(OnDiskHashAggregation<T, M>),
}

impl<T: TxnStorageTrait, M: MemPool> BlockingOp<T, M> {
    pub fn schema(&self) -> &SchemaRef {
        match self {
            BlockingOp::Dummy(plan) => plan.schema(),
            BlockingOp::OnDiskSort(sort) => sort.schema(),
            BlockingOp::OnDiskHashTableCreation(creation) => creation.schema(),
            BlockingOp::OnDiskHashAggregate(agg) => agg.schema(),
        }
    }

    pub fn deps(&self) -> HashSet<PipelineID> {
        match self {
            BlockingOp::Dummy(plan) => plan.deps(),
            BlockingOp::OnDiskSort(sort) => sort.deps(),
            BlockingOp::OnDiskHashTableCreation(creation) => creation.deps(),
            BlockingOp::OnDiskHashAggregate(agg) => agg.deps(),
        }
    }

    pub fn print_inner(&self, indent: usize, out: &mut String) {
        match self {
            BlockingOp::Dummy(plan) => plan.print_inner(indent, out),
            BlockingOp::OnDiskSort(sort) => sort.print_inner(indent, out),
            BlockingOp::OnDiskHashTableCreation(creation) => creation.print_inner(indent, out),
            BlockingOp::OnDiskHashAggregate(agg) => agg.print_inner(indent, out),
        }
    }

    pub fn execute(
        &mut self,
        context: &HashMap<PipelineID, Arc<OnDiskBuffer<T, M>>>,
        policy: &Arc<MemoryPolicy>,
        mem_pool: &Arc<M>,
        dest_c_key: ContainerKey,
    ) -> Result<Arc<OnDiskBuffer<T, M>>, ExecError> {
        match self {
            BlockingOp::Dummy(plan) => {
                log_debug!("Dummy blocking op");
                let output = Arc::new(AppendOnlyStore::new(dest_c_key, mem_pool.clone()));
                while let Some(tuple) = plan.next(context)? {
                    output.append(&[], &tuple.to_bytes())?
                }
                Ok(Arc::new(OnDiskBuffer::AppendOnlyStore(output)))
            }
            BlockingOp::OnDiskSort(sort) => sort.execute(context, policy, mem_pool, dest_c_key),
            BlockingOp::OnDiskHashTableCreation(creation) => {
                creation.execute(context, policy, mem_pool, dest_c_key)
            }
            BlockingOp::OnDiskHashAggregate(agg) => {
                agg.execute(context, policy, mem_pool, dest_c_key)
            }
        }
    }
}

impl<T: TxnStorageTrait, M: MemPool> From<NonBlockingOp<T, M>> for BlockingOp<T, M> {
    fn from(plan: NonBlockingOp<T, M>) -> Self {
        BlockingOp::Dummy(plan)
    }
}

pub type PipelineID = u16;

pub struct Pipeline<T: TxnStorageTrait, M: MemPool> {
    id: PipelineID,
    context: HashMap<PipelineID, Arc<OnDiskBuffer<T, M>>>,
    exec_plan: BlockingOp<T, M>,
    deps_cache: HashSet<PipelineID>,
    policy: Arc<MemoryPolicy>,
    mem_pool: Arc<M>,
    dest_c_key: ContainerKey,
}

impl<T: TxnStorageTrait, M: MemPool> Pipeline<T, M> {
    pub fn new(
        id: PipelineID,
        execution_plan: BlockingOp<T, M>,
        mem_pool: &Arc<M>,
        policy: &Arc<MemoryPolicy>,
        dest_c_key: ContainerKey,
    ) -> Self {
        Self {
            id,
            context: HashMap::new(),
            deps_cache: execution_plan.deps(),
            exec_plan: execution_plan,
            mem_pool: mem_pool.clone(),
            policy: policy.clone(),
            dest_c_key,
        }
    }

    pub fn new_with_context(
        id: PipelineID,
        execution_plan: BlockingOp<T, M>,
        context: HashMap<PipelineID, Arc<OnDiskBuffer<T, M>>>,
        mem_pool: &Arc<M>,
        policy: &Arc<MemoryPolicy>,
        dest_c_key: ContainerKey,
    ) -> Self {
        Self {
            id,
            context,
            deps_cache: execution_plan.deps(),
            exec_plan: execution_plan,
            mem_pool: mem_pool.clone(),
            policy: policy.clone(),
            dest_c_key,
        }
    }

    pub fn schema(&self) -> &SchemaRef {
        self.exec_plan.schema()
    }

    pub fn get_id(&self) -> PipelineID {
        self.id
    }

    pub fn deps(&self) -> &HashSet<PipelineID> {
        &self.deps_cache
    }

    pub fn is_ready(&self) -> bool {
        self.deps_cache
            .iter()
            .all(|id| self.context.contains_key(id))
    }

    /// Set context.
    pub fn set_context(&mut self, id: PipelineID, buffer: Arc<OnDiskBuffer<T, M>>) {
        self.context.insert(id, buffer);
    }

    pub fn print_inner(&self, indent: usize, out: &mut String) {
        out.push_str(&format!("{}Pipeline ID: {}\n", " ".repeat(indent), self.id));
        out.push_str(&format!(
            "{}Schema: {}\n",
            " ".repeat(indent),
            self.schema()
        ));
        out.push_str(&format!(
            "{}Dependencies: {:?}\n",
            " ".repeat(indent),
            self.deps()
        ));
        out.push_str(&format!(
            "{}Is ready: {}\n",
            " ".repeat(indent),
            self.is_ready()
        ));
        self.exec_plan.print_inner(indent, out);
    }

    pub fn execute(&mut self) -> Result<Arc<OnDiskBuffer<T, M>>, ExecError> {
        self.exec_plan
            .execute(&self.context, &self.policy, &self.mem_pool, self.dest_c_key)
    }
}

pub struct OnDiskPipelineGraph<T: TxnStorageTrait, M: MemPool> {
    pipelines: Vec<Pipeline<T, M>>,
    queue: VecDeque<Pipeline<T, M>>,
}

impl<T: TxnStorageTrait, M: MemPool> OnDiskPipelineGraph<T, M> {
    pub fn new(
        db_id: DatabaseId,
        intermediate_dest_c_id: ContainerId,
        catalog: &CatalogRef,
        storage: &Arc<T>,
        mem_pool: &Arc<M>,
        policy: &Arc<MemoryPolicy>,
        physical_plan: PhysicalRelExpr,
        exclude_last_pipeline: bool, // This option is used to exclude the last pipeline from the graph
    ) -> Self {
        let converter = PhysicalRelExprToPipelineQueue::new(
            db_id,
            intermediate_dest_c_id,
            storage,
            mem_pool,
            policy,
        );

        converter
            .convert(catalog.clone(), physical_plan, exclude_last_pipeline)
            .expect("Failed to convert physical plan")
    }

    fn empty() -> Self {
        Self {
            pipelines: Vec::new(),
            queue: VecDeque::new(),
        }
    }

    fn add_pipeline(&mut self, pipeline: Pipeline<T, M>) {
        self.pipelines.push(pipeline);
    }

    // Identify the pipelines that do not have any dependencies and push that to the queue
    // This pipeline's dependencies can be removed from the other pipelines since it is empty
    fn push_no_deps_to_queue(&mut self) {
        // Push the pipelines that do not have any dependencies to the queue
        // drain_filter is not stable yet so use retain instead
        let mut with_deps: Vec<Pipeline<T, M>> = Vec::new();
        for p in self.pipelines.drain(..) {
            if p.is_ready() {
                self.queue.push_back(p);
            } else {
                with_deps.push(p);
            }
        }
        self.pipelines = with_deps;
    }

    // Remove the completed pipeline from the dependencies of other pipelines
    // Set the output of the completed pipeline as the input of the dependent pipelines
    fn notify_dependants(&mut self, id: PipelineID, output: Arc<OnDiskBuffer<T, M>>) {
        for p in self.pipelines.iter_mut() {
            if p.deps().contains(&id) {
                // p is a dependant of the completed pipeline
                p.set_context(id, output.clone())
            }
        }
    }
}

impl<T: TxnStorageTrait, M: MemPool> Executor<T> for OnDiskPipelineGraph<T, M> {
    type Buffer = OnDiskBuffer<T, M>;

    fn to_pretty_string(&self) -> String {
        let mut result = String::new();
        for pipeline in self.pipelines.iter() {
            let id = pipeline.get_id();
            result.push_str(&format!("Pipeline ID: {}\n", id));
            pipeline.print_inner(2, &mut result);
        }
        result
    }

    fn execute(mut self, _txn: &T::TxnHandle) -> Result<Arc<OnDiskBuffer<T, M>>, ExecError> {
        let mut result = None;
        self.push_no_deps_to_queue();
        log_info!(
            "Initial queue: {:?}",
            self.queue.iter().map(|p| p.get_id()).collect::<Vec<_>>()
        );
        while let Some(mut pipeline) = self.queue.pop_front() {
            let current_result = pipeline.execute()?;
            log_info!(
                "Pipeline ID: {} executed with output size: {:?}",
                pipeline.get_id(),
                current_result.num_tuples()
            );
            self.notify_dependants(pipeline.get_id(), current_result.clone());
            self.push_no_deps_to_queue();
            log_info!(
                "Queue: {:?}",
                self.queue.iter().map(|p| p.get_id()).collect::<Vec<_>>()
            );
            result = Some(current_result);
        }
        result.ok_or(ExecError::Pipeline("No pipeline executed".to_string()))
    }
}

struct PhysicalRelExprToPipelineQueue<T: TxnStorageTrait, M: MemPool> {
    current_id: PipelineID, // Incremental pipeline ID
    db_id: DatabaseId,
    dest_c_id: ContainerId,
    storage: Arc<T>,
    mem_pool: Arc<M>,
    policy: Arc<MemoryPolicy>,
    pipeline_queue: OnDiskPipelineGraph<T, M>,
}

type ColIdToIdx = BTreeMap<ColumnId, usize>;

impl<T: TxnStorageTrait, M: MemPool> PhysicalRelExprToPipelineQueue<T, M> {
    fn new(
        db_id: DatabaseId,
        intermediate_dest_c_id: ContainerId,
        storage: &Arc<T>,
        mem_pool: &Arc<M>,
        policy: &Arc<MemoryPolicy>,
    ) -> Self {
        Self {
            current_id: 0,
            db_id,
            dest_c_id: intermediate_dest_c_id,
            storage: storage.clone(),
            mem_pool: mem_pool.clone(),
            policy: policy.clone(),
            pipeline_queue: OnDiskPipelineGraph::empty(),
        }
    }

    fn fetch_add_id(&mut self) -> PipelineID {
        let id = self.current_id;
        self.current_id += 1;
        id
    }

    fn convert(
        mut self,
        catalog: CatalogRef,
        expr: PhysicalRelExpr,
        exclude_last_pipeline: bool,
    ) -> Result<OnDiskPipelineGraph<T, M>, ExecError> {
        let (op, context, _) = self.convert_inner(catalog, expr)?;
        // if let NonBlockingOp::Scan(PScanIter {
        //     schema,
        //     id,
        //     column_indices,
        //     iter,
        // }) = &op
        // {
        //     // If column_indices are the same as the original schema, we do not need this scan.
        //     let num_cols = schema.columns().len();
        //     if column_indices.len() == num_cols
        //         && column_indices
        //             .iter()
        //             .enumerate()
        //             .all(|(idx, col_id)| *col_id == idx)
        //     {
        //         return Ok(self.pipeline_queue);
        //     }
        // }
        // // Other cases.
        if !exclude_last_pipeline {
            let blocking_op = BlockingOp::Dummy(op);
            let pipeline = Pipeline::new_with_context(
                self.fetch_add_id(),
                blocking_op,
                context,
                &self.mem_pool,
                &self.policy,
                ContainerKey::new(self.db_id, self.dest_c_id),
            );
            self.pipeline_queue.add_pipeline(pipeline);
        }
        Ok(self.pipeline_queue)
    }

    fn convert_inner(
        &mut self,
        catalog: CatalogRef,
        expr: PhysicalRelExpr,
    ) -> Result<
        (
            NonBlockingOp<T, M>,
            HashMap<PipelineID, Arc<OnDiskBuffer<T, M>>>,
            ColIdToIdx,
        ),
        ExecError,
    > {
        match expr {
            PhysicalRelExpr::Scan {
                db_id,
                c_id,
                table_name: _,
                column_indices,
            } => {
                let col_id_to_idx = column_indices
                    .iter()
                    .enumerate()
                    .map(|(idx, col_id)| (*col_id, idx))
                    .collect();

                let in_schema = catalog
                    .get_schema(c_id)
                    .ok_or(ExecError::Catalog("Schema not found".to_string()))?;
                let out_schema = Arc::new(in_schema.project(&column_indices));
                let p_id = self.fetch_add_id();
                let scan_buffer = Arc::new(OnDiskBuffer::txn_storage(
                    in_schema,
                    db_id,
                    c_id,
                    self.storage.clone(),
                ));

                let scan = NonBlockingOp::Scan(PScanIter::new(out_schema, p_id, column_indices));
                Ok((
                    scan,
                    [(p_id, scan_buffer)].into_iter().collect(),
                    col_id_to_idx,
                ))
            }
            PhysicalRelExpr::Rename { src, src_to_dest } => {
                let (input_op, context, col_id_to_idx) = self.convert_inner(catalog, *src)?;
                let col_id_to_idx = src_to_dest
                    .iter()
                    .map(|(src, dest)| (*dest, col_id_to_idx[src]))
                    .collect();
                Ok((input_op, context, col_id_to_idx))
            }
            PhysicalRelExpr::Select { src, predicates } => {
                let (input_op, context, col_id_to_idx) = self.convert_inner(catalog, *src)?;
                let predicate = Expression::merge_conjunction(predicates);
                let expr: ByteCodeExpr = ByteCodeExpr::from_ast(predicate, &col_id_to_idx)?;
                let filter = PFilterIter::new(input_op.schema().clone(), Box::new(input_op), expr);
                Ok((NonBlockingOp::Filter(filter), context, col_id_to_idx))
            }
            PhysicalRelExpr::Project { src, column_names } => {
                let (input_op, context, col_id_to_idx) = self.convert_inner(catalog, *src)?;
                let (column_indices, new_col_id_to_idx) = {
                    let mut new_col_id_to_idx = ColIdToIdx::new();
                    let mut column_indices = Vec::new();
                    for (idx, col_id) in column_names.iter().enumerate() {
                        new_col_id_to_idx.insert(*col_id, idx);
                        column_indices.push(col_id_to_idx[col_id]);
                    }
                    (column_indices, new_col_id_to_idx)
                };
                let schema = input_op.schema().project(&column_indices);
                let project =
                    PProjectIter::new(Arc::new(schema), Box::new(input_op), column_indices);
                Ok((NonBlockingOp::Project(project), context, new_col_id_to_idx))
            }
            PhysicalRelExpr::Map { input, exprs } => {
                let (input_op, context, mut col_id_to_idx) = self.convert_inner(catalog, *input)?;
                let mut bytecode_exprs = Vec::new();
                let input_schema = input_op.schema();
                let mut new_cols = Vec::new();
                for (dest, expr) in exprs {
                    let expr = ByteCodeExpr::from_ast(expr, &col_id_to_idx)?;
                    bytecode_exprs.push(expr);
                    col_id_to_idx.insert(dest, col_id_to_idx.len());
                    new_cols.push(ColumnDef::new("dest", DataType::Unknown, true));
                }
                let schema = input_schema.merge(&Schema::new(new_cols, Vec::new()));
                let map = PMapIter::new(Arc::new(schema), Box::new(input_op), bytecode_exprs);
                Ok((NonBlockingOp::Map(map), context, col_id_to_idx))
            }
            PhysicalRelExpr::NestedLoopJoin {
                join_type: _,
                left,
                right,
                predicates,
            } => {
                let (left_op, left_context, left_col_id_to_idx) =
                    self.convert_inner(catalog.clone(), *left)?;
                let left_len = left_col_id_to_idx.len();
                let (right_op, right_context, right_col_id_to_idx) =
                    self.convert_inner(catalog, *right)?;
                let context = left_context.into_iter().chain(right_context).collect();

                let mut col_id_to_idx = left_col_id_to_idx;
                for (col_name, col_idx) in right_col_id_to_idx {
                    col_id_to_idx.insert(col_name, col_idx + left_len);
                }
                let schema = Arc::new(left_op.schema().merge(right_op.schema()));
                let cross_join =
                    PNestedLoopJoinIter::new(schema.clone(), Box::new(left_op), Box::new(right_op));
                if predicates.is_empty() {
                    Ok((
                        NonBlockingOp::NestedLoopJoin(cross_join),
                        context,
                        col_id_to_idx,
                    ))
                } else {
                    let filter = ByteCodeExpr::from_ast(
                        Expression::merge_conjunction(predicates),
                        &col_id_to_idx,
                    )?;
                    let filter = PFilterIter::new(
                        schema.clone(),
                        Box::new(NonBlockingOp::NestedLoopJoin(cross_join)),
                        filter,
                    );
                    Ok((NonBlockingOp::Filter(filter), context, col_id_to_idx))
                }
            }
            PhysicalRelExpr::HashJoin {
                join_type,
                left,
                right,
                equalities,
                filter,
            } => {
                let (left_op, left_context, left_col_id_to_idx) =
                    self.convert_inner(catalog.clone(), *left)?;
                let (right_op, right_context, right_col_id_to_idx) =
                    self.convert_inner(catalog, *right)?;
                let left_len = left_col_id_to_idx.len();

                // The left will be the build side and the right will be the probe side
                // The physical plan should not contain left outer/semi/anti join
                let mut left_exprs = Vec::new();
                let mut right_exprs = Vec::new();
                for (left_expr, right_expr) in equalities {
                    left_exprs.push(ByteCodeExpr::from_ast(left_expr, &left_col_id_to_idx)?);
                    right_exprs.push(ByteCodeExpr::from_ast(right_expr, &right_col_id_to_idx)?);
                }

                // First, create the pipeline for the hash table build side
                let left_schema = left_op.schema().clone();
                let left_id = self.fetch_add_id();
                let left_hash_table_creation = BlockingOp::OnDiskHashTableCreation(
                    OnDiskHashTableCreation::new(left_schema.clone(), left_op, left_exprs),
                );
                let left_p = Pipeline::new_with_context(
                    left_id,
                    left_hash_table_creation,
                    left_context,
                    &self.mem_pool,
                    &self.policy,
                    ContainerKey::new(self.db_id, self.dest_c_id),
                );
                self.pipeline_queue.add_pipeline(left_p); // Add the build side to the graph

                // Then create the non-blocking pipeline for the probe side
                let (iter, col_id_to_idx) = match join_type {
                    JoinType::CrossJoin => {
                        panic!("Cross join is not supported in hash join")
                    }
                    JoinType::Inner => {
                        let mut col_id_to_idx = left_col_id_to_idx;
                        for (col_name, col_idx) in right_col_id_to_idx {
                            let res = col_id_to_idx.insert(col_name, col_idx + left_len);
                            assert_eq!(res, None);
                        }
                        let schema = left_schema.merge(right_op.schema());
                        let probe = PHashJoinIter::Inner(PHashJoinInnerIter::new(
                            Arc::new(schema),
                            Box::new(right_op),
                            left_id,
                            right_exprs,
                        ));
                        (probe, col_id_to_idx)
                    }
                    JoinType::RightOuter => {
                        let mut col_id_to_idx = left_col_id_to_idx;
                        for (col_name, col_idx) in right_col_id_to_idx {
                            let res = col_id_to_idx.insert(col_name, col_idx + left_len);
                            assert_eq!(res, None);
                        }
                        // Make the left side columns nullable
                        let left_schema = left_schema.make_nullable();
                        let schema = left_schema.merge(right_op.schema());
                        let nulls = Tuple::from_fields(
                            left_schema
                                .columns()
                                .iter()
                                .map(|col| Field::null(col.data_type()))
                                .collect(),
                        );
                        let probe = PHashJoinIter::RightOuter(PHashJoinRightOuterIter::new(
                            Arc::new(schema),
                            Box::new(right_op),
                            left_id,
                            right_exprs,
                            nulls,
                        ));
                        (probe, col_id_to_idx)
                    }
                    JoinType::RightSemi => {
                        let probe = PHashJoinIter::RightSemi(PHashJoinRightSemiIter::new(
                            right_op.schema().clone(),
                            Box::new(right_op),
                            left_id,
                            right_exprs,
                        ));
                        (probe, right_col_id_to_idx)
                    }
                    JoinType::RightAnti => {
                        let probe = PHashJoinIter::RightAnti(PHashJoinRightAntiIter::new(
                            right_op.schema().clone(),
                            Box::new(right_op),
                            left_id,
                            right_exprs,
                        ));
                        (probe, right_col_id_to_idx)
                    }
                    JoinType::RightMarkJoin(col_id) => {
                        // insert col_id to right_col_id_to_idx
                        let right_len = right_col_id_to_idx.len();
                        let mut col_id_to_idx = right_col_id_to_idx;
                        let res = col_id_to_idx.insert(col_id, right_len);
                        assert_eq!(res, None);

                        // Mark join returns the right side with a new nullable boolean column
                        let right_schema = right_op.schema();
                        let col_def = ColumnDef::new("mark", DataType::Boolean, true);
                        let schema = right_schema.merge(&Schema::new(vec![col_def], Vec::new()));
                        let probe = PHashJoinIter::RightMark(PHashJoinRightMarkIter::new(
                            Arc::new(schema),
                            Box::new(right_op),
                            left_id,
                            right_exprs,
                        ));
                        (probe, col_id_to_idx)
                    }
                    other => {
                        panic!("Join type {:?} is not supported", other)
                    }
                };

                // Add the filter if necessary
                let iter = if filter.is_empty() {
                    NonBlockingOp::HashJoin(iter)
                } else {
                    let filter = ByteCodeExpr::from_ast(
                        Expression::merge_conjunction(filter),
                        &col_id_to_idx,
                    )?;
                    let filter = PFilterIter::new(
                        iter.schema().clone(),
                        Box::new(NonBlockingOp::HashJoin(iter)),
                        filter,
                    );
                    NonBlockingOp::Filter(filter)
                };

                Ok((iter, right_context, col_id_to_idx))
            }
            PhysicalRelExpr::HashAggregate {
                src,
                group_by,
                aggrs,
            } => {
                let (input_op, context, col_id_to_idx) = self.convert_inner(catalog, *src)?;
                let group_by_indices = group_by
                    .iter()
                    .map(|col_id| col_id_to_idx[col_id])
                    .collect();
                let agg_op_indices = aggrs
                    .iter()
                    .map(|(_dest, (src, op))| (*op, col_id_to_idx[src]))
                    .collect();
                // Project the group by and aggregation columns
                let input_schema = input_op.schema();
                let mut schema = input_schema.project(&group_by_indices);
                for &(op, col) in &agg_op_indices {
                    match op {
                        AggOp::Count => {
                            schema.push_column(ColumnDef::new("dest", DataType::Int, false))
                        }
                        AggOp::Sum | AggOp::Max | AggOp::Min => {
                            let col_type = input_schema.get_column(col).data_type();
                            schema.push_column(ColumnDef::new("dest", col_type.clone(), true))
                        }
                        AggOp::Avg => {
                            // float
                            schema.push_column(ColumnDef::new("dest", DataType::Float, true))
                        }
                    }
                }
                let schema = Arc::new(schema);
                let hash_agg = BlockingOp::OnDiskHashAggregate(OnDiskHashAggregation::new(
                    schema.clone(),
                    input_op,
                    group_by_indices,
                    agg_op_indices,
                ));
                let mut new_col_id_to_idx: ColIdToIdx = group_by
                    .iter()
                    .enumerate()
                    .map(|(idx, col_id)| (*col_id, idx))
                    .collect();
                let group_by_len = group_by.len();
                for (idx, (dest, _)) in aggrs.iter().enumerate() {
                    new_col_id_to_idx.insert(*dest, group_by_len + idx);
                }

                // Add the aggregation pipeline to the graph
                let agg_id = self.fetch_add_id();
                let p = Pipeline::new_with_context(
                    agg_id,
                    hash_agg,
                    context,
                    &self.mem_pool,
                    &self.policy,
                    ContainerKey::new(self.db_id, self.dest_c_id),
                );
                self.pipeline_queue.add_pipeline(p);

                // Create the scan operator for the aggregation pipeline
                let column_indices = (0..schema.columns().len()).collect();
                let scan = PScanIter::new(schema, agg_id, column_indices);

                Ok((NonBlockingOp::Scan(scan), HashMap::new(), new_col_id_to_idx))
            }
            PhysicalRelExpr::Sort { src, column_names } => {
                let (input_op, context, col_id_to_idx) = self.convert_inner(catalog, *src)?;
                let sort_cols = column_names
                    .iter()
                    .map(|(col_id, asc, nulls_first)| (col_id_to_idx[col_id], *asc, *nulls_first))
                    .collect();
                let schema = input_op.schema().clone();
                let num_quantiles = std::env::var("BENCH_NUM_QUANTILES") //xtx janky way of updating the number of quantiles/threads
                    .unwrap_or_else(|_| "10".to_string())
                    .parse::<usize>()
                    .expect("Invalid num quantiles");
                let sort = BlockingOp::OnDiskSort(OnDiskSort::new(
                    schema.clone(),
                    input_op,
                    sort_cols,
                    num_quantiles,
                ));
                // BlockingOp::OnDiskSort(OnDiskSort::new(schema.clone(), input_op, sort_cols));
                let sort_id = self.fetch_add_id();
                let p = Pipeline::new_with_context(
                    sort_id,
                    sort,
                    context,
                    &self.mem_pool,
                    &self.policy,
                    ContainerKey::new(self.db_id, self.dest_c_id),
                );
                self.pipeline_queue.add_pipeline(p);

                // Create the scan operator for the sort pipeline
                let scan = PScanIter::new(
                    schema.clone(),
                    sort_id,
                    (0..schema.columns().len()).collect(),
                );
                Ok((NonBlockingOp::Scan(scan), HashMap::new(), col_id_to_idx))
            }
            other => {
                panic!("PhysicalRelExpr {:?} is not currently supported", other)
            }
        }
    }
}
