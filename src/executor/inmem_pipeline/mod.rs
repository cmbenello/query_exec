mod inmem_buffer;

use crate::log;
use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    sync::Arc,
};

use fbtree::prelude::TxnStorageTrait;

use crate::{
    error::ExecError,
    expression::{AggOp, Expression, JoinType},
    log_debug, log_info,
    optimizer::PhysicalRelExpr,
    prelude::{CatalogRef, ColumnDef, DataType, Schema, SchemaRef},
    quantile_lib::QuantileMethod,
    tuple::{FromBool, Tuple},
    ColumnId, Field,
};

use super::{bytecode_expr::ByteCodeExpr, Executor};
use inmem_buffer::{InMemBuffer, InMemBufferIter};

// Pipeline iterators are non-blocking.
pub enum NonBlockingOp<T: TxnStorageTrait> {
    Scan(PScanIter<T>),
    Filter(PFilterIter<T>),
    Project(PProjectIter<T>),
    Map(PMapIter<T>),
    HashJoin(PHashJoinIter<T>),
    NestedLoopJoin(PNestedLoopJoinIter<T>),
}

impl<T: TxnStorageTrait> NonBlockingOp<T> {
    pub fn schema(&self) -> &SchemaRef {
        match self {
            NonBlockingOp::Scan(iter) => iter.schema(),
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
            NonBlockingOp::Filter(iter) => iter.print_inner(indent, out),
            NonBlockingOp::Project(iter) => iter.print_inner(indent, out),
            NonBlockingOp::Map(iter) => iter.print_inner(indent, out),
            NonBlockingOp::HashJoin(iter) => iter.print_inner(indent, out),
            NonBlockingOp::NestedLoopJoin(iter) => iter.print_inner(indent, out),
        }
    }

    pub fn next(
        &mut self,
        context: &HashMap<PipelineID, Arc<InMemBuffer<T>>>,
    ) -> Result<Option<Tuple>, ExecError> {
        match self {
            NonBlockingOp::Scan(iter) => iter.next(context),
            NonBlockingOp::Filter(iter) => iter.next(context),
            NonBlockingOp::Project(iter) => iter.next(context),
            NonBlockingOp::Map(iter) => iter.next(context),
            NonBlockingOp::HashJoin(iter) => iter.next(context),
            NonBlockingOp::NestedLoopJoin(iter) => iter.next(context),
        }
    }
}

pub struct PScanIter<T: TxnStorageTrait> {
    schema: SchemaRef, // Output schema
    id: PipelineID,
    column_indices: Vec<ColumnId>,
    iter: Option<InMemBufferIter<T>>,
}

impl<T: TxnStorageTrait> PScanIter<T> {
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
        context: &HashMap<PipelineID, Arc<InMemBuffer<T>>>,
    ) -> Result<Option<Tuple>, ExecError> {
        log_debug!("ScanIter::next");
        if let Some(iter) = &mut self.iter {
            let next = iter.next();
            Ok(next.map(|tuple| tuple.project(&self.column_indices)))
        } else {
            self.iter = context.get(&self.id).map(|buf| buf.iter_all());
            Ok(self
                .iter
                .as_ref()
                .and_then(|iter| iter.next().map(|tuple| tuple.project(&self.column_indices))))
        }
    }
}

pub struct PFilterIter<T: TxnStorageTrait> {
    schema: SchemaRef, // Output schema
    input: Box<NonBlockingOp<T>>,
    expr: ByteCodeExpr,
}

impl<T: TxnStorageTrait> PFilterIter<T> {
    pub fn new(schema: SchemaRef, input: Box<NonBlockingOp<T>>, expr: ByteCodeExpr) -> Self {
        Self {
            schema,
            input,
            expr,
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
        context: &HashMap<PipelineID, Arc<InMemBuffer<T>>>,
    ) -> Result<Option<Tuple>, ExecError> {
        log_debug!("FilterIter::next");
        while let Some(tuple) = self.input.next(context)? {
            if self.expr.eval(&tuple)? == Field::from_bool(true) {
                return Ok(Some(tuple));
            }
        }
        Ok(None)
    }
}

pub struct PProjectIter<T: TxnStorageTrait> {
    schema: SchemaRef, // Output schema
    input: Box<NonBlockingOp<T>>,
    column_indices: Vec<ColumnId>,
}

impl<T: TxnStorageTrait> PProjectIter<T> {
    pub fn new(
        schema: SchemaRef,
        input: Box<NonBlockingOp<T>>,
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
        context: &HashMap<PipelineID, Arc<InMemBuffer<T>>>,
    ) -> Result<Option<Tuple>, ExecError> {
        log_debug!("ProjectIter::next");
        if let Some(tuple) = self.input.next(context)? {
            let new_tuple = tuple.project(&self.column_indices);
            Ok(Some(new_tuple))
        } else {
            Ok(None)
        }
    }
}

pub struct PMapIter<T: TxnStorageTrait> {
    schema: SchemaRef, // Output schema
    input: Box<NonBlockingOp<T>>,
    exprs: Vec<ByteCodeExpr>,
}

impl<T: TxnStorageTrait> PMapIter<T> {
    pub fn new(schema: SchemaRef, input: Box<NonBlockingOp<T>>, exprs: Vec<ByteCodeExpr>) -> Self {
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
        context: &HashMap<PipelineID, Arc<InMemBuffer<T>>>,
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
}

pub enum PHashJoinIter<T: TxnStorageTrait> {
    Inner(PHashJoinInnerIter<T>),
    RightOuter(PHashJoinRightOuterIter<T>), // Probe side is the right
    RightSemi(PHashJoinRightSemiIter<T>),   // Probe side is the right
    RightAnti(PHashJoinRightAntiIter<T>),   // Probe side is the right
    RightMark(PHashJoinRightMarkIter<T>),   // Probe side is the right
}

impl<T: TxnStorageTrait> PHashJoinIter<T> {
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
        context: &HashMap<PipelineID, Arc<InMemBuffer<T>>>,
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

pub struct PHashJoinInnerIter<T: TxnStorageTrait> {
    schema: SchemaRef, // Output schema
    probe_side: Box<NonBlockingOp<T>>,
    build_side: PipelineID,
    exprs: Vec<ByteCodeExpr>,
    current: Option<(Tuple, InMemBufferIter<T>)>,
}

impl<T: TxnStorageTrait> PHashJoinInnerIter<T> {
    pub fn new(
        schema: SchemaRef, // Output schema
        probe_side: Box<NonBlockingOp<T>>,
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
        context: &HashMap<PipelineID, Arc<InMemBuffer<T>>>,
    ) -> Result<Option<Tuple>, ExecError> {
        log_debug!("HashJoinInnerIter::next");
        if let Some((probe, build_iter)) = &mut self.current {
            if let Some(build) = build_iter.next() {
                let result = build.merge_mut(probe);
                return Ok(Some(result));
            }
        }
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
                let build_iter = context.get(&self.build_side).unwrap().iter_key(key);
                if let Some(iter) = build_iter {
                    // There should be at least one tuple in the build side iterator.
                    if let Some(build) = iter.next() {
                        let result = build.merge_mut(&probe);
                        self.current = Some((probe, iter));
                        return Ok(Some(result));
                    } else {
                        unreachable!("The build side returned an empty iterator")
                    }
                }
                // No match found. Continue to the next probe tuple.
            } else {
                return Ok(None);
            }
        }
    }
}

pub struct PHashJoinRightOuterIter<T: TxnStorageTrait> {
    schema: SchemaRef,                 // Output schema
    probe_side: Box<NonBlockingOp<T>>, // Probe side is the right. All tuples in the probe side will be preserved.
    build_side: PipelineID,
    exprs: Vec<ByteCodeExpr>,
    current: Option<(Tuple, InMemBufferIter<T>)>,
    nulls: Tuple,
}

impl<T: TxnStorageTrait> PHashJoinRightOuterIter<T> {
    pub fn new(
        schema: SchemaRef, // Output schema
        probe_side: Box<NonBlockingOp<T>>,
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
        context: &HashMap<PipelineID, Arc<InMemBuffer<T>>>,
    ) -> Result<Option<Tuple>, ExecError> {
        log_debug!("HashJoinRightOuterIter::next");
        if let Some((probe, build_iter)) = &mut self.current {
            if let Some(build) = build_iter.next() {
                let result = build.merge_mut(probe);
                return Ok(Some(result));
            }
        }
        // Reset the current tuple and build iterator.
        if let Some(probe) = self.probe_side.next(context)? {
            let key = self
                .exprs
                .iter()
                .map(|expr| expr.eval(&probe))
                .collect::<Result<Vec<_>, _>>()?;

            let build_iter = context.get(&self.build_side).unwrap().iter_key(key);
            let result = if let Some(iter) = build_iter {
                // Try to iterate the build side once to check if there is any match.

                if let Some(build) = iter.next() {
                    let result = build.merge_mut(&probe);
                    self.current = Some((probe, iter));
                    result
                } else {
                    // There should be at least one tuple in the build side iterator.
                    unreachable!("The build side returned an empty iterator");
                }
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

pub struct PHashJoinRightSemiIter<T: TxnStorageTrait> {
    schema: SchemaRef,                 // Output schema
    probe_side: Box<NonBlockingOp<T>>, // Probe side is the right. All tuples in the probe side will be preserved.
    build_side: PipelineID,
    exprs: Vec<ByteCodeExpr>,
}

impl<T: TxnStorageTrait> PHashJoinRightSemiIter<T> {
    pub fn new(
        schema: SchemaRef, // Output schema
        probe_side: Box<NonBlockingOp<T>>,
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
        context: &HashMap<PipelineID, Arc<InMemBuffer<T>>>,
    ) -> Result<Option<Tuple>, ExecError> {
        log_debug!("HashJoinRightSemiIter::next");
        // If there is a match in the build side, output the probe tuple.
        // Otherwise go to the next probe tuple
        loop {
            if let Some(probe) = self.probe_side.next(context)? {
                let key = self
                    .exprs
                    .iter()
                    .map(|expr| expr.eval(&probe))
                    .collect::<Result<Vec<_>, _>>()?;
                let build_iter = context.get(&self.build_side).unwrap().iter_key(key);
                if let Some(iter) = build_iter {
                    if iter.next().is_some() {
                        return Ok(Some(probe));
                    } else {
                        unreachable!("The build side returned an empty iterator")
                    }
                }
                // No match found. Continue to the next probe tuple.
            } else {
                return Ok(None);
            }
        }
    }
}

pub struct PHashJoinRightAntiIter<T: TxnStorageTrait> {
    schema: SchemaRef,                 // Output schema
    probe_side: Box<NonBlockingOp<T>>, // Probe side is the right. All tuples in the probe side will be preserved.
    build_side: PipelineID,
    exprs: Vec<ByteCodeExpr>,
}

impl<T: TxnStorageTrait> PHashJoinRightAntiIter<T> {
    pub fn new(
        schema: SchemaRef, // Output schema
        probe_side: Box<NonBlockingOp<T>>,
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
        context: &HashMap<PipelineID, Arc<InMemBuffer<T>>>,
    ) -> Result<Option<Tuple>, ExecError> {
        log_debug!("HashJoinRightAntiIter::next");
        // If there is no match in the build side, output the probe tuple.
        // Otherwise go to the next probe tuple
        loop {
            if let Some(probe) = self.probe_side.next(context)? {
                let key = self
                    .exprs
                    .iter()
                    .map(|expr| expr.eval(&probe))
                    .collect::<Result<Vec<_>, _>>()?;
                let build_iter = context.get(&self.build_side).unwrap().iter_key(key);
                if let Some(_iter) = build_iter {
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
pub struct PHashJoinRightMarkIter<T: TxnStorageTrait> {
    schema: SchemaRef,                 // Output schema
    probe_side: Box<NonBlockingOp<T>>, // Probe side is the right. All tuples in the probe side will be preserved.
    build_side: PipelineID,
    exprs: Vec<ByteCodeExpr>,
}

impl<T: TxnStorageTrait> PHashJoinRightMarkIter<T> {
    pub fn new(
        schema: SchemaRef, // Output schema
        probe_side: Box<NonBlockingOp<T>>,
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
        context: &HashMap<PipelineID, Arc<InMemBuffer<T>>>,
    ) -> Result<Option<Tuple>, ExecError> {
        log_debug!("HashJoinRightMarkIter::next");
        // If there is a match in the build side, output the probe tuple.
        // Otherwise go to the next probe tuple
        let build_side = context.get(&self.build_side).unwrap();
        loop {
            if let Some(mut probe) = self.probe_side.next(context)? {
                let key = self
                    .exprs
                    .iter()
                    .map(|expr| expr.eval(&probe))
                    .collect::<Result<Vec<_>, _>>()?;
                let build_iter = build_side.iter_key(key);
                let mark = if let Some(iter) = build_iter {
                    if iter.next().is_some() {
                        Field::from_bool(true)
                    } else {
                        unreachable!("The build side returned an empty iterator")
                    }
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

pub struct PNestedLoopJoinIter<T: TxnStorageTrait> {
    schema: SchemaRef,            // Output schema
    outer: Box<NonBlockingOp<T>>, // Outer loop of NLJ
    inner: Box<NonBlockingOp<T>>, // Inner loop of NLJ
    current_outer: Option<Tuple>,
    current_inner: Option<Tuple>,
}

impl<T: TxnStorageTrait> PNestedLoopJoinIter<T> {
    pub fn new(
        schema: SchemaRef,
        outer: Box<NonBlockingOp<T>>,
        inner: Box<NonBlockingOp<T>>,
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
        context: &HashMap<PipelineID, Arc<InMemBuffer<T>>>,
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

pub enum BlockingOp<T: TxnStorageTrait> {
    Dummy(NonBlockingOp<T>),
    InMemSort(InMemSort<T>),
    InMemHashTableCreation(InMemHashTableCreation<T>),
    InMemHashAggregate(InMemHashAggregation<T>),
}

impl<T: TxnStorageTrait> BlockingOp<T> {
    pub fn schema(&self) -> &SchemaRef {
        match self {
            BlockingOp::Dummy(plan) => plan.schema(),
            BlockingOp::InMemSort(sort) => sort.schema(),
            BlockingOp::InMemHashTableCreation(creation) => creation.schema(),
            BlockingOp::InMemHashAggregate(agg) => agg.schema(),
        }
    }

    pub fn deps(&self) -> HashSet<PipelineID> {
        match self {
            BlockingOp::Dummy(plan) => plan.deps(),
            BlockingOp::InMemSort(sort) => sort.deps(),
            BlockingOp::InMemHashTableCreation(creation) => creation.deps(),
            BlockingOp::InMemHashAggregate(agg) => agg.deps(),
        }
    }

    pub fn print_inner(&self, indent: usize, out: &mut String) {
        match self {
            BlockingOp::Dummy(plan) => plan.print_inner(indent, out),
            BlockingOp::InMemSort(sort) => sort.print_inner(indent, out),
            BlockingOp::InMemHashTableCreation(creation) => creation.print_inner(indent, out),
            BlockingOp::InMemHashAggregate(agg) => agg.print_inner(indent, out),
        }
    }

    pub fn execute(
        &mut self,
        context: &HashMap<PipelineID, Arc<InMemBuffer<T>>>,
    ) -> Result<Arc<InMemBuffer<T>>, ExecError> {
        match self {
            BlockingOp::Dummy(plan) => {
                log_debug!("Dummy blocking op");
                let output = Arc::new(InMemBuffer::vec());
                while let Some(tuple) = plan.next(context)? {
                    output.append(tuple)?;
                }
                Ok(output)
            }
            BlockingOp::InMemSort(sort) => sort.execute(context),
            BlockingOp::InMemHashTableCreation(creation) => creation.execute(context),
            BlockingOp::InMemHashAggregate(agg) => agg.execute(context),
        }
    }
}

impl<T: TxnStorageTrait> From<NonBlockingOp<T>> for BlockingOp<T> {
    fn from(plan: NonBlockingOp<T>) -> Self {
        BlockingOp::Dummy(plan)
    }
}

pub struct InMemSort<T: TxnStorageTrait> {
    schema: SchemaRef, // Output schema
    exec_plan: NonBlockingOp<T>,
    sort_cols: Vec<(ColumnId, bool, bool)>, // ColumnId, ascending, nulls_first
}

impl<T: TxnStorageTrait> InMemSort<T> {
    pub fn new(
        schema: SchemaRef, // Output schema
        exec_plan: NonBlockingOp<T>,
        sort_cols: Vec<(ColumnId, bool, bool)>,
    ) -> Self {
        Self {
            schema,
            exec_plan,
            sort_cols,
        }
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn deps(&self) -> HashSet<PipelineID> {
        self.exec_plan.deps()
    }

    pub fn print_inner(&self, indent: usize, out: &mut String) {
        out.push_str(&format!("{}->sort(", " ".repeat(indent)));
        let mut split = "";
        out.push('[');
        for (col_id, asc, nulls_first) in &self.sort_cols {
            out.push_str(split);
            out.push_str(&format!(
                "{} {}{}",
                col_id,
                if *asc { "asc" } else { "desc" },
                if *nulls_first { " nulls first" } else { "" }
            ));
            split = ", ";
        }
        out.push_str("])\n");
        self.exec_plan.print_inner(indent + 2, out);
    }

    pub fn execute(
        &mut self,
        context: &HashMap<PipelineID, Arc<InMemBuffer<T>>>,
    ) -> Result<Arc<InMemBuffer<T>>, ExecError> {
        log_debug!("InMemSort::execute");
        let output = Arc::new(InMemBuffer::vec());
        let mut tuples = Vec::new();
        while let Some(tuple) = self.exec_plan.next(context)? {
            tuples.push(tuple);
        }
        tuples.sort_by(|a, b| {
            a.to_normalized_key_bytes(&self.sort_cols)
                .cmp(&b.to_normalized_key_bytes(&self.sort_cols))
        });
        for tuple in tuples {
            output.append(tuple)?;
        }
        Ok(output)
    }
}

pub struct InMemHashTableCreation<T: TxnStorageTrait> {
    schema: SchemaRef, // Output schema
    exec_plan: NonBlockingOp<T>,
    exprs: Vec<ByteCodeExpr>, // Hash key expressions
}

impl<T: TxnStorageTrait> InMemHashTableCreation<T> {
    pub fn new(
        schema: SchemaRef, // Output schema
        exec_plan: NonBlockingOp<T>,
        exprs: Vec<ByteCodeExpr>,
    ) -> Self {
        Self {
            schema,
            exec_plan,
            exprs,
        }
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn deps(&self) -> HashSet<PipelineID> {
        self.exec_plan.deps()
    }

    pub fn print_inner(&self, indent: usize, out: &mut String) {
        out.push_str(&format!("{}->hash_table(", " ".repeat(indent)));
        let mut split = "";
        out.push('[');
        for expr in &self.exprs {
            out.push_str(split);
            out.push_str(&format!("{}", expr));
            split = ", ";
        }
        out.push_str("])\n");
        self.exec_plan.print_inner(indent + 2, out);
    }

    pub fn execute(
        &mut self,
        context: &HashMap<PipelineID, Arc<InMemBuffer<T>>>,
    ) -> Result<Arc<InMemBuffer<T>>, ExecError> {
        log_debug!("InMemHashTableCreation::execute");
        let output = Arc::new(InMemBuffer::hash_table(self.exprs.clone()));
        while let Some(tuple) = self.exec_plan.next(context)? {
            output.append(tuple)?;
        }
        Ok(output)
    }
}

pub struct InMemHashAggregation<T: TxnStorageTrait> {
    schema: SchemaRef, // Output schema
    exec_plan: NonBlockingOp<T>,
    group_by: Vec<ColumnId>,
    agg_op: Vec<(AggOp, ColumnId)>,
}

impl<T: TxnStorageTrait> InMemHashAggregation<T> {
    pub fn new(
        schema: SchemaRef, // Output schema
        exec_plan: NonBlockingOp<T>,
        group_by: Vec<ColumnId>,
        agg_op: Vec<(AggOp, ColumnId)>,
    ) -> Self {
        Self {
            schema,
            exec_plan,
            group_by,
            agg_op,
        }
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn deps(&self) -> HashSet<PipelineID> {
        self.exec_plan.deps()
    }

    pub fn print_inner(&self, indent: usize, out: &mut String) {
        out.push_str(&format!("{}->hash_aggregate(", " ".repeat(indent)));
        let mut split = "";
        out.push('[');
        for col_id in &self.group_by {
            out.push_str(split);
            out.push_str(&format!("{}", col_id));
            split = ", ";
        }
        out.push_str("], [");
        split = "";
        for (agg_op, col_id) in &self.agg_op {
            out.push_str(split);
            out.push_str(&format!("{:?}({})", agg_op, col_id));
            split = ", ";
        }
        out.push_str("])\n");
        self.exec_plan.print_inner(indent + 2, out);
    }

    pub fn execute(
        &mut self,
        context: &HashMap<PipelineID, Arc<InMemBuffer<T>>>,
    ) -> Result<Arc<InMemBuffer<T>>, ExecError> {
        log_debug!("InMemHashAggregation::execute");
        let output = Arc::new(InMemBuffer::hash_aggregate_table(
            self.group_by.clone(),
            self.agg_op.clone(),
        ));
        while let Some(tuple) = self.exec_plan.next(context)? {
            output.append(tuple)?;
        }
        Ok(output)
    }
}

pub type PipelineID = u16;

pub struct Pipeline<T: TxnStorageTrait> {
    id: PipelineID,
    context: HashMap<PipelineID, Arc<InMemBuffer<T>>>,
    exec_plan: BlockingOp<T>,
    deps_cache: HashSet<PipelineID>,
}

impl<T: TxnStorageTrait> Pipeline<T> {
    pub fn new(id: PipelineID, execution_plan: BlockingOp<T>) -> Self {
        Self {
            id,
            context: HashMap::new(),
            deps_cache: execution_plan.deps(),
            exec_plan: execution_plan,
        }
    }

    pub fn new_with_context(
        id: PipelineID,
        execution_plan: BlockingOp<T>,
        context: HashMap<PipelineID, Arc<InMemBuffer<T>>>,
    ) -> Self {
        Self {
            id,
            context,
            deps_cache: execution_plan.deps(),
            exec_plan: execution_plan,
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
    pub fn set_context(&mut self, id: PipelineID, buffer: Arc<InMemBuffer<T>>) {
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

    pub fn execute(&mut self) -> Result<Arc<InMemBuffer<T>>, ExecError> {
        self.exec_plan.execute(&self.context)
    }
}

pub struct InMemPipelineGraph<T: TxnStorageTrait> {
    pipelines: Vec<Pipeline<T>>,
    queue: VecDeque<Pipeline<T>>,
}

impl<T: TxnStorageTrait> InMemPipelineGraph<T> {
    pub fn new(catalog: &CatalogRef, storage: &Arc<T>, physical_plan: PhysicalRelExpr) -> Self {
        let converter = PhysicalRelExprToPipelineQueue::new(storage);

        converter
            .convert(catalog, physical_plan)
            .expect("Failed to convert physical plan")
    }

    fn empty() -> Self {
        Self {
            pipelines: Vec::new(),
            queue: VecDeque::new(),
        }
    }

    fn add_pipeline(&mut self, pipeline: Pipeline<T>) {
        self.pipelines.push(pipeline);
    }

    // Identify the pipelines that do not have any dependencies and push that to the queue
    // This pipeline's dependencies can be removed from the other pipelines since it is empty
    fn push_no_deps_to_queue(&mut self) {
        // Push the pipelines that do not have any dependencies to the queue
        // drain_filter is not stable yet so use retain instead
        let mut with_deps: Vec<Pipeline<T>> = Vec::new();
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
    fn notify_dependants(&mut self, id: PipelineID, output: Arc<InMemBuffer<T>>) {
        for p in self.pipelines.iter_mut() {
            if p.deps().contains(&id) {
                // p is a dependant of the completed pipeline
                p.set_context(id, output.clone())
            }
        }
    }
}

impl<T: TxnStorageTrait> Executor<T> for InMemPipelineGraph<T> {
    type Buffer = InMemBuffer<T>;

    fn to_pretty_string(&self) -> String {
        let mut result = String::new();
        for pipeline in self.pipelines.iter() {
            let id = pipeline.get_id();
            result.push_str(&format!("Pipeline ID: {}\n", id));
            pipeline.print_inner(2, &mut result);
        }
        result
    }

    fn execute(mut self, _txn: &T::TxnHandle) -> Result<Arc<InMemBuffer<T>>, ExecError> {
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

    //xtx temp
    fn quantile_generation_execute(
        mut self,
        _txn: &T::TxnHandle,
        data_source: &str,
        query_id: u8,
        methods: &[QuantileMethod],
        num_quantiles_per_run: usize,
        estimated_store_json: &str,
        actual_store_json: &str,
        evaluation_json: &str,
    ) -> Result<Arc<InMemBuffer<T>>, ExecError> {
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

pub struct PhysicalRelExprToPipelineQueue<T: TxnStorageTrait> {
    pub current_id: PipelineID, // Incremental pipeline ID
    pub storage: Arc<T>,
    pub pipeline_queue: InMemPipelineGraph<T>,
}

type ColIdToIdx = BTreeMap<ColumnId, usize>;

impl<T: TxnStorageTrait> PhysicalRelExprToPipelineQueue<T> {
    pub fn new(storage: &Arc<T>) -> Self {
        Self {
            current_id: 0,
            storage: storage.clone(),
            pipeline_queue: InMemPipelineGraph::empty(),
        }
    }

    fn fetch_add_id(&mut self) -> PipelineID {
        let id = self.current_id;
        self.current_id += 1;
        id
    }

    pub fn convert(
        mut self,
        catalog: &CatalogRef,
        expr: PhysicalRelExpr,
    ) -> Result<InMemPipelineGraph<T>, ExecError> {
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
        let blocking_op = BlockingOp::Dummy(op);
        let pipeline = Pipeline::new_with_context(self.fetch_add_id(), blocking_op, context);
        self.pipeline_queue.add_pipeline(pipeline);
        Ok(self.pipeline_queue)
    }

    fn convert_inner(
        &mut self,
        catalog: &CatalogRef,
        expr: PhysicalRelExpr,
    ) -> Result<
        (
            NonBlockingOp<T>,
            HashMap<PipelineID, Arc<InMemBuffer<T>>>,
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
                let scan_buffer = Arc::new(InMemBuffer::txn_storage(
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
                    self.convert_inner(catalog, *left)?;
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
                    self.convert_inner(catalog, *left)?;
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
                let left_hash_table_creation = BlockingOp::InMemHashTableCreation(
                    InMemHashTableCreation::new(left_schema.clone(), left_op, left_exprs),
                );
                let left_p =
                    Pipeline::new_with_context(left_id, left_hash_table_creation, left_context);
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
                let hash_agg = BlockingOp::InMemHashAggregate(InMemHashAggregation::new(
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
                let p = Pipeline::new_with_context(agg_id, hash_agg, context);
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
                let sort =
                    BlockingOp::InMemSort(InMemSort::new(schema.clone(), input_op, sort_cols));

                let sort_id = self.fetch_add_id();
                let p = Pipeline::new_with_context(sort_id, sort, context);
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
                panic!("PhysicalRelExpr {:?} is not supported", other)
            }
        }
    }
}

/*
#[cfg(test)]
mod tests {
    use fbtree::prelude::InMemStorage;

    use crate::{executor::bytecode_expr::colidx_expr, tuple};

    use super::*;

    fn check_result(
        actual: &mut Vec<Tuple>,
        expected: &mut Vec<Tuple>,
        sorted: bool,
        verbose: bool,
    ) {
        if sorted {
            let tuple_len = actual[0].fields().len();
            let sort_cols = (0..tuple_len)
                .map(|i| (i as ColumnId, true, false))
                .collect::<Vec<(ColumnId, bool, bool)>>(); // ColumnId, ascending, nulls_first
            actual.sort_by(|a, b| {
                a.to_normalized_key_bytes(&sort_cols)
                    .cmp(&b.to_normalized_key_bytes(&sort_cols))
            });
            expected.sort_by(|a, b| {
                a.to_normalized_key_bytes(&sort_cols)
                    .cmp(&b.to_normalized_key_bytes(&sort_cols))
            });
        }
        let actual_string = actual
            .iter()
            .map(|t| t.to_pretty_string())
            .collect::<Vec<_>>()
            .join("\n");
        let expected_string = expected
            .iter()
            .map(|t: &Tuple| t.to_pretty_string())
            .collect::<Vec<_>>()
            .join("\n");
        if verbose {
            println!("--- Result ---\n{}", actual_string);
            println!("--- Expected ---\n{}", expected_string);
        }
        assert_eq!(
            actual, expected,
            "\n--- Result ---\n{}\n--- Expected ---\n{}\n",
            actual_string, expected_string
        );
    }

    fn vec_buffer() -> Arc<TupleBuffer<InMemStorage>> {
        Arc::new(TupleBuffer::vec())
    }

    fn hash_table_buffer(exprs: Vec<ByteCodeExpr>) -> Arc<TupleBuffer<InMemStorage>> {
        Arc::new(TupleBuffer::hash_table(exprs))
    }

    #[test]
    fn test_pipeline_scan() {
        let input = vec_buffer();
        let tuple = Tuple::from_fields(vec![1.into(), 2.into(), 3.into()]);
        input.append(tuple.copy()).unwrap();

        let mut pipeline = Pipeline::new(1, NonBlockingOp::Scan(PScanIter::new(0)).into());
        pipeline.set_context(0, input);

        let result = pipeline.execute().unwrap();
        let iter = result.iter_all();
        let mut actual = Vec::new();
        while let Some(tuple) = iter.next() {
            actual.push(tuple);
        }

        let mut expected = vec![tuple];
        check_result(&mut actual, &mut expected, false, true);
    }

    #[test]
    fn test_pipeline_merge_scan() {
        // Prepare two sorted inputs.
        let input1 = vec_buffer();
        let mut expected1 = vec![
            Tuple::from_fields(vec![1.into(), "a".into()]),
            Tuple::from_fields(vec![3.into(), "b".into()]),
            Tuple::from_fields(vec![5.into(), "c".into()]),
        ];
        for tuple in expected1.iter() {
            input1.append(tuple.copy()).unwrap();
        }

        let input2 = vec_buffer();
        let mut expected2 = vec![
            Tuple::from_fields(vec![2.into(), "d".into()]),
            Tuple::from_fields(vec![4.into(), "e".into()]),
            Tuple::from_fields(vec![6.into(), "f".into()]),
        ];
        for tuple in expected2.iter() {
            input2.append(tuple.copy()).unwrap();
        }

        // Merge the two inputs.
        let mut pipeline = Pipeline::new(1, NonBlockingOp::Scan(PScanIter::new(0)).into());

        let runs = Arc::new(TupleBuffer::runs(vec![
            (input1, vec![(0, true, false)]),
            (input2, vec![(0, true, false)]),
        ]));

        pipeline.set_context(0, runs);

        let result = pipeline.execute().unwrap();
        let iter = result.iter_all();
        let mut actual = Vec::new();
        while let Some(tuple) = iter.next() {
            actual.push(tuple);
        }

        expected1.append(&mut expected2);
        // expected1.sort();

        check_result(&mut actual, &mut expected1, true, true);
    }

    #[test]
    fn test_pipeline_merge_scan_with_3_inputs() {
        // Prepare three sorted inputs.
        let input1 = vec_buffer();
        let mut expected1 = vec![
            Tuple::from_fields(vec![1.into(), "a".into()]),
            Tuple::from_fields(vec![4.into(), "d".into()]),
            Tuple::from_fields(vec![7.into(), "g".into()]),
        ];
        for tuple in expected1.iter() {
            input1.append(tuple.copy()).unwrap();
        }

        let input2 = vec_buffer();
        let mut expected2 = vec![
            Tuple::from_fields(vec![2.into(), "b".into()]),
            Tuple::from_fields(vec![5.into(), "e".into()]),
            Tuple::from_fields(vec![8.into(), "h".into()]),
        ];
        for tuple in expected2.iter() {
            input2.append(tuple.copy()).unwrap();
        }

        let input3 = vec_buffer();
        let mut expected3 = vec![
            Tuple::from_fields(vec![3.into(), "c".into()]),
            Tuple::from_fields(vec![6.into(), "f".into()]),
            Tuple::from_fields(vec![9.into(), "i".into()]),
        ];
        for tuple in expected3.iter() {
            input3.append(tuple.copy()).unwrap();
        }

        // Merge the three inputs.
        let mut pipeline = Pipeline::new(1, NonBlockingOp::Scan(PScanIter::new(0)).into());

        let runs = Arc::new(TupleBuffer::runs(vec![
            (input1, vec![(0, true, false)]),
            (input2, vec![(0, true, false)]),
            (input3, vec![(0, true, false)]),
        ]));

        pipeline.set_context(0, runs);

        let result = pipeline.execute().unwrap();
        let iter = result.iter_all();
        let mut actual = Vec::new();
        while let Some(tuple) = iter.next() {
            actual.push(tuple);
        }

        expected1.append(&mut expected2);
        expected1.append(&mut expected3);

        check_result(&mut actual, &mut expected1, true, true);
    }

    #[test]
    fn test_hash_table_iter_key() {
        let exprs = vec![colidx_expr(0)];
        let input = hash_table_buffer(exprs.clone());
        let tuple1 = Tuple::from_fields(vec!["a".into(), 1.into()]);
        let tuple2 = Tuple::from_fields(vec!["b".into(), 2.into()]);
        let tuple3 = Tuple::from_fields(vec!["a".into(), 3.into()]);
        let tuple4 = Tuple::from_fields(vec!["b".into(), 4.into()]);
        input.append(tuple1.copy()).unwrap();
        input.append(tuple2.copy()).unwrap();
        input.append(tuple3.copy()).unwrap();
        input.append(tuple4.copy()).unwrap();

        let iter_key = input.iter_key(vec!["a".into()]).unwrap();
        let mut actual = Vec::new();
        while let Some(tuple) = iter_key.next() {
            actual.push(tuple);
        }
        let mut expected = vec![tuple1.copy(), tuple3.copy()];
        check_result(&mut actual, &mut expected, true, true);

        let iter_key = input.iter_key(vec!["b".into()]).unwrap();
        let mut actual = Vec::new();
        while let Some(tuple) = iter_key.next() {
            actual.push(tuple);
        }
        let mut expected = vec![tuple2.copy(), tuple4.copy()];
        check_result(&mut actual, &mut expected, true, true);

        assert!(input.iter_key(vec!["c".into()]).is_none());

        let iter_all = input.iter_all();
        let mut actual = Vec::new();
        while let Some(tuple) = iter_all.next() {
            actual.push(tuple);
        }
        let mut expected = vec![tuple1, tuple2, tuple3, tuple4];
        check_result(&mut actual, &mut expected, true, true);
    }

    #[test]
    fn test_pipeline_hash_table_iter_all() {
        let exprs = vec![colidx_expr(0)];
        let input = hash_table_buffer(exprs.clone());
        let tuple1 = Tuple::from_fields(vec![1.into(), 2.into()]);
        let tuple2 = Tuple::from_fields(vec![3.into(), 4.into()]);
        let tuple3 = Tuple::from_fields(vec![1.into(), 2.into()]);
        let tuple4 = Tuple::from_fields(vec![3.into(), 4.into()]);
        input.append(tuple1.copy()).unwrap();
        input.append(tuple2.copy()).unwrap();
        input.append(tuple3.copy()).unwrap();
        input.append(tuple4.copy()).unwrap();

        let mut pipeline = Pipeline::new(1, NonBlockingOp::Scan(PScanIter::new(0)).into());
        pipeline.set_context(0, input);

        let result = pipeline.execute().unwrap();
        let iter = result.iter_all();
        let mut actual = Vec::new();
        while let Some(tuple) = iter.next() {
            actual.push(tuple);
        }
        let mut expected = vec![tuple1, tuple3, tuple2, tuple4];
        check_result(&mut actual, &mut expected, true, true);
    }

    #[test]
    fn test_pipeline_hashjoin_inner() {
        let exprs = vec![colidx_expr(0)];
        // Build input
        let input1 = Arc::new(TupleBuffer::hash_table(exprs));
        let tuple1 = Tuple::from_fields(vec![1.into(), "a".into()]);
        let tuple2 = Tuple::from_fields(vec![3.into(), "b".into()]);
        let tuple3 = Tuple::from_fields(vec![1.into(), "c".into()]);
        let tuple4 = Tuple::from_fields(vec![3.into(), "d".into()]);
        input1.append(tuple1.copy()).unwrap();
        input1.append(tuple2.copy()).unwrap();
        input1.append(tuple3.copy()).unwrap();
        input1.append(tuple4.copy()).unwrap();

        // Probe input
        let input0 = vec_buffer();
        let tuple5 = Tuple::from_fields(vec![1.into(), 2.into(), 3.into()]);
        let tuple6 = Tuple::from_fields(vec![3.into(), 4.into(), 5.into()]);
        let tuple7 = Tuple::from_fields(vec![1.into(), 2.into(), 3.into()]);
        let tuple8 = Tuple::from_fields(vec![3.into(), 4.into(), 5.into()]);
        input0.append(tuple5.copy()).unwrap();
        input0.append(tuple6.copy()).unwrap();
        input0.append(tuple7.copy()).unwrap();
        input0.append(tuple8.copy()).unwrap();

        let mut pipeline = Pipeline::new(
            2,
            NonBlockingOp::HashJoin(PHashJoinIter::Inner(PHashJoinInnerIter {
                probe_side: Box::new(NonBlockingOp::Scan(PScanIter::new(0))),
                build_side: 1,
                exprs: vec![colidx_expr(0)],
                current: None,
            }))
            .into(),
        );
        pipeline.set_context(0, input0); // Probe input
        pipeline.set_context(1, input1); // Build input

        let result = pipeline.execute().unwrap();
        let iter = result.iter_all();
        let mut actual = Vec::new();
        while let Some(tuple) = iter.next() {
            actual.push(tuple);
        }
        let mut expected = vec![
            Tuple::from_fields(vec![1.into(), "a".into(), 1.into(), 2.into(), 3.into()]),
            Tuple::from_fields(vec![1.into(), "a".into(), 1.into(), 2.into(), 3.into()]),
            Tuple::from_fields(vec![3.into(), "b".into(), 3.into(), 4.into(), 5.into()]),
            Tuple::from_fields(vec![3.into(), "b".into(), 3.into(), 4.into(), 5.into()]),
            Tuple::from_fields(vec![1.into(), "c".into(), 1.into(), 2.into(), 3.into()]),
            Tuple::from_fields(vec![1.into(), "c".into(), 1.into(), 2.into(), 3.into()]),
            Tuple::from_fields(vec![3.into(), "d".into(), 3.into(), 4.into(), 5.into()]),
            Tuple::from_fields(vec![3.into(), "d".into(), 3.into(), 4.into(), 5.into()]),
        ];
        check_result(&mut actual, &mut expected, true, true);
    }

    #[test]
    fn test_pipeline_hashjoin_outer() {
        let exprs = vec![colidx_expr(0)];
        // Build input
        let input1 = Arc::new(TupleBuffer::hash_table(exprs));
        let tuple1 = Tuple::from_fields(vec![1.into(), "a".into()]);
        let tuple2 = Tuple::from_fields(vec![2.into(), "b".into()]);
        input1.append(tuple1.copy()).unwrap();
        input1.append(tuple2.copy()).unwrap();

        // Probe input
        let input0 = vec_buffer();
        let tuple5 = Tuple::from_fields(vec![1.into(), 2.into(), 3.into()]);
        let tuple6 = Tuple::from_fields(vec![3.into(), 4.into(), 5.into()]);
        let tuple7 = Tuple::from_fields(vec![1.into(), 2.into(), 3.into()]);
        let tuple8 = Tuple::from_fields(vec![3.into(), 4.into(), 5.into()]);
        input0.append(tuple5.copy()).unwrap();
        input0.append(tuple6.copy()).unwrap();
        input0.append(tuple7.copy()).unwrap();
        input0.append(tuple8.copy()).unwrap();

        let mut pipeline = Pipeline::new(
            2,
            NonBlockingOp::HashJoin(PHashJoinIter::RightOuter(PHashJoinRightOuterIter {
                probe_side: Box::new(NonBlockingOp::Scan(PScanIter::new(0))),
                build_side: 1,
                exprs: vec![colidx_expr(0)],
                current: None,
                nulls: Tuple::from_fields(vec![Field::Int(None), Field::String(None)]),
            }))
            .into(),
        );
        pipeline.set_context(0, input0); // Probe input
        pipeline.set_context(1, input1); // Build input

        let result = pipeline.execute().unwrap();
        let iter = result.iter_all();
        let mut actual = Vec::new();
        while let Some(tuple) = iter.next() {
            actual.push(tuple);
        }

        let mut expected = vec![
            Tuple::from_fields(vec![1.into(), "a".into(), 1.into(), 2.into(), 3.into()]),
            Tuple::from_fields(vec![1.into(), "a".into(), 1.into(), 2.into(), 3.into()]),
            Tuple::from_fields(vec![
                Field::Int(None),
                Field::String(None),
                3.into(),
                4.into(),
                5.into(),
            ]),
            Tuple::from_fields(vec![
                Field::Int(None),
                Field::String(None),
                3.into(),
                4.into(),
                5.into(),
            ]),
        ];

        check_result(&mut actual, &mut expected, true, true);
    }

    #[test]
    fn test_pipeline_hashjoin_semi() {
        let exprs = vec![colidx_expr(0)];
        // Build input
        let input1 = Arc::new(TupleBuffer::hash_table(exprs));
        let tuple1 = Tuple::from_fields(vec![1.into(), "a".into()]);
        let tuple2 = Tuple::from_fields(vec![2.into(), "b".into()]);
        input1.append(tuple1.copy()).unwrap();
        input1.append(tuple2.copy()).unwrap();

        // Probe input
        let input0 = vec_buffer();
        let tuple5 = Tuple::from_fields(vec![1.into(), 2.into(), 3.into()]);
        let tuple6 = Tuple::from_fields(vec![3.into(), 4.into(), 5.into()]);
        let tuple7 = Tuple::from_fields(vec![1.into(), 2.into(), 3.into()]);
        let tuple8 = Tuple::from_fields(vec![3.into(), 4.into(), 5.into()]);
        input0.append(tuple5.copy()).unwrap();
        input0.append(tuple6.copy()).unwrap();
        input0.append(tuple7.copy()).unwrap();
        input0.append(tuple8.copy()).unwrap();

        let mut pipeline = Pipeline::new(
            2,
            NonBlockingOp::HashJoin(PHashJoinIter::RightSemi(PHashJoinRightSemiIter {
                probe_side: Box::new(NonBlockingOp::Scan(PScanIter::new(0))),
                build_side: 1,
                exprs: vec![colidx_expr(0)],
            }))
            .into(),
        );
        pipeline.set_context(0, input0); // Probe input
        pipeline.set_context(1, input1); // Build input

        let result = pipeline.execute().unwrap();
        let iter = result.iter_all();
        let mut actual = Vec::new();
        while let Some(tuple) = iter.next() {
            actual.push(tuple);
        }

        let mut expected = vec![
            Tuple::from_fields(vec![1.into(), 2.into(), 3.into()]),
            Tuple::from_fields(vec![1.into(), 2.into(), 3.into()]),
        ];

        check_result(&mut actual, &mut expected, true, true);
    }

    #[test]
    fn test_pipeline_hashjoin_anti() {
        let exprs = vec![colidx_expr(0)];
        // Build input
        let input1 = Arc::new(TupleBuffer::hash_table(exprs));
        let tuple1 = Tuple::from_fields(vec![1.into(), "a".into()]);
        let tuple2 = Tuple::from_fields(vec![2.into(), "b".into()]);
        input1.append(tuple1.copy()).unwrap();
        input1.append(tuple2.copy()).unwrap();

        // Probe input
        let input0 = vec_buffer();
        let tuple5 = Tuple::from_fields(vec![1.into(), 2.into(), 3.into()]);
        let tuple6 = Tuple::from_fields(vec![3.into(), 4.into(), 5.into()]);
        let tuple7 = Tuple::from_fields(vec![1.into(), 2.into(), 3.into()]);
        let tuple8 = Tuple::from_fields(vec![3.into(), 4.into(), 5.into()]);
        input0.append(tuple5.copy()).unwrap();
        input0.append(tuple6.copy()).unwrap();
        input0.append(tuple7.copy()).unwrap();
        input0.append(tuple8.copy()).unwrap();

        let mut pipeline = Pipeline::new(
            2,
            NonBlockingOp::HashJoin(PHashJoinIter::RightAnti(PHashJoinRightAntiIter {
                probe_side: Box::new(NonBlockingOp::Scan(PScanIter::new(0))),
                build_side: 1,
                exprs: vec![colidx_expr(0)],
            }))
            .into(),
        );
        pipeline.set_context(0, input0); // Probe input
        pipeline.set_context(1, input1); // Build input

        let result = pipeline.execute().unwrap();
        let iter = result.iter_all();
        let mut actual = Vec::new();
        while let Some(tuple) = iter.next() {
            actual.push(tuple);
        }

        let mut expected = vec![
            Tuple::from_fields(vec![3.into(), 4.into(), 5.into()]),
            Tuple::from_fields(vec![3.into(), 4.into(), 5.into()]),
        ];

        check_result(&mut actual, &mut expected, true, true);
    }

    #[test]
    fn test_pipeline_hashjoin_mark_without_nulls() {
        // Mark join is similar to semi/anti join.
        // It returns all the probe side tuples with an additional mark at the end of the tuple.
        // If there is a matching tuple, the mark is true.
        // If there is no matching tuple, if the build side has nulls, the mark is null.
        // Otherwise, the mark is false.

        let exprs = vec![colidx_expr(0)];
        // Build input
        let input1 = Arc::new(TupleBuffer::hash_table(exprs));
        let tuple1 = Tuple::from_fields(vec![1.into(), "a".into()]);
        let tuple2 = Tuple::from_fields(vec![2.into(), "b".into()]);
        input1.append(tuple1.copy()).unwrap();
        input1.append(tuple2.copy()).unwrap();

        // Probe input
        let input0 = vec_buffer();
        let tuple5 = Tuple::from_fields(vec![1.into(), 2.into(), 3.into()]);
        let tuple6 = Tuple::from_fields(vec![3.into(), 4.into(), 5.into()]);
        let tuple7 = Tuple::from_fields(vec![1.into(), 2.into(), 3.into()]);
        let tuple8 = Tuple::from_fields(vec![3.into(), 4.into(), 5.into()]);
        input0.append(tuple5.copy()).unwrap();
        input0.append(tuple6.copy()).unwrap();
        input0.append(tuple7.copy()).unwrap();
        input0.append(tuple8.copy()).unwrap();

        let mut pipeline = Pipeline::new(
            2,
            NonBlockingOp::HashJoin(PHashJoinIter::RightMark(PHashJoinRightMarkIter {
                probe_side: Box::new(NonBlockingOp::Scan(PScanIter::new(0))),
                build_side: 1,
                exprs: vec![colidx_expr(0)],
            }))
            .into(),
        );
        pipeline.set_context(0, input0); // Probe input
        pipeline.set_context(1, input1); // Build input

        let result = pipeline.execute().unwrap();
        let iter = result.iter_all();
        let mut actual = Vec::new();
        while let Some(tuple) = iter.next() {
            actual.push(tuple);
        }

        let mut expected = vec![
            Tuple::from_fields(vec![1.into(), 2.into(), 3.into(), Field::from_bool(true)]),
            Tuple::from_fields(vec![3.into(), 4.into(), 5.into(), Field::from_bool(false)]),
            Tuple::from_fields(vec![1.into(), 2.into(), 3.into(), Field::from_bool(true)]),
            Tuple::from_fields(vec![3.into(), 4.into(), 5.into(), Field::from_bool(false)]),
        ];

        check_result(&mut actual, &mut expected, true, true);
    }

    #[test]
    fn test_pipeline_hashjoin_mark_with_nulls() {
        // Mark join is similar to semi/anti join.
        // It returns all the probe side tuples with an additional mark at the end of the tuple.
        // If there is a matching tuple, the mark is true.
        // If there is no matching tuple, if the build side has nulls, the mark is null.
        // Otherwise, the mark is false.

        let exprs = vec![colidx_expr(0)];
        // Build input
        let input1 = Arc::new(TupleBuffer::hash_table(exprs));
        let tuple1 = Tuple::from_fields(vec![1.into(), "a".into()]);
        let tuple2 = Tuple::from_fields(vec![2.into(), "b".into()]);
        let tuple3 = Tuple::from_fields(vec![Field::Int(None), "c".into()]);
        input1.append(tuple1.copy()).unwrap();
        input1.append(tuple2.copy()).unwrap();
        input1.append(tuple3.copy()).unwrap();

        // Probe input
        let input0 = vec_buffer();
        let tuple5 = Tuple::from_fields(vec![1.into(), 2.into(), 3.into()]);
        let tuple6 = Tuple::from_fields(vec![3.into(), 4.into(), 5.into()]);
        let tuple7 = Tuple::from_fields(vec![1.into(), 2.into(), 3.into()]);
        let tuple8 = Tuple::from_fields(vec![3.into(), 4.into(), 5.into()]);
        input0.append(tuple5.copy()).unwrap();
        input0.append(tuple6.copy()).unwrap();
        input0.append(tuple7.copy()).unwrap();
        input0.append(tuple8.copy()).unwrap();

        let mut pipeline = Pipeline::new(
            2,
            NonBlockingOp::HashJoin(PHashJoinIter::RightMark(PHashJoinRightMarkIter {
                probe_side: Box::new(NonBlockingOp::Scan(PScanIter::new(0))),
                build_side: 1,
                exprs: vec![colidx_expr(0)],
            }))
            .into(),
        );
        pipeline.set_context(0, input0); // Probe input
        pipeline.set_context(1, input1); // Build input

        let result = pipeline.execute().unwrap();
        let iter = result.iter_all();
        let mut actual = Vec::new();
        while let Some(tuple) = iter.next() {
            actual.push(tuple);
        }

        let mut expected = vec![
            Tuple::from_fields(vec![1.into(), 2.into(), 3.into(), Field::from_bool(true)]),
            Tuple::from_fields(vec![3.into(), 4.into(), 5.into(), Field::Boolean(None)]),
            Tuple::from_fields(vec![1.into(), 2.into(), 3.into(), Field::from_bool(true)]),
            Tuple::from_fields(vec![3.into(), 4.into(), 5.into(), Field::Boolean(None)]),
        ];

        check_result(&mut actual, &mut expected, true, true);
    }

    #[test]
    fn test_pipeline_in_mem_sort() {
        let input = vec_buffer();
        let tuple1 = Tuple::from_fields(vec![1.into(), "a".into()]);
        let tuple2 = Tuple::from_fields(vec![3.into(), "b".into()]);
        let tuple3 = Tuple::from_fields(vec![2.into(), "c".into()]);
        input.append(tuple1.copy()).unwrap();
        input.append(tuple2.copy()).unwrap();
        input.append(tuple3.copy()).unwrap();

        let mut pipeline = Pipeline::new(
            1,
            BlockingOp::in_mem_sort(
                NonBlockingOp::Scan(PScanIter::new(0)),
                vec![(0, true, false)],
            ),
        );
        pipeline.set_context(0, input);

        let result = pipeline.execute().unwrap();
        let iter = result.iter_all();
        let mut actual = Vec::new();
        while let Some(tuple) = iter.next() {
            actual.push(tuple);
        }

        let mut expected = vec![tuple1, tuple3, tuple2];
        check_result(&mut actual, &mut expected, false, true);
    }

    #[test]
    fn test_pipeline_in_mem_hashtable_creation() {
        let input = vec_buffer();
        let tuple1 = Tuple::from_fields(vec![1.into(), "a".into()]);
        let tuple2 = Tuple::from_fields(vec![3.into(), "b".into()]);
        let tuple3 = Tuple::from_fields(vec![1.into(), "c".into()]);
        let tuple4 = Tuple::from_fields(vec![3.into(), "d".into()]);
        input.append(tuple1.copy()).unwrap();
        input.append(tuple2.copy()).unwrap();
        input.append(tuple3.copy()).unwrap();
        input.append(tuple4.copy()).unwrap();

        let mut pipeline = Pipeline::new(
            1,
            BlockingOp::in_mem_hash_table_creation(
                NonBlockingOp::Scan(PScanIter::new(0)),
                vec![colidx_expr(0)],
            ),
        );
        pipeline.set_context(0, input);

        let result = pipeline.execute().unwrap();

        let iter = result.iter_key(vec![1.into()]).unwrap();
        let mut actual = Vec::new();
        while let Some(tuple) = iter.next() {
            actual.push(tuple);
        }
        let mut expected = vec![tuple1.copy(), tuple3.copy()];
        check_result(&mut actual, &mut expected, true, true);

        let iter = result.iter_key(vec![3.into()]).unwrap();
        let mut actual = Vec::new();
        while let Some(tuple) = iter.next() {
            actual.push(tuple);
        }
        let mut expected = vec![tuple2.copy(), tuple4.copy()];
        check_result(&mut actual, &mut expected, true, true);

        let iter = result.iter_all();
        let mut actual = Vec::new();
        while let Some(tuple) = iter.next() {
            actual.push(tuple);
        }

        let mut expected = vec![tuple1, tuple2, tuple3, tuple4];
        check_result(&mut actual, &mut expected, true, true);
    }

    #[test]
    fn test_pipeline_in_mem_hash_aggregation() {
        let input = vec_buffer();
        let tuple1 = Tuple::from_fields(vec![1.into(), 10.into(), "a".into()]);
        let tuple2 = Tuple::from_fields(vec![1.into(), 10.into(), "b".into()]);
        let tuple3 = Tuple::from_fields(vec![2.into(), 20.into(), "a".into()]);
        let tuple4 = Tuple::from_fields(vec![2.into(), 20.into(), Field::String(None)]);
        input.append(tuple1.copy()).unwrap();
        input.append(tuple2.copy()).unwrap();
        input.append(tuple3.copy()).unwrap();
        input.append(tuple4.copy()).unwrap();

        let mut pipeline = Pipeline::new(
            1,
            BlockingOp::InMemHashAggregate(InMemHashAggregation {
                exec_plan: NonBlockingOp::Scan(PScanIter::new(0)),
                group_by: vec![0],
                agg_op: vec![(AggOp::Sum, 1), (AggOp::Count, 2)],
            }),
        );
        pipeline.set_context(0, input);

        let result = pipeline.execute().unwrap();
        let iter = result.iter_all();
        let mut actual = Vec::new();
        while let Some(tuple) = iter.next() {
            actual.push(tuple);
        }

        let mut expected = vec![
            Tuple::from_fields(vec![1.into(), 20.into(), 2.into()]),
            Tuple::from_fields(vec![2.into(), 40.into(), 1.into()]),
        ];
        check_result(&mut actual, &mut expected, true, true);
    }

    #[test]
    fn test_pipeline_in_mem_hash_aggregation_with_null_group() {
        let input = vec_buffer();
        let tuple1 = Tuple::from_fields(vec![1.into(), 10.into(), "a".into()]);
        let tuple2 = Tuple::from_fields(vec![1.into(), 10.into(), "b".into()]);
        let tuple3 = Tuple::from_fields(vec![Field::Int(None), 20.into(), "a".into()]);
        let tuple4 = Tuple::from_fields(vec![2.into(), 20.into(), Field::String(None)]);
        input.append(tuple1.copy()).unwrap();
        input.append(tuple2.copy()).unwrap();
        input.append(tuple3.copy()).unwrap();
        input.append(tuple4.copy()).unwrap();

        let mut pipeline = Pipeline::new(
            1,
            BlockingOp::InMemHashAggregate(InMemHashAggregation {
                exec_plan: NonBlockingOp::Scan(PScanIter::new(0)),
                group_by: vec![0],
                agg_op: vec![(AggOp::Sum, 1), (AggOp::Count, 2)],
            }),
        );
        pipeline.set_context(0, input);

        let result = pipeline.execute().unwrap();
        let iter = result.iter_all();
        let mut actual = Vec::new();
        while let Some(tuple) = iter.next() {
            actual.push(tuple);
        }

        let mut expected = vec![
            Tuple::from_fields(vec![1.into(), 20.into(), 2.into()]),
            Tuple::from_fields(vec![2.into(), 20.into(), 0.into()]),
        ];
        check_result(&mut actual, &mut expected, true, true);
    }

    #[test]
    fn test_pipeline_nested_loop_join() {
        let input1 = vec_buffer();
        let tuple1 = Tuple::from_fields(vec![1.into(), "a".into()]);
        let tuple2 = Tuple::from_fields(vec![2.into(), "b".into()]);
        input1.append(tuple1.copy()).unwrap();
        input1.append(tuple2.copy()).unwrap();

        let input2 = vec_buffer();
        let tuple3 = Tuple::from_fields(vec![1.into(), 10.into()]);
        let tuple4 = Tuple::from_fields(vec![2.into(), 20.into()]);
        input2.append(tuple3.copy()).unwrap();
        input2.append(tuple4.copy()).unwrap();

        let mut pipeline = Pipeline::new(
            2,
            NonBlockingOp::NestedLoopJoin(PNestedLoopJoinIter::new(
                Box::new(NonBlockingOp::Scan(PScanIter::new(0))),
                Box::new(NonBlockingOp::Scan(PScanIter::new(1))),
            ))
            .into(),
        );
        pipeline.set_context(0, input1);
        pipeline.set_context(1, input2);

        let result = pipeline.execute().unwrap();
        let iter = result.iter_all();
        let mut actual = Vec::new();
        while let Some(tuple) = iter.next() {
            actual.push(tuple);
        }

        let mut expected = vec![
            Tuple::from_fields(vec![1.into(), "a".into(), 1.into(), 10.into()]),
            Tuple::from_fields(vec![2.into(), "b".into(), 1.into(), 10.into()]),
            Tuple::from_fields(vec![1.into(), "a".into(), 2.into(), 20.into()]),
            Tuple::from_fields(vec![2.into(), "b".into(), 2.into(), 20.into()]),
        ];
        check_result(&mut actual, &mut expected, true, true);
    }

    #[test]
    fn test_dependent_pipelines() {
        // Prepare two pipelines.
        // One pipeline builds a hash table from the input.
        // Another pipeline runs a hash join with the hash table.
        //
        //
        //          Hash Join
        //       /          \
        //  Hash Table        |
        //      |             |
        //   input0         input1

        let input0 = vec_buffer();
        let tuple1 = Tuple::from_fields(vec![1.into(), "a".into()]);
        let tuple2 = Tuple::from_fields(vec![1.into(), "b".into()]);
        let tuple3 = Tuple::from_fields(vec![2.into(), "c".into()]);
        let tuple4 = Tuple::from_fields(vec![2.into(), "d".into()]);
        input0.append(tuple1.copy()).unwrap();
        input0.append(tuple2.copy()).unwrap();
        input0.append(tuple3.copy()).unwrap();
        input0.append(tuple4.copy()).unwrap();

        let input1 = vec_buffer();
        let tuple5 = Tuple::from_fields(vec![1.into(), 10.into()]);
        let tuple6 = Tuple::from_fields(vec![2.into(), 20.into()]);
        input1.append(tuple5.copy()).unwrap();
        input1.append(tuple6.copy()).unwrap();

        let mut p2 = Pipeline::new(
            2,
            // Build hash table from input0.
            BlockingOp::in_mem_hash_table_creation(
                NonBlockingOp::Scan(PScanIter::new(0)),
                vec![colidx_expr(0)],
            ),
        );
        p2.set_context(0, input0);

        let mut p3 = Pipeline::new(
            3,
            // Hash join with the hash table.
            NonBlockingOp::HashJoin(PHashJoinIter::Inner(PHashJoinInnerIter {
                probe_side: Box::new(NonBlockingOp::Scan(PScanIter::new(1))),
                build_side: 2,
                exprs: vec![colidx_expr(0)],
                current: None,
            }))
            .into(),
        );
        p3.set_context(1, input1);

        let mut queue = PipelineQueue::new();
        queue.add_pipeline(p2);
        queue.add_pipeline(p3);
        queue.add_dependencies(3, [2]);

        let result = queue.execute().unwrap();
        let iter = result.iter_all();
        let mut actual = Vec::new();
        while let Some(tuple) = iter.next() {
            actual.push(tuple);
        }

        let mut expected = vec![
            Tuple::from_fields(vec![1.into(), "a".into(), 1.into(), 10.into()]),
            Tuple::from_fields(vec![1.into(), "b".into(), 1.into(), 10.into()]),
            Tuple::from_fields(vec![2.into(), "c".into(), 2.into(), 20.into()]),
            Tuple::from_fields(vec![2.into(), "d".into(), 2.into(), 20.into()]),
        ];

        check_result(&mut actual, &mut expected, true, true);
    }

    #[test]
    fn test_dependent_pipelines_with_two_hashtables() {
        // Prepare three pipelines.
        // Two pipelines build hash tables from the input.
        // Another pipeline runs a hash join with the hash tables.
        //
        //
        //         Hash Join
        //           /   \
        //         /      \
        //       /     Hash Join
        //  Hash Table1   /  \
        //      |        /    \
        //   input0     /    input2
        //             /
        //        Hash Table2
        //            |
        //         input1

        let input0 = vec_buffer();
        let tuple1 = Tuple::from_fields(vec![1.into(), "aa".into()]);
        let tuple2 = Tuple::from_fields(vec![1.into(), "bb".into()]);
        let tuple3 = Tuple::from_fields(vec![2.into(), "cc".into()]);
        let tuple4 = Tuple::from_fields(vec![2.into(), "dd".into()]);
        input0.append(tuple1.copy()).unwrap();
        input0.append(tuple2.copy()).unwrap();
        input0.append(tuple3.copy()).unwrap();
        input0.append(tuple4.copy()).unwrap();

        let input1 = vec_buffer();
        let tuple5 = Tuple::from_fields(vec!["a".into(), 1.into()]);
        let tuple6 = Tuple::from_fields(vec!["b".into(), 2.into()]);
        input1.append(tuple5.copy()).unwrap();
        input1.append(tuple6.copy()).unwrap();

        let input2 = vec_buffer();
        let tuple7 = Tuple::from_fields(vec!["a".into()]);
        let tuple8 = Tuple::from_fields(vec!["b".into()]);
        input2.append(tuple7.copy()).unwrap();
        input2.append(tuple8.copy()).unwrap();

        let mut p3 = Pipeline::new(
            3,
            // Build hash table from input1.
            BlockingOp::in_mem_hash_table_creation(
                NonBlockingOp::Scan(PScanIter::new(1)),
                vec![colidx_expr(0)],
            ),
        );
        p3.set_context(1, input1);

        let mut p4 = Pipeline::new(
            4,
            // Build hash table from input0.
            BlockingOp::in_mem_hash_table_creation(
                NonBlockingOp::Scan(PScanIter::new(0)),
                vec![colidx_expr(0)],
            ),
        );
        p4.set_context(0, input0);

        let mut p5 = Pipeline::new(
            5,
            // Hash join with the hash tables.
            NonBlockingOp::HashJoin(PHashJoinIter::Inner(PHashJoinInnerIter::new(
                Box::new(NonBlockingOp::HashJoin(PHashJoinIter::Inner(
                    PHashJoinInnerIter::new(
                        Box::new(NonBlockingOp::Scan(PScanIter::new(2))),
                        3,
                        vec![colidx_expr(0)],
                    ),
                ))),
                4,
                vec![colidx_expr(1)],
            )))
            .into(),
        );
        p5.set_context(2, input2);

        let mut queue = PipelineQueue::new();
        queue.add_pipeline(p3);
        queue.add_pipeline(p4);
        queue.add_pipeline(p5);
        queue.add_dependencies(5, [3, 4]);

        let result = queue.execute().unwrap();
        let iter = result.iter_all();
        let mut actual = Vec::new();
        while let Some(tuple) = iter.next() {
            actual.push(tuple);
        }

        let mut expected = vec![
            Tuple::from_fields(vec![
                1.into(),
                "aa".into(),
                "a".into(),
                1.into(),
                "a".into(),
            ]),
            Tuple::from_fields(vec![
                1.into(),
                "bb".into(),
                "a".into(),
                1.into(),
                "a".into(),
            ]),
            Tuple::from_fields(vec![
                2.into(),
                "cc".into(),
                "b".into(),
                2.into(),
                "b".into(),
            ]),
            Tuple::from_fields(vec![
                2.into(),
                "dd".into(),
                "b".into(),
                2.into(),
                "b".into(),
            ]),
        ];

        check_result(&mut actual, &mut expected, true, true);
    }
}
*/
