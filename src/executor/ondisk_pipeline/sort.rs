// Append only page

// Page layout:
// 4 byte: next page id
// 4 byte: next frame id
// 2 byte: total bytes used (PAGE_HEADER_SIZE + slots + records)
// 2 byte: slot count
// 2 byte: free space offset

use core::panic;
use fbtree::bp::MemPoolStatus;
use rayon::{iter, prelude::*, result};
use std::env;
use std::time::Instant;
use std::{
    cmp::{max, min, Reverse},
    collections::{BinaryHeap, HashMap, HashSet},
    sync::Arc,
};

use crate::{
    error::ExecError,
    executor::TupleBuffer,
    prelude::{Page, PageId, SchemaRef, AVAILABLE_PAGE_SIZE},
    tuple::Tuple,
    ColumnId,
};

use std::sync::atomic::{AtomicU16, Ordering};

use crate::quantile_lib::*;

use fbtree::access_method::sorted_run_store::{BigSortedRunStore, SortedRunStore};

#[derive(Clone, Copy)]
pub enum MergeStrategy {
    Kraska,
    ParallelBSS,
}

// Helper struct and methods for storage statistics
struct StorageStats {
    total_pages: usize,
    total_records: usize,
    avg_records_per_page: f64,
}

#[derive(Clone, Debug)]
pub struct SingleRunQuantiles {
    num_quantiles: usize,
    quantiles: Vec<Vec<u8>>, // Stores quantiles from each run
}

impl SingleRunQuantiles {
    pub fn new(num_quantiles: usize) -> Self {
        SingleRunQuantiles {
            num_quantiles,
            quantiles: Vec::new(),
        }
    }

    pub fn merge(&mut self, other: &SingleRunQuantiles) {
        assert_eq!(self.num_quantiles, other.num_quantiles);
        if self.quantiles.is_empty() {
            self.quantiles = other.quantiles.clone();
            return;
        }
        for i in 0..self.num_quantiles {
            if i == 0 {
                // For the first quantile, we take the min of all the runs
                let smaller = min(&self.quantiles[i], &other.quantiles[i]);
                self.quantiles[i] = smaller.clone();
            } else if i == self.num_quantiles - 1 {
                // For the last quantile, we take the max of all the runs
                let larger = max(&self.quantiles[i], &other.quantiles[i]);
                self.quantiles[i] = larger.clone();
            } else {
                // For other values, we choose the value randomly from one of the runs
                let idx = i % 2;
                if idx == 0 {
                    self.quantiles[i] = self.quantiles[i].clone();
                } else {
                    self.quantiles[i] = other.quantiles[i].clone();
                }
            }
        }
    }

    pub fn get_quantiles(&self) -> &Vec<Vec<u8>> {
        &self.quantiles
    }
}

impl std::fmt::Display for SingleRunQuantiles {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Write one quantile per line
        for q in &self.quantiles {
            write!(f, "{:?}\n", q)?;
        }
        Ok(())
    }
}

mod slot {
    pub const SLOT_SIZE: usize = 6;

    pub struct Slot {
        offset: u16,
        key_size: u16,
        val_size: u16,
    }

    impl Slot {
        pub fn from_bytes(bytes: &[u8; SLOT_SIZE]) -> Self {
            let offset = u16::from_be_bytes([bytes[0], bytes[1]]);
            let key_size = u16::from_be_bytes([bytes[2], bytes[3]]);
            let val_size = u16::from_be_bytes([bytes[4], bytes[5]]);
            Slot {
                offset,
                key_size,
                val_size,
            }
        }

        pub fn to_bytes(&self) -> [u8; SLOT_SIZE] {
            let mut bytes = [0; SLOT_SIZE];
            bytes[0..2].copy_from_slice(&self.offset.to_be_bytes());
            bytes[2..4].copy_from_slice(&self.key_size.to_be_bytes());
            bytes[4..6].copy_from_slice(&self.val_size.to_be_bytes());
            bytes
        }

        pub fn new(offset: u16, key_size: u16, val_size: u16) -> Self {
            Slot {
                offset,
                key_size,
                val_size,
            }
        }

        pub fn offset(&self) -> u16 {
            self.offset
        }

        pub fn set_offset(&mut self, offset: u16) {
            self.offset = offset;
        }

        pub fn key_size(&self) -> u16 {
            self.key_size
        }

        pub fn set_key_size(&mut self, key_size: u16) {
            self.key_size = key_size;
        }

        pub fn val_size(&self) -> u16 {
            self.val_size
        }

        pub fn set_val_size(&mut self, val_size: u16) {
            self.val_size = val_size;
        }

        pub fn size(&self) -> u16 {
            self.key_size + self.val_size
        }

        pub fn set_size(&mut self, key_size: u16, val_size: u16) {
            self.key_size = key_size;
            self.val_size = val_size;
        }
    }
}
use fbtree::{
    access_method::chain,
    bp::{ContainerKey, DatabaseId, FrameWriteGuard, MemPool},
    prelude::{AppendOnlyStore, AppendOnlyStoreScanner},
    txn_storage::TxnStorageTrait,
};
use slot::*;

use super::{disk_buffer::OnDiskBuffer, MemoryPolicy, NonBlockingOp, PipelineID};
const PAGE_HEADER_SIZE: usize = 14;

pub trait AppendOnlyKVPage {
    fn init(&mut self);
    fn max_kv_size() -> usize {
        AVAILABLE_PAGE_SIZE - PAGE_HEADER_SIZE - SLOT_SIZE
    }

    // Header operations
    fn next_page(&self) -> Option<(PageId, u32)>; // (next_page_id, next_frame_id)
    fn set_next_page(&mut self, next_page_id: PageId, frame_id: u32);
    fn total_bytes_used(&self) -> u16;
    fn total_free_space(&self) -> u16 {
        AVAILABLE_PAGE_SIZE as u16 - self.total_bytes_used()
    }
    fn set_total_bytes_used(&mut self, total_bytes_used: u16);
    fn slot_count(&self) -> u16;
    fn set_slot_count(&mut self, slot_count: u16);
    fn increment_slot_count(&mut self) {
        let slot_count = self.slot_count();
        self.set_slot_count(slot_count + 1);
    }

    fn rec_start_offset(&self) -> u16;
    fn set_rec_start_offset(&mut self, rec_start_offset: u16);

    // Helpers
    fn slot_offset(&self, slot_id: u16) -> usize {
        PAGE_HEADER_SIZE + slot_id as usize * SLOT_SIZE
    }
    fn slot(&self, slot_id: u16) -> Option<Slot>;

    // Append a slot at the end of the slots.
    // Increment the slot count.
    // The rec_start_offset is also updated.
    // Only call this function when there is enough space for the slot and record.
    fn append_slot(&mut self, slot: &Slot);

    /// Try to append a key-value to the page.
    /// If the record(key+value) is too large to fit in the page, return false.
    /// When false is returned, the page is not modified.
    /// Otherwise, the record is appended to the page and the page is modified.
    fn append(&mut self, key: &[u8], val: &[u8]) -> bool;

    /// Get the record at the slot_id.
    /// If the slot_id is invalid, panic.
    fn get_key(&self, slot_id: u16) -> &[u8];

    /// Get the value at the slot_id.
    /// If the slot_id is invalid, panic.
    fn get_val(&self, slot_id: u16) -> &[u8];
}

impl AppendOnlyKVPage for Page {
    fn init(&mut self) {
        let next_page_id = PageId::MAX;
        let next_frame_id = u32::MAX;
        let total_bytes_used = PAGE_HEADER_SIZE as u16;
        let slot_count = 0;
        let rec_start_offset = AVAILABLE_PAGE_SIZE as u16;

        self.set_next_page(next_page_id, next_frame_id);
        self.set_total_bytes_used(total_bytes_used);
        self.set_slot_count(slot_count);
        self.set_rec_start_offset(rec_start_offset);
    }

    fn next_page(&self) -> Option<(PageId, u32)> {
        let next_page_id = u32::from_be_bytes([self[0], self[1], self[2], self[3]]);
        let next_frame_id = u32::from_be_bytes([self[4], self[5], self[6], self[7]]);
        if next_page_id == PageId::MAX {
            None
        } else {
            Some((next_page_id, next_frame_id))
        }
    }

    fn set_next_page(&mut self, next_page_id: PageId, frame_id: u32) {
        self[0..4].copy_from_slice(&next_page_id.to_be_bytes());
        self[4..8].copy_from_slice(&frame_id.to_be_bytes());
    }

    fn total_bytes_used(&self) -> u16 {
        u16::from_be_bytes([self[8], self[9]])
    }

    fn set_total_bytes_used(&mut self, total_bytes_used: u16) {
        self[8..10].copy_from_slice(&total_bytes_used.to_be_bytes());
    }

    fn slot_count(&self) -> u16 {
        u16::from_be_bytes([self[10], self[11]])
    }

    fn set_slot_count(&mut self, slot_count: u16) {
        self[10..12].copy_from_slice(&slot_count.to_be_bytes());
    }

    fn rec_start_offset(&self) -> u16 {
        u16::from_be_bytes([self[12], self[13]])
    }

    fn set_rec_start_offset(&mut self, rec_start_offset: u16) {
        self[12..14].copy_from_slice(&rec_start_offset.to_be_bytes());
    }

    fn slot(&self, slot_id: u16) -> Option<Slot> {
        if slot_id < self.slot_count() {
            let offset = self.slot_offset(slot_id);
            let slot_bytes: [u8; SLOT_SIZE] = self[offset..offset + SLOT_SIZE].try_into().unwrap();
            Some(Slot::from_bytes(&slot_bytes))
        } else {
            None
        }
    }

    fn append_slot(&mut self, slot: &Slot) {
        let slot_id = self.slot_count();
        self.increment_slot_count();

        // Update the slot
        let slot_offset = self.slot_offset(slot_id);
        self[slot_offset..slot_offset + SLOT_SIZE].copy_from_slice(&slot.to_bytes());

        // Update the header
        let offset = self.rec_start_offset().min(slot.offset());
        self.set_rec_start_offset(offset);
    }

    fn append(&mut self, key: &[u8], value: &[u8]) -> bool {
        // Check if the page has enough space for slot and the record
        if self.total_free_space() < SLOT_SIZE as u16 + key.len() as u16 + value.len() as u16 {
            false
        } else {
            // Append the slot and the key-value record
            let rec_start_offset = self.rec_start_offset() - key.len() as u16 - value.len() as u16;
            self[rec_start_offset as usize..rec_start_offset as usize + key.len()]
                .copy_from_slice(key);
            self[rec_start_offset as usize + key.len()
                ..rec_start_offset as usize + key.len() + value.len()]
                .copy_from_slice(value);
            let slot = Slot::new(rec_start_offset, key.len() as u16, value.len() as u16);
            self.append_slot(&slot);

            // Update the total bytes used
            self.set_total_bytes_used(
                self.total_bytes_used() + SLOT_SIZE as u16 + key.len() as u16 + value.len() as u16,
            );

            true
        }
    }

    fn get_key(&self, slot_id: u16) -> &[u8] {
        let slot = self.slot(slot_id).unwrap();
        let offset = slot.offset() as usize;
        &self[offset..offset + slot.key_size() as usize]
    }

    fn get_val(&self, slot_id: u16) -> &[u8] {
        let slot = self.slot(slot_id).unwrap();
        let offset = slot.offset() as usize + slot.key_size() as usize;
        &self[offset..offset + slot.val_size() as usize]
    }
}
pub struct SortBuffer<M: MemPool> {
    mem_pool: Arc<M>,
    dest_c_key: ContainerKey,
    policy: Arc<MemoryPolicy>,
    sort_cols: Vec<(ColumnId, bool, bool)>, // (column_id, asc, nulls_first)
    ptrs: Vec<(usize, u16)>,                // Slot pointers. (page index, slot_id)
    data_buffer: Vec<FrameWriteGuard<'static>>,
    current_page_idx: usize,
}

// Implement the method to access the first page
impl<M: MemPool> SortBuffer<M> {
    pub fn get_first_page(&self) -> &Page {
        &self.data_buffer[0]
    }
}

impl<M: MemPool> SortBuffer<M> {
    pub fn new(
        mem_pool: &Arc<M>,
        dest_c_key: ContainerKey,
        policy: &Arc<MemoryPolicy>,
        sort_cols: Vec<(ColumnId, bool, bool)>,
    ) -> Self {
        Self {
            mem_pool: mem_pool.clone(),
            dest_c_key,
            policy: policy.clone(),
            sort_cols,
            ptrs: Vec::new(),
            data_buffer: Vec::new(), //xtx temp = true container
            current_page_idx: 0,
        }
    }

    pub fn reset(&mut self) {
        self.ptrs.clear();
        self.current_page_idx = 0;
        self.data_buffer.clear();
    }

    pub fn append(&mut self, tuple: &Tuple) -> bool {
        let key = tuple.to_normalized_key_bytes(&self.sort_cols);
        let val = tuple.to_bytes();

        /* ---------- ensure there is at least one page ------------------------ */
        if self.data_buffer.is_empty() {
            self.current_page_idx = 0;
            let frame = match self.mem_pool.create_new_page_for_write(self.dest_c_key) {
                Ok(f) => f,
                Err(MemPoolStatus::CannotEvictPage) => return false, // caller will flush & retry
                Err(e) => panic!("mem-pool error: {e:?}"),
            };
            let mut frame =
                unsafe { std::mem::transmute::<FrameWriteGuard, FrameWriteGuard<'static>>(frame) };
            frame.init();
            self.data_buffer.push(frame);
        }

        /* ---------- try to append to the current page ------------------------ */
        let page = self.data_buffer.get_mut(self.current_page_idx).unwrap();
        if page.append(&key, &val) {
            self.ptrs
                .push((self.current_page_idx, page.slot_count() - 1));
            return true;
        }

        /* ---------- need a new page ----------------------------------------- */
        let next_idx = self.current_page_idx + 1;
        if next_idx < self.data_buffer.len() {
            self.current_page_idx = next_idx;
            let page = self.data_buffer.get_mut(self.current_page_idx).unwrap();
            assert!(page.append(&key, &val), "record too large to fit in a page");
            self.ptrs
                .push((self.current_page_idx, page.slot_count() - 1));
            return true;
        }

        /* ---------- allocate fresh page (obeying policy) --------------------- */
        match self.policy.as_ref() {
            MemoryPolicy::FixedSizeLimit(max_pages) if self.data_buffer.len() >= *max_pages => {
                return false; // buffer full → caller flushes
            }
            MemoryPolicy::FixedSizeLimit(_) => {
                let frame = match self.mem_pool.create_new_page_for_write(self.dest_c_key) {
                    Ok(f) => f,
                    Err(MemPoolStatus::CannotEvictPage) => return false,
                    Err(e) => panic!("mem-pool error: {e:?}"),
                };
                let mut frame = unsafe {
                    std::mem::transmute::<FrameWriteGuard, FrameWriteGuard<'static>>(frame)
                };
                frame.init();
                self.data_buffer.push(frame);
                self.current_page_idx = next_idx;

                let page = self.data_buffer.get_mut(self.current_page_idx).unwrap();
                assert!(page.append(&key, &val), "record too large to fit in a page");
                self.ptrs
                    .push((self.current_page_idx, page.slot_count() - 1));
                true
            }
            _ => unimplemented!("Memory policy not implemented"),
        }
    }

    pub fn set_dest_c_key(&mut self, dest_c_key: ContainerKey) {
        self.dest_c_key = dest_c_key;
    }

    pub fn sort(&mut self) {
        // Sort the ptrs
        self.ptrs.sort_by(|a, b| {
            let page_a = &self.data_buffer[a.0];
            let page_b = &self.data_buffer[b.0];
            let key_a = page_a.get_key(a.1);
            let key_b = page_b.get_key(b.1);
            key_a.cmp(key_b)
        });
    }

    // Compute quantiles for the run.
    // The first and the last value is always included in the returned quantiles.
    // num_quantiles should be at least 2.
    //
    // Example:
    // [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], num_quantiles = 4
    // The returned quantiles are [1, 4, 7, 10]
    // [1, 3, 5, 7, 9], num_quantiles = 4
    // The returned quantiles are [1, 3, 5, 9]
    pub fn sample_quantiles(&self, num_quantiles: usize) -> SingleRunQuantiles {
        assert!(num_quantiles >= 2);
        let mut quantiles = Vec::new();
        let num_tuples = self.ptrs.len();

        for i in 0..num_quantiles {
            let idx = if i == num_quantiles - 1 {
                num_tuples - 1
            } else {
                i * num_tuples / num_quantiles
            };
            let (page_idx, slot_id) = self.ptrs[idx];
            let page = &self.data_buffer[page_idx];
            let key = page.get_key(slot_id);
            quantiles.push(key.to_vec());
        }
        SingleRunQuantiles {
            num_quantiles,
            quantiles,
        }
    }
}

impl<M: MemPool> Drop for SortBuffer<M> {
    fn drop(&mut self) {
        // Make all the pages undirty because they don't need to be written back.
        for page in &mut self.data_buffer {
            page.dirty()
                .store(false, std::sync::atomic::Ordering::Release);
        }
    }
}

/// Iterator for sort buffer. Output key, value by sorting order.
pub struct SortBufferIter<'a, M: MemPool> {
    sort_buffer: &'a SortBuffer<M>,
    idx: usize,
}

impl<'a, M: MemPool> SortBufferIter<'a, M> {
    pub fn new(sort_buffer: &'a SortBuffer<M>) -> Self {
        Self {
            sort_buffer,
            idx: 0,
        }
    }
}

impl<'a, M: MemPool> Iterator for SortBufferIter<'a, M> {
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx < self.sort_buffer.ptrs.len() {
            let (page_idx, slot_id) = self.sort_buffer.ptrs[self.idx];
            let page = &self.sort_buffer.data_buffer[page_idx];
            let key = page.get_key(slot_id);
            let val = page.get_val(slot_id);
            self.idx += 1;
            Some((key, val))
        } else {
            None
        }
    }
}

pub struct MergeIter<I: Iterator<Item = (Vec<u8>, Vec<u8>)>> {
    run_iters: Vec<I>,
    // order by key, break ties with run-index
    heap: BinaryHeap<Reverse<(Vec<u8>, usize)>>, // (key, run_idx)
    values: Vec<Option<Vec<u8>>>,
}

impl<I: Iterator<Item = (Vec<u8>, Vec<u8>)>> MergeIter<I> {
    pub fn new(mut run_iters: Vec<I>) -> Self {
        let mut heap = BinaryHeap::with_capacity(run_iters.len());
        let mut values = vec![None; run_iters.len()];

        for (i, iter) in run_iters.iter_mut().enumerate() {
            if let Some((k, v)) = iter.next() {
                heap.push(Reverse((k, i)));
                values[i] = Some(v);
            }
        }

        Self {
            run_iters,
            heap,
            values,
        }
    }
}

impl<I: Iterator<Item = (Vec<u8>, Vec<u8>)>> Iterator for MergeIter<I> {
    type Item = (Vec<u8>, Vec<u8>);
    fn next(&mut self) -> Option<Self::Item> {
        let Reverse((key, run_idx)) = self.heap.pop()?; // ← key now first
        let val = self.values[run_idx].take().unwrap();
        if let Some((next_key, next_val)) = self.run_iters[run_idx].next() {
            self.heap.push(Reverse((next_key, run_idx)));
            self.values[run_idx] = Some(next_val);
        }
        Some((key, val))
    }
}

pub struct OnDiskSort<T: TxnStorageTrait, M: MemPool> {
    schema: SchemaRef,
    exec_plan: NonBlockingOp<T, M>,
    sort_cols: Vec<(ColumnId, bool, bool)>,
    quantiles: SingleRunQuantiles,
}

impl<T: TxnStorageTrait, M: MemPool> OnDiskSort<T, M> {
    pub fn new(
        schema: SchemaRef,
        exec_plan: NonBlockingOp<T, M>,
        sort_cols: Vec<(ColumnId, bool, bool)>,
        num_quantiles: usize,
    ) -> Self {
        Self {
            schema,
            exec_plan,
            sort_cols,
            quantiles: SingleRunQuantiles::new(num_quantiles),
        }
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn deps(&self) -> HashSet<PipelineID> {
        self.exec_plan.deps()
    }

    pub fn print_inner(&self, indent: usize, out: &mut String) {
        out.push_str(&format!("{}->sort_disk(", " ".repeat(indent)));
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

    pub fn run_generation_5(
        &mut self,
        policy: &Arc<MemoryPolicy>,
        context: &HashMap<PipelineID, Arc<OnDiskBuffer<T, M>>>,
        mem_pool: &Arc<M>,
        dest_c_key: ContainerKey,
    ) -> Result<Vec<Arc<SortedRunStore<M>>>, ExecError> {
        /* ---------- config & bookkeeping ------------------------------------- */
        let t1 = Instant::now();
        let total_tuples: usize = env::var("NUM_TUPLES")
            .unwrap_or_else(|_| "6005720".into())
            .parse()
            .expect("NUM_TUPLES");
        let num_threads: usize = env::var("NUM_THREADS")
            .unwrap_or_else(|_| "8".into())
            .parse()
            .expect("NUM_THREADS");

        let chunk_size = (total_tuples + num_threads - 1) / num_threads;
        let work_pages = match policy.as_ref() {
            MemoryPolicy::FixedSizeLimit(p) => *p,
            _ => unreachable!(),
        };
        let thr_pages = if num_threads == 1 {
            // leave at most half the pool free for the copy destination
            (work_pages).max(1) * 0.8 as usize
        } else {
            (work_pages / num_threads).max(1)
        };
        let thr_policy = Arc::new(MemoryPolicy::FixedSizeLimit(thr_pages));

        let ranges: Vec<_> = (0..num_threads)
            .map(|i| {
                let lo = i * chunk_size;
                let hi = if i == num_threads - 1 {
                    total_tuples
                } else {
                    (i + 1) * chunk_size
                };
                (lo, hi)
            })
            .collect();

        let plans: Vec<_> = ranges
            .iter()
            .map(|&(s, e)| self.exec_plan.clone_with_range(s, e))
            .collect();

        /* ---------- run generation in parallel ------------------------------- */
        let sort_cols = self.sort_cols.clone();
        let num_q = self.quantiles.num_quantiles;
        println!("Time taken to prepare sort: {:.2}s", t1.elapsed().as_secs_f64());
        let worker_results = plans
            .into_par_iter()
            .enumerate()
            .map(|(tid, mut plan)| {
                let t0 = Instant::now();
                let mut local_cid = (tid as u16) * 2000;
                let mut next_cid = |cid: &mut u16| {
                    let id = *cid;
                    *cid = cid.wrapping_add(1);
                    id
                };

                let mut scratch_key = ContainerKey {
                    db_id: dest_c_key.db_id,
                    c_id: next_cid(&mut local_cid),
                };
                let mut sbuf =
                    SortBuffer::new(mem_pool, scratch_key, &thr_policy, sort_cols.clone());

                let mut runs = Vec::<Arc<SortedRunStore<M>>>::new();
                let mut qtls = Vec::<SingleRunQuantiles>::new();
                let mut seen = 0usize;
                const BATCH: usize = 1000;
                let mut batch = Vec::with_capacity(BATCH);

                let mut flush = |sbuf: &mut SortBuffer<M>,
                                 key: ContainerKey,
                                 runs: &mut Vec<_>,
                                 qtls: &mut Vec<_>| {
                    if sbuf.ptrs.is_empty() {
                        return;
                    }
                    sbuf.sort();
                    qtls.push(sbuf.sample_quantiles(num_q));
                    runs.push(Arc::new(SortedRunStore::new(
                        key,
                        mem_pool.clone(),
                        SortBufferIter::new(sbuf),
                    )));
                };

                while let Some(t) = plan.next(context)? {
                    batch.push(t);
                    if batch.len() == BATCH {
                        for tup in batch.drain(..) {
                            seen += 1;
                            if !sbuf.append(&tup) {
                                flush(&mut sbuf, scratch_key, &mut runs, &mut qtls);

                                scratch_key = ContainerKey {
                                    db_id: dest_c_key.db_id,
                                    c_id: next_cid(&mut local_cid),
                                };
                                sbuf.reset();
                                sbuf.set_dest_c_key(scratch_key);
                                sbuf.append(&tup);
                            }
                        }
                    }
                }
                for tup in batch {
                    // tail
                    seen += 1;
                    if !sbuf.append(&tup) {
                        flush(&mut sbuf, scratch_key, &mut runs, &mut qtls);
                        scratch_key = ContainerKey {
                            db_id: dest_c_key.db_id,
                            c_id: next_cid(&mut local_cid),
                        };
                        sbuf.reset();
                        sbuf.set_dest_c_key(scratch_key);
                        sbuf.append(&tup);
                    }
                }
                flush(&mut sbuf, scratch_key, &mut runs, &mut qtls);

                let mut thr_q = SingleRunQuantiles::new(num_q);
                for q in qtls {
                    thr_q.merge(&q);
                }

                println!(
                    "Thread {tid}: {seen} tuples → {} runs in {:.2}s",
                    runs.len(),
                    t0.elapsed().as_secs_f64()
                );
                Ok((runs, thr_q))
            })
            .collect::<Result<Vec<_>, ExecError>>()?;

        /* ---------- assemble final result ------------------------------------ */
        let mut all_runs = Vec::new();
        let mut global_q = SingleRunQuantiles::new(self.quantiles.num_quantiles);

        for (r, q) in worker_results {
            all_runs.extend(r);
            global_q.merge(&q);
        }
        self.quantiles = global_q;
        Ok(all_runs)
    }

    fn run_generation_kraska(
        &mut self,
        policy: &Arc<MemoryPolicy>,
        context: &HashMap<PipelineID, Arc<OnDiskBuffer<T, M>>>,
        mem_pool: &Arc<M>,
        dest_c_key: ContainerKey,
    ) -> Result<Vec<Arc<SortedRunStore<M>>>, ExecError> {
        let total_tuples = env::var("NUM_TUPLES")
            .unwrap_or_else(|_| "6005720".to_string())
            .parse()
            .expect("NUM_TUPLES must be a valid number");

        match policy.as_ref() {
            MemoryPolicy::FixedSizeLimit(working_mem) => {
                let max_parallel_partitions = std::cmp::min(*working_mem, 8);
                println!("Total tuples estimated: {}", total_tuples);
                println!("Working memory limit (runs): {}", working_mem);
                println!("Maximum parallel partitions: {}", max_parallel_partitions);

                // First generate initial sorted runs in parallel
                let target_tuples_per_run = total_tuples / max_parallel_partitions;

                let initial_runs: Vec<Arc<SortedRunStore<M>>> = (0..max_parallel_partitions)
                    .into_par_iter()
                    .map(|run_id| {
                        let start = run_id * target_tuples_per_run;
                        let end = if run_id == max_parallel_partitions - 1 {
                            total_tuples
                        } else {
                            std::cmp::min(start + target_tuples_per_run, total_tuples)
                        };

                        let mut exec_plan = self.exec_plan.clone_with_range(start, end);
                        let run_container_key = ContainerKey {
                            db_id: dest_c_key.db_id,
                            c_id: dest_c_key.c_id + run_id as u16,
                        };

                        let mut sort_buffer = SortBuffer::new(
                            &mem_pool,
                            run_container_key,
                            policy,
                            self.sort_cols.clone(),
                        );

                        let mut runs = Vec::new();

                        // Fill buffer with tuples for this range
                        while let Some(tuple) = exec_plan.next(context)? {
                            if !sort_buffer.append(&tuple) {
                                sort_buffer.sort();
                                let run = Arc::new(SortedRunStore::new(
                                    run_container_key,
                                    mem_pool.clone(),
                                    SortBufferIter::new(&sort_buffer),
                                ));
                                runs.push(run);
                                sort_buffer.reset();
                                assert!(sort_buffer.append(&tuple));
                            }
                        }

                        // Handle remaining tuples
                        if sort_buffer.ptrs.len() > 0 {
                            sort_buffer.sort();
                            let run = Arc::new(SortedRunStore::new(
                                run_container_key,
                                mem_pool.clone(),
                                SortBufferIter::new(&sort_buffer),
                            ));
                            runs.push(run);
                        }

                        // For each range, create a BigSortedRunStore from its runs
                        let mut big_store = BigSortedRunStore::new();
                        for run in runs {
                            big_store.add_store(run);
                        }

                        // Return a single merged run for this range
                        let merge_iter = big_store.scan();
                        Ok(Arc::new(SortedRunStore::new(
                            run_container_key,
                            mem_pool.clone(),
                            merge_iter,
                        )))
                    })
                    .collect::<Result<Vec<_>, ExecError>>()?;

                // Create a BigSortedRunStore from initial runs to get quantiles
                let mut big_store = BigSortedRunStore::new();
                for run in initial_runs.iter() {
                    big_store.add_store(run.clone());
                }
                let big_store = Arc::new(big_store);

                // Calculate partition boundaries using the BigSortedRunStore
                let mut global_quantiles = estimate_quantiles(
                    &[big_store],
                    max_parallel_partitions + 1,
                    QuantileMethod::Actual,
                );

                // Ensure boundary conditions
                if global_quantiles.is_empty() {
                    global_quantiles = vec![vec![0; 9]; max_parallel_partitions + 1];
                }
                global_quantiles[0] = vec![0; 9];
                global_quantiles[max_parallel_partitions] = vec![255; 9];

                // Now redistribute records into final partitions
                let final_runs: Vec<Arc<SortedRunStore<M>>> = (0..max_parallel_partitions)
                    .into_par_iter()
                    .map(|partition_id| {
                        let lower = global_quantiles[partition_id].clone();
                        let upper = global_quantiles[partition_id + 1].clone();

                        let partition_key = ContainerKey {
                            db_id: dest_c_key.db_id,
                            c_id: dest_c_key.c_id + partition_id as u16,
                        };

                        // Create merger for this partition's range
                        let run_segments = initial_runs
                            .iter()
                            .map(|run| run.scan_range(&lower, &upper))
                            .collect::<Vec<_>>();

                        let merge_iter = MergeIter::new(run_segments);
                        let merged_store = Arc::new(SortedRunStore::new(
                            partition_key,
                            mem_pool.clone(),
                            merge_iter,
                        ));

                        println!(
                            "Partition {} contains {} records",
                            partition_id,
                            merged_store.len()
                        );

                        Ok(merged_store)
                    })
                    .collect::<Result<Vec<_>, ExecError>>()?;

                // Verify total record count
                let total_records: usize = final_runs.iter().map(|r| r.len()).sum();
                println!("Total records: {}", total_records);
                assert_eq!(
                    total_records, total_tuples,
                    "Record count mismatch! Expected {} but got {}",
                    total_tuples, total_records
                );

                Ok(final_runs)
            }
            _ => panic!("Only FixedSizeLimit policy supported"),
        }
    }

    fn run_merge_kraska(
        &mut self,
        policy: &Arc<MemoryPolicy>,
        runs: Vec<Arc<BigSortedRunStore<M>>>,
        mem_pool: &Arc<M>,
        dest_c_key: ContainerKey,
        num_threads: usize,
    ) -> Result<Arc<BigSortedRunStore<M>>, ExecError> {
        let verbose = true;

        match policy.as_ref() {
            MemoryPolicy::FixedSizeLimit(working_mem) => {
                let working_mem = 400;
                let overall_start = Instant::now();
                let total_records: usize = runs.iter().map(|r| r.len()).sum();
                let max_parallel_partitions = std::cmp::min(working_mem, num_threads);

                if verbose {
                    println!("Working memory (max runs): {}", working_mem);
                    println!("Number of threads available: {}", num_threads);
                    println!("Maximum parallel partitions: {}", max_parallel_partitions);
                    println!("Total runs to merge: {}", runs.len());
                }

                let mut global_quantiles =
                    estimate_quantiles(&runs, max_parallel_partitions + 1, QuantileMethod::Actual);

                if global_quantiles.is_empty() {
                    global_quantiles = vec![vec![0; 9]; max_parallel_partitions + 1];
                }
                global_quantiles[0] = vec![0; 9];
                global_quantiles[max_parallel_partitions] = vec![255; 9];

                let processing_start = Instant::now();

                // Process all partitions in parallel and collect their stores
                let partition_results: Vec<Vec<Arc<SortedRunStore<M>>>> = (0
                    ..max_parallel_partitions)
                    .into_par_iter()
                    .map(|i| {
                        let thread_start = Instant::now();
                        let lower = global_quantiles[i].clone();
                        let upper = global_quantiles[i + 1].clone();

                        if verbose {
                            println!(
                                "Partition {} starting - Range: {:?} to {:?} (half-open)",
                                i, lower, upper
                            );
                        }

                        // Process each run's portion of this partition
                        let partition_stores: Vec<Arc<SortedRunStore<M>>> = runs
                            .iter()
                            .enumerate()
                            .map(|(run_idx, run)| {
                                let partition_key = ContainerKey {
                                    db_id: dest_c_key.db_id,
                                    c_id: dest_c_key.c_id + (i * runs.len() + run_idx) as u16,
                                };

                                Arc::new(SortedRunStore::new(
                                    partition_key,
                                    mem_pool.clone(),
                                    run.scan_range(&lower, &upper),
                                ))
                            })
                            .filter(|store| store.len() > 0)
                            .collect();

                        let partition_records: usize =
                            partition_stores.iter().map(|s| s.len()).sum();
                        let thread_duration = thread_start.elapsed();

                        if verbose {
                            println!(
                                "Partition {} finished in {:.2}s ({} records)",
                                i,
                                thread_duration.as_secs_f64(),
                                partition_records
                            );
                        }

                        partition_stores
                    })
                    .collect();

                // Create final store and add all stores in order
                let mut final_store = BigSortedRunStore::new();
                let mut total_processed = 0;

                // Add stores from all partitions in order
                for stores in partition_results {
                    for store in stores {
                        total_processed += store.len();
                        final_store.add_store(store);
                    }
                }

                if verbose {
                    let overall_sec = overall_start.elapsed().as_secs_f64();
                    println!("\nPartitioning Statistics:");
                    println!("  - Total records processed: {}", total_processed);
                    println!(
                        "  - Overall throughput: {:.2}M records/s",
                        (total_processed as f64) / (1_000_000.0 * overall_sec)
                    );
                    println!(
                        "  - Processing time: {:.2}s",
                        processing_start.elapsed().as_secs_f64()
                    );

                    assert_eq!(
                        total_records, total_processed,
                        "Record count mismatch! Expected {} but got {}",
                        total_records, total_processed
                    );
                }

                Ok(Arc::new(final_store))
            }
            _ => {
                panic!("Only FixedSizeLimit policy is implemented");
            }
        }
    }

    //Run merge parllel but uses a big sorted store
    fn parallel_merge_step_bss(
        &self,
        runs: Vec<Arc<BigSortedRunStore<M>>>,
        mem_pool: &Arc<M>,
        dest_c_key: ContainerKey,
        num_threads: usize,
        verbose: bool,
    ) -> Result<Arc<BigSortedRunStore<M>>, ExecError> {
        let merge_start = Instant::now();
        
        // FIXED: Verify memory constraints before starting
        let total_memory_needed = runs.len() * num_threads + num_threads * 2;
        let available_memory = mem_pool.capacity();
        
        // if total_memory_needed > available_memory {
        //     return Err(ExecError::Other(format!(
        //         "Insufficient memory: need {} pages, have {} pages (fan-in: {}, workers: {})", 
        //         total_memory_needed, available_memory, runs.len(), num_threads
        //     )));
        // }
    
        if verbose {
            println!("Memory check: need {} pages, have {} pages", 
                     total_memory_needed, available_memory);
        }
    
        let q_cnt = num_threads + 1;
        let mut global_q = estimate_quantiles(&runs, q_cnt, QuantileMethod::Actual);
        global_q[0] = vec![0; 9];
        global_q[q_cnt - 1] = vec![255; 9];
        let global_q = Arc::new(global_q);
    
        let worker_results = std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(num_threads);
    
            for part in 0..num_threads {
                let runs = runs.clone();
                let mem_pool = Arc::clone(mem_pool);
                let global_q = Arc::clone(&global_q);
    
                handles.push(scope.spawn(move || -> Result<(usize, SortedRunStore<M>, usize), ExecError> {
                    let t0 = Instant::now();
    
                    let lower = global_q[part].clone();
                    
                    // FIXED: Proper upper bound calculation
                    let upper = if part == num_threads - 1 {
                        // Make last partition inclusive by using max possible key
                        vec![255u8; 32] // Use a large key that's guaranteed to be >= any real key
                    } else {
                        // For non-last partitions, use exclusive upper bound
                        let upper = if part == num_threads - 1 {
                            Vec::new()            // empty  => “no upper bound” for the last bucket
                        } else {
                            global_q[part + 1].clone()   // already exclusive
                        };
                        upper
                    };
    
                    // Build per-run iterators
                    let segs: Vec<_> = runs
                        .iter()
                        .map(|r| BigSortedRunStore::scan_range_arc(r, &lower, &upper))
                        .collect();
    
                    let merge_iter = MergeIter::new(segs);
                    let tmp_key = ContainerKey {
                        db_id: dest_c_key.db_id,
                        c_id: dest_c_key.c_id + part as u16,
                    };
                    
                    let store = SortedRunStore::new(tmp_key, mem_pool, merge_iter);
                    let tuples = store.len();
    
                    if verbose {
                        println!("worker {part}: {tuples} recs, {} pages, {:.2}s",
                                 store.num_pages(), t0.elapsed().as_secs_f64());
                    }
                    
                    Ok((part, store, tuples))
                }));
            }
    
            // Collect results in order
            let mut acc = vec![None; num_threads];
            for h in handles {
                match h.join() {
                    Ok(Ok((idx, st, cnt))) => acc[idx] = Some((st, cnt)),
                    Ok(Err(e)) => return Err(e),
                    Err(_) => return Err(ExecError::Other("Worker thread panicked".to_string())),
                }
            }
            
            Ok(acc.into_iter().map(|o| o.unwrap()).collect::<Vec<_>>())
        })?;
    
        let mut final_bss = BigSortedRunStore::new();
        let mut total_tuples = 0usize;
    
        for (store, cnt) in worker_results {
            total_tuples += cnt;
            final_bss.add_store(Arc::new(store));
        }
    
        if verbose {
            println!("parallel_merge_step_bss: merged {} input-runs → {} tuples in {:.2}s",
                     runs.len(), total_tuples, merge_start.elapsed().as_secs_f64());
        }
    
        Ok(Arc::new(final_bss))
    }

    /// Performs a single parallel merge step combining multiple sorted runs into a BigSortedRunStore
    fn run_merge_parallel_bss(
        &self,
        policy: &Arc<MemoryPolicy>,
        mut runs: Vec<Arc<BigSortedRunStore<M>>>,
        mem_pool: &Arc<M>,
        dest_c_key: ContainerKey,
        num_threads: usize,
        verbose: bool,
    ) -> Result<Arc<BigSortedRunStore<M>>, ExecError> {
        let mut next_cid: u16 = dest_c_key.c_id.saturating_add(10_000);
    
        const READ_FRAMES_PER_RUN: usize = 1;
        const WRITE_FRAMES_PER_WRK: usize = 2;
    
        let pool_pages = ((mem_pool.capacity().max(1) as f64) * 0.9) as usize;
        let workers = num_threads.max(1).min(pool_pages);
    
        if verbose {
            println!("» workers = {workers} (pool holds {pool_pages} frames)");
            println!("\n⟪ hierarchical merge begins ⟫");
        }
    
        let mut fanins = Vec::<usize>::new();
    
        while runs.len() > 1 {
            // FIXED: Correct memory calculation
            // Total memory needed = fan_in * workers (reading) + workers * 2 (writing)
            // So: fan_in * workers + workers * 2 <= pool_pages
            // Therefore: fan_in <= (pool_pages - workers * 2) / workers
            let memory_for_reads = pool_pages.saturating_sub(workers * WRITE_FRAMES_PER_WRK);
            let max_fan_in_by_memory = if workers > 0 {
                memory_for_reads / workers
            } else {
                1
            };
            
            let fan_in = runs.len()
                .min(max_fan_in_by_memory)
                .max(2);
            // let fan_in = 320;
            
            fanins.push(fan_in);
            if verbose {
                println!("→ merging {fan_in} of {} runs (memory allows max {})", 
                         runs.len(), max_fan_in_by_memory);
                println!("  total memory needed: {} pages", fan_in * workers + workers * WRITE_FRAMES_PER_WRK);
            }
    
            let step_inputs: Vec<_> = runs.drain(0..fan_in).collect();
            let step_base_key = ContainerKey {
                db_id: dest_c_key.db_id,
                c_id: next_cid,
            };
            next_cid = next_cid.wrapping_add(workers as u16);
    
            let merged_bss = self.parallel_merge_step_bss(
                step_inputs.clone(),
                mem_pool,
                step_base_key,
                workers,
                verbose,
            )?;
    
            runs.push(merged_bss);
    
            // FIXED: Immediate cleanup of processed runs
            for bss in step_inputs.iter() {
                for store in bss.sorted_run_stores.iter() {
                    let _ = mem_pool.drop_container(store.c_key);
                }
            }
        }
    
        if verbose {
            println!("⟪ hierarchical merge done – fan-ins {:?} ⟫", fanins);
        }
    
        Ok(runs.pop().unwrap())
    }

    fn compute_actual_quantiles(
        &self,
        final_store: &Arc<AppendOnlyStore<M>>,
        num_quantiles: usize,
    ) -> Vec<Vec<u8>> {
        // Step 1: Count the total number of tuples
        let total_tuples = final_store.scan().count();

        if total_tuples == 0 {
            println!("No tuples to compute quantiles.");
            return Vec::new();
        }

        // Step 2: Determine the indices for each quantile
        let mut quantile_indices = Vec::new();
        for i in 1..num_quantiles {
            let idx = (i * total_tuples) / num_quantiles;
            quantile_indices.push(idx);
        }

        // Step 3: Traverse the final_store and capture keys at quantile indices
        let mut actual_quantiles = Vec::new();
        let mut current_index = 0;
        let mut q = 0;

        for (key, _) in final_store.scan() {
            if q >= quantile_indices.len() {
                break;
            }
            if current_index == quantile_indices[q] {
                actual_quantiles.push(key.clone());
                q += 1;
            }
            current_index += 1;
        }

        // Handle the edge case where last quantile is the last element
        if actual_quantiles.len() < num_quantiles - 1 && total_tuples > 0 {
            if let Some((last_key, _)) = final_store.scan().last() {
                actual_quantiles.push(last_key.clone());
            }
        }

        actual_quantiles
    }

    fn merge_step(
        &mut self,
        runs: Vec<Arc<AppendOnlyStore<M>>>,
        mem_pool: &Arc<M>,
        dest_c_key: ContainerKey,
    ) -> Arc<AppendOnlyStore<M>> {
        let merge_iter = MergeIter::new(runs.iter().map(|r| r.scan()).collect());
        Arc::new(AppendOnlyStore::bulk_insert_create(
            dest_c_key,
            mem_pool.clone(),
            merge_iter,
        ))
    }

    fn merge_step_sorted_store(
        &mut self,
        runs: Vec<Arc<SortedRunStore<M>>>,
        mem_pool: &Arc<M>,
        dest_c_key: ContainerKey,
    ) -> Arc<AppendOnlyStore<M>> {
        let merge_iter = MergeIter::new(runs.iter().map(|r| r.scan()).collect());
        Arc::new(AppendOnlyStore::bulk_insert_create(
            dest_c_key,
            mem_pool.clone(),
            merge_iter,
        ))
    }

    pub fn execute(
        &mut self,
        context: &HashMap<PipelineID, Arc<OnDiskBuffer<T, M>>>,
        policy: &Arc<MemoryPolicy>,
        mem_pool: &Arc<M>,
        dest_c_key: ContainerKey,
    ) -> Result<Arc<OnDiskBuffer<T, M>>, ExecError> {
        self.execute_with_strategy(
            context,
            policy,
            mem_pool,
            dest_c_key,
            MergeStrategy::ParallelBSS,
        )
    }

    pub fn execute_with_strategy(
        &mut self,
        context: &HashMap<PipelineID, Arc<OnDiskBuffer<T, M>>>,
        policy: &Arc<MemoryPolicy>,
        mem_pool: &Arc<M>,
        dest_c_key: ContainerKey,
        strategy: MergeStrategy,
    ) -> Result<Arc<OnDiskBuffer<T, M>>, ExecError> {
        // println!("bp stats before {}", mem_pool.stats());
        // -------------- Run Generation Phase --------------
        let start_generation = Instant::now();
        let runs = match strategy {
            MergeStrategy::Kraska => {
                println!("Using Kraska run generation strategy");
                self.run_generation_kraska(policy, context, mem_pool, dest_c_key)
            }
            MergeStrategy::ParallelBSS => {
                println!("Using ParallelBSS run generation strategy");
                self.run_generation_5(policy, context, mem_pool, dest_c_key)
            }
        }?;
        let duration_generation = start_generation.elapsed();
        println!("generation duration {:?}", duration_generation);
        // println!("bp stats after rg {}", mem_pool.stats());

        // Join the runs from the run generation into one big sorted store xtx update here to control the size of the runs
        let mut big_runs = Vec::new();
        for run in runs {
            let mut temp = BigSortedRunStore::new();
            temp.add_store(run);
            big_runs.push(Arc::new(temp));
        }

        // Print pre-merge statistics
        let pre_merge_stats = self.calculate_storage_stats(&big_runs);
        self.print_storage_stats("Pre-merge", &pre_merge_stats);

        // -------------- Run Merge Phase --------------
        let merge_num_threads = env::var("NUM_THREADS")
            .unwrap_or_else(|_| 8.to_string())
            .parse()
            .expect("NUM_THREADS must be a valid number");
        let start_merge = Instant::now();
        let verbose = true;

        // Execute merge based on strategy
        let final_run: Arc<BigSortedRunStore<M>> = match strategy {
            MergeStrategy::Kraska => {
                println!("Using Kraska merge strategy");
                self.run_merge_kraska(policy, big_runs, mem_pool, dest_c_key, merge_num_threads)?
            }
            MergeStrategy::ParallelBSS => {
                println!("Using Parallel BSS merge strategy");
                self.run_merge_parallel_bss(
                    policy,
                    big_runs,
                    mem_pool,
                    dest_c_key,
                    merge_num_threads,
                    verbose,
                )?
            }
        };
        let duration_merge = start_merge.elapsed();
        // println!("bp stats after merge {}", mem_pool.stats());

        // // Print post-merge statistics
        // let post_merge_stats = StorageStats {
        //     total_pages: final_run.num_pages(),
        //     total_records: final_run.len(),
        //     avg_records_per_page: if final_run.num_pages() > 0 {
        //         final_run.len() as f64 / final_run.num_pages() as f64
        //     } else {
        //         0.0
        //     },
        // };
        // self.print_storage_stats("Post-merge", &post_merge_stats);

        // // Print efficiency changes
        // self.print_efficiency_changes(&pre_merge_stats, &post_merge_stats);

        println!("merge duration {:?}", duration_merge);
        verify_sorted_store_full_bss(final_run.clone(), &[(0, true, false)], false, 1);

        if verbose {
            verify_sorted_store_full_bss(final_run.clone(), &[(0, true, false)], false, 1);

            let data_source = env::var("DATA_SOURCE").unwrap_or_else(|_| (&"TPCH").to_string());
            let sf = env::var("SF")
                .unwrap_or_else(|_| 1.to_string())
                .parse()
                .expect("SF must be a valid number");
            let query_num = env::var("QUERY_NUM")
                .unwrap_or_else(|_| 100.to_string())
                .parse()
                .expect("QUERY_NUM must be a valid number");
            let num_tuples = env::var("NUM_TUPLES")
                .unwrap_or_else(|_| 6005720.to_string())
                .parse()
                .expect("NUM_TUPLES must be a valid number");
            let max_num_quantiles = 50;
            write_quantiles_to_json_file(
                final_run.clone(),
                &data_source,
                sf,
                query_num,
                num_tuples,
                max_num_quantiles,
            )?;
        }

        Ok(Arc::new(OnDiskBuffer::BigSortedRunStore(final_run)))
    }

    fn calculate_storage_stats(&self, runs: &[Arc<BigSortedRunStore<M>>]) -> StorageStats {
        let total_pages: usize = runs.iter().map(|run| run.num_pages()).sum();
        let total_records: usize = runs.iter().map(|run| run.len()).sum();
        let avg_records_per_page = if total_pages > 0 {
            total_records as f64 / total_pages as f64
        } else {
            0.0
        };

        StorageStats {
            total_pages,
            total_records,
            avg_records_per_page,
        }
    }

    fn print_storage_stats(&self, phase: &str, stats: &StorageStats) {
        println!("\n{} storage statistics:", phase);
        println!("  Total pages used: {}", stats.total_pages);
        println!("  Total records: {}", stats.total_records);
        println!(
            "  Average records per page: {:.2}",
            stats.avg_records_per_page
        );
    }

    fn print_efficiency_changes(&self, pre: &StorageStats, post: &StorageStats) {
        let page_reduction = if pre.total_pages > 0 {
            (pre.total_pages as f64 - post.total_pages as f64) / pre.total_pages as f64 * 100.0
        } else {
            0.0
        };

        let density_improvement = if pre.avg_records_per_page > 0.0 {
            (post.avg_records_per_page - pre.avg_records_per_page) / pre.avg_records_per_page
                * 100.0
        } else {
            0.0
        };

        println!("\nStorage efficiency changes:");
        println!("  Page reduction: {:.1}%", page_reduction);
        println!("  Record density improvement: {:.1}%", density_improvement);
    }

    // / Generates runs, computes "estimated" run-level quantiles,
    // / checks/creates "actual" quantiles from a fully merged store,
    // / then evaluates the difference and writes out results as JSON.
    pub fn quantile_generation_execute(
        &mut self,
        context: &HashMap<PipelineID, Arc<OnDiskBuffer<T, M>>>,
        policy: &Arc<MemoryPolicy>,
        mem_pool: &Arc<M>,
        dest_c_key: ContainerKey,
        data_source: &str,
        query_id: u8,
        methods: &[QuantileMethod],
        num_quantiles_per_run: usize,
        estimated_store_json: &str,
        actual_store_json: &str,
        evaluation_json: &str,
    ) -> Result<Arc<OnDiskBuffer<T, M>>, ExecError> {
        // 1. Run Generation
        let runs = self.run_generation_5(policy, context, mem_pool, dest_c_key)?;

        for method in methods {
            // 2. Call estimate_quantiles from quantile_lib
            // println!("estimating quantiles");
            // let estimated_quantiles = estimate_quantiles(&runs, num_quantiles_per_run, method.clone());
            // println!("done estimating quantiles");
            // let estimated_stor_json_path = estimated_store_json.replace("***", &method.to_string());
            // // 3. Store estimated quantiles
            // write_quantiles_to_json(
            //     &estimated_quantiles,
            //     data_source,
            //     query_id,
            //     method.clone(),
            //     &estimated_stor_json_path,
            // )?;

            // 4. Check if we need to compute actual quantiles
            if !check_actual_quantiles_exist(
                data_source,
                query_id,
                num_quantiles_per_run,
                actual_store_json,
            )? {
                // let merged_store = self.run_merge_3(policy, runs.clone(), mem_pool, dest_c_key)?;
                // let actual_quantiles = compute_actual_quantiles_helper(&merged_store, num_quantiles_per_run);
                // write_quantiles_to_json(
                //     &actual_quantiles,
                //     data_source,
                //     query_id,
                //     QuantileMethod::Actual,
                //     actual_store_json,
                // )?;
            }

            // 5. Load and evaluate
            let actual_quantiles = load_quantiles_from_json(
                data_source,
                query_id,
                num_quantiles_per_run,
                actual_store_json,
            )?;

            let evaluation_json_path = evaluation_json.replace("***", &method.to_string());
            // evaluate_and_store_quantiles_custom(
            //     &estimated_quantiles,
            //     &actual_quantiles,
            //     data_source,
            //     query_id,
            //     method.clone(),
            //     &evaluation_json_path,
            // )?;
        }
        self.execute(context, policy, mem_pool, dest_c_key)
    }
}

fn verify_sorted_results(
    result: &[Tuple],
    sort_cols: &[(usize, bool, bool)],
) -> Result<(), String> {
    for i in 1..result.len() {
        let prev = &result[i - 1];
        let curr = &result[i];

        for &(col_idx, asc, _) in sort_cols {
            let prev_value = prev.get(col_idx);
            let curr_value = curr.get(col_idx);

            let cmp_result = if asc {
                prev_value.partial_cmp(curr_value).unwrap()
            } else {
                curr_value.partial_cmp(prev_value).unwrap()
            };

            if cmp_result == std::cmp::Ordering::Greater {
                return Err(format!(
                    "Sort verification failed at row {}:\n\
                    Previous tuple: {:?}\n\
                    Current tuple: {:?}\n\
                    Column index {} - Expected: {:?} should be {:?}, but found {:?} is {:?}.",
                    i,
                    prev,
                    curr,
                    col_idx,
                    prev_value,
                    if asc { "<=" } else { ">=" },
                    prev_value,
                    curr_value
                ));
            }
            if cmp_result == std::cmp::Ordering::Less {
                // If this field is in the correct order, no need to check further
                break;
            }
        }
    }
    Ok(())
}

fn verify_sorted_store<T: MemPool>(
    store: Arc<AppendOnlyStore<T>>,
    sort_cols: &[(usize, bool, bool)], // (column_index, ascending, nulls_first)
    verbose: bool,
) -> Result<(), String> {
    let mut scanner = store.scan();

    // Get first record
    let first = match scanner.next() {
        Some(kv) => kv,
        None => return Ok(()), // Empty store is considered sorted
    };

    let mut prev = first;
    let mut count = 1;

    // Compare each consecutive pair
    while let Some(curr) = scanner.next() {
        count += 1;

        // Check each sort column in order
        for &(col_idx, asc, _nulls_first) in sort_cols {
            // Extract values for the column (assuming key for col_idx 0, value otherwise)
            let prev_value = if col_idx == 0 { &prev.0 } else { &prev.1 };
            let curr_value = if col_idx == 0 { &curr.0 } else { &curr.1 };

            let cmp_result = if asc {
                prev_value.cmp(curr_value)
            } else {
                curr_value.cmp(prev_value)
            };

            match cmp_result {
                std::cmp::Ordering::Greater => {
                    if verbose {
                        return Err(format!(
                            "Sort verification failed at row {}:\n\
                            Previous record: key={:?}, value={:?}\n\
                            Current record: key={:?}, value={:?}\n\
                            Column {} - Expected {:?} should be {:?} but found {:?} is {:?}.",
                            count,
                            prev.0,
                            prev.1,
                            curr.0,
                            curr.1,
                            col_idx,
                            prev_value,
                            if asc { "<=" } else { ">=" },
                            prev_value,
                            curr_value
                        ));
                    } else {
                        return Err(format!("Sort violation found at position {}", count));
                    }
                }
                std::cmp::Ordering::Less => {
                    // If this column is properly ordered, skip checking remaining columns
                    break;
                }
                std::cmp::Ordering::Equal => {
                    // If equal, continue to next column
                    continue;
                }
            }
        }

        prev = curr;
    }

    if verbose {
        println!("Store is correctly sorted! ({} records verified)", count);
    }

    Ok(())
}

// Version that collects all violations
fn verify_sorted_store_full<T: MemPool>(
    store: Arc<AppendOnlyStore<T>>,
    sort_cols: &[(usize, bool, bool)],
    verbose: bool,
    num_threads: usize,
) -> Vec<(usize, Vec<u8>, Vec<u8>)> {
    let mut violations = Vec::new();
    let mut scanner = store.scan();
    let total_tuples: usize = env::var("NUM_TUPLES")
        .unwrap_or_else(|_| 6005720.to_string())
        .parse()
        .expect("NUM_TUPLES must be a valid number");

    // Get first record
    let first = match scanner.next() {
        Some(kv) => kv,
        None => return violations, // Empty store has no violations
    };

    let mut prev_key = first.clone().0;
    let mut count = 1;

    if count == 1 {
        println!("{:?}", first.0);
    }

    while let Some((curr_key, _curr_value)) = scanner.next() {
        if count % (total_tuples / (num_threads)) == 0 {
            println!("{:?}", curr_key.clone());
        }

        count += 1;
        let mut violation_found = false;

        for &(col_idx, asc, _nulls_first) in sort_cols {
            let cmp_result = if asc {
                // println!("prev_value {:?} curr_value {:?}", prev_key, curr_key);
                prev_key.cmp(&curr_key)
            } else {
                curr_key.cmp(&prev_key)
            };

            match cmp_result {
                std::cmp::Ordering::Greater => {
                    violation_found = true;
                    violations.push((count, prev_key.clone(), curr_key.clone()));
                    if verbose {
                        println!(
                            "Violation at position {}:\n\
                            Previous: key={:?}, \n\
                            Current:  key={:?},\n\
                            Column {} violation",
                            count, prev_key, curr_key, col_idx
                        );
                    }
                    break;
                }
                std::cmp::Ordering::Less => break,
                std::cmp::Ordering::Equal => continue,
            }
        }

        if !violation_found {
            prev_key = curr_key;
        } else {
            break;
        }
    }

    if verbose {
        if violations.is_empty() {
            println!("Store is correctly sorted! ({} records verified)", count);
        } else {
            println!("Found {} violations in {} records", violations.len(), count);
        }
    }

    violations
}

// Version that collects all violations
fn verify_sorted_store_full_bss<T: MemPool>(
    store: Arc<BigSortedRunStore<T>>,
    sort_cols: &[(usize, bool, bool)],
    verbose: bool,
    num_threads: usize,
) -> Vec<(usize, Vec<u8>, Vec<u8>)> {
    let mut violations = Vec::new();
    let mut scanner = store.scan();
    let total_tuples: usize = env::var("NUM_TUPLES")
        .unwrap_or_else(|_| 6005720.to_string())
        .parse()
        .expect("NUM_TUPLES must be a valid number");

    // Get first record
    let first = match scanner.next() {
        Some(kv) => kv,
        None => return violations, // Empty store has no violations
    };

    let mut prev_key = first.clone().0;
    let mut count = 1;

    if count == 1 {
        if verbose {
            println!("{:?}", first.0)
        };
    }

    let mut last: Vec<u8> = Vec::new();

    while let Some((curr_key, _curr_value)) = scanner.next() {
        if count % (total_tuples / num_threads) == 0 {
            if verbose {
                println!("{:?}", curr_key.clone())
            };
        }

        count += 1;
        let mut violation_found = false;

        for &(col_idx, asc, _nulls_first) in sort_cols {
            let cmp_result = if asc {
                // println!("prev_value {:?} curr_value {:?}", prev_key, curr_key);
                prev_key.cmp(&curr_key.clone())
            } else {
                curr_key.clone().cmp(&prev_key)
            };

            match cmp_result {
                std::cmp::Ordering::Greater => {
                    violation_found = true;
                    violations.push((count, prev_key.clone(), curr_key.clone()));
                    if verbose {
                        println!(
                            "Violation at position {}:\n\
                            Previous: key={:?}, \n\
                            Current:  key={:?},\n\
                            Column {} violation",
                            count, prev_key, curr_key, col_idx
                        );
                    }
                    break;
                }
                std::cmp::Ordering::Less => break,
                std::cmp::Ordering::Equal => continue,
            }
        }

        if !violation_found {
            prev_key = curr_key.clone();
        } else {
            break;
        }
        last = curr_key.clone();
    }

    if count == total_tuples {
        if verbose {
            println!("{:?}", last)
        };
    }

    if verbose {
        if violations.is_empty() {
            println!("Store is correctly sorted! ({} records verified)", count);
        } else {
            println!("Found {} violations in {} records", violations.len(), count);
        }
    }

    violations
}
#[cfg(test)]
mod tests {
    use super::*;

    mod append_only_kv_page {
        use super::*;

        #[test]
        fn test_page_initialization() {
            let mut page = Page::new_empty();
            page.init();

            assert_eq!(page.total_bytes_used(), PAGE_HEADER_SIZE as u16);
            assert_eq!(page.slot_count(), 0);
            assert_eq!(
                page.total_free_space(),
                (AVAILABLE_PAGE_SIZE - PAGE_HEADER_SIZE) as u16
            );
            assert_eq!(page.next_page(), None);
        }

        #[test]
        fn test_set_next_page() {
            let mut page = Page::new_empty();
            page.set_next_page(123, 456);

            assert_eq!(page.next_page(), Some((123, 456)));
        }

        #[test]
        fn test_slot_handling() {
            let mut page = Page::new_empty();
            page.init();

            let slot = Slot::new(100, 50, 100);
            page.append_slot(&slot);

            assert_eq!(page.slot_count(), 1);
            assert_eq!(page.slot(0).unwrap().offset(), 100);
            assert_eq!(page.slot(0).unwrap().size(), 150);
        }

        #[test]
        fn test_kv_append() {
            let mut page = Page::new_empty();
            page.init();

            let key = vec![1, 2, 3];
            let val = vec![4, 5, 6];

            let success = page.append(&key, &val);

            assert!(success);
            assert_eq!(page.get_key(0), key.as_slice());
            assert_eq!(page.get_val(0), val.as_slice());
            assert_eq!(page.slot_count(), 1);
            assert_eq!(
                page.total_bytes_used(),
                (PAGE_HEADER_SIZE + SLOT_SIZE + key.len() + val.len()) as u16
            );
        }

        #[test]
        fn test_record_append_failure_due_to_size() {
            let mut page = Page::new_empty();
            page.init();

            let key = vec![0; AVAILABLE_PAGE_SIZE + 1]; // Exceeding available page size
            let val = vec![0; 1];
            let success = page.append(&key, &val);

            assert!(!success);
            assert_eq!(page.slot_count(), 0); // No slots should have been added
        }

        #[test]
        fn test_get_invalid_slot() {
            let page = Page::new_empty();
            let result = std::panic::catch_unwind(|| {
                page.get_val(0); // Should panic because slot_id 0 is invalid without any appends
            });

            assert!(result.is_err());
        }
    }

    mod sort_buffer {
        use fbtree::{
            bp::{get_test_bp, ContainerKey},
            random::gen_random_permutation,
        };

        use super::*;

        fn c_key() -> ContainerKey {
            ContainerKey::new(0, 0)
        }

        #[test]
        fn test_sort_buffer_append() {
            let bp = get_test_bp(10);
            let c_key = c_key();

            let policy = Arc::new(MemoryPolicy::FixedSizeLimit(16));
            let sort_cols = vec![(0, true, true)];

            let mut sort_buffer = SortBuffer::new(&bp, c_key, &policy, sort_cols);

            let tuple = Tuple::from_fields(vec![0.into(), 1.into(), 2.into(), 3.into()]);
            let success = sort_buffer.append(&tuple);

            assert!(success);
            assert_eq!(sort_buffer.ptrs.len(), 1);
            assert_eq!(sort_buffer.current_page_idx, 0);
        }

        #[test]
        fn test_sort_buffer_append_to_next_page() {
            let bp = get_test_bp(10);
            let c_key = c_key();

            let sort_cols = vec![(0, true, true)];
            let buffer_size = 10;
            let policy = Arc::new(MemoryPolicy::FixedSizeLimit(buffer_size));
            let mut sort_buffer = SortBuffer::new(&bp, c_key, &policy, sort_cols);

            // Keep appending until the all the pages are full.
            let tuple = Tuple::from_fields(vec![0.into(), 1.into(), 2.into(), 3.into()]);
            loop {
                let success = sort_buffer.append(&tuple);
                if !success {
                    break;
                }
            }

            assert_eq!(sort_buffer.current_page_idx, buffer_size - 1);
        }

        #[test]
        fn test_sort_buffer_sort() {
            let bp = get_test_bp(10);
            let c_key = c_key();

            let sort_cols = vec![(0, true, true)];
            let buffer_size = 10;
            let policy = Arc::new(MemoryPolicy::FixedSizeLimit(buffer_size));
            let mut sort_buffer = SortBuffer::new(&bp, c_key, &policy, sort_cols);

            // Keep appending until the all the pages are full.
            let mut tuples = Vec::new();
            let num_tuples = 500;
            for i in 0..num_tuples {
                let tuple = Tuple::from_fields(vec![i.into(), 1.into(), 2.into(), 3.into()]);
                tuples.push(tuple);
            }
            let tuples = gen_random_permutation(tuples);

            for tuple in tuples {
                let success = sort_buffer.append(&tuple);
                assert!(success);
            }

            sort_buffer.sort();

            let mut result = Vec::new();
            let iter = SortBufferIter::new(&sort_buffer);
            for (_k, v) in iter {
                let val = Tuple::from_bytes(v);
                result.push(val);
            }

            assert_eq!(result.len(), num_tuples as usize);

            for (i, t) in result.iter().enumerate() {
                assert_eq!(t.get(0), &(i as i64).into());
                assert_eq!(t.get(1), &1.into());
                assert_eq!(t.get(2), &2.into());
                assert_eq!(t.get(3), &3.into());
            }
        }

        #[test]
        fn test_sort_buffer_reuse() {
            let bp = get_test_bp(10);
            let c_key = c_key();

            let sort_cols = vec![(0, true, true)];
            let buffer_size = 10;
            let policy = Arc::new(MemoryPolicy::FixedSizeLimit(buffer_size));
            let mut sort_buffer = SortBuffer::new(&bp, c_key, &policy, sort_cols);

            // Dataset 1. Insert tuples to the sort buffer and sort them.
            let mut tuples_1 = Vec::new();
            let num_tuples = 500;
            for i in 0..num_tuples {
                let tuple = Tuple::from_fields(vec![i.into(), 1.into(), 2.into(), 3.into()]);
                tuples_1.push(tuple);
            }
            let tuples_1 = gen_random_permutation(tuples_1);

            for tuple in tuples_1 {
                let success = sort_buffer.append(&tuple);
                assert!(success);
            }

            sort_buffer.sort();

            let mut result = Vec::new();
            let iter = SortBufferIter::new(&sort_buffer);
            for (_k, v) in iter {
                let val = Tuple::from_bytes(v);
                result.push(val);
            }

            assert_eq!(result.len(), num_tuples as usize);

            for (i, t) in result.iter().enumerate() {
                assert_eq!(t.get(0), &(i as i64).into());
                assert_eq!(t.get(1), &1.into());
                assert_eq!(t.get(2), &2.into());
                assert_eq!(t.get(3), &3.into());
            }

            // Check sort buffer is reset properly
            sort_buffer.reset();
            let mut result = Vec::new();
            let iter = SortBufferIter::new(&sort_buffer);
            for (_k, v) in iter {
                let val = Tuple::from_bytes(v);
                result.push(val);
            }
            assert!(result.is_empty());

            // Dataset 2
            let mut tuples_2 = Vec::new();
            for i in 0..num_tuples {
                let tuple =
                    Tuple::from_fields(vec![(i + num_tuples).into(), 1.into(), 2.into(), 3.into()]);
                tuples_2.push(tuple);
            }
            let tuples_2 = gen_random_permutation(tuples_2);

            for tuple in tuples_2 {
                let success = sort_buffer.append(&tuple);
                assert!(success);
            }

            sort_buffer.sort();

            let mut result = Vec::new();
            let iter = SortBufferIter::new(&sort_buffer);
            for (_k, v) in iter {
                let val = Tuple::from_bytes(v);
                result.push(val);
            }

            assert_eq!(result.len(), num_tuples as usize);

            for (i, t) in result.iter().enumerate() {
                assert_eq!(t.get(0), &(i as i64 + num_tuples).into());
                assert_eq!(t.get(1), &1.into());
                assert_eq!(t.get(2), &2.into());
                assert_eq!(t.get(3), &3.into());
            }
        }
    }

    mod external_sort {
        use fbtree::{
            bp::{get_test_bp, BufferPool, ContainerKey},
            prelude::AppendOnlyStore,
            random::{gen_random_permutation, RandomKVs},
            txn_storage::InMemStorage,
        };

        use crate::{
            executor::{ondisk_pipeline::PScanIter, TupleBufferIter},
            prelude::{ColumnDef, DataType, Schema},
        };

        use super::*;

        fn get_c_key(c_id: u16) -> ContainerKey {
            ContainerKey::new(0, c_id)
        }

        #[test]
        fn test_merge() {
            // Generate three foster btrees
            let bp = get_test_bp(100);
            let mut runs = Vec::new();
            let num_runs = 3;
            let kvs = RandomKVs::new(true, true, num_runs, 3000, 50, 100, 100);
            for (i, kv) in kvs.iter().enumerate() {
                let c_key = get_c_key(i as u16);
                // Each kv is sorted so we can bulk insert them
                let tree = Arc::new(AppendOnlyStore::bulk_insert_create(
                    c_key,
                    bp.clone(),
                    kv.iter(),
                ));
                runs.push(tree);
            }

            // Merge the runs and check if they contain the same kvs
            let merge = MergeIter::new(runs.iter().map(|r| r.scan()).collect());
            let mut result = Vec::new();
            for (k, v) in merge {
                result.push((k, v));
            }

            let mut expected = Vec::new();
            for kv in kvs.iter() {
                for (k, v) in kv.iter() {
                    expected.push((k.clone(), v.clone()));
                }
            }
            expected.sort();

            assert_eq!(result.len(), expected.len());
            println!("result len: {}", result.len());
            println!("expected len: {}", expected.len());

            for (i, (k, v)) in result.iter().enumerate() {
                assert_eq!(k, &expected[i].0);
                assert_eq!(v, &expected[i].1);
            }
        }

        #[test]
        fn test_sort_verifier_with_multiple_columns() {
            // Create an append-only store with random key-value pairs
            let bp = get_test_bp(100);
            let c_key = get_c_key(0);
            let num_kvs = 10000;
            let append_only_store = Arc::new(AppendOnlyStore::new(c_key, bp.clone()));
            let keys = gen_random_permutation((0..num_kvs).collect::<Vec<_>>());
            let mut expected = Vec::new();
            for k in keys {
                let tuple =
                    Tuple::from_fields(vec![k.into(), (k % 10).into(), (k % 100).into(), 3.into()]);
                append_only_store.append(&[], &tuple.to_bytes()).unwrap();
                expected.push(tuple);
            }

            let schema = Arc::new(Schema::new(
                vec![
                    ColumnDef::new("col1", DataType::Int, false),
                    ColumnDef::new("col2", DataType::Int, false),
                    ColumnDef::new("col3", DataType::Int, false),
                    ColumnDef::new("col4", DataType::Int, false),
                ],
                vec![],
            ));

            // Scan the append-only store with the scan operator
            let scan =
                PScanIter::<InMemStorage, BufferPool>::new(schema.clone(), 0, (0..4).collect());

            let mut context = HashMap::new();
            context.insert(
                0,
                Arc::new(OnDiskBuffer::AppendOnlyStore(append_only_store)),
            );

            // Sort iterator
            let dest_c_key = get_c_key(1);
            let sort_cols = vec![
                (1, true, true), // Sort by the second column
                (0, true, true), // Then by the first column
                (2, true, true), // Finally by the third column
            ];
            let policy = Arc::new(MemoryPolicy::FixedSizeLimit(10)); // Use 10 pages for the sort buffer
            let mut external_sort = OnDiskSort::new(
                schema.clone(),
                NonBlockingOp::Scan(scan),
                sort_cols.clone(),
                10,
            );

            let final_run = external_sort
                .execute(&context, &policy, &bp, dest_c_key)
                .unwrap();

            let mut result = Vec::new();
            let iter = final_run.iter();
            while let Some(t) = iter.next().unwrap() {
                result.push(t);
            }

            // Verify that the result is correctly sorted
            // if let Err(error) = verify_sorted_results(&result, &sort_cols) {
            //     panic!("Sort verification failed: {}", error);
            // }

            expected.sort_by_key(|t| t.to_normalized_key_bytes(&sort_cols));

            assert_eq!(result.len(), expected.len());

            for (i, t) in result.iter().enumerate() {
                assert_eq!(t, &expected[i]);
            }
        }

        // XTX this is failing even without parallel
        #[test]
        fn test_sort_verifier_with_strings_and_integers() {
            // Create an append-only store with random key-value pairs
            let bp = get_test_bp(100);
            let c_key = get_c_key(0);
            let num_kvs = 10000;
            let append_only_store = Arc::new(AppendOnlyStore::new(c_key, bp.clone()));
            let mut expected = Vec::new();
            for i in 0..num_kvs {
                let tuple = Tuple::from_fields(vec![
                    (i % 100).into(),                     // First column: Int
                    format!("str{}", num_kvs - i).into(), // Second column: String
                    (i / 100).into(),                     // Third column: Int
                ]);
                append_only_store.append(&[], &tuple.to_bytes()).unwrap();
                expected.push(tuple);
            }

            let schema = Arc::new(Schema::new(
                vec![
                    ColumnDef::new("col1", DataType::Int, false),
                    ColumnDef::new("col2", DataType::String, false),
                    ColumnDef::new("col3", DataType::Int, false),
                ],
                vec![],
            ));

            // Scan the append-only store with the scan operator
            let scan =
                PScanIter::<InMemStorage, BufferPool>::new(schema.clone(), 0, (0..3).collect());

            let mut context = HashMap::new();
            context.insert(
                0,
                Arc::new(OnDiskBuffer::AppendOnlyStore(append_only_store)),
            );

            // Sort iterator
            let dest_c_key = get_c_key(1);
            let sort_cols = vec![
                (1, true, true), // Sort by the string column
                (2, true, true), // Then by the third integer column
                (0, true, true), // Finally by the first integer column
            ];
            let policy = Arc::new(MemoryPolicy::FixedSizeLimit(10)); // Use 10 pages for the sort buffer
            let mut external_sort = OnDiskSort::new(
                schema.clone(),
                NonBlockingOp::Scan(scan),
                sort_cols.clone(),
                10,
            );

            let final_run = external_sort
                .execute(&context, &policy, &bp, dest_c_key)
                .unwrap();

            let mut result = Vec::new();
            let iter = final_run.iter();
            while let Some(t) = iter.next().unwrap() {
                result.push(t);
            }

            // Verify that the result is correctly sorted
            // if let Err(error) = verify_sorted_results(&result, &sort_cols) {
            //     panic!("Sort verification failed: {}", error);
            // }

            expected.sort_by_key(|t| t.to_normalized_key_bytes(&sort_cols));

            assert_eq!(result.len(), expected.len());

            for (i, t) in result.iter().enumerate() {
                assert_eq!(t, &expected[i]);
            }
        }
    }
}
