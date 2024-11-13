// Append only page

// Page layout:
// 4 byte: next page id
// 4 byte: next frame id
// 2 byte: total bytes used (PAGE_HEADER_SIZE + slots + records)
// 2 byte: slot count
// 2 byte: free space offset

use chrono::format::Item;
use std::cell::UnsafeCell;
use std::time::Instant;
use rayon::{iter, prelude::*};
use core::num;
use std::{
    cmp::{max, min, Reverse},
    collections::{BinaryHeap, HashMap, HashSet},
    sync::Arc,
    sync::Mutex,
};

use crate::{
    error::ExecError,
    executor::TupleBuffer,
    prelude::{Page, PageId, SchemaRef, AVAILABLE_PAGE_SIZE},
    tuple::Tuple,
    ColumnId,
};

<<<<<<< HEAD

use std::sync::atomic::{AtomicU16, Ordering};
use crossbeam::channel::{bounded, unbounded};
use crossbeam::thread;


=======
use fbtree::access_method::sorted_run_store::SortedRunStore;
>>>>>>> sorted_run_store

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
            data_buffer: Vec::new(),
            current_page_idx: 0,
        }
    }

    pub fn reset(&mut self) {
        self.ptrs.clear();
        self.current_page_idx = 0;
        for page in &mut self.data_buffer {
            page.init();
        }
    }

    pub fn append(&mut self, tuple: &Tuple) -> bool {
        let key = tuple.to_normalized_key_bytes(&self.sort_cols);
        let val = tuple.to_bytes();

        if self.data_buffer.is_empty() {
            self.current_page_idx = 0;
            let frame = self
                .mem_pool
                .create_new_page_for_write(self.dest_c_key)
                .unwrap();
            let mut frame =
                unsafe { std::mem::transmute::<FrameWriteGuard, FrameWriteGuard<'static>>(frame) };
            frame.init();
            self.data_buffer.push(frame);
        }

        let page = self.data_buffer.get_mut(self.current_page_idx).unwrap();
        if page.append(&key, &val) {
            self.ptrs
                .push((self.current_page_idx, page.slot_count() - 1));
            true
        } else {
            // If the current page is full, try to use the next page.
            // If the next page is not available, allocate a new page based on the memory policy.
            // If allocation is not possible, return false.
            let next_page_idx = self.current_page_idx + 1;
            if next_page_idx < self.data_buffer.len() {
                self.current_page_idx = next_page_idx;
                let page = self.data_buffer.get_mut(self.current_page_idx).unwrap();
                assert!(page.append(&key, &val), "Record too large to fit in a page");
                self.ptrs
                    .push((self.current_page_idx, page.slot_count() - 1));
                true
            } else {
                assert!(next_page_idx == self.data_buffer.len());
                match self.policy.as_ref() {
                    MemoryPolicy::FixedSizeLimit(max_length) => {
                        if self.data_buffer.len() < *max_length {
                            self.current_page_idx += 1;
                            let frame = self
                                .mem_pool
                                .create_new_page_for_write(self.dest_c_key)
                                .unwrap();
                            let mut frame = unsafe {
                                std::mem::transmute::<FrameWriteGuard, FrameWriteGuard<'static>>(
                                    frame,
                                )
                            };
                            frame.init();
                            self.data_buffer.push(frame);
                            let page = self.data_buffer.get_mut(self.current_page_idx).unwrap();
                            assert!(page.append(&key, &val), "Record too large to fit in a page");
                            self.ptrs
                                .push((self.current_page_idx, page.slot_count() - 1));
                            true
                        } else {
                            false
                        }
                    }
                    _ => unimplemented!("Memory policy is not implemented yet"),
                }
            }
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
    heap: BinaryHeap<Reverse<(Vec<u8>, usize, Vec<u8>)>>,
}

impl<I: Iterator<Item = (Vec<u8>, Vec<u8>)>> MergeIter<I> {
    pub fn new(mut run_iters: Vec<I>) -> Self {
        let mut heap = BinaryHeap::new();
        for (i, iter) in run_iters.iter_mut().enumerate() {
            if let Some((k, v)) = iter.next() {
                heap.push(Reverse((k, i, v)));
            }
        }
        Self { run_iters, heap }
    }
}

impl<I: Iterator<Item = (Vec<u8>, Vec<u8>)>> Iterator for MergeIter<I> {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(Reverse((k, i, v))) = self.heap.pop() {
            let iter = &mut self.run_iters[i];
            if let Some((next_k, next_v)) = iter.next() {
                self.heap.push(Reverse((next_k, i, next_v)));
            }
            Some((k, v))
        } else {
            None
        }
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

    // Single thread
    fn run_generation_old(
        &mut self,
        policy: &Arc<MemoryPolicy>,
        context: &HashMap<PipelineID, Arc<OnDiskBuffer<T, M>>>,
        mem_pool: &Arc<M>,
        dest_c_key: ContainerKey,
    ) -> Result<Vec<Arc<AppendOnlyStore<M>>>, ExecError> {
        // const TEMP_DB_ID: DatabaseId = dest_c_key.db_id;
        const TEMP_DB_ID: DatabaseId = 321; //xtx magic number
        const TEMP_C_ID_BASE: u16 = 321; // xtx magic number
        let mut temp_c_id_counter = TEMP_C_ID_BASE;

        let mut sort_buffer = SortBuffer::new(
            mem_pool,
            ContainerKey::new(TEMP_DB_ID, temp_c_id_counter),
            policy,
            self.sort_cols.clone(),
        );
        let mut result_buffers = Vec::new();

        while let Some(tuple) = self.exec_plan.next(context)? {
            if sort_buffer.append(&tuple) {
                continue;
            } else {
                sort_buffer.sort();
                let quantiles = sort_buffer.sample_quantiles(self.quantiles.num_quantiles);
                self.quantiles.merge(&quantiles);

                let iter = SortBufferIter::new(&sort_buffer);

                // Create temporary container key
                temp_c_id_counter += 1;
                let temp_container_key = ContainerKey {
                    db_id: TEMP_DB_ID,
                    c_id: temp_c_id_counter,
                };

                let output = Arc::new(AppendOnlyStore::bulk_insert_create(
                    temp_container_key,
                    mem_pool.clone(),
                    SortBufferIter::new(&sort_buffer),
                ));
                result_buffers.push(output);

                sort_buffer.reset();
                if !sort_buffer.append(&tuple) {
                    panic!("Record too large to fit in a page");
                }
            }
        }

        // Finally sort and output the remaining records
        sort_buffer.sort();
        // Compute quantiles for the last run
        let quantiles = sort_buffer.sample_quantiles(self.quantiles.num_quantiles);
        self.quantiles.merge(&quantiles);

        let iter = SortBufferIter::new(&sort_buffer);

        // Create temporary container key
        temp_c_id_counter += 1;
        let temp_container_key = ContainerKey {
            db_id: TEMP_DB_ID,
            c_id: temp_c_id_counter,
        };
        let output = Arc::new(AppendOnlyStore::bulk_insert_create(
            temp_container_key,
            mem_pool.clone(),
            SortBufferIter::new(&sort_buffer),
        ));
        result_buffers.push(output);

        Ok(result_buffers)
    }


<<<<<<< HEAD
    // Reads then splits then sorts 
    fn run_generation_1(
        &mut self,
        policy: &Arc<MemoryPolicy>,
        context: &HashMap<PipelineID, Arc<OnDiskBuffer<T, M>>>,
        mem_pool: &Arc<M>,
        dest_c_key: ContainerKey,
    ) -> Result<Vec<Arc<AppendOnlyStore<M>>>, ExecError> {
        const TEMP_DB_ID: DatabaseId = 321; // Magic number for temporary database ID
        const TEMP_C_ID_BASE: u16 = 321;    // Magic number for starting container ID
    
        // Step 1: Collect the tuples into chunks
        let mut chunks = Vec::new();
        let chunk_size = 10000;
        let mut current_chunk = Vec::with_capacity(chunk_size);
    
        while let Some(tuple) = self.exec_plan.next(context)? {
            current_chunk.push(tuple);
            if current_chunk.len() >= chunk_size {
                chunks.push(std::mem::replace(&mut current_chunk, Vec::with_capacity(chunk_size)));
            }
        }
        // Add the last chunk if it's not empty
        if !current_chunk.is_empty() {
            chunks.push(current_chunk);
        }
    
        // Create an atomic counter for container IDs to ensure uniqueness across threads
        let c_id_counter = Arc::new(AtomicU16::new(TEMP_C_ID_BASE));
    
        let num_quantiles = self.quantiles.num_quantiles; // Capture for closure
        let sort_cols = self.sort_cols.clone();
        let policy = policy.clone();
        let mem_pool = mem_pool.clone();
    
        // Step 2: Process each chunk in parallel
        let runs_and_quantiles: Vec<_> = chunks
            .into_par_iter()
            .map(|chunk| {
                let c_id_counter = c_id_counter.clone();
    
                let mut runs = Vec::new();
                let mut run_quantiles = Vec::new();
    
                let mut c_id = c_id_counter.fetch_add(1, Ordering::SeqCst);
                let mut temp_container_key = ContainerKey {
                    db_id: TEMP_DB_ID,
                    c_id,
                };
    
                let mut sort_buffer = SortBuffer::new(
                    &mem_pool,
                    temp_container_key,
                    &policy,
                    sort_cols.clone(),
                );
    
                for tuple in chunk {
                    if sort_buffer.append(&tuple) {
                        continue;
                    } else {
                        // Process the current buffer
                        sort_buffer.sort();
                        let quantiles = sort_buffer.sample_quantiles(num_quantiles);
                        run_quantiles.push(quantiles);
    
                        // Generate the run (sorted chunk) and write it back to disk
                        let output = Arc::new(AppendOnlyStore::bulk_insert_create(
                            temp_container_key,
                            mem_pool.clone(),
                            SortBufferIter::new(&sort_buffer),
                        ));
    
                        runs.push(output);
    
                        // Prepare for next run
                        sort_buffer.reset();
    
                        // Generate a new container key for the next run
                        c_id = c_id_counter.fetch_add(1, Ordering::SeqCst);
                        temp_container_key = ContainerKey {
                            db_id: TEMP_DB_ID,
                            c_id,
                        };
                        sort_buffer.set_dest_c_key(temp_container_key);
    
                        // Try appending the tuple again
                        if !sort_buffer.append(&tuple) {
                            panic!("Record too large to fit in a page");
                        }
                    }
                }
    
                // After the loop, process any remaining tuples in the sort_buffer
                if !sort_buffer.ptrs.is_empty() {
                    sort_buffer.sort();
                    let quantiles = sort_buffer.sample_quantiles(num_quantiles);
                    run_quantiles.push(quantiles);
    
                    let output = Arc::new(AppendOnlyStore::bulk_insert_create(
                        temp_container_key,
                        mem_pool.clone(),
                        SortBufferIter::new(&sort_buffer),
                    ));
    
                    runs.push(output);
                }
    
                // Merge the quantiles from runs within this chunk
                let mut chunk_quantiles = SingleRunQuantiles::new(num_quantiles);
                for q in run_quantiles {
                    chunk_quantiles.merge(&q);
                }
    
                (runs, chunk_quantiles)
            })
            .collect();
    
        // Step 3: Collect the runs and merge quantiles
        let mut result_buffers = Vec::new();
        let mut total_quantiles = SingleRunQuantiles::new(self.quantiles.num_quantiles);
    
        for (chunk_runs, quantiles) in runs_and_quantiles {
            result_buffers.extend(chunk_runs);
            total_quantiles.merge(&quantiles);
        }
        self.quantiles = total_quantiles; // Update the global quantiles
    
        Ok(result_buffers)
    }

    // Reads and sorts concurrently
    fn run_generation_2(
        &mut self,
        policy: &Arc<MemoryPolicy>,
        context: &HashMap<PipelineID, Arc<OnDiskBuffer<T, M>>>,
        mem_pool: &Arc<M>,
        dest_c_key: ContainerKey,
    ) -> Result<Vec<Arc<AppendOnlyStore<M>>>, ExecError> {
        let num_threads = 1;
        const TEMP_DB_ID: DatabaseId = 321; // Magic number for temporary database ID
        const TEMP_C_ID_BASE: u16 = 321;    // Magic number for starting container ID
    
        // Step 1: Set up channels
        let (chunk_sender, chunk_receiver) = bounded::<Vec<Tuple>>(num_threads * 2);
        let (result_sender, result_receiver) = unbounded::<(Vec<Arc<AppendOnlyStore<M>>>, SingleRunQuantiles)>();
    
        // Step 2: Create an atomic counter for container IDs
        let c_id_counter = Arc::new(AtomicU16::new(TEMP_C_ID_BASE));
    
        let num_quantiles = self.quantiles.num_quantiles; // Capture for closure
        let sort_cols = self.sort_cols.clone();
        let policy = policy.clone();
        let mem_pool = mem_pool.clone();
    
        thread::scope(|s| -> Result<(), ExecError> {
            // Spawn worker threads
            for _ in 0..num_threads {
                let chunk_receiver = chunk_receiver.clone();
                let result_sender = result_sender.clone();
                let c_id_counter = c_id_counter.clone();
                let sort_cols = sort_cols.clone();
                let policy = policy.clone();
                let mem_pool = mem_pool.clone();
    
                s.spawn(move |_| {
                    // Worker thread code
                    while let Ok(chunk) = chunk_receiver.recv() {
                        // Process the chunk
                        let mut runs = Vec::new();
                        let mut run_quantiles = Vec::new();
    
                        let mut c_id = c_id_counter.fetch_add(1, Ordering::SeqCst);
                        let mut temp_container_key = ContainerKey {
                            db_id: TEMP_DB_ID,
                            c_id,
                        };
    
                        let mut sort_buffer = SortBuffer::new(
                            &mem_pool,
                            temp_container_key,
                            &policy,
                            sort_cols.clone(),
                        );
    
                        for tuple in chunk {
                            if sort_buffer.append(&tuple) {
                                continue;
                            } else {
                                // Process the current buffer
                                sort_buffer.sort();
                                let quantiles = sort_buffer.sample_quantiles(num_quantiles);
                                run_quantiles.push(quantiles);
    
                                // Generate the run (sorted chunk) and write it back to disk
                                let output = Arc::new(AppendOnlyStore::bulk_insert_create(
                                    temp_container_key,
                                    mem_pool.clone(),
                                    SortBufferIter::new(&sort_buffer),
                                ));
    
                                runs.push(output);
    
                                // Prepare for next run
                                sort_buffer.reset();
    
                                // Generate a new container key for the next run
                                c_id = c_id_counter.fetch_add(1, Ordering::SeqCst);
                                temp_container_key = ContainerKey {
                                    db_id: TEMP_DB_ID,
                                    c_id,
                                };
                                sort_buffer.set_dest_c_key(temp_container_key);
    
                                // Try appending the tuple again
                                if !sort_buffer.append(&tuple) {
                                    panic!("Record too large to fit in a page");
                                }
                            }
                        }
    
                        // After the loop, process any remaining tuples in the sort_buffer
                        if !sort_buffer.ptrs.is_empty() {
                            sort_buffer.sort();
                            let quantiles = sort_buffer.sample_quantiles(num_quantiles);
                            run_quantiles.push(quantiles);
    
                            let output = Arc::new(AppendOnlyStore::bulk_insert_create(
                                temp_container_key,
                                mem_pool.clone(),
                                SortBufferIter::new(&sort_buffer),
                            ));
    
                            runs.push(output);
                        }
    
                        // Merge the quantiles from runs within this chunk
                        let mut chunk_quantiles = SingleRunQuantiles::new(num_quantiles);
                        for q in run_quantiles {
                            chunk_quantiles.merge(&q);
                        }
    
                        // Send the results back
                        if let Err(e) = result_sender.send((runs, chunk_quantiles)) {
                            eprintln!("Failed to send results: {:?}", e);
                            break; // Exit the loop if sending fails
                        }
                    }
                    // `result_sender` will be dropped here
                });
            }
    
            // Drop the main thread's `result_sender` to allow receiver to terminate
            drop(result_sender);
    
            // Step 4: Producer reads data and sends chunks
            let chunk_size = 10_000; //xtx need to figure out the size
            let mut current_chunk = Vec::with_capacity(chunk_size);
    
            while let Some(tuple) = self.exec_plan.next(context)? {
                current_chunk.push(tuple);
                if current_chunk.len() >= chunk_size {
                    chunk_sender.send(current_chunk).unwrap();
                    current_chunk = Vec::with_capacity(chunk_size);
                }
            }
            // Send the last chunk if it's not empty
            if !current_chunk.is_empty() {
                chunk_sender.send(current_chunk).unwrap();
            }
    
            // Close the sender to signal workers that no more chunks are coming
            drop(chunk_sender);
    
            // Wait for workers to finish
            Ok(())
        }).map_err(|e| {
            // Handle any panics that occurred in the threads
            let err_msg = if let Some(s) = e.downcast_ref::<&str>() {
                s.to_string()
            } else if let Some(s) = e.downcast_ref::<String>() {
                s.clone()
            } else {
                format!("Thread panicked with unknown error: {:?}", e)
            };
            ExecError::Pipeline(err_msg)
        })??;
    
        // Step 5: Collect the results
        let mut result_buffers = Vec::new();
        let mut total_quantiles = SingleRunQuantiles::new(self.quantiles.num_quantiles);
    
        for (chunk_runs, quantiles) in result_receiver.iter() {
            result_buffers.extend(chunk_runs);
            total_quantiles.merge(&quantiles);
        }
        self.quantiles = total_quantiles; // Update the global quantiles
    
        Ok(result_buffers)
    }

    // Doesn't store in chunks goes straight into sort buffers
    fn run_generation_3(
        &mut self,
        policy: &Arc<MemoryPolicy>,
        context: &HashMap<PipelineID, Arc<OnDiskBuffer<T, M>>>,
        mem_pool: &Arc<M>,
        dest_c_key: ContainerKey,
    ) -> Result<Vec<Arc<AppendOnlyStore<M>>>, ExecError> {
        use crossbeam::channel::{bounded, unbounded, Receiver, Sender};
        use crossbeam::scope;
        use crossbeam::select;
        use std::sync::{Arc, Mutex};
    
        println!("Starting run_generation_3");
    
        const TEMP_DB_ID: DatabaseId = 321; // Magic number for temporary database ID
        let c_id_counter = AtomicU16::new(321);
    
        let num_quantiles = self.quantiles.num_quantiles;
        let sort_cols = self.sort_cols.clone();
        let policy = Arc::clone(policy);
        let mem_pool = Arc::clone(mem_pool);
    
        // Channels for communication
        let max_threads = 8;
        let (buffer_sender, buffer_receiver) = bounded::<SortBuffer<M>>(max_threads * 2);
        let (result_sender, result_receiver) = unbounded::<(Arc<AppendOnlyStore<M>>, SingleRunQuantiles)>();
    
        // Shared quantiles
        let total_quantiles = Mutex::new(SingleRunQuantiles::new(num_quantiles));
    
        // Use a scoped thread to avoid lifetime issues
        let result_buffers = scope(|s| -> Result<Vec<Arc<AppendOnlyStore<M>>>, ExecError> {
            println!("Inside crossbeam scope");
    
            // Start worker threads
            for i in 0..max_threads {
                println!("Spawning worker thread {}", i);
                let buffer_receiver = buffer_receiver.clone();
                let result_sender = result_sender.clone();
                let mem_pool = &mem_pool; // Borrow mem_pool
                let num_quantiles = num_quantiles; // Copy num_quantiles
    
                s.spawn(move |_| {
                    println!("Worker thread {} started", i);
                    while let Ok(mut sort_buffer) = buffer_receiver.recv() {
                        println!("Worker thread {} received a sort buffer", i);
                        // Process the sort buffer
                        sort_buffer.sort();
                        let quantiles = sort_buffer.sample_quantiles(num_quantiles);
    
                        let output = Arc::new(AppendOnlyStore::bulk_insert_create(
                            sort_buffer.dest_c_key,
                            mem_pool.clone(),
                            SortBufferIter::new(&sort_buffer),
                        ));
    
                        // Send the result
                        if let Err(e) = result_sender.send((output, quantiles)) {
                            eprintln!("Worker thread {} failed to send result: {:?}", i, e);
                            break;
                        }
                    }
                    println!("Worker thread {} finished", i);
                });
            }
    
            println!("making it to here");
    
            // Main thread reads data and fills sort buffers
            let mut current_sort_buffer = SortBuffer::new(
                &mem_pool,
                ContainerKey::new(TEMP_DB_ID, c_id_counter.fetch_add(1, Ordering::SeqCst)),
                &policy,
                sort_cols.clone(),
            );
    
            let mut result_buffers = Vec::new();
    
            println!("Starting main loop");
            loop {
                select! {
                    recv(result_receiver) -> res => {
                        if let Ok((output, quantiles)) = res {
                            println!("Received result from worker thread");
                            result_buffers.push(output);
                            // Merging quantiles
                            {
                                let mut total_q = total_quantiles.lock().unwrap();
                                total_q.merge(&quantiles);
                            }
                        } else {
                            // All senders are dropped; break the loop
                            println!("All result senders dropped");
                            break;
                        }
                    },
                    default => {
                        // Read the next tuple
                        println!("Reading next tuple");
                        match self.exec_plan.next(context)? {
                            Some(tuple) => {
                                println!("Received a tuple");
                                if current_sort_buffer.append(&tuple) {
                                    // Continue appending
                                } else {
                                    // Sort buffer is full, send it to worker threads
                                    let full_sort_buffer = std::mem::replace(
                                        &mut current_sort_buffer,
                                        SortBuffer::new(
                                            &mem_pool,
                                            ContainerKey::new(TEMP_DB_ID, c_id_counter.fetch_add(1, Ordering::SeqCst)),
                                            &policy,
                                            sort_cols.clone(),
                                        ),
                                    );
    
                                    println!("Sort buffer is full; sending to worker threads");
    
                                    if let Err(e) = buffer_sender.send(full_sort_buffer) {
                                        eprintln!("Failed to send sort buffer: {:?}", e);
                                        break;
                                    }
                                }
                            },
                            None => {
                                // No more tuples; send remaining buffer and break
                                println!("No more tuples");
                                if !current_sort_buffer.ptrs.is_empty() {
                                    println!("Sending remaining sort buffer to worker threads");
                                    if let Err(e) = buffer_sender.send(current_sort_buffer) {
                                        eprintln!("Failed to send sort buffer: {:?}", e);
                                    }
                                }
                                drop(buffer_sender); // Close the sender
                                break;
                            }
                        }
                    }
                }
            }
    
            println!("making it to here 2");
    
            // Process remaining results
            for res in result_receiver {
                println!("Processing remaining results");
                let (output, quantiles) = res;
                result_buffers.push(output);
                // Merging quantiles
                {
                    let mut total_q = total_quantiles.lock().unwrap();
                    total_q.merge(&quantiles);
                }
            }
    
            // Update the quantiles
            self.quantiles = total_quantiles.into_inner().unwrap();
    
            println!("I make it here 1");
            // Return the result_buffers without wrapping in Ok(...)
            Ok(result_buffers)
            // The scope ends here, ensuring all threads have completed
        })
        .map_err(|e| {
            // Convert the panic payload (e) into an ExecError
            eprintln!("Scope panicked with error: {:?}", e);
            if let Some(s) = e.downcast_ref::<&str>() {
                ExecError::Pipeline(s.to_string())
            } else if let Some(s) = e.downcast_ref::<String>() {
                ExecError::Pipeline(s.clone())
            } else {
                ExecError::Pipeline("Thread panicked with unknown error".to_string())
            }
        })??; // Use double `?` to unwrap both Results
    
        println!("I make it here ");
        Ok(result_buffers)
    }


    fn run_generation_4(
        &mut self,
        policy: &Arc<MemoryPolicy>,
        context: &HashMap<PipelineID, Arc<OnDiskBuffer<T, M>>>,
        mem_pool: &Arc<M>,
        dest_c_key: ContainerKey,
    ) -> Result<Vec<Arc<AppendOnlyStore<M>>>, ExecError> {
        // Estimate the total number of tuples
        // let total_tuples = self.exec_plan.estimate_num_tuples(context);
        let total_tuples = 6005720;
        println!("Total tuples estimated: {}", total_tuples);
    
        // Decide on the number of threads
        let num_threads = 8;
    
        // Calculate chunk size
        let chunk_size = (total_tuples + num_threads - 1) / num_threads;
    
        // Generate ranges
        let ranges: Vec<(usize, usize)> = (0..num_threads)
            .map(|i| {
                let start = i * chunk_size;
                let end = if i == num_threads - 1 {
                    total_tuples
                } else {
                    (i + 1) * chunk_size
                };
                (start, end)
            })
            .collect();
    
        // Create execution plans for each range
        let plans: Vec<_> = ranges
            .iter()
            .map(|&(start, end)| {
                println!("Creating plan for range {} - {}", start, end);
                self.exec_plan.clone_with_range(start, end)
            })
            .collect::<Vec<_>>();
    
        // Prepare for parallel execution
        let c_id_counter = Arc::new(AtomicU16::new(321)); // Starting container ID
        let num_quantiles = self.quantiles.num_quantiles;
        let sort_cols = self.sort_cols.clone();
        let policy = policy.clone();
        let mem_pool = mem_pool.clone();

         // Start timing for parallel execution
        let parallel_start_time = Instant::now();

        // Process each plan in parallel
       let runs_and_quantiles = plans
        .into_par_iter()
        .enumerate()
        .map(|(thread_index, mut exec_plan)| {
             // Start timing for this thread
            let thread_start_time = Instant::now();
                let c_id_counter = c_id_counter.clone();
                let mut runs = Vec::new();
                let mut run_quantiles = Vec::new();
    
                let mut c_id = c_id_counter.fetch_add(1, Ordering::SeqCst);
                let mut temp_container_key = ContainerKey {
                    db_id: dest_c_key.db_id,
                    c_id,
                };
    
                let mut sort_buffer = SortBuffer::new(
                    &mem_pool,
                    temp_container_key,
                    &policy,
                    sort_cols.clone(),
                );
    
                let mut tuples_processed = 0;

                println!("handling thread {} before while", thread_index);

                while let Some(tuple) = exec_plan.next(context)? {
                    tuples_processed += 1;
                    if sort_buffer.append(&tuple) {
                        continue;
                    } else {
                        // Sort and process the current buffer
                        sort_buffer.sort();
                        let quantiles = sort_buffer.sample_quantiles(num_quantiles);
                        run_quantiles.push(quantiles);
    
                        // Create the run and write it back to disk
                        let output = Arc::new(AppendOnlyStore::bulk_insert_create(
                            temp_container_key,
                            mem_pool.clone(),
                            SortBufferIter::new(&sort_buffer),
                        ));
                        runs.push(output);
    
                        // Reset for the next run
                        sort_buffer.reset();
                        c_id = c_id_counter.fetch_add(1, Ordering::SeqCst);
                        temp_container_key = ContainerKey {
                            db_id: dest_c_key.db_id,
                            c_id,
                        };
                        sort_buffer.set_dest_c_key(temp_container_key);
    
                        // Try appending the tuple again
                        if !sort_buffer.append(&tuple) {
                            panic!("Record too large to fit in a page");
                        }
                    }
                }
    
                // Process any remaining tuples in the sort_buffer
                if !sort_buffer.ptrs.is_empty() {
                    sort_buffer.sort();
                    let quantiles = sort_buffer.sample_quantiles(num_quantiles);
                    run_quantiles.push(quantiles);
    
                    let output = Arc::new(AppendOnlyStore::bulk_insert_create(
                        temp_container_key,
                        mem_pool.clone(),
                        SortBufferIter::new(&sort_buffer),
                    ));
                    runs.push(output);
                }
    
               let thread_duration = thread_start_time.elapsed();
                println!(
                    "Thread {} processed {} tuples, generated {} runs in {:?}",
                    thread_index,
                    tuples_processed,
                    runs.len(),
                    thread_duration
                ); 
                // Merge quantiles within this thread
                let mut chunk_quantiles = SingleRunQuantiles::new(num_quantiles);
                for q in run_quantiles {
                    chunk_quantiles.merge(&q);
                }
    
                Ok((runs, chunk_quantiles))
            })
            .collect::<Result<Vec<_>, ExecError>>()?;
        let parallel_duration = parallel_start_time.elapsed();
         println!("Parallel execution took {:?}", parallel_duration);
        // Collect runs and merge quantiles
        let mut result_buffers = Vec::new();
        let mut total_quantiles = SingleRunQuantiles::new(self.quantiles.num_quantiles);
    
        for (chunk_runs, quantiles) in runs_and_quantiles {
            result_buffers.extend(chunk_runs);
            total_quantiles.merge(&quantiles);
        }
        self.quantiles = total_quantiles; // Update global quantiles
    
        Ok(result_buffers)
    }
    
    
    
    
=======
    fn run_generation_sorted_store(
        &mut self,
        policy: &Arc<MemoryPolicy>,
        context: &HashMap<PipelineID, Arc<OnDiskBuffer<T, M>>>,
        mem_pool: &Arc<M>,   
        dest_c_key: ContainerKey,
    ) -> Result<Vec<Arc<SortedRunStore<M>>>, ExecError> {
        // const TEMP_DB_ID: DatabaseId = dest_c_key.db_id;
        const TEMP_DB_ID: DatabaseId = 321; //xtx magic number
        const TEMP_C_ID_BASE: u16 = 321; // xtx magic number
        let mut temp_c_id_counter = TEMP_C_ID_BASE;

        let mut sort_buffer = SortBuffer::new(
            mem_pool,
            ContainerKey::new(TEMP_DB_ID, temp_c_id_counter),
            policy,
            self.sort_cols.clone(),
        );
        let mut result_buffers: Vec<Arc<SortedRunStore<M>>> = Vec::new();

        while let Some(tuple) = self.exec_plan.next(context)? {
            if sort_buffer.append(&tuple) {
                continue;
            } else {
                sort_buffer.sort();
                let quantiles = sort_buffer.sample_quantiles(self.quantiles.num_quantiles);
                self.quantiles.merge(&quantiles);

                let iter = SortBufferIter::new(&sort_buffer);

                // Create temporary container key
                temp_c_id_counter += 1;
                let temp_container_key = ContainerKey {
                    db_id: TEMP_DB_ID,
                    c_id: temp_c_id_counter,
                };

                let output = Arc::new(SortedRunStore::new(
                    temp_container_key,
                    mem_pool.clone(),
                    SortBufferIter::new(&sort_buffer),
                ));
                result_buffers.push(output);

                sort_buffer.reset();
                if !sort_buffer.append(&tuple) {
                    panic!("Record too large to fit in a page");
                }
            }
        }

        // Finally sort and output the remaining records
        sort_buffer.sort();
        // Compute quantiles for the last run
        let quantiles = sort_buffer.sample_quantiles(self.quantiles.num_quantiles);
        self.quantiles.merge(&quantiles);

        let iter = SortBufferIter::new(&sort_buffer);

        // Create temporary container key
        temp_c_id_counter += 1;
        let temp_container_key = ContainerKey {
            db_id: TEMP_DB_ID,
            c_id: temp_c_id_counter,
        };
        let output = Arc::new(SortedRunStore::new(
            temp_container_key,
            mem_pool.clone(),
            SortBufferIter::new(&sort_buffer),
        ));
        result_buffers.push(output);

        Ok(result_buffers)
    }
>>>>>>> sorted_run_store


    fn run_generation_sorted_store(
        &mut self,
        policy: &Arc<MemoryPolicy>,
        context: &HashMap<PipelineID, Arc<OnDiskBuffer<T, M>>>,
        mem_pool: &Arc<M>,   
        dest_c_key: ContainerKey,
    ) -> Result<Vec<Arc<SortedRunStore<M>>>, ExecError> {
        // const TEMP_DB_ID: DatabaseId = dest_c_key.db_id;
        const TEMP_DB_ID: DatabaseId = 321; //xtx magic number
        const TEMP_C_ID_BASE: u16 = 321; // xtx magic number
        let mut temp_c_id_counter = TEMP_C_ID_BASE;

        let mut sort_buffer = SortBuffer::new(
            mem_pool,
            ContainerKey::new(TEMP_DB_ID, temp_c_id_counter),
            policy,
            self.sort_cols.clone(),
        );
        let mut result_buffers: Vec<Arc<SortedRunStore<M>>> = Vec::new();

        while let Some(tuple) = self.exec_plan.next(context)? {
            if sort_buffer.append(&tuple) {
                continue;
            } else {
                sort_buffer.sort();
                let quantiles = sort_buffer.sample_quantiles(self.quantiles.num_quantiles);
                self.quantiles.merge(&quantiles);

                let iter = SortBufferIter::new(&sort_buffer);

                // Create temporary container key
                temp_c_id_counter += 1;
                let temp_container_key = ContainerKey {
                    db_id: TEMP_DB_ID,
                    c_id: temp_c_id_counter,
                };

                let output = Arc::new(SortedRunStore::new(
                    temp_container_key,
                    mem_pool.clone(),
                    SortBufferIter::new(&sort_buffer),
                ));
                result_buffers.push(output);

                sort_buffer.reset();
                if !sort_buffer.append(&tuple) {
                    panic!("Record too large to fit in a page");
                }
            }
        }

        // Finally sort and output the remaining records
        sort_buffer.sort();
        // Compute quantiles for the last run
        let quantiles = sort_buffer.sample_quantiles(self.quantiles.num_quantiles);
        self.quantiles.merge(&quantiles);

        let iter = SortBufferIter::new(&sort_buffer);

        // Create temporary container key
        temp_c_id_counter += 1;
        let temp_container_key = ContainerKey {
            db_id: TEMP_DB_ID,
            c_id: temp_c_id_counter,
        };
        let output = Arc::new(SortedRunStore::new(
            temp_container_key,
            mem_pool.clone(),
            SortBufferIter::new(&sort_buffer),
        ));
        result_buffers.push(output);

        Ok(result_buffers)
    }


    // Reads then splits then sorts 
    fn run_generation_1(
        &mut self,
        policy: &Arc<MemoryPolicy>,
        context: &HashMap<PipelineID, Arc<OnDiskBuffer<T, M>>>,
        mem_pool: &Arc<M>,
        dest_c_key: ContainerKey,
    ) -> Result<Vec<Arc<AppendOnlyStore<M>>>, ExecError> {
        const TEMP_DB_ID: DatabaseId = 321; // Magic number for temporary database ID
        const TEMP_C_ID_BASE: u16 = 321;    // Magic number for starting container ID
    
        // Step 1: Collect the tuples into chunks
        let mut chunks = Vec::new();
        let chunk_size = 10000;
        let mut current_chunk = Vec::with_capacity(chunk_size);
    
        while let Some(tuple) = self.exec_plan.next(context)? {
            current_chunk.push(tuple);
            if current_chunk.len() >= chunk_size {
                chunks.push(std::mem::replace(&mut current_chunk, Vec::with_capacity(chunk_size)));
            }
        }
        // Add the last chunk if it's not empty
        if !current_chunk.is_empty() {
            chunks.push(current_chunk);
        }
    
        // Create an atomic counter for container IDs to ensure uniqueness across threads
        let c_id_counter = Arc::new(AtomicU16::new(TEMP_C_ID_BASE));
    
        let num_quantiles = self.quantiles.num_quantiles; // Capture for closure
        let sort_cols = self.sort_cols.clone();
        let policy = policy.clone();
        let mem_pool = mem_pool.clone();
    
        // Step 2: Process each chunk in parallel
        let runs_and_quantiles: Vec<_> = chunks
            .into_par_iter()
            .map(|chunk| {
                let c_id_counter = c_id_counter.clone();
    
                let mut runs = Vec::new();
                let mut run_quantiles = Vec::new();
    
                let mut c_id = c_id_counter.fetch_add(1, Ordering::SeqCst);
                let mut temp_container_key = ContainerKey {
                    db_id: TEMP_DB_ID,
                    c_id,
                };
    
                let mut sort_buffer = SortBuffer::new(
                    &mem_pool,
                    temp_container_key,
                    &policy,
                    sort_cols.clone(),
                );
    
                for tuple in chunk {
                    if sort_buffer.append(&tuple) {
                        continue;
                    } else {
                        // Process the current buffer
                        sort_buffer.sort();
                        let quantiles = sort_buffer.sample_quantiles(num_quantiles);
                        run_quantiles.push(quantiles);
    
                        // Generate the run (sorted chunk) and write it back to disk
                        let output = Arc::new(AppendOnlyStore::bulk_insert_create(
                            temp_container_key,
                            mem_pool.clone(),
                            SortBufferIter::new(&sort_buffer),
                        ));
    
                        runs.push(output);
    
                        // Prepare for next run
                        sort_buffer.reset();
    
                        // Generate a new container key for the next run
                        c_id = c_id_counter.fetch_add(1, Ordering::SeqCst);
                        temp_container_key = ContainerKey {
                            db_id: TEMP_DB_ID,
                            c_id,
                        };
                        sort_buffer.set_dest_c_key(temp_container_key);
    
                        // Try appending the tuple again
                        if !sort_buffer.append(&tuple) {
                            panic!("Record too large to fit in a page");
                        }
                    }
                }
    
                // After the loop, process any remaining tuples in the sort_buffer
                if !sort_buffer.ptrs.is_empty() {
                    sort_buffer.sort();
                    let quantiles = sort_buffer.sample_quantiles(num_quantiles);
                    run_quantiles.push(quantiles);
    
                    let output = Arc::new(AppendOnlyStore::bulk_insert_create(
                        temp_container_key,
                        mem_pool.clone(),
                        SortBufferIter::new(&sort_buffer),
                    ));
    
                    runs.push(output);
                }
    
                // Merge the quantiles from runs within this chunk
                let mut chunk_quantiles = SingleRunQuantiles::new(num_quantiles);
                for q in run_quantiles {
                    chunk_quantiles.merge(&q);
                }
    
                (runs, chunk_quantiles)
            })
            .collect();
    
        // Step 3: Collect the runs and merge quantiles
        let mut result_buffers = Vec::new();
        let mut total_quantiles = SingleRunQuantiles::new(self.quantiles.num_quantiles);
    
        for (chunk_runs, quantiles) in runs_and_quantiles {
            result_buffers.extend(chunk_runs);
            total_quantiles.merge(&quantiles);
        }
        self.quantiles = total_quantiles; // Update the global quantiles
    
        Ok(result_buffers)
    }

    // Reads and sorts concurrently
    fn run_generation_2(
        &mut self,
        policy: &Arc<MemoryPolicy>,
        context: &HashMap<PipelineID, Arc<OnDiskBuffer<T, M>>>,
        mem_pool: &Arc<M>,
        dest_c_key: ContainerKey,
    ) -> Result<Vec<Arc<AppendOnlyStore<M>>>, ExecError> {
        let num_threads = 1;
        const TEMP_DB_ID: DatabaseId = 321; // Magic number for temporary database ID
        const TEMP_C_ID_BASE: u16 = 321;    // Magic number for starting container ID
    
        // Step 1: Set up channels
        let (chunk_sender, chunk_receiver) = bounded::<Vec<Tuple>>(num_threads * 2);
        let (result_sender, result_receiver) = unbounded::<(Vec<Arc<AppendOnlyStore<M>>>, SingleRunQuantiles)>();
    
        // Step 2: Create an atomic counter for container IDs
        let c_id_counter = Arc::new(AtomicU16::new(TEMP_C_ID_BASE));
    
        let num_quantiles = self.quantiles.num_quantiles; // Capture for closure
        let sort_cols = self.sort_cols.clone();
        let policy = policy.clone();
        let mem_pool = mem_pool.clone();
    
        thread::scope(|s| -> Result<(), ExecError> {
            // Spawn worker threads
            for _ in 0..num_threads {
                let chunk_receiver = chunk_receiver.clone();
                let result_sender = result_sender.clone();
                let c_id_counter = c_id_counter.clone();
                let sort_cols = sort_cols.clone();
                let policy = policy.clone();
                let mem_pool = mem_pool.clone();
    
                s.spawn(move |_| {
                    // Worker thread code
                    while let Ok(chunk) = chunk_receiver.recv() {
                        // Process the chunk
                        let mut runs = Vec::new();
                        let mut run_quantiles = Vec::new();
    
                        let mut c_id = c_id_counter.fetch_add(1, Ordering::SeqCst);
                        let mut temp_container_key = ContainerKey {
                            db_id: TEMP_DB_ID,
                            c_id,
                        };
    
                        let mut sort_buffer = SortBuffer::new(
                            &mem_pool,
                            temp_container_key,
                            &policy,
                            sort_cols.clone(),
                        );
    
                        for tuple in chunk {
                            if sort_buffer.append(&tuple) {
                                continue;
                            } else {
                                // Process the current buffer
                                sort_buffer.sort();
                                let quantiles = sort_buffer.sample_quantiles(num_quantiles);
                                run_quantiles.push(quantiles);
    
                                // Generate the run (sorted chunk) and write it back to disk
                                let output = Arc::new(AppendOnlyStore::bulk_insert_create(
                                    temp_container_key,
                                    mem_pool.clone(),
                                    SortBufferIter::new(&sort_buffer),
                                ));
    
                                runs.push(output);
    
                                // Prepare for next run
                                sort_buffer.reset();
    
                                // Generate a new container key for the next run
                                c_id = c_id_counter.fetch_add(1, Ordering::SeqCst);
                                temp_container_key = ContainerKey {
                                    db_id: TEMP_DB_ID,
                                    c_id,
                                };
                                sort_buffer.set_dest_c_key(temp_container_key);
    
                                // Try appending the tuple again
                                if !sort_buffer.append(&tuple) {
                                    panic!("Record too large to fit in a page");
                                }
                            }
                        }
    
                        // After the loop, process any remaining tuples in the sort_buffer
                        if !sort_buffer.ptrs.is_empty() {
                            sort_buffer.sort();
                            let quantiles = sort_buffer.sample_quantiles(num_quantiles);
                            run_quantiles.push(quantiles);
    
                            let output = Arc::new(AppendOnlyStore::bulk_insert_create(
                                temp_container_key,
                                mem_pool.clone(),
                                SortBufferIter::new(&sort_buffer),
                            ));
    
                            runs.push(output);
                        }
    
                        // Merge the quantiles from runs within this chunk
                        let mut chunk_quantiles = SingleRunQuantiles::new(num_quantiles);
                        for q in run_quantiles {
                            chunk_quantiles.merge(&q);
                        }
    
                        // Send the results back
                        if let Err(e) = result_sender.send((runs, chunk_quantiles)) {
                            eprintln!("Failed to send results: {:?}", e);
                            break; // Exit the loop if sending fails
                        }
                    }
                    // `result_sender` will be dropped here
                });
            }
    
            // Drop the main thread's `result_sender` to allow receiver to terminate
            drop(result_sender);
    
            // Step 4: Producer reads data and sends chunks
            let chunk_size = 10_000; //xtx need to figure out the size
            let mut current_chunk = Vec::with_capacity(chunk_size);
    
            while let Some(tuple) = self.exec_plan.next(context)? {
                current_chunk.push(tuple);
                if current_chunk.len() >= chunk_size {
                    chunk_sender.send(current_chunk).unwrap();
                    current_chunk = Vec::with_capacity(chunk_size);
                }
            }
            // Send the last chunk if it's not empty
            if !current_chunk.is_empty() {
                chunk_sender.send(current_chunk).unwrap();
            }
    
            // Close the sender to signal workers that no more chunks are coming
            drop(chunk_sender);
    
            // Wait for workers to finish
            Ok(())
        }).map_err(|e| {
            // Handle any panics that occurred in the threads
            let err_msg = if let Some(s) = e.downcast_ref::<&str>() {
                s.to_string()
            } else if let Some(s) = e.downcast_ref::<String>() {
                s.clone()
            } else {
                format!("Thread panicked with unknown error: {:?}", e)
            };
            ExecError::Pipeline(err_msg)
        })??;
    
        // Step 5: Collect the results
        let mut result_buffers = Vec::new();
        let mut total_quantiles = SingleRunQuantiles::new(self.quantiles.num_quantiles);
    
        for (chunk_runs, quantiles) in result_receiver.iter() {
            result_buffers.extend(chunk_runs);
            total_quantiles.merge(&quantiles);
        }
        self.quantiles = total_quantiles; // Update the global quantiles
    
        Ok(result_buffers)
    }

    // Doesn't store in chunks goes straight into sort buffers
    fn run_generation_3(
        &mut self,
        policy: &Arc<MemoryPolicy>,
        context: &HashMap<PipelineID, Arc<OnDiskBuffer<T, M>>>,
        mem_pool: &Arc<M>,
        dest_c_key: ContainerKey,
    ) -> Result<Vec<Arc<AppendOnlyStore<M>>>, ExecError> {
        use crossbeam::channel::{bounded, unbounded, Receiver, Sender};
        use crossbeam::scope;
        use crossbeam::select;
        use std::sync::{Arc, Mutex};
    
        println!("Starting run_generation_3");
    
        const TEMP_DB_ID: DatabaseId = 321; // Magic number for temporary database ID
        let c_id_counter = AtomicU16::new(321);
    
        let num_quantiles = self.quantiles.num_quantiles;
        let sort_cols = self.sort_cols.clone();
        let policy = Arc::clone(policy);
        let mem_pool = Arc::clone(mem_pool);
    
        // Channels for communication
        let max_threads = 8;
        let (buffer_sender, buffer_receiver) = bounded::<SortBuffer<M>>(max_threads * 2);
        let (result_sender, result_receiver) = unbounded::<(Arc<AppendOnlyStore<M>>, SingleRunQuantiles)>();
    
        // Shared quantiles
        let total_quantiles = Mutex::new(SingleRunQuantiles::new(num_quantiles));
    
        // Use a scoped thread to avoid lifetime issues
        let result_buffers = scope(|s| -> Result<Vec<Arc<AppendOnlyStore<M>>>, ExecError> {
            println!("Inside crossbeam scope");
    
            // Start worker threads
            for i in 0..max_threads {
                println!("Spawning worker thread {}", i);
                let buffer_receiver = buffer_receiver.clone();
                let result_sender = result_sender.clone();
                let mem_pool = &mem_pool; // Borrow mem_pool
                let num_quantiles = num_quantiles; // Copy num_quantiles
    
                s.spawn(move |_| {
                    println!("Worker thread {} started", i);
                    while let Ok(mut sort_buffer) = buffer_receiver.recv() {
                        println!("Worker thread {} received a sort buffer", i);
                        // Process the sort buffer
                        sort_buffer.sort();
                        let quantiles = sort_buffer.sample_quantiles(num_quantiles);
    
                        let output = Arc::new(AppendOnlyStore::bulk_insert_create(
                            sort_buffer.dest_c_key,
                            mem_pool.clone(),
                            SortBufferIter::new(&sort_buffer),
                        ));
    
                        // Send the result
                        if let Err(e) = result_sender.send((output, quantiles)) {
                            eprintln!("Worker thread {} failed to send result: {:?}", i, e);
                            break;
                        }
                    }
                    println!("Worker thread {} finished", i);
                });
            }
    
            println!("making it to here");
    
            // Main thread reads data and fills sort buffers
            let mut current_sort_buffer = SortBuffer::new(
                &mem_pool,
                ContainerKey::new(TEMP_DB_ID, c_id_counter.fetch_add(1, Ordering::SeqCst)),
                &policy,
                sort_cols.clone(),
            );
    
            let mut result_buffers = Vec::new();
    
            println!("Starting main loop");
            loop {
                select! {
                    recv(result_receiver) -> res => {
                        if let Ok((output, quantiles)) = res {
                            println!("Received result from worker thread");
                            result_buffers.push(output);
                            // Merging quantiles
                            {
                                let mut total_q = total_quantiles.lock().unwrap();
                                total_q.merge(&quantiles);
                            }
                        } else {
                            // All senders are dropped; break the loop
                            println!("All result senders dropped");
                            break;
                        }
                    },
                    default => {
                        // Read the next tuple
                        println!("Reading next tuple");
                        match self.exec_plan.next(context)? {
                            Some(tuple) => {
                                println!("Received a tuple");
                                if current_sort_buffer.append(&tuple) {
                                    // Continue appending
                                } else {
                                    // Sort buffer is full, send it to worker threads
                                    let full_sort_buffer = std::mem::replace(
                                        &mut current_sort_buffer,
                                        SortBuffer::new(
                                            &mem_pool,
                                            ContainerKey::new(TEMP_DB_ID, c_id_counter.fetch_add(1, Ordering::SeqCst)),
                                            &policy,
                                            sort_cols.clone(),
                                        ),
                                    );
    
                                    println!("Sort buffer is full; sending to worker threads");
    
                                    if let Err(e) = buffer_sender.send(full_sort_buffer) {
                                        eprintln!("Failed to send sort buffer: {:?}", e);
                                        break;
                                    }
                                }
                            },
                            None => {
                                // No more tuples; send remaining buffer and break
                                println!("No more tuples");
                                if !current_sort_buffer.ptrs.is_empty() {
                                    println!("Sending remaining sort buffer to worker threads");
                                    if let Err(e) = buffer_sender.send(current_sort_buffer) {
                                        eprintln!("Failed to send sort buffer: {:?}", e);
                                    }
                                }
                                drop(buffer_sender); // Close the sender
                                break;
                            }
                        }
                    }
                }
            }
    
            println!("making it to here 2");
    
            // Process remaining results
            for res in result_receiver {
                println!("Processing remaining results");
                let (output, quantiles) = res;
                result_buffers.push(output);
                // Merging quantiles
                {
                    let mut total_q = total_quantiles.lock().unwrap();
                    total_q.merge(&quantiles);
                }
            }
    
            // Update the quantiles
            self.quantiles = total_quantiles.into_inner().unwrap();
    
            println!("I make it here 1");
            // Return the result_buffers without wrapping in Ok(...)
            Ok(result_buffers)
            // The scope ends here, ensuring all threads have completed
        })
        .map_err(|e| {
            // Convert the panic payload (e) into an ExecError
            eprintln!("Scope panicked with error: {:?}", e);
            if let Some(s) = e.downcast_ref::<&str>() {
                ExecError::Pipeline(s.to_string())
            } else if let Some(s) = e.downcast_ref::<String>() {
                ExecError::Pipeline(s.clone())
            } else {
                ExecError::Pipeline("Thread panicked with unknown error".to_string())
            }
        })??; // Use double `?` to unwrap both Results
    
        println!("I make it here ");
        Ok(result_buffers)
    }


    fn run_generation_4(
        &mut self,
        policy: &Arc<MemoryPolicy>,
        context: &HashMap<PipelineID, Arc<OnDiskBuffer<T, M>>>,
        mem_pool: &Arc<M>,
        dest_c_key: ContainerKey,
    ) -> Result<Vec<Arc<AppendOnlyStore<M>>>, ExecError> {
        // Estimate the total number of tuples
        // let total_tuples = self.exec_plan.estimate_num_tuples(context);
        let total_tuples = 6005720;
        println!("Total tuples estimated: {}", total_tuples);
    
        // Decide on the number of threads
        let num_threads = 8;
    
        // Calculate chunk size
        let chunk_size = (total_tuples + num_threads - 1) / num_threads;
    
        // Generate ranges
        let ranges: Vec<(usize, usize)> = (0..num_threads)
            .map(|i| {
                let start = i * chunk_size;
                let end = if i == num_threads - 1 {
                    total_tuples
                } else {
                    (i + 1) * chunk_size
                };
                (start, end)
            })
            .collect();
    
        // Create execution plans for each range
        let plans: Vec<_> = ranges
            .iter()
            .map(|&(start, end)| {
                println!("Creating plan for range {} - {}", start, end);
                self.exec_plan.clone_with_range(start, end)
            })
            .collect::<Vec<_>>();
    
        // Prepare for parallel execution
        let c_id_counter = Arc::new(AtomicU16::new(321)); // Starting container ID
        let num_quantiles = self.quantiles.num_quantiles;
        let sort_cols = self.sort_cols.clone();
        let policy = policy.clone();
        let mem_pool = mem_pool.clone();

         // Start timing for parallel execution
        let parallel_start_time = Instant::now();

        // Process each plan in parallel
       let runs_and_quantiles = plans
        .into_par_iter()
        .enumerate()
        .map(|(thread_index, mut exec_plan)| {
             // Start timing for this thread
            let thread_start_time = Instant::now();
                let c_id_counter = c_id_counter.clone();
                let mut runs = Vec::new();
                let mut run_quantiles = Vec::new();
    
                let mut c_id = c_id_counter.fetch_add(1, Ordering::SeqCst);
                let mut temp_container_key = ContainerKey {
                    db_id: dest_c_key.db_id,
                    c_id,
                };
    
                let mut sort_buffer = SortBuffer::new(
                    &mem_pool,
                    temp_container_key,
                    &policy,
                    sort_cols.clone(),
                );
    
                let mut tuples_processed = 0;

                println!("handling thread {} before while", thread_index);

                while let Some(tuple) = exec_plan.next(context)? {
                    tuples_processed += 1;
                    if sort_buffer.append(&tuple) {
                        continue;
                    } else {
                        // Sort and process the current buffer
                        sort_buffer.sort();
                        let quantiles = sort_buffer.sample_quantiles(num_quantiles);
                        run_quantiles.push(quantiles);
    
                        // Create the run and write it back to disk
                        let output = Arc::new(AppendOnlyStore::bulk_insert_create(
                            temp_container_key,
                            mem_pool.clone(),
                            SortBufferIter::new(&sort_buffer),
                        ));
                        runs.push(output);
    
                        // Reset for the next run
                        sort_buffer.reset();
                        c_id = c_id_counter.fetch_add(1, Ordering::SeqCst);
                        temp_container_key = ContainerKey {
                            db_id: dest_c_key.db_id,
                            c_id,
                        };
                        sort_buffer.set_dest_c_key(temp_container_key);
    
                        // Try appending the tuple again
                        if !sort_buffer.append(&tuple) {
                            panic!("Record too large to fit in a page");
                        }
                    }
                }
    
                // Process any remaining tuples in the sort_buffer
                if !sort_buffer.ptrs.is_empty() {
                    sort_buffer.sort();
                    let quantiles = sort_buffer.sample_quantiles(num_quantiles);
                    run_quantiles.push(quantiles);
    
                    let output = Arc::new(AppendOnlyStore::bulk_insert_create(
                        temp_container_key,
                        mem_pool.clone(),
                        SortBufferIter::new(&sort_buffer),
                    ));
                    runs.push(output);
                }
    
               let thread_duration = thread_start_time.elapsed();
                println!(
                    "Thread {} processed {} tuples, generated {} runs in {:?}",
                    thread_index,
                    tuples_processed,
                    runs.len(),
                    thread_duration
                ); 
                // Merge quantiles within this thread
                let mut chunk_quantiles = SingleRunQuantiles::new(num_quantiles);
                for q in run_quantiles {
                    chunk_quantiles.merge(&q);
                }
    
                Ok((runs, chunk_quantiles))
            })
            .collect::<Result<Vec<_>, ExecError>>()?;
        let parallel_duration = parallel_start_time.elapsed();
         println!("Parallel execution took {:?}", parallel_duration);
        // Collect runs and merge quantiles
        let mut result_buffers = Vec::new();
        let mut total_quantiles = SingleRunQuantiles::new(self.quantiles.num_quantiles);
    
        for (chunk_runs, quantiles) in runs_and_quantiles {
            result_buffers.extend(chunk_runs);
            total_quantiles.merge(&quantiles);
        }
        self.quantiles = total_quantiles; // Update global quantiles
    
        Ok(result_buffers)
    }
    
    
    
    

    fn run_merge(
        &mut self,
        policy: &Arc<MemoryPolicy>,
        mut runs: Vec<Arc<AppendOnlyStore<M>>>,
        mem_pool: &Arc<M>,
        dest_c_key: ContainerKey,
    ) -> Result<Arc<AppendOnlyStore<M>>, ExecError> {
<<<<<<< HEAD
        println!("entering run merge ");
=======
        let total_pages: usize = runs.iter().map(|run| run.num_pages()).sum();
        println!("Total number of pages across all SortedRunStores: {}", total_pages);
>>>>>>> sorted_run_store
        let result = match policy.as_ref() {
            MemoryPolicy::FixedSizeLimit(working_mem) => {
                // Get global quantiles from previously computed quantiles
                // println!("Global quantiles:"); xtx
                // println!("{}", self.quantiles);
                let global_quantiles = self.quantiles.get_quantiles();


                // If quantile is [a, b, c, d], there are 3 threads each responsible for [a, b), [b, c), [c, d].
                // Note that last thread is responsible for [c, d] inclusive.
                let num_threads = global_quantiles.len() - 1;
                println!("num_threads = {}", num_threads);
                // println!("global quantiles {:?}", global_quantiles);
                let mut merged_buffers = (0..num_threads)
                    .into_par_iter()
                    .map(|i| {
                        // Define the regions for each thread
                        let lower = global_quantiles[i].clone(); // Inclusive
                        let upper = global_quantiles[i + 1].clone(); // Exclusive except for the last one
                        let upper = if i == num_threads - 1 {
                            // For the last thread, the upper bound is inclusive.
                            // A hack to make the last thread inclusive is to add 1 to the last byte.
                            // We need to be careful about overflow. If the last byte is 255, then it will increment
                            // the second last byte and set the last byte to 0. This continues until it finds a byte that
                            // does not overflow.
                            let mut upper = upper.clone();
                            let mut carry = 1;
                            for byte in upper.iter_mut().rev() {
                                let (new_byte, new_carry) = byte.overflowing_add(carry);
                                *byte = new_byte;
                                if !new_carry {
                                    carry = 0;
                                    break;
                                }
                                carry = 1;
                            }
                            if carry != 0 {
                                // Push a new byte to the front of the vec
                                upper.insert(0, 1);
                            }
                            upper
                        } else {
                            upper
                        };
                        // println!("Thread {} Merging range [{:?}, {:?})", i, lower, upper); xtx

                        let run_segments = runs
                            .iter()
                            .map(|r| {
                                let lower = lower.clone();
                                let upper = upper.clone();
                                r.scan().filter_map(move |(key, value)| {
                                    if key >= lower && key < upper {
                                        Some((key.to_vec(), value.to_vec()))
                                    } else {
                                        None
                                    }
                                })
                            })
                            .collect::<Vec<_>>();

                        let merge_iter = MergeIter::new(run_segments);

                        let temp_container_key = ContainerKey {
                            db_id: dest_c_key.db_id,
                            c_id: dest_c_key.c_id + i as u16,
                        };

                        (
                            i,
                            Arc::new(AppendOnlyStore::bulk_insert_create(
                                temp_container_key,
                                mem_pool.clone(),
                                merge_iter,
                            )),
                        )
                    })
                    .collect::<Vec<_>>();

                // Sort the merged buffers by the thread id
                merged_buffers.sort_by_key(|(i, _)| *i);

                // Chain all the merged buffers into one iterator
                let empty_iter =
                    Box::new(std::iter::empty()) as Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)>>;
                let chained_iter = merged_buffers
                    .into_iter()
                    .fold(empty_iter, |acc, (_, iter)| {
                        Box::new(acc.chain(iter.scan()))
                    });

                Arc::new(AppendOnlyStore::bulk_insert_create(
                    dest_c_key,
                    mem_pool.clone(),
                    chained_iter,
                ))
            }
            MemoryPolicy::Unbounded => self.merge_step(runs, mem_pool, dest_c_key),
            MemoryPolicy::Proportional(rate) => {
                unimplemented!("Proportional memory policy is not implemented yet");
            }
        };
        Ok(result)
    }


    fn run_merge_sorted_store(
        &mut self,
        policy: &Arc<MemoryPolicy>,
        mut runs: Vec<Arc<SortedRunStore<M>>>,
        mem_pool: &Arc<M>,
        dest_c_key: ContainerKey,
    ) -> Result<Arc<AppendOnlyStore<M>>, ExecError> {
        let total_pages: usize = runs.iter().map(|run| run.num_pages()).sum();
        println!("Total number of pages across all SortedRunStores: {}", total_pages);
        let result = match policy.as_ref() {
            MemoryPolicy::FixedSizeLimit(working_mem) => {
                // Get global quantiles from previously computed quantiles
                // println!("Global quantiles:"); 
                // println!("{}", self.quantiles);
                // for run in runs.iter(){
                //     for key in run.min_keys(){
                //         println!("{:?}", key);
                //     }
                // }
                let global_quantiles = self.quantiles.get_quantiles();

                // If quantile is [a, b, c, d], there are 3 threads each responsible for [a, b), [b, c), [c, d].
                // Note that last thread is responsible for [c, d] inclusive.
                let num_threads = global_quantiles.len() - 1;
                println!("num_threads = {}", num_threads);
                // println!("global quantiles {:?}", global_quantiles);
                let mut merged_buffers = (0..num_threads)
                    .into_par_iter()
                    .map(|i| {
                        // Define the regions for each thread
                        let lower = global_quantiles[i].clone(); // Inclusive
                        let upper = global_quantiles[i + 1].clone(); // Exclusive except for the last one
                        let upper = if i == num_threads - 1 {
                            // For the last thread, the upper bound is inclusive.
                            // A hack to make the last thread inclusive is to add 1 to the last byte.
                            // We need to be careful about overflow. If the last byte is 255, then it will increment
                            // the second last byte and set the last byte to 0. This continues until it finds a byte that
                            // does not overflow.
                            let mut upper = upper.clone();
                            let mut carry = 1;
                            for byte in upper.iter_mut().rev() {
                                let (new_byte, new_carry) = byte.overflowing_add(carry);
                                *byte = new_byte;
                                if !new_carry {
                                    carry = 0;
                                    break;
                                }
                                carry = 1;
                            }
                            if carry != 0 {
                                // Push a new byte to the front of the vec
                                upper.insert(0, 1);
                            }
                            upper
                        } else {
                            upper
                        };

                        let run_segments = runs
                        .iter()
                        .map(|r| r.scan_range(&lower, &upper))
                        .collect::<Vec<_>>();

                        let merge_iter = MergeIter::new(run_segments);

                        let temp_container_key = ContainerKey {
                            db_id: dest_c_key.db_id,
                            c_id: dest_c_key.c_id + i as u16,
                        };

                        (
                            i,
                            Arc::new(AppendOnlyStore::bulk_insert_create(
                                temp_container_key,
                                mem_pool.clone(),
                                merge_iter,
                            )),
                        )
                    })
                    .collect::<Vec<_>>();

                // Sort the merged buffers by the thread id
                merged_buffers.sort_by_key(|(i, _)| *i);

                // Chain all the merged buffers into one iterator
                let empty_iter =
                    Box::new(std::iter::empty()) as Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)>>;
                let chained_iter = merged_buffers
                    .into_iter()
                    .fold(empty_iter, |acc, (_, iter)| {
                        Box::new(acc.chain(iter.scan()))
                    });

                Arc::new(AppendOnlyStore::bulk_insert_create(
                    dest_c_key,
                    mem_pool.clone(),
                    chained_iter,
                ))
            }
            MemoryPolicy::Unbounded => self.merge_step_sorted_store(runs, mem_pool, dest_c_key),
            MemoryPolicy::Proportional(rate) => {
                unimplemented!("Proportional memory policy is not implemented yet");
            }
        };
        Ok(result)
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
        // -------------- Run Generation Phase --------------
        let start_generation = Instant::now();
<<<<<<< HEAD
        let runs = self.run_generation_4(policy, context, mem_pool, dest_c_key)?;
=======
        let runs = self.run_generation(policy, context, mem_pool, dest_c_key)?;
        // let runs = self.run_generation_sorted_store(policy, context, mem_pool, dest_c_key)?;
>>>>>>> sorted_run_store
        let duration_generation = start_generation.elapsed();


        println!("Run generation took: {:.2?} seconds", duration_generation);

        println!("num runs {}", runs.len());

        // -------------- Run Merge Phase --------------
        let start_merge = Instant::now();
        // let final_run = self.run_merge_sorted_store(policy, runs, mem_pool, dest_c_key)?;
        let final_run = self.run_merge(policy, runs, mem_pool, dest_c_key)?;
        let duration_merge = start_merge.elapsed();

        println!("Run merge took: {:.2?} seconds", duration_merge);
        Ok(Arc::new(OnDiskBuffer::AppendOnlyStore(final_run)))
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
        fn test_run_generation() {
            // Create a append only store with 10000 random kvs
            let bp = get_test_bp(100);
            let c_key = get_c_key(0);
            let num_kvs = 10000;
            let append_only_store = Arc::new(AppendOnlyStore::new(c_key, bp.clone()));
            let keys = gen_random_permutation((0..num_kvs).collect::<Vec<_>>());
            let mut expected = Vec::new();
            for k in keys {
                let tuple = Tuple::from_fields(vec![k.into(), 1.into(), 2.into(), 3.into()]);
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

            // Scan the append only store with the scan operator
            let scan =
                PScanIter::<InMemStorage, BufferPool>::new(schema.clone(), 0, (0..4).collect());

            let mut context = HashMap::new();
            context.insert(
                0,
                Arc::new(OnDiskBuffer::AppendOnlyStore(append_only_store)),
            );

            // Sort iterator
            let dest_c_key = get_c_key(1);
            let sort_cols = vec![(0, true, true)];
            let policy = Arc::new(MemoryPolicy::FixedSizeLimit(10)); // Use 10 pages for the sort buffer
            let mut external_sort = OnDiskSort::new(
                schema.clone(),
                NonBlockingOp::Scan(scan),
                sort_cols.clone(),
                10,
            );

            let runs = external_sort
                .run_generation_old(&policy, &context, &bp, dest_c_key)
                .unwrap();

            println!("Num runs: {}", runs.len());

            // Check if the result contains all the kvs
            let merge = MergeIter::new(runs.iter().map(|r| r.scan()).collect());
            let mut result = Vec::new();

            for (k, v) in merge {
                let tuple = Tuple::from_bytes(&v);
                result.push(tuple);
            }

            expected.sort_by_key(|t| t.to_normalized_key_bytes(&sort_cols));

            println!("result len: {}", result.len());
            println!("expected len: {}", expected.len());

            assert_eq!(result.len(), expected.len());

            for (i, t) in result.iter().enumerate() {
                assert_eq!(t, &expected[i]);
            }
        }

        #[test]
        fn test_external_sorting() {
            // Create a append only store with 10000 random kvs
            let bp = get_test_bp(100);
            let c_key = get_c_key(0);
            let num_kvs = 10000;
            // let num_kvs = 5000;
            let append_only_store: Arc<AppendOnlyStore<BufferPool>> = Arc::new(AppendOnlyStore::new(c_key, bp.clone()));
            let keys = gen_random_permutation((0..num_kvs).collect::<Vec<_>>());
            let mut expected = Vec::new();
            for k in keys {
                let tuple = Tuple::from_fields(vec![k.into(), 1.into(), 2.into(), 3.into()]);
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

            // Scan the append only store with the scan operator
            let scan =
                PScanIter::<InMemStorage, BufferPool>::new(schema.clone(), 0, (0..4).collect());

            let mut context = HashMap::new();
            context.insert(
                0,
                Arc::new(OnDiskBuffer::AppendOnlyStore(append_only_store)),
            );

            // Sort iterator
            let dest_c_key = get_c_key(1);
            let sort_cols = vec![(0, true, true)];
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

            expected.sort_by_key(|t| t.to_normalized_key_bytes(&sort_cols));

            // println!("result {:?}", result);
            println!("result len: {}", result.len());
            println!("expected len: {}", expected.len());

            let mut temp = 0;
            // If the length are different, identify the missing value
            if result.len() != expected.len() {
                for i in 0..min(result.len(), expected.len()) {
                    if result[i] != expected[i] {
                        
                        if temp == 0{
                            println!("prev was {:?}", result[i - 1]);
                            temp = 1;
                        }
                        println!("Mismatch at index {}", i);
                        println!("Expected: {:?}", expected[i]);
                        println!("Result: {:?}", result[i]);
                    }
                }
                // Print the remaining values
                if result.len() > expected.len() {
                    for i in expected.len()..result.len() {
                        println!("Extra result: {:?}", result[i]);
                    }
                } 
                // else {
                //     for i in result.len()..expected.len() {
                //         println!("Missing expected: {:?}", expected[i]);
                //     }
                // }
            }

            for (i, t) in result.iter().enumerate() {
                assert_eq!(t, &expected[i]);
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
            if let Err(error) = verify_sorted_results(&result, &sort_cols) {
                panic!("Sort verification failed: {}", error);
            }

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
            if let Err(error) = verify_sorted_results(&result, &sort_cols) {
                panic!("Sort verification failed: {}", error);
            }

            expected.sort_by_key(|t| t.to_normalized_key_bytes(&sort_cols));

            assert_eq!(result.len(), expected.len());

            for (i, t) in result.iter().enumerate() {
                assert_eq!(t, &expected[i]);
            }
        }
    }
}
