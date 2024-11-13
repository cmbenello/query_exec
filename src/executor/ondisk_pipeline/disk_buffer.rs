use std::sync::{Arc, Mutex};

use fbtree::{
    access_method::{
        append_only_store::AppendOnlyStore,
        fbt::{FosterBtree, FosterBtreeRangeScanner},
        hash_fbt::HashFosterBtreeIter,
        UniqueKeyIndex,
    },
    bp::{ContainerId, ContainerKey, DatabaseId, MemPool},
    prelude::{AppendOnlyStoreScanner, HashFosterBtree},
    txn_storage::{ScanOptions, TxnOptions, TxnStorageTrait},
};

use crate::{error::ExecError, executor::TupleBuffer, prelude::SchemaRef, tuple::Tuple};

use super::{
    hash_table::{HashAggregateTable, HashAggregationTableIter, HashTable},
    TupleBufferIter,
};

pub enum OnDiskBuffer<T: TxnStorageTrait, M: MemPool> {
    TxnStorage(TxnStorage<T>),
    AppendOnlyStore(Arc<AppendOnlyStore<M>>),
    BTree(Arc<FosterBtree<M>>),
    HashIndex(Arc<HashFosterBtree<M>>),
    HashTable(Arc<HashTable<M>>),
    HashAggregateTable(Arc<HashAggregateTable<M>>),
}

impl<T: TxnStorageTrait, M: MemPool> OnDiskBuffer<T, M> {
    pub fn txn_storage(
        schema: SchemaRef,
        db_id: DatabaseId,
        c_id: ContainerId,
        storage: Arc<T>,
    ) -> Self {
        OnDiskBuffer::TxnStorage(TxnStorage::new(schema, db_id, c_id, storage))
    }

    pub fn vec(db_id: DatabaseId, c_id: ContainerId, mem_pool: Arc<M>) -> Self {
        let c_key = ContainerKey::new(db_id, c_id);
        OnDiskBuffer::AppendOnlyStore(Arc::new(AppendOnlyStore::new(c_key, mem_pool)))
    }

    pub fn btree(db_id: DatabaseId, c_id: ContainerId, mem_pool: Arc<M>) -> Self {
        let c_key = ContainerKey::new(db_id, c_id);
        OnDiskBuffer::BTree(Arc::new(FosterBtree::new(c_key, mem_pool)))
    }

    pub fn bulk_insert_btree<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        db_id: DatabaseId,
        c_id: ContainerId,
        mem_pool: Arc<M>,
        iter: impl Iterator<Item = (K, V)>,
    ) -> Self {
        let c_key = ContainerKey::new(db_id, c_id);
        OnDiskBuffer::BTree(Arc::new(FosterBtree::bulk_insert_create(
            c_key, mem_pool, iter,
        )))
    }

    /// Provides access to the AppendOnlyStore if the buffer is of that type
    pub fn as_append_only_store(&self) -> Option<Arc<AppendOnlyStore<M>>> {
        if let OnDiskBuffer::AppendOnlyStore(store) = self {
            Some(store.clone())
        } else {
            None
        }
    }
}

impl<T: TxnStorageTrait, M: MemPool> TupleBuffer for OnDiskBuffer<T, M> {
    type Iter = OnDiskBufferIter<T, M>;

    fn num_tuples(&self) -> usize {
        match self {
            OnDiskBuffer::TxnStorage(storage) => {
                let db_id = storage.db_id;
                let c_id = storage.container_id;
                let txn_options = TxnOptions::default();
                let txn = storage.storage.begin_txn(&db_id, txn_options).unwrap();
                let count = storage.storage.num_values(&txn, &c_id).unwrap();
                storage.storage.commit_txn(&txn, false).unwrap();
                count
            }
            OnDiskBuffer::AppendOnlyStore(store) => store.num_kvs(),
            OnDiskBuffer::BTree(tree) => tree.num_kvs(),
            OnDiskBuffer::HashIndex(hash_fbt) => hash_fbt.num_kvs(),
            OnDiskBuffer::HashTable(hash_table) => hash_table.num_kvs(),
            OnDiskBuffer::HashAggregateTable(hash_agg_table) => hash_agg_table.num_kvs(),
        }
    }

    fn iter(self: &Arc<Self>) -> Self::Iter {
        match self.as_ref() {
            OnDiskBuffer::TxnStorage(storage) => {
                let db_id = storage.db_id;
                let c_id = storage.container_id;
                let txn_options = TxnOptions::default();
                let txn = storage.storage.begin_txn(&db_id, txn_options).unwrap();
                let scan_options = ScanOptions::default();
                let iter = storage
                    .storage
                    .scan_range(&txn, &c_id, scan_options)
                    .unwrap();
                OnDiskBufferIter::TxnStorage(TxnStorageIter::new(
                    storage.storage.clone(),
                    iter,
                    txn,
                ))
            }
            OnDiskBuffer::AppendOnlyStore(store) => {
                OnDiskBufferIter::AppendOnlyStore(Mutex::new(store.scan()))
            }
            OnDiskBuffer::BTree(tree) => OnDiskBufferIter::FosterBTree(Mutex::new(tree.scan())),
            OnDiskBuffer::HashIndex(hash) => OnDiskBufferIter::HashIndex(Mutex::new(hash.scan())),
            OnDiskBuffer::HashTable(hash_table) => {
                unimplemented!()
                // OnDiskBufferIter::HashTable(Mutex::new(hash_table.iter()))
            }
            OnDiskBuffer::HashAggregateTable(hash_agg_table) => {
                OnDiskBufferIter::HashAggregateTable(Mutex::new(hash_agg_table.iter()))
            }
        }
    }
}

pub enum OnDiskBufferIter<T: TxnStorageTrait, M: MemPool> {
    TxnStorage(TxnStorageIter<T>),
    AppendOnlyStore(Mutex<AppendOnlyStoreScanner<M>>),
    FosterBTree(Mutex<FosterBtreeRangeScanner<M>>),
    HashIndex(Mutex<HashFosterBtreeIter<FosterBtreeRangeScanner<M>>>),
    // HashTable(Mutex<HashTableIter<M>>),
    HashAggregateTable(Mutex<HashAggregationTableIter<M>>),
}

unsafe impl<T: TxnStorageTrait, M: MemPool> Send for OnDiskBufferIter<T, M> {}
unsafe impl<T: TxnStorageTrait, M: MemPool> Sync for OnDiskBufferIter<T, M> {}

pub struct TxnStorage<T: TxnStorageTrait> {
    schema: SchemaRef,
    db_id: DatabaseId,
    container_id: ContainerId,
    storage: Arc<T>,
}

impl<T: TxnStorageTrait> TxnStorage<T> {
    pub fn new(
        schema: SchemaRef,
        db_id: DatabaseId,
        container_id: ContainerId,
        storage: Arc<T>,
    ) -> Self {
        Self {
            schema,
            db_id,
            container_id,
            storage,
        }
    }
}

pub struct TxnStorageIter<T: TxnStorageTrait> {
    storage: Arc<T>,
    iter: T::IteratorHandle,
    txn: T::TxnHandle,
}

impl<T: TxnStorageTrait> TxnStorageIter<T> {
    pub fn new(storage: Arc<T>, iter: T::IteratorHandle, txn: T::TxnHandle) -> Self {
        Self { storage, iter, txn }
    }

    pub fn next(&self) -> Result<Option<(Vec<u8>, Vec<u8>)>, ExecError> {
        Ok(self.storage.iter_next(&self.iter)?)
    }
}

impl<T: TxnStorageTrait> Drop for TxnStorageIter<T> {
    fn drop(&mut self) {
        self.storage.commit_txn(&self.txn, false).unwrap();
    }
}

impl<T: TxnStorageTrait, M: MemPool> TupleBufferIter for OnDiskBufferIter<T, M> {
    fn next(&mut self) -> Result<Option<Tuple>, ExecError> {
        match self {
            OnDiskBufferIter::TxnStorage(iter) => {
                Ok(iter.next()?.map(|(_, v)| Tuple::from_bytes(&v)))
            }
            OnDiskBufferIter::AppendOnlyStore(iter) => Ok(iter
                .lock()
                .unwrap()
                .next()
                .map(|(_, v)| Tuple::from_bytes(&v))),
            OnDiskBufferIter::FosterBTree(iter) => Ok(iter
                .lock()
                .unwrap()
                .next()
                .map(|(_k, v)| Tuple::from_bytes(&v))),
            OnDiskBufferIter::HashIndex(iter) => Ok(iter
                .lock()
                .unwrap()
                .next()
                .map(|(_k, v)| Tuple::from_bytes(&v))),
            // OnDiskBufferIter::HashTable(iter) => iter.lock().unwrap().next(),
            OnDiskBufferIter::HashAggregateTable(iter) => iter.lock().unwrap().next(),
        }
    }
}
