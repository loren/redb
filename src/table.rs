use crate::tree_store::{
    AccessGuardMut, Btree, BtreeMut, BtreeRangeIter, Checksum, PageNumber, TransactionalMemory,
};
use crate::types::{RedbKey, RedbValue};
use crate::Result;
use crate::{AccessGuard, WriteTransaction};
use std::borrow::Borrow;
use std::cell::RefCell;
use std::ops::RangeBounds;
use std::rc::Rc;

/// A table containing key-value mappings
pub struct Table<'db, 'txn, K: RedbKey + ?Sized, V: RedbValue + ?Sized> {
    name: String,
    transaction: &'txn WriteTransaction<'db>,
    tree: BtreeMut<'txn, K, V>,
}

impl<'db, 'txn, K: RedbKey + ?Sized, V: RedbValue + ?Sized> Table<'db, 'txn, K, V> {
    pub(crate) fn new(
        name: &str,
        table_root: Option<(PageNumber, Checksum)>,
        freed_pages: Rc<RefCell<Vec<PageNumber>>>,
        mem: &'db TransactionalMemory,
        transaction: &'txn WriteTransaction<'db>,
    ) -> Table<'db, 'txn, K, V> {
        Table {
            name: name.to_string(),
            transaction,
            tree: BtreeMut::new(table_root, mem, freed_pages),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn print_debug(&self, include_values: bool) {
        self.tree.print_debug(include_values);
    }

    /// Insert mapping of the given key to the given value
    ///
    /// Returns the old value, if the key was present in the table
    pub fn insert(&mut self, key: &K, value: &V) -> Result<Option<AccessGuard<V>>> {
        // Safety: No other references to this table can exist.
        // Tables can only be opened mutably in one location (see Error::TableAlreadyOpen),
        // and we borrow &mut self.
        unsafe { self.tree.insert(key, value) }
    }

    /// Reserve space to insert a key-value pair
    /// The returned reference will have length equal to value_length
    // TODO: return type should be V, not [u8]
    pub fn insert_reserve(
        &mut self,
        key: &K,
        value_length: usize,
    ) -> Result<AccessGuardMut<K, [u8]>> {
        // Safety: No other references to this table can exist.
        // Tables can only be opened mutably in one location (see Error::TableAlreadyOpen),
        // and we borrow &mut self.
        unsafe { self.tree.insert_reserve(key, value_length) }
    }

    /// Removes the given key
    ///
    /// Returns the old value, if the key was present in the table
    pub fn remove(&mut self, key: &K) -> Result<Option<AccessGuard<V>>> {
        // Safety: No other references to this table can exist.
        // Tables can only be opened mutably in one location (see Error::TableAlreadyOpen),
        // and we borrow &mut self.
        unsafe { self.tree.remove(key) }
    }
}

impl<'db, 'txn, K: RedbKey + ?Sized, V: RedbValue + ?Sized> ReadableTable<K, V>
    for Table<'db, 'txn, K, V>
{
    fn get(&self, key: &K) -> Result<Option<V::View<'_>>> {
        self.tree.get(key)
    }

    fn range<'a, T: RangeBounds<KR>, KR: Borrow<K> + 'a>(
        &'a self,
        range: T,
    ) -> Result<RangeIter<K, V>> {
        self.tree.range(range).map(RangeIter::new)
    }

    fn len(&self) -> Result<usize> {
        self.tree.len()
    }

    fn is_empty(&self) -> Result<bool> {
        self.len().map(|x| x == 0)
    }
}

impl<'db, 'txn, K: RedbKey + ?Sized, V: RedbValue + ?Sized> Drop for Table<'db, 'txn, K, V> {
    fn drop(&mut self) {
        self.transaction.close_table(&self.name, &mut self.tree);
    }
}

pub trait ReadableTable<K: RedbKey + ?Sized, V: RedbValue + ?Sized> {
    /// Returns the value corresponding to the given key
    fn get(&self, key: &K) -> Result<Option<V::View<'_>>>;

    /// Returns a double-ended iterator over a range of elements in the table
    ///
    /// # Examples
    ///
    /// Usage:
    /// ```rust
    /// use redb::*;
    /// # use tempfile::NamedTempFile;
    /// const TABLE: TableDefinition<str, u64> = TableDefinition::new("my_data");
    ///
    /// # fn main() -> Result<(), Error> {
    /// # let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    /// # let filename = tmpfile.path();
    /// # let db_max_size = 1024 * 1024;
    /// let db = unsafe { Database::create(filename, db_max_size)? };
    /// let write_txn = db.begin_write()?;
    /// {
    ///     let mut table = write_txn.open_table(TABLE)?;
    ///     table.insert("a", &0)?;
    ///     table.insert("b", &1)?;
    ///     table.insert("c", &2)?;
    /// }
    /// write_txn.commit()?;
    ///
    /// let read_txn = db.begin_read()?;
    /// let table = read_txn.open_table(TABLE)?;
    /// let mut iter = table.range("a".."c")?;
    /// assert_eq!(Some(("a", 0)), iter.next());
    /// # Ok(())
    /// # }
    /// ```
    fn range<'a, T: RangeBounds<KR>, KR: Borrow<K> + 'a>(
        &'a self,
        range: T,
    ) -> Result<RangeIter<K, V>>;

    /// Returns the number of entries in the table
    fn len(&self) -> Result<usize>;

    /// Returns `true` if the table is empty
    fn is_empty(&self) -> Result<bool>;
}

/// A read-only table
pub struct ReadOnlyTable<'txn, K: RedbKey + ?Sized, V: RedbValue + ?Sized> {
    tree: Btree<'txn, K, V>,
}

impl<'txn, K: RedbKey + ?Sized, V: RedbValue + ?Sized> ReadOnlyTable<'txn, K, V> {
    pub(crate) fn new(
        root_page: Option<(PageNumber, Checksum)>,
        mem: &'txn TransactionalMemory,
    ) -> ReadOnlyTable<'txn, K, V> {
        ReadOnlyTable {
            tree: Btree::new(root_page, mem),
        }
    }
}

impl<'txn, K: RedbKey + ?Sized, V: RedbValue + ?Sized> ReadableTable<K, V>
    for ReadOnlyTable<'txn, K, V>
{
    fn get(&self, key: &K) -> Result<Option<V::View<'_>>> {
        self.tree.get(key)
    }

    fn range<'a, T: RangeBounds<KR>, KR: Borrow<K> + 'a>(
        &'a self,
        range: T,
    ) -> Result<RangeIter<K, V>> {
        self.tree.range(range).map(RangeIter::new)
    }

    fn len(&self) -> Result<usize> {
        self.tree.len()
    }

    fn is_empty(&self) -> Result<bool> {
        self.len().map(|x| x == 0)
    }
}

pub struct RangeIter<'a, K: RedbKey + ?Sized + 'a, V: RedbValue + ?Sized + 'a> {
    inner: BtreeRangeIter<'a, K, V>,
}

impl<'a, K: RedbKey + ?Sized + 'a, V: RedbValue + ?Sized + 'a> RangeIter<'a, K, V> {
    fn new(inner: BtreeRangeIter<'a, K, V>) -> Self {
        Self { inner }
    }
}

impl<'a, K: RedbKey + ?Sized + 'a, V: RedbValue + ?Sized + 'a> Iterator for RangeIter<'a, K, V> {
    type Item = (K::View<'a>, V::View<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(entry) = self.inner.next() {
            let key = K::from_bytes(entry.key());
            let value = V::from_bytes(entry.value());
            Some((key, value))
        } else {
            None
        }
    }
}

impl<'a, K: RedbKey + ?Sized + 'a, V: RedbValue + ?Sized + 'a> DoubleEndedIterator
    for RangeIter<'a, K, V>
{
    fn next_back(&mut self) -> Option<Self::Item> {
        if let Some(entry) = self.inner.next_back() {
            let key = K::from_bytes(entry.key());
            let value = V::from_bytes(entry.value());
            Some((key, value))
        } else {
            None
        }
    }
}
