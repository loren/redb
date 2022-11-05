use std::env::current_dir;
use std::fs;
use std::mem::size_of;
use tempfile::{NamedTempFile, TempDir};

mod common;
use common::*;

use rand::Rng;
use redb::WriteStrategy;
use std::time::{Duration, Instant};

const ITERATIONS: usize = 1;
const ELEMENTS: usize = 1_000_000;
const KEY_SIZE: usize = 24;
const VALUE_SIZE: usize = 10_000;
const RNG_SEED: u64 = 3;

fn fill_slice(slice: &mut [u8], rng: &mut fastrand::Rng) {
    let mut i = 0;
    while i + size_of::<u128>() < slice.len() {
        let tmp = rng.u128(..);
        slice[i..(i + size_of::<u128>())].copy_from_slice(&tmp.to_le_bytes());
        i += size_of::<u128>()
    }
    if i + size_of::<u64>() < slice.len() {
        let tmp = rng.u64(..);
        slice[i..(i + size_of::<u64>())].copy_from_slice(&tmp.to_le_bytes());
        i += size_of::<u64>()
    }
    if i + size_of::<u32>() < slice.len() {
        let tmp = rng.u32(..);
        slice[i..(i + size_of::<u32>())].copy_from_slice(&tmp.to_le_bytes());
        i += size_of::<u32>()
    }
    if i + size_of::<u16>() < slice.len() {
        let tmp = rng.u16(..);
        slice[i..(i + size_of::<u16>())].copy_from_slice(&tmp.to_le_bytes());
        i += size_of::<u16>()
    }
    if i + size_of::<u8>() < slice.len() {
        slice[i] = rng.u8(..);
    }
}

/// Returns pairs of key, value
fn gen_pair(rng: &mut fastrand::Rng) -> ([u8; KEY_SIZE], Vec<u8>) {
    let mut key = [0u8; KEY_SIZE];
    fill_slice(&mut key, rng);
    let mut value = vec![0u8; VALUE_SIZE];
    fill_slice(&mut value, rng);

    (key, value)
}

fn make_rng() -> fastrand::Rng {
    fastrand::Rng::with_seed(RNG_SEED)
}

fn benchmark<T: BenchDatabase>(mut db: T) -> Vec<(&'static str, Duration)> {
    let mut rng = make_rng();
    let mut results = Vec::new();

    let start = Instant::now();
    let mut txn = db.write_transaction();
    let mut inserter = txn.get_inserter();
    {
        for _ in 0..ELEMENTS {
            let (key, value) = gen_pair(&mut rng);
            inserter.insert(&key, &value).unwrap();
        }
    }
    drop(inserter);
    txn.commit().unwrap();

    let end = Instant::now();
    let duration = end - start;
    println!(
        "{}: Bulk loaded {} items in {}ms",
        T::db_type_name(),
        ELEMENTS,
        duration.as_millis()
    );
    results.push(("bulk load", duration));

    let txn = db.read_transaction();
    {
        for _ in 0..ITERATIONS {
            let mut rng = make_rng();
            let start = Instant::now();
            let mut checksum = 0u64;
            let mut expected_checksum = 0u64;
            let reader = txn.get_reader();
            for _ in 0..1_000_000 {
                let (key, value) = gen_pair(&mut rng);
                let result = reader.get(&key).unwrap();
                checksum += result.as_ref()[0] as u64;
                expected_checksum += value[0] as u64;
            }
            assert_eq!(checksum, expected_checksum);
            let end = Instant::now();
            let duration = end - start;
            println!(
                "{}: Random read {} items in {}ms",
                T::db_type_name(),
                1_000_000,
                duration.as_millis()
            );
            results.push(("random reads", duration));
        }
    }
    drop(txn);

    let start = Instant::now();
    let deletes = 10_000;
    {
        let mut rng = make_rng();
        let mut txn = db.write_transaction();
        let mut inserter = txn.get_inserter();
        for _ in 0..deletes {
            let (key, _value) = gen_pair(&mut rng);
            inserter.remove(&key).unwrap();
        }
        drop(inserter);
        txn.commit().unwrap();
    }

    let end = Instant::now();
    let duration = end - start;
    println!(
        "{}: Removed {} items in {}ms",
        T::db_type_name(),
        deletes,
        duration.as_millis()
    );
    results.push(("removals", duration));

    results
}

fn main() {
    // Fill up most of available memory to be sure that the page cache doesn't bias our results
    let mut junk = vec![0u64; 3 * 1024 * 1024 * 1024 + 256 * 1024 * 1024];
    for x in junk.iter_mut() {
        *x = rand::thread_rng().gen();
    }
    println!("Ok, begin!");
    let mut rows = Vec::new();

    let size_4k = {
        let tmpfile: NamedTempFile = NamedTempFile::new_in(current_dir().unwrap()).unwrap();
        let db = unsafe {
            redb::Database::builder()
                .set_write_strategy(WriteStrategy::TwoPhase)
                .set_page_size(4096)
                .create(tmpfile.path(), 200 * 4096 * 1024 * 1024)
                .unwrap()
        };
        let table = RedbBenchDatabase::new(&db);
        benchmark(table)
    };

    let size_16k = {
        let tmpfile: NamedTempFile = NamedTempFile::new_in(current_dir().unwrap()).unwrap();
        let db = unsafe {
            redb::Database::builder()
                .set_write_strategy(WriteStrategy::TwoPhase)
                .set_page_size(4 * 4096)
                .create(tmpfile.path(), 200 * 4096 * 1024 * 1024)
                .unwrap()
        };
        let table = RedbBenchDatabase::new(&db);
        benchmark(table)
    };

    let size_64k = {
        let tmpfile: NamedTempFile = NamedTempFile::new_in(current_dir().unwrap()).unwrap();
        let db = unsafe {
            redb::Database::builder()
                .set_write_strategy(WriteStrategy::TwoPhase)
                .set_page_size(4 * 4 * 4096)
                .create(tmpfile.path(), 200 * 4096 * 1024 * 1024)
                .unwrap()
        };
        let table = RedbBenchDatabase::new(&db);
        benchmark(table)
    };

    for (benchmark, _duration) in &size_4k {
        rows.push(vec![benchmark.to_string()]);
    }

    for results in [size_4k, size_16k, size_64k] {
        for (i, (_benchmark, duration)) in results.iter().enumerate() {
            rows[i].push(format!("{}ms", duration.as_millis()));
        }
    }

    let mut table = comfy_table::Table::new();
    table.set_width(100);
    table.set_header(["", "2PC - 4k", "2PC - 16k", "2PC - 64k"]);
    for row in rows {
        table.add_row(row);
    }

    println!();
    println!("{table}");
    drop(junk);
}
