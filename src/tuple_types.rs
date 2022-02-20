use crate::types::{RedbKey, RedbValue};
use std::cmp::Ordering;
use std::mem::size_of;

fn serialize_tuple_elements(slices: &[&[u8]]) -> Vec<u8> {
    let total_len: usize = slices.iter().map(|x| x.len()).sum();
    let mut output = Vec::with_capacity((slices.len() - 1) * size_of::<u32>() + total_len);
    for len in slices.iter().map(|x| x.len()).take(slices.len() - 1) {
        output.extend_from_slice(&(len as u32).to_le_bytes());
    }

    for slice in slices {
        output.extend_from_slice(slice);
    }

    output
}

fn parse_lens<const N: usize>(data: &[u8]) -> [usize; N] {
    let mut result = [0; N];
    for i in 0..N {
        result[i] = u32::from_le_bytes(data[4 * i..4 * (i + 1)].try_into().unwrap()) as usize;
    }
    result
}

fn not_equal<T: RedbKey>(data1: &[u8], data2: &[u8]) -> Option<Ordering> {
    match T::compare(data1, data2) {
        Ordering::Less => Some(Ordering::Less),
        Ordering::Equal => None,
        Ordering::Greater => Some(Ordering::Greater),
    }
}

// TODO: some macros would probably make this file a lot shorter and less error prone

impl<T0: RedbValue, T1: RedbValue> RedbValue for (T0, T1) {
    type View<'a> = (
        T0::View<'a>,
        T1::View<'a>,
    )
    where
        Self: 'a;
    type AsBytes<'a> = Vec<u8>
    where
        Self: 'a;

    fn from_bytes<'a>(data: &'a [u8]) -> Self::View<'a>
    where
        Self: 'a,
    {
        let lens: [usize; 1] = parse_lens(data);
        let mut offset = size_of::<u32>();
        let t0 = T0::from_bytes(&data[offset..(offset + lens[0])]);
        offset += lens[0];
        let t1 = T1::from_bytes(&data[offset..]);
        (t0, t1)
    }

    fn as_bytes(&self) -> Vec<u8> {
        serialize_tuple_elements(&[self.0.as_bytes().as_ref(), self.1.as_bytes().as_ref()])
    }

    fn redb_type_name() -> String {
        format!("({},{})", T0::redb_type_name(), T1::redb_type_name())
    }
}

impl<T0: RedbValue, T1: RedbValue, T2: RedbValue> RedbValue for (T0, T1, T2) {
    type View<'a> = (
        T0::View<'a>,
        T1::View<'a>,
        T2::View<'a>,
    )
    where
        Self: 'a;
    type AsBytes<'a> = Vec<u8>
    where
        Self: 'a;

    fn from_bytes<'a>(data: &'a [u8]) -> Self::View<'a>
    where
        Self: 'a,
    {
        let lens: [usize; 2] = parse_lens(data);
        let mut offset = 2 * size_of::<u32>();
        let t0 = T0::from_bytes(&data[offset..(offset + lens[0])]);
        offset += lens[0];
        let t1 = T1::from_bytes(&data[offset..(offset + lens[1])]);
        offset += lens[1];
        let t2 = T2::from_bytes(&data[offset..]);
        (t0, t1, t2)
    }

    fn as_bytes(&self) -> Vec<u8> {
        serialize_tuple_elements(&[
            self.0.as_bytes().as_ref(),
            self.1.as_bytes().as_ref(),
            self.2.as_bytes().as_ref(),
        ])
    }

    fn redb_type_name() -> String {
        format!(
            "({},{},{})",
            T0::redb_type_name(),
            T1::redb_type_name(),
            T2::redb_type_name()
        )
    }
}

impl<T0: RedbValue, T1: RedbValue, T2: RedbValue, T3: RedbValue> RedbValue for (T0, T1, T2, T3) {
    type View<'a> = (
        T0::View<'a>,
        T1::View<'a>,
        T2::View<'a>,
        T3::View<'a>,
    )
    where
        Self: 'a;
    type AsBytes<'a> = Vec<u8>
    where
        Self: 'a;

    fn from_bytes<'a>(data: &'a [u8]) -> Self::View<'a>
    where
        Self: 'a,
    {
        let lens: [usize; 3] = parse_lens(data);
        let mut offset = 3 * size_of::<u32>();
        let t0 = T0::from_bytes(&data[offset..(offset + lens[0])]);
        offset += lens[0];
        let t1 = T1::from_bytes(&data[offset..(offset + lens[1])]);
        offset += lens[1];
        let t2 = T2::from_bytes(&data[offset..(offset + lens[2])]);
        offset += lens[2];
        let t3 = T3::from_bytes(&data[offset..]);
        (t0, t1, t2, t3)
    }

    fn as_bytes(&self) -> Vec<u8> {
        serialize_tuple_elements(&[
            self.0.as_bytes().as_ref(),
            self.1.as_bytes().as_ref(),
            self.2.as_bytes().as_ref(),
            self.3.as_bytes().as_ref(),
        ])
    }

    fn redb_type_name() -> String {
        format!(
            "({},{},{},{})",
            T0::redb_type_name(),
            T1::redb_type_name(),
            T2::redb_type_name(),
            T3::redb_type_name()
        )
    }
}

impl<T0: RedbValue, T1: RedbValue, T2: RedbValue, T3: RedbValue, T4: RedbValue> RedbValue
    for (T0, T1, T2, T3, T4)
{
    type View<'a> = (
        T0::View<'a>,
        T1::View<'a>,
        T2::View<'a>,
        T3::View<'a>,
        T4::View<'a>,
    )
    where
        Self: 'a;
    type AsBytes<'a> = Vec<u8>
    where
        Self: 'a;

    fn from_bytes<'a>(data: &'a [u8]) -> Self::View<'a>
    where
        Self: 'a,
    {
        let lens: [usize; 4] = parse_lens(data);
        let mut offset = 4 * size_of::<u32>();
        let t0 = T0::from_bytes(&data[offset..(offset + lens[0])]);
        offset += lens[0];
        let t1 = T1::from_bytes(&data[offset..(offset + lens[1])]);
        offset += lens[1];
        let t2 = T2::from_bytes(&data[offset..(offset + lens[2])]);
        offset += lens[2];
        let t3 = T3::from_bytes(&data[offset..(offset + lens[3])]);
        offset += lens[3];
        let t4 = T4::from_bytes(&data[offset..]);
        (t0, t1, t2, t3, t4)
    }

    fn as_bytes(&self) -> Vec<u8> {
        serialize_tuple_elements(&[
            self.0.as_bytes().as_ref(),
            self.1.as_bytes().as_ref(),
            self.2.as_bytes().as_ref(),
            self.3.as_bytes().as_ref(),
            self.4.as_bytes().as_ref(),
        ])
    }

    fn redb_type_name() -> String {
        format!(
            "({},{},{},{},{})",
            T0::redb_type_name(),
            T1::redb_type_name(),
            T2::redb_type_name(),
            T3::redb_type_name(),
            T4::redb_type_name()
        )
    }
}

impl<T0: RedbValue, T1: RedbValue, T2: RedbValue, T3: RedbValue, T4: RedbValue, T5: RedbValue>
    RedbValue for (T0, T1, T2, T3, T4, T5)
{
    type View<'a> = (
        T0::View<'a>,
        T1::View<'a>,
        T2::View<'a>,
        T3::View<'a>,
        T4::View<'a>,
        T5::View<'a>,
    )
    where
        Self: 'a;
    type AsBytes<'a> = Vec<u8>
    where
        Self: 'a;

    fn from_bytes<'a>(data: &'a [u8]) -> Self::View<'a>
    where
        Self: 'a,
    {
        let lens: [usize; 5] = parse_lens(data);
        let mut offset = 5 * size_of::<u32>();
        let t0 = T0::from_bytes(&data[offset..(offset + lens[0])]);
        offset += lens[0];
        let t1 = T1::from_bytes(&data[offset..(offset + lens[1])]);
        offset += lens[1];
        let t2 = T2::from_bytes(&data[offset..(offset + lens[2])]);
        offset += lens[2];
        let t3 = T3::from_bytes(&data[offset..(offset + lens[3])]);
        offset += lens[3];
        let t4 = T4::from_bytes(&data[offset..(offset + lens[4])]);
        offset += lens[4];
        let t5 = T5::from_bytes(&data[offset..]);
        (t0, t1, t2, t3, t4, t5)
    }

    fn as_bytes(&self) -> Vec<u8> {
        serialize_tuple_elements(&[
            self.0.as_bytes().as_ref(),
            self.1.as_bytes().as_ref(),
            self.2.as_bytes().as_ref(),
            self.3.as_bytes().as_ref(),
            self.4.as_bytes().as_ref(),
            self.5.as_bytes().as_ref(),
        ])
    }

    fn redb_type_name() -> String {
        format!(
            "({},{},{},{},{},{})",
            T0::redb_type_name(),
            T1::redb_type_name(),
            T2::redb_type_name(),
            T3::redb_type_name(),
            T4::redb_type_name(),
            T5::redb_type_name()
        )
    }
}

impl<
        T0: RedbValue,
        T1: RedbValue,
        T2: RedbValue,
        T3: RedbValue,
        T4: RedbValue,
        T5: RedbValue,
        T6: RedbValue,
    > RedbValue for (T0, T1, T2, T3, T4, T5, T6)
{
    type View<'a> = (
        T0::View<'a>,
        T1::View<'a>,
        T2::View<'a>,
        T3::View<'a>,
        T4::View<'a>,
        T5::View<'a>,
        T6::View<'a>,
    )
    where
        Self: 'a;
    type AsBytes<'a> = Vec<u8>
    where
        Self: 'a;

    fn from_bytes<'a>(data: &'a [u8]) -> Self::View<'a>
    where
        Self: 'a,
    {
        let lens: [usize; 6] = parse_lens(data);
        let mut offset = 6 * size_of::<u32>();
        let t0 = T0::from_bytes(&data[offset..(offset + lens[0])]);
        offset += lens[0];
        let t1 = T1::from_bytes(&data[offset..(offset + lens[1])]);
        offset += lens[1];
        let t2 = T2::from_bytes(&data[offset..(offset + lens[2])]);
        offset += lens[2];
        let t3 = T3::from_bytes(&data[offset..(offset + lens[3])]);
        offset += lens[3];
        let t4 = T4::from_bytes(&data[offset..(offset + lens[4])]);
        offset += lens[4];
        let t5 = T5::from_bytes(&data[offset..(offset + lens[5])]);
        offset += lens[5];
        let t6 = T6::from_bytes(&data[offset..]);
        (t0, t1, t2, t3, t4, t5, t6)
    }

    fn as_bytes(&self) -> Vec<u8> {
        serialize_tuple_elements(&[
            self.0.as_bytes().as_ref(),
            self.1.as_bytes().as_ref(),
            self.2.as_bytes().as_ref(),
            self.3.as_bytes().as_ref(),
            self.4.as_bytes().as_ref(),
            self.5.as_bytes().as_ref(),
            self.6.as_bytes().as_ref(),
        ])
    }

    fn redb_type_name() -> String {
        format!(
            "({},{},{},{},{},{},{})",
            T0::redb_type_name(),
            T1::redb_type_name(),
            T2::redb_type_name(),
            T3::redb_type_name(),
            T4::redb_type_name(),
            T5::redb_type_name(),
            T6::redb_type_name()
        )
    }
}

impl<
        T0: RedbValue,
        T1: RedbValue,
        T2: RedbValue,
        T3: RedbValue,
        T4: RedbValue,
        T5: RedbValue,
        T6: RedbValue,
        T7: RedbValue,
    > RedbValue for (T0, T1, T2, T3, T4, T5, T6, T7)
{
    type View<'a> = (
        T0::View<'a>,
        T1::View<'a>,
        T2::View<'a>,
        T3::View<'a>,
        T4::View<'a>,
        T5::View<'a>,
        T6::View<'a>,
        T7::View<'a>,
    )
    where
        Self: 'a;
    type AsBytes<'a> = Vec<u8>
    where
        Self: 'a;

    fn from_bytes<'a>(data: &'a [u8]) -> Self::View<'a>
    where
        Self: 'a,
    {
        let lens: [usize; 7] = parse_lens(data);
        let mut offset = 7 * size_of::<u32>();
        let t0 = T0::from_bytes(&data[offset..(offset + lens[0])]);
        offset += lens[0];
        let t1 = T1::from_bytes(&data[offset..(offset + lens[1])]);
        offset += lens[1];
        let t2 = T2::from_bytes(&data[offset..(offset + lens[2])]);
        offset += lens[2];
        let t3 = T3::from_bytes(&data[offset..(offset + lens[3])]);
        offset += lens[3];
        let t4 = T4::from_bytes(&data[offset..(offset + lens[4])]);
        offset += lens[4];
        let t5 = T5::from_bytes(&data[offset..(offset + lens[5])]);
        offset += lens[5];
        let t6 = T6::from_bytes(&data[offset..(offset + lens[6])]);
        offset += lens[6];
        let t7 = T7::from_bytes(&data[offset..]);
        (t0, t1, t2, t3, t4, t5, t6, t7)
    }

    fn as_bytes(&self) -> Vec<u8> {
        serialize_tuple_elements(&[
            self.0.as_bytes().as_ref(),
            self.1.as_bytes().as_ref(),
            self.2.as_bytes().as_ref(),
            self.3.as_bytes().as_ref(),
            self.4.as_bytes().as_ref(),
            self.5.as_bytes().as_ref(),
            self.6.as_bytes().as_ref(),
            self.7.as_bytes().as_ref(),
        ])
    }

    fn redb_type_name() -> String {
        format!(
            "({},{},{},{},{},{},{},{})",
            T0::redb_type_name(),
            T1::redb_type_name(),
            T2::redb_type_name(),
            T3::redb_type_name(),
            T4::redb_type_name(),
            T5::redb_type_name(),
            T6::redb_type_name(),
            T7::redb_type_name()
        )
    }
}

impl<
        T0: RedbValue,
        T1: RedbValue,
        T2: RedbValue,
        T3: RedbValue,
        T4: RedbValue,
        T5: RedbValue,
        T6: RedbValue,
        T7: RedbValue,
        T8: RedbValue,
    > RedbValue for (T0, T1, T2, T3, T4, T5, T6, T7, T8)
{
    type View<'a> = (
        T0::View<'a>,
        T1::View<'a>,
        T2::View<'a>,
        T3::View<'a>,
        T4::View<'a>,
        T5::View<'a>,
        T6::View<'a>,
        T7::View<'a>,
        T8::View<'a>,
    )
    where
        Self: 'a;
    type AsBytes<'a> = Vec<u8>
    where
        Self: 'a;

    fn from_bytes<'a>(data: &'a [u8]) -> Self::View<'a>
    where
        Self: 'a,
    {
        let lens: [usize; 8] = parse_lens(data);
        #[allow(clippy::manual_bits)]
        let mut offset = 8 * size_of::<u32>();
        let t0 = T0::from_bytes(&data[offset..(offset + lens[0])]);
        offset += lens[0];
        let t1 = T1::from_bytes(&data[offset..(offset + lens[1])]);
        offset += lens[1];
        let t2 = T2::from_bytes(&data[offset..(offset + lens[2])]);
        offset += lens[2];
        let t3 = T3::from_bytes(&data[offset..(offset + lens[3])]);
        offset += lens[3];
        let t4 = T4::from_bytes(&data[offset..(offset + lens[4])]);
        offset += lens[4];
        let t5 = T5::from_bytes(&data[offset..(offset + lens[5])]);
        offset += lens[5];
        let t6 = T6::from_bytes(&data[offset..(offset + lens[6])]);
        offset += lens[6];
        let t7 = T7::from_bytes(&data[offset..(offset + lens[7])]);
        offset += lens[7];
        let t8 = T8::from_bytes(&data[offset..]);
        (t0, t1, t2, t3, t4, t5, t6, t7, t8)
    }

    fn as_bytes(&self) -> Vec<u8> {
        serialize_tuple_elements(&[
            self.0.as_bytes().as_ref(),
            self.1.as_bytes().as_ref(),
            self.2.as_bytes().as_ref(),
            self.3.as_bytes().as_ref(),
            self.4.as_bytes().as_ref(),
            self.5.as_bytes().as_ref(),
            self.6.as_bytes().as_ref(),
            self.7.as_bytes().as_ref(),
            self.8.as_bytes().as_ref(),
        ])
    }

    fn redb_type_name() -> String {
        format!(
            "({},{},{},{},{},{},{},{},{})",
            T0::redb_type_name(),
            T1::redb_type_name(),
            T2::redb_type_name(),
            T3::redb_type_name(),
            T4::redb_type_name(),
            T5::redb_type_name(),
            T6::redb_type_name(),
            T7::redb_type_name(),
            T8::redb_type_name()
        )
    }
}

impl<
        T0: RedbValue,
        T1: RedbValue,
        T2: RedbValue,
        T3: RedbValue,
        T4: RedbValue,
        T5: RedbValue,
        T6: RedbValue,
        T7: RedbValue,
        T8: RedbValue,
        T9: RedbValue,
    > RedbValue for (T0, T1, T2, T3, T4, T5, T6, T7, T8, T9)
{
    type View<'a> = (
        T0::View<'a>,
        T1::View<'a>,
        T2::View<'a>,
        T3::View<'a>,
        T4::View<'a>,
        T5::View<'a>,
        T6::View<'a>,
        T7::View<'a>,
        T8::View<'a>,
        T9::View<'a>,
    )
    where
        Self: 'a;
    type AsBytes<'a> = Vec<u8>
    where
        Self: 'a;

    fn from_bytes<'a>(data: &'a [u8]) -> Self::View<'a>
    where
        Self: 'a,
    {
        let lens: [usize; 9] = parse_lens(data);
        let mut offset = 9 * size_of::<u32>();
        let t0 = T0::from_bytes(&data[offset..(offset + lens[0])]);
        offset += lens[0];
        let t1 = T1::from_bytes(&data[offset..(offset + lens[1])]);
        offset += lens[1];
        let t2 = T2::from_bytes(&data[offset..(offset + lens[2])]);
        offset += lens[2];
        let t3 = T3::from_bytes(&data[offset..(offset + lens[3])]);
        offset += lens[3];
        let t4 = T4::from_bytes(&data[offset..(offset + lens[4])]);
        offset += lens[4];
        let t5 = T5::from_bytes(&data[offset..(offset + lens[5])]);
        offset += lens[5];
        let t6 = T6::from_bytes(&data[offset..(offset + lens[6])]);
        offset += lens[6];
        let t7 = T7::from_bytes(&data[offset..(offset + lens[7])]);
        offset += lens[7];
        let t8 = T8::from_bytes(&data[offset..(offset + lens[8])]);
        offset += lens[8];
        let t9 = T9::from_bytes(&data[offset..]);
        (t0, t1, t2, t3, t4, t5, t6, t7, t8, t9)
    }

    fn as_bytes(&self) -> Vec<u8> {
        serialize_tuple_elements(&[
            self.0.as_bytes().as_ref(),
            self.1.as_bytes().as_ref(),
            self.2.as_bytes().as_ref(),
            self.3.as_bytes().as_ref(),
            self.4.as_bytes().as_ref(),
            self.5.as_bytes().as_ref(),
            self.6.as_bytes().as_ref(),
            self.7.as_bytes().as_ref(),
            self.8.as_bytes().as_ref(),
            self.9.as_bytes().as_ref(),
        ])
    }

    fn redb_type_name() -> String {
        format!(
            "({},{},{},{},{},{},{},{},{},{})",
            T0::redb_type_name(),
            T1::redb_type_name(),
            T2::redb_type_name(),
            T3::redb_type_name(),
            T4::redb_type_name(),
            T5::redb_type_name(),
            T6::redb_type_name(),
            T7::redb_type_name(),
            T8::redb_type_name(),
            T9::redb_type_name()
        )
    }
}

impl<T0: RedbKey, T1: RedbKey> RedbKey for (T0, T1) {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        let lens0: [usize; 1] = parse_lens(data1);
        let lens1: [usize; 1] = parse_lens(data2);

        let mut offset0 = size_of::<u32>();
        let mut offset1 = size_of::<u32>();
        let index = 0;
        if let Some(order) = not_equal::<T0>(
            &data1[offset0..(offset0 + lens0[index])],
            &data2[offset1..(offset1 + lens1[index])],
        ) {
            return order;
        }
        offset0 += lens0[index];
        offset1 += lens1[index];

        T1::compare(&data1[offset0..], &data2[offset1..])
    }
}

impl<T0: RedbKey, T1: RedbKey, T2: RedbKey> RedbKey for (T0, T1, T2) {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        let lens0: [usize; 2] = parse_lens(data1);
        let lens1: [usize; 2] = parse_lens(data2);

        let mut offset0 = 2 * size_of::<u32>();
        let mut offset1 = 2 * size_of::<u32>();
        let index = 0;
        if let Some(order) = not_equal::<T0>(
            &data1[offset0..(offset0 + lens0[index])],
            &data2[offset1..(offset1 + lens1[index])],
        ) {
            return order;
        }
        offset0 += lens0[index];
        offset1 += lens1[index];

        let index = 1;
        if let Some(order) = not_equal::<T1>(
            &data1[offset0..(offset0 + lens0[index])],
            &data2[offset1..(offset1 + lens1[index])],
        ) {
            return order;
        }
        offset0 += lens0[index];
        offset1 += lens1[index];

        T2::compare(&data1[offset0..], &data2[offset1..])
    }
}

impl<T0: RedbKey, T1: RedbKey, T2: RedbKey, T3: RedbKey> RedbKey for (T0, T1, T2, T3) {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        let lens0: [usize; 3] = parse_lens(data1);
        let lens1: [usize; 3] = parse_lens(data2);

        let mut offset0 = 3 * size_of::<u32>();
        let mut offset1 = 3 * size_of::<u32>();
        let index = 0;
        if let Some(order) = not_equal::<T0>(
            &data1[offset0..(offset0 + lens0[index])],
            &data2[offset1..(offset1 + lens1[index])],
        ) {
            return order;
        }
        offset0 += lens0[index];
        offset1 += lens1[index];

        let index = 1;
        if let Some(order) = not_equal::<T1>(
            &data1[offset0..(offset0 + lens0[index])],
            &data2[offset1..(offset1 + lens1[index])],
        ) {
            return order;
        }
        offset0 += lens0[index];
        offset1 += lens1[index];

        let index = 2;
        if let Some(order) = not_equal::<T2>(
            &data1[offset0..(offset0 + lens0[index])],
            &data2[offset1..(offset1 + lens1[index])],
        ) {
            return order;
        }
        offset0 += lens0[index];
        offset1 += lens1[index];

        T3::compare(&data1[offset0..], &data2[offset1..])
    }
}

impl<T0: RedbKey, T1: RedbKey, T2: RedbKey, T3: RedbKey, T4: RedbKey> RedbKey
    for (T0, T1, T2, T3, T4)
{
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        let lens0: [usize; 4] = parse_lens(data1);
        let lens1: [usize; 4] = parse_lens(data2);

        let mut offset0 = 4 * size_of::<u32>();
        let mut offset1 = 4 * size_of::<u32>();
        let index = 0;
        if let Some(order) = not_equal::<T0>(
            &data1[offset0..(offset0 + lens0[index])],
            &data2[offset1..(offset1 + lens1[index])],
        ) {
            return order;
        }
        offset0 += lens0[index];
        offset1 += lens1[index];

        let index = 1;
        if let Some(order) = not_equal::<T1>(
            &data1[offset0..(offset0 + lens0[index])],
            &data2[offset1..(offset1 + lens1[index])],
        ) {
            return order;
        }
        offset0 += lens0[index];
        offset1 += lens1[index];

        let index = 2;
        if let Some(order) = not_equal::<T2>(
            &data1[offset0..(offset0 + lens0[index])],
            &data2[offset1..(offset1 + lens1[index])],
        ) {
            return order;
        }
        offset0 += lens0[index];
        offset1 += lens1[index];

        let index = 3;
        if let Some(order) = not_equal::<T3>(
            &data1[offset0..(offset0 + lens0[index])],
            &data2[offset1..(offset1 + lens1[index])],
        ) {
            return order;
        }
        offset0 += lens0[index];
        offset1 += lens1[index];

        T4::compare(&data1[offset0..], &data2[offset1..])
    }
}

impl<T0: RedbKey, T1: RedbKey, T2: RedbKey, T3: RedbKey, T4: RedbKey, T5: RedbKey> RedbKey
    for (T0, T1, T2, T3, T4, T5)
{
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        let lens0: [usize; 5] = parse_lens(data1);
        let lens1: [usize; 5] = parse_lens(data2);

        let mut offset0 = 5 * size_of::<u32>();
        let mut offset1 = 5 * size_of::<u32>();
        let index = 0;
        if let Some(order) = not_equal::<T0>(
            &data1[offset0..(offset0 + lens0[index])],
            &data2[offset1..(offset1 + lens1[index])],
        ) {
            return order;
        }
        offset0 += lens0[index];
        offset1 += lens1[index];

        let index = 1;
        if let Some(order) = not_equal::<T1>(
            &data1[offset0..(offset0 + lens0[index])],
            &data2[offset1..(offset1 + lens1[index])],
        ) {
            return order;
        }
        offset0 += lens0[index];
        offset1 += lens1[index];

        let index = 2;
        if let Some(order) = not_equal::<T2>(
            &data1[offset0..(offset0 + lens0[index])],
            &data2[offset1..(offset1 + lens1[index])],
        ) {
            return order;
        }
        offset0 += lens0[index];
        offset1 += lens1[index];

        let index = 3;
        if let Some(order) = not_equal::<T3>(
            &data1[offset0..(offset0 + lens0[index])],
            &data2[offset1..(offset1 + lens1[index])],
        ) {
            return order;
        }
        offset0 += lens0[index];
        offset1 += lens1[index];

        let index = 4;
        if let Some(order) = not_equal::<T4>(
            &data1[offset0..(offset0 + lens0[index])],
            &data2[offset1..(offset1 + lens1[index])],
        ) {
            return order;
        }
        offset0 += lens0[index];
        offset1 += lens1[index];

        T5::compare(&data1[offset0..], &data2[offset1..])
    }
}

impl<T0: RedbKey, T1: RedbKey, T2: RedbKey, T3: RedbKey, T4: RedbKey, T5: RedbKey, T6: RedbKey>
    RedbKey for (T0, T1, T2, T3, T4, T5, T6)
{
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        let lens0: [usize; 6] = parse_lens(data1);
        let lens1: [usize; 6] = parse_lens(data2);

        let mut offset0 = 6 * size_of::<u32>();
        let mut offset1 = 6 * size_of::<u32>();
        let index = 0;
        if let Some(order) = not_equal::<T0>(
            &data1[offset0..(offset0 + lens0[index])],
            &data2[offset1..(offset1 + lens1[index])],
        ) {
            return order;
        }
        offset0 += lens0[index];
        offset1 += lens1[index];

        let index = 1;
        if let Some(order) = not_equal::<T1>(
            &data1[offset0..(offset0 + lens0[index])],
            &data2[offset1..(offset1 + lens1[index])],
        ) {
            return order;
        }
        offset0 += lens0[index];
        offset1 += lens1[index];

        let index = 2;
        if let Some(order) = not_equal::<T2>(
            &data1[offset0..(offset0 + lens0[index])],
            &data2[offset1..(offset1 + lens1[index])],
        ) {
            return order;
        }
        offset0 += lens0[index];
        offset1 += lens1[index];

        let index = 3;
        if let Some(order) = not_equal::<T3>(
            &data1[offset0..(offset0 + lens0[index])],
            &data2[offset1..(offset1 + lens1[index])],
        ) {
            return order;
        }
        offset0 += lens0[index];
        offset1 += lens1[index];

        let index = 4;
        if let Some(order) = not_equal::<T4>(
            &data1[offset0..(offset0 + lens0[index])],
            &data2[offset1..(offset1 + lens1[index])],
        ) {
            return order;
        }
        offset0 += lens0[index];
        offset1 += lens1[index];

        let index = 5;
        if let Some(order) = not_equal::<T5>(
            &data1[offset0..(offset0 + lens0[index])],
            &data2[offset1..(offset1 + lens1[index])],
        ) {
            return order;
        }
        offset0 += lens0[index];
        offset1 += lens1[index];

        T6::compare(&data1[offset0..], &data2[offset1..])
    }
}

impl<
        T0: RedbKey,
        T1: RedbKey,
        T2: RedbKey,
        T3: RedbKey,
        T4: RedbKey,
        T5: RedbKey,
        T6: RedbKey,
        T7: RedbKey,
    > RedbKey for (T0, T1, T2, T3, T4, T5, T6, T7)
{
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        let lens0: [usize; 7] = parse_lens(data1);
        let lens1: [usize; 7] = parse_lens(data2);

        let mut offset0 = 7 * size_of::<u32>();
        let mut offset1 = 7 * size_of::<u32>();
        let index = 0;
        if let Some(order) = not_equal::<T0>(
            &data1[offset0..(offset0 + lens0[index])],
            &data2[offset1..(offset1 + lens1[index])],
        ) {
            return order;
        }
        offset0 += lens0[index];
        offset1 += lens1[index];

        let index = 1;
        if let Some(order) = not_equal::<T1>(
            &data1[offset0..(offset0 + lens0[index])],
            &data2[offset1..(offset1 + lens1[index])],
        ) {
            return order;
        }
        offset0 += lens0[index];
        offset1 += lens1[index];

        let index = 2;
        if let Some(order) = not_equal::<T2>(
            &data1[offset0..(offset0 + lens0[index])],
            &data2[offset1..(offset1 + lens1[index])],
        ) {
            return order;
        }
        offset0 += lens0[index];
        offset1 += lens1[index];

        let index = 3;
        if let Some(order) = not_equal::<T3>(
            &data1[offset0..(offset0 + lens0[index])],
            &data2[offset1..(offset1 + lens1[index])],
        ) {
            return order;
        }
        offset0 += lens0[index];
        offset1 += lens1[index];

        let index = 4;
        if let Some(order) = not_equal::<T4>(
            &data1[offset0..(offset0 + lens0[index])],
            &data2[offset1..(offset1 + lens1[index])],
        ) {
            return order;
        }
        offset0 += lens0[index];
        offset1 += lens1[index];

        let index = 5;
        if let Some(order) = not_equal::<T5>(
            &data1[offset0..(offset0 + lens0[index])],
            &data2[offset1..(offset1 + lens1[index])],
        ) {
            return order;
        }
        offset0 += lens0[index];
        offset1 += lens1[index];

        let index = 6;
        if let Some(order) = not_equal::<T6>(
            &data1[offset0..(offset0 + lens0[index])],
            &data2[offset1..(offset1 + lens1[index])],
        ) {
            return order;
        }
        offset0 += lens0[index];
        offset1 += lens1[index];

        T7::compare(&data1[offset0..], &data2[offset1..])
    }
}

impl<
        T0: RedbKey,
        T1: RedbKey,
        T2: RedbKey,
        T3: RedbKey,
        T4: RedbKey,
        T5: RedbKey,
        T6: RedbKey,
        T7: RedbKey,
        T8: RedbKey,
    > RedbKey for (T0, T1, T2, T3, T4, T5, T6, T7, T8)
{
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        let lens0: [usize; 8] = parse_lens(data1);
        let lens1: [usize; 8] = parse_lens(data2);

        #[allow(clippy::manual_bits)]
        let mut offset0 = 8 * size_of::<u32>();
        #[allow(clippy::manual_bits)]
        let mut offset1 = 8 * size_of::<u32>();
        let index = 0;
        if let Some(order) = not_equal::<T0>(
            &data1[offset0..(offset0 + lens0[index])],
            &data2[offset1..(offset1 + lens1[index])],
        ) {
            return order;
        }
        offset0 += lens0[index];
        offset1 += lens1[index];

        let index = 1;
        if let Some(order) = not_equal::<T1>(
            &data1[offset0..(offset0 + lens0[index])],
            &data2[offset1..(offset1 + lens1[index])],
        ) {
            return order;
        }
        offset0 += lens0[index];
        offset1 += lens1[index];

        let index = 2;
        if let Some(order) = not_equal::<T2>(
            &data1[offset0..(offset0 + lens0[index])],
            &data2[offset1..(offset1 + lens1[index])],
        ) {
            return order;
        }
        offset0 += lens0[index];
        offset1 += lens1[index];

        let index = 3;
        if let Some(order) = not_equal::<T3>(
            &data1[offset0..(offset0 + lens0[index])],
            &data2[offset1..(offset1 + lens1[index])],
        ) {
            return order;
        }
        offset0 += lens0[index];
        offset1 += lens1[index];

        let index = 4;
        if let Some(order) = not_equal::<T4>(
            &data1[offset0..(offset0 + lens0[index])],
            &data2[offset1..(offset1 + lens1[index])],
        ) {
            return order;
        }
        offset0 += lens0[index];
        offset1 += lens1[index];

        let index = 5;
        if let Some(order) = not_equal::<T5>(
            &data1[offset0..(offset0 + lens0[index])],
            &data2[offset1..(offset1 + lens1[index])],
        ) {
            return order;
        }
        offset0 += lens0[index];
        offset1 += lens1[index];

        let index = 6;
        if let Some(order) = not_equal::<T6>(
            &data1[offset0..(offset0 + lens0[index])],
            &data2[offset1..(offset1 + lens1[index])],
        ) {
            return order;
        }
        offset0 += lens0[index];
        offset1 += lens1[index];

        let index = 7;
        if let Some(order) = not_equal::<T7>(
            &data1[offset0..(offset0 + lens0[index])],
            &data2[offset1..(offset1 + lens1[index])],
        ) {
            return order;
        }
        offset0 += lens0[index];
        offset1 += lens1[index];

        T8::compare(&data1[offset0..], &data2[offset1..])
    }
}

impl<
        T0: RedbKey,
        T1: RedbKey,
        T2: RedbKey,
        T3: RedbKey,
        T4: RedbKey,
        T5: RedbKey,
        T6: RedbKey,
        T7: RedbKey,
        T8: RedbKey,
        T9: RedbKey,
    > RedbKey for (T0, T1, T2, T3, T4, T5, T6, T7, T8, T9)
{
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        let lens0: [usize; 9] = parse_lens(data1);
        let lens1: [usize; 9] = parse_lens(data2);

        let mut offset0 = 9 * size_of::<u32>();
        let mut offset1 = 9 * size_of::<u32>();
        let index = 0;
        if let Some(order) = not_equal::<T0>(
            &data1[offset0..(offset0 + lens0[index])],
            &data2[offset1..(offset1 + lens1[index])],
        ) {
            return order;
        }
        offset0 += lens0[index];
        offset1 += lens1[index];

        let index = 1;
        if let Some(order) = not_equal::<T1>(
            &data1[offset0..(offset0 + lens0[index])],
            &data2[offset1..(offset1 + lens1[index])],
        ) {
            return order;
        }
        offset0 += lens0[index];
        offset1 += lens1[index];

        let index = 2;
        if let Some(order) = not_equal::<T2>(
            &data1[offset0..(offset0 + lens0[index])],
            &data2[offset1..(offset1 + lens1[index])],
        ) {
            return order;
        }
        offset0 += lens0[index];
        offset1 += lens1[index];

        let index = 3;
        if let Some(order) = not_equal::<T3>(
            &data1[offset0..(offset0 + lens0[index])],
            &data2[offset1..(offset1 + lens1[index])],
        ) {
            return order;
        }
        offset0 += lens0[index];
        offset1 += lens1[index];

        let index = 4;
        if let Some(order) = not_equal::<T4>(
            &data1[offset0..(offset0 + lens0[index])],
            &data2[offset1..(offset1 + lens1[index])],
        ) {
            return order;
        }
        offset0 += lens0[index];
        offset1 += lens1[index];

        let index = 5;
        if let Some(order) = not_equal::<T5>(
            &data1[offset0..(offset0 + lens0[index])],
            &data2[offset1..(offset1 + lens1[index])],
        ) {
            return order;
        }
        offset0 += lens0[index];
        offset1 += lens1[index];

        let index = 6;
        if let Some(order) = not_equal::<T6>(
            &data1[offset0..(offset0 + lens0[index])],
            &data2[offset1..(offset1 + lens1[index])],
        ) {
            return order;
        }
        offset0 += lens0[index];
        offset1 += lens1[index];

        let index = 7;
        if let Some(order) = not_equal::<T7>(
            &data1[offset0..(offset0 + lens0[index])],
            &data2[offset1..(offset1 + lens1[index])],
        ) {
            return order;
        }
        offset0 += lens0[index];
        offset1 += lens1[index];

        let index = 8;
        if let Some(order) = not_equal::<T8>(
            &data1[offset0..(offset0 + lens0[index])],
            &data2[offset1..(offset1 + lens1[index])],
        ) {
            return order;
        }
        offset0 += lens0[index];
        offset1 += lens1[index];

        T9::compare(&data1[offset0..], &data2[offset1..])
    }
}
