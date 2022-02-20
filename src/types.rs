use std::cmp::Ordering;
use std::convert::TryInto;
use std::fmt::Debug;
use std::mem::size_of;

pub trait RedbValue {
    type View<'a>: Debug + 'a
    where
        Self: 'a;
    type AsBytes<'a>: AsRef<[u8]> + 'a
    where
        Self: 'a;

    /// Deserializes data
    /// Implementations may return a view over data, or an owned type
    fn from_bytes<'a>(data: &'a [u8]) -> Self::View<'a>
    where
        Self: 'a;

    /// Serialize the key to a slice
    fn as_bytes(&self) -> Self::AsBytes<'_>;

    /// Globally unique identifier for this type
    fn redb_type_name() -> String;
}

pub trait RedbKey: RedbValue {
    /// Compare data1 with data2
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering;
}

impl RedbValue for [u8] {
    type View<'a> = &'a [u8]
    where
        Self: 'a;
    type AsBytes<'a> = &'a [u8]
    where
        Self: 'a;

    fn from_bytes<'a>(data: &'a [u8]) -> &'a [u8]
    where
        Self: 'a,
    {
        data
    }

    fn as_bytes(&self) -> &[u8] {
        self
    }

    fn redb_type_name() -> String {
        "[u8]".to_string()
    }
}

impl RedbKey for [u8] {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        data1.cmp(data2)
    }
}

impl RedbValue for str {
    type View<'a> = &'a str
    where
        Self: 'a;
    type AsBytes<'a> = &'a str
    where
        Self: 'a;

    fn from_bytes<'a>(data: &'a [u8]) -> &'a str
    where
        Self: 'a,
    {
        std::str::from_utf8(data).unwrap()
    }

    fn as_bytes(&self) -> &str {
        self
    }

    fn redb_type_name() -> String {
        "str".to_string()
    }
}

impl RedbKey for str {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        let str1 = str::from_bytes(data1);
        let str2 = str::from_bytes(data2);
        str1.cmp(str2)
    }
}

impl RedbValue for &str {
    type View<'a> = &'a str
    where
    Self: 'a;
    type AsBytes<'a> = &'a str
    where
    Self: 'a;

    fn from_bytes<'a>(data: &'a [u8]) -> &'a str
    where
        Self: 'a,
    {
        std::str::from_utf8(data).unwrap()
    }

    fn as_bytes(&self) -> &str {
        self
    }

    fn redb_type_name() -> String {
        "str".to_string()
    }
}

impl RedbKey for &str {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        let str1 = str::from_bytes(data1);
        let str2 = str::from_bytes(data2);
        str1.cmp(str2)
    }
}

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
        let t0_len = u32::from_be_bytes(data[0..size_of::<u32>()].try_into().unwrap()) as usize;
        let t0 = T0::from_bytes(&data[size_of::<u32>()..(size_of::<u32>() + t0_len)]);
        let t1 = T1::from_bytes(&data[(size_of::<u32>() + t0_len)..]);
        (t0, t1)
    }

    fn as_bytes(&self) -> Vec<u8> {
        let t0_bytes = self.0.as_bytes();
        let t1_bytes = self.1.as_bytes();
        let t0_bytes_ref = t0_bytes.as_ref();
        let t1_bytes_ref = t1_bytes.as_ref();
        let mut output =
            Vec::with_capacity(size_of::<u32>() + t0_bytes_ref.len() + t1_bytes_ref.len());
        output.extend_from_slice(&(t0_bytes_ref.len() as u32).to_be_bytes());
        output.extend_from_slice(t0_bytes_ref);
        output.extend_from_slice(t1_bytes_ref);

        output
    }

    fn redb_type_name() -> String {
        format!("({},{})", T0::redb_type_name(), T1::redb_type_name())
    }
}

macro_rules! be_value {
    ($t:ty) => {
        impl RedbValue for $t {
            type View<'a> = $t;
            type AsBytes<'a> = [u8; std::mem::size_of::<$t>()] where Self: 'a;

            fn from_bytes<'a>(data: &'a [u8]) -> $t
            where
                Self: 'a,
            {
                <$t>::from_le_bytes(data.try_into().unwrap())
            }

            fn as_bytes(&self) -> [u8; std::mem::size_of::<$t>()] {
                self.to_le_bytes()
            }

            fn redb_type_name() -> String {
                stringify!($t).to_string()
            }
        }
    };
}

macro_rules! be_impl {
    ($t:ty) => {
        be_value!($t);

        impl RedbKey for $t {
            fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
                Self::from_bytes(data1).cmp(&Self::from_bytes(data2))
            }
        }
    };
}

be_impl!(u8);
be_impl!(u16);
be_impl!(u32);
be_impl!(u64);
be_impl!(u128);
be_impl!(i8);
be_impl!(i16);
be_impl!(i32);
be_impl!(i64);
be_impl!(i128);
be_value!(f32);
be_value!(f64);
