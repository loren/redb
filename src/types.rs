use std::cmp::Ordering;
use std::convert::TryInto;
use std::fmt::Debug;

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

/// An interface for types to be in redb tables
///
/// Implement this trait to allow a type to be used as a value in a [TableDefinition](crate::TableDefinition).
/// Types which implement [VariableWidthValue] and [Ord] can be used as keys in a [TableDefinition](crate::TableDefinition)
pub trait VariableWidthValue: Debug {
    type AsBytes<'a>: AsRef<[u8]> + 'a
    where
        Self: 'a;

    /// Deserialize a stored series of bytes
    fn from_bytes<'a>(data: &'a [u8]) -> Self
    where
        Self: 'a;

    /// Serialize the value to a series of bytes
    fn to_bytes(&self) -> Self::AsBytes<'_>;

    /// Globally unique identifier for this type
    ///
    /// Note to implementors: This string is stored in the database file. Changing it indicates
    /// that it is a different type. It is recommended that you use the fully qualified name of your
    /// type, including the name of the crate.
    fn type_name() -> String;
}

impl<V> RedbValue for V
where
    V: VariableWidthValue,
{
    type View<'a> = V
    where
        Self: 'a;
    type AsBytes<'a> = V::AsBytes<'a>
    where
        Self: 'a;

    fn from_bytes<'a>(data: &'a [u8]) -> V
    where
        Self: 'a,
    {
        <V as VariableWidthValue>::from_bytes(data)
    }

    fn as_bytes(&self) -> V::AsBytes<'_> {
        self.to_bytes()
    }

    fn redb_type_name() -> String {
        V::type_name()
    }
}

impl<K> RedbKey for K
where
    K: VariableWidthValue + Ord,
{
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        let k1 = K::from_bytes(data1);
        let k2 = K::from_bytes(data2);
        k1.cmp(&k2)
    }
}

impl RedbValue for &[u8] {
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

impl RedbKey for &[u8] {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        data1.cmp(data2)
    }
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
