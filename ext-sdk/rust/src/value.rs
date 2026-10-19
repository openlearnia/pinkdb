//! The typed value model: what an extension function's argument and return
//! types must implement so the registration macro can generate the ABI glue.
//!
//! The traits are deliberately small and closed: every implementor is one of
//! the ABI v1 types, so the macro can derive a function's declared argument
//! tags ([`ExtArgType::EXT_TYPE`]) and return tag
//! ([`IntoExtValue::RET_TYPE`]) from the Rust types the author writes. A
//! custom implementor cannot invent a new ABI type — it can only name an
//! existing one — which is exactly the guarantee the host relies on.

use pinkdb_ext_loader::{ExtArg, ExtType, ExtValue};

/// The ABI type a Rust argument type maps to.
///
/// Implemented for the seven ABI v1 value types and for `Option<T>`, which
/// wraps the same tag and additionally tells the generated glue that NULL is
/// a legal input (see [`FromExtArg`]).
pub trait ExtArgType {
    /// The `PINKDB_EXT_TYPE_*` tag this type is registered under.
    const EXT_TYPE: ExtType;
}

impl ExtArgType for bool {
    const EXT_TYPE: ExtType = ExtType::Boolean;
}
impl ExtArgType for i32 {
    const EXT_TYPE: ExtType = ExtType::Integer;
}
impl ExtArgType for i64 {
    const EXT_TYPE: ExtType = ExtType::Bigint;
}
impl ExtArgType for f64 {
    const EXT_TYPE: ExtType = ExtType::Double;
}
impl ExtArgType for String {
    const EXT_TYPE: ExtType = ExtType::Varchar;
}
impl ExtArgType for &str {
    const EXT_TYPE: ExtType = ExtType::Varchar;
}
impl ExtArgType for Date {
    const EXT_TYPE: ExtType = ExtType::Date;
}
impl ExtArgType for Timestamp {
    const EXT_TYPE: ExtType = ExtType::Timestamp;
}
impl ExtArgType for Interval {
    const EXT_TYPE: ExtType = ExtType::Interval;
}
/// An optional argument keeps its inner type's tag and accepts NULL.
impl<T: ExtArgType> ExtArgType for Option<T> {
    const EXT_TYPE: ExtType = T::EXT_TYPE;
}
/// A possibly-absent argument keeps its inner type's tag.
impl<T: ExtArgType> ExtArgType for Optional<T> {
    const EXT_TYPE: ExtType = T::EXT_TYPE;
}

/// One argument of a function declared with `optional(min)`: it may not have
/// been passed at all.
///
/// The three states are the three things a ranged-arity call can say about a
/// position, and they are deliberately distinct — `log(10)` (absent) is a
/// base-10 logarithm while `log(2, NULL)` is SQL NULL:
///
/// - [`Optional::Absent`]: the caller stopped before this position;
/// - [`Optional::Null`]: the caller passed SQL NULL;
/// - [`Optional::Value`]: the caller passed a value.
///
/// A parameter *below* the declared minimum is always `Value` (the binder and
/// the loader both refuse a shorter call); decode such a position with a
/// plain type to say so in the signature.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Optional<T> {
    /// The argument was not passed.
    Absent,
    /// The argument was SQL NULL.
    Null,
    /// The argument was a value of the declared type.
    Value(T),
}

impl<T> Optional<T> {
    /// Whether the argument was not passed at all.
    pub fn is_absent(&self) -> bool {
        matches!(self, Optional::Absent)
    }

    /// Whether the argument was SQL NULL.
    pub fn is_null(&self) -> bool {
        matches!(self, Optional::Null)
    }

    /// The value, if a value was passed (`None` for absent *and* NULL).
    pub fn value(&self) -> Option<&T> {
        match self {
            Optional::Value(value) => Some(value),
            Optional::Absent | Optional::Null => None,
        }
    }

    /// Consume the argument, yielding the value if a value was passed.
    pub fn into_value(self) -> Option<T> {
        match self {
            Optional::Value(value) => Some(value),
            Optional::Absent | Optional::Null => None,
        }
    }

    /// The value, or `default` for absent *and* NULL.
    pub fn value_or(self, default: T) -> T {
        self.into_value().unwrap_or(default)
    }
}

/// Decode one ABI argument into the Rust type an author wrote.
///
/// A NULL argument arrives as [`ExtArg::Null`] carrying the declared tag. The
/// plain implementations reject it with a message naming the fix (`Option<T>`
/// is the way to receive NULL); [`Option<T>`](Option)'s implementation maps it
/// to `None`. The host's binder already guarantees the declared type, so a
/// tag mismatch reaching a `decode` is an ABI/engine bug: it is reported as a
/// clear error rather than a silent reinterpretation.
///
/// `&'a str` borrows the host's VARCHAR bytes, which are only valid for the
/// duration of the call — copy to a `String` to keep them.
pub trait FromExtArg<'a>: Sized {
    /// Decode the argument (never called with a type the host did not declare).
    fn from_ext_arg(arg: &'a ExtArg<'a>) -> Result<Self, String>;
}

/// The message a non-optional type produces for a NULL argument. `rust` is
/// the Rust type the author should reach for, `sql` the ABI type name used
/// for a plain tag mismatch.
fn type_message(sql: &str, rust: &str, arg: &ExtArg<'_>) -> String {
    match arg {
        ExtArg::Null(_) => format!(
            "NULL (declare the parameter `Option<{rust}>` to receive NULL, or `Option<_>` to \
             return NULL)"
        ),
        other => format!("expected {sql}, got {}", other.ext_type()),
    }
}

impl FromExtArg<'_> for bool {
    fn from_ext_arg(arg: &ExtArg<'_>) -> Result<bool, String> {
        match arg {
            ExtArg::Boolean(b) => Ok(*b),
            other => Err(type_message("BOOLEAN", "bool", other)),
        }
    }
}

impl FromExtArg<'_> for i32 {
    fn from_ext_arg(arg: &ExtArg<'_>) -> Result<i32, String> {
        match arg {
            ExtArg::Integer(i) => Ok(*i),
            other => Err(type_message("INTEGER", "i32", other)),
        }
    }
}

impl FromExtArg<'_> for i64 {
    fn from_ext_arg(arg: &ExtArg<'_>) -> Result<i64, String> {
        match arg {
            ExtArg::Bigint(i) => Ok(*i),
            other => Err(type_message("BIGINT", "i64", other)),
        }
    }
}

impl FromExtArg<'_> for f64 {
    fn from_ext_arg(arg: &ExtArg<'_>) -> Result<f64, String> {
        match arg {
            ExtArg::Double(x) => Ok(*x),
            other => Err(type_message("DOUBLE", "f64", other)),
        }
    }
}

impl FromExtArg<'_> for String {
    fn from_ext_arg(arg: &ExtArg<'_>) -> Result<String, String> {
        match arg {
            ExtArg::Varchar(bytes) => String::from_utf8(bytes.to_vec())
                .map_err(|_| "VARCHAR bytes are not valid UTF-8".to_string()),
            other => Err(type_message("VARCHAR", "String", other)),
        }
    }
}

impl<'a> FromExtArg<'a> for &'a str {
    fn from_ext_arg(arg: &'a ExtArg<'a>) -> Result<&'a str, String> {
        match arg {
            ExtArg::Varchar(bytes) => std::str::from_utf8(bytes)
                .map_err(|_| "VARCHAR bytes are not valid UTF-8".to_string()),
            other => Err(type_message("VARCHAR", "&str", other)),
        }
    }
}

impl FromExtArg<'_> for Date {
    fn from_ext_arg(arg: &ExtArg<'_>) -> Result<Date, String> {
        match arg {
            ExtArg::Date(days) => Ok(Date(*days)),
            other => Err(type_message("DATE", "Date", other)),
        }
    }
}

impl FromExtArg<'_> for Timestamp {
    fn from_ext_arg(arg: &ExtArg<'_>) -> Result<Timestamp, String> {
        match arg {
            ExtArg::Timestamp(micros) => Ok(Timestamp(*micros)),
            other => Err(type_message("TIMESTAMP", "Timestamp", other)),
        }
    }
}

impl FromExtArg<'_> for Interval {
    fn from_ext_arg(arg: &ExtArg<'_>) -> Result<Interval, String> {
        match arg {
            ExtArg::Interval {
                months,
                days,
                micros,
            } => Ok(Interval {
                months: *months,
                days: *days,
                micros: *micros,
            }),
            other => Err(type_message("INTERVAL", "Interval", other)),
        }
    }
}

impl<'a, T: FromExtArg<'a>> FromExtArg<'a> for Option<T> {
    fn from_ext_arg(arg: &'a ExtArg<'a>) -> Result<Option<T>, String> {
        match arg {
            ExtArg::Null(_) => Ok(None),
            other => T::from_ext_arg(other).map(Some),
        }
    }
}

/// Decode argument `index` of a call whose arity may be **short** (a function
/// declared with `optional(min)`).
///
/// Implemented for the ABI v1 types and for [`Optional<T>`]. The two families
/// differ in exactly one way: an index past the end of the call is
///
/// - an error for a plain type — the parameter is required (the loader and
///   the engine both refuse a call below the declared minimum, so this arm is
///   a defensive one);
/// - [`Optional::Absent`] for [`Optional<T>`], which is the whole point.
///
/// The generated glue uses this trait for every parameter of an
/// `optional(min)` declaration; a fixed-arity declaration keeps using
/// [`FromExtArg`] (and never sees a short call).
pub trait FromExtArgAt<'a>: Sized {
    /// Decode the argument at `index`, distinguishing "not passed".
    fn from_ext_arg_at(args: &'a [ExtArg<'a>], index: usize) -> Result<Self, String>;
}

/// The plain types are decoded exactly like [`FromExtArg`]; a missing
/// argument is an error naming the parameter (never a silent NULL).
macro_rules! required_at {
    ($($ty:ty),* $(,)?) => {
        $(
            impl<'a> FromExtArgAt<'a> for $ty {
                fn from_ext_arg_at(args: &'a [ExtArg<'a>], index: usize) -> Result<Self, String> {
                    match args.get(index) {
                        Some(arg) => <$ty as FromExtArg<'a>>::from_ext_arg(arg),
                        None => Err(format!(
                            "argument {} was not passed (this parameter is required)",
                            index + 1
                        )),
                    }
                }
            }
        )*
    };
}

required_at!(bool, i32, i64, f64, String, &'a str, Date, Timestamp);

impl<'a, T: FromExtArg<'a>> FromExtArgAt<'a> for Option<T> {
    fn from_ext_arg_at(args: &'a [ExtArg<'a>], index: usize) -> Result<Self, String> {
        match args.get(index) {
            Some(arg) => <Option<T> as FromExtArg<'a>>::from_ext_arg(arg),
            None => Err(format!(
                "argument {} was not passed (this parameter is required)",
                index + 1
            )),
        }
    }
}

impl<'a, T: FromExtArg<'a>> FromExtArgAt<'a> for Optional<T> {
    fn from_ext_arg_at(args: &'a [ExtArg<'a>], index: usize) -> Result<Self, String> {
        match args.get(index) {
            None => Ok(Optional::Absent),
            Some(ExtArg::Null(_)) => Ok(Optional::Null),
            Some(arg) => T::from_ext_arg(arg).map(Optional::Value),
        }
    }
}

/// Decode a whole argument list into `Vec<T>` — the tail of a function
/// declared with `variadic(min)` (`Vec<Option<i64>>` for a NULL-able tail).
///
/// The borrow is tied to `args`, so `Vec<&str>` borrows the host's VARCHAR
/// bytes for the duration of the call (copy to `String` to keep them).
/// Errors name the 1-based argument, like every other decode path.
pub fn decode_arguments<'a, T: FromExtArg<'a>>(args: &'a [ExtArg<'a>]) -> Result<Vec<T>, String> {
    args.iter()
        .enumerate()
        .map(|(i, arg)| T::from_ext_arg(arg).map_err(|e| format!("argument {}: {e}", i + 1)))
        .collect()
}

/// Encode a Rust return value into an ABI result.
///
/// `Result<_, String>` wraps another implementor and turns its `Err` into the
/// statement error the host reports (on native targets the C ABI carries a
/// code, not text — see the crate docs). `Option<T>` turns `None` into SQL
/// NULL. `RET_TYPE` is the tag the function is registered under; the host
/// checks every non-NULL result against it.
pub trait IntoExtValue {
    /// The `PINKDB_EXT_TYPE_*` tag the function is registered under.
    const RET_TYPE: ExtType;
    /// Encode the value; `Err` fails the enclosing SQL statement.
    fn into_ext_value(self) -> Result<ExtValue, String>;
}

impl IntoExtValue for bool {
    const RET_TYPE: ExtType = ExtType::Boolean;
    fn into_ext_value(self) -> Result<ExtValue, String> {
        Ok(ExtValue::Boolean(self))
    }
}

impl IntoExtValue for i32 {
    const RET_TYPE: ExtType = ExtType::Integer;
    fn into_ext_value(self) -> Result<ExtValue, String> {
        Ok(ExtValue::Integer(self))
    }
}

impl IntoExtValue for i64 {
    const RET_TYPE: ExtType = ExtType::Bigint;
    fn into_ext_value(self) -> Result<ExtValue, String> {
        Ok(ExtValue::Bigint(self))
    }
}

impl IntoExtValue for f64 {
    const RET_TYPE: ExtType = ExtType::Double;
    fn into_ext_value(self) -> Result<ExtValue, String> {
        Ok(ExtValue::Double(self))
    }
}

impl IntoExtValue for String {
    const RET_TYPE: ExtType = ExtType::Varchar;
    fn into_ext_value(self) -> Result<ExtValue, String> {
        Ok(ExtValue::Varchar(self.into_bytes()))
    }
}

impl IntoExtValue for &'static str {
    const RET_TYPE: ExtType = ExtType::Varchar;
    fn into_ext_value(self) -> Result<ExtValue, String> {
        Ok(ExtValue::Varchar(self.as_bytes().to_vec()))
    }
}

impl IntoExtValue for Date {
    const RET_TYPE: ExtType = ExtType::Date;
    fn into_ext_value(self) -> Result<ExtValue, String> {
        Ok(ExtValue::Date(self.0))
    }
}

impl IntoExtValue for Timestamp {
    const RET_TYPE: ExtType = ExtType::Timestamp;
    fn into_ext_value(self) -> Result<ExtValue, String> {
        Ok(ExtValue::Timestamp(self.0))
    }
}

impl IntoExtValue for Interval {
    const RET_TYPE: ExtType = ExtType::Interval;
    fn into_ext_value(self) -> Result<ExtValue, String> {
        Ok(ExtValue::Interval {
            months: self.months,
            days: self.days,
            micros: self.micros,
        })
    }
}

impl<T: IntoExtValue> IntoExtValue for Option<T> {
    const RET_TYPE: ExtType = T::RET_TYPE;
    fn into_ext_value(self) -> Result<ExtValue, String> {
        match self {
            Some(value) => value.into_ext_value(),
            None => Ok(ExtValue::Null),
        }
    }
}

impl<T: IntoExtValue> IntoExtValue for Result<T, String> {
    const RET_TYPE: ExtType = T::RET_TYPE;
    fn into_ext_value(self) -> Result<ExtValue, String> {
        self.and_then(T::into_ext_value)
    }
}

/// A SQL `DATE`: days since 1970-01-01, exactly the ABI's representation.
///
/// The SDK does no calendar arithmetic — build one from
/// [`FromExtArg`]`::from_ext_arg`, an integer day count, or a date library;
/// the engine prints it in ISO form.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Date(pub i32);

/// A SQL `TIMESTAMP`: microseconds since 1970-01-01 00:00:00 UTC, exactly the
/// ABI's representation (no time zone is carried).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(pub i64);

/// A SQL `INTERVAL`: DuckDB's three-field calendar interval — whole `months`,
/// whole `days`, and a `micros` time part.
///
/// The fields are independent and signed; the engine normalizes nothing
/// (`age(TIMESTAMP '2024-03-01', TIMESTAMP '2023-01-15')` returns exactly the
/// `1 year 1 month 17 days` decomposition as `months = 13, days = 17,
/// micros = 0`). The by-value ABI payload is produced by the loader's
/// `pack_interval` (micros in `int64`, months/days in the bits of `double`);
/// this SDK does that marshalling, so authors just construct the struct.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Interval {
    /// Whole calendar months (12 per year when displayed).
    pub months: i32,
    /// Whole days.
    pub days: i32,
    /// Time part in microseconds.
    pub micros: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_abi_type_has_a_tag_and_option_keeps_it() {
        assert_eq!(bool::EXT_TYPE, ExtType::Boolean);
        assert_eq!(i32::EXT_TYPE, ExtType::Integer);
        assert_eq!(i64::EXT_TYPE, ExtType::Bigint);
        assert_eq!(f64::EXT_TYPE, ExtType::Double);
        assert_eq!(<String as ExtArgType>::EXT_TYPE, ExtType::Varchar);
        assert_eq!(<&str as ExtArgType>::EXT_TYPE, ExtType::Varchar);
        assert_eq!(Date::EXT_TYPE, ExtType::Date);
        assert_eq!(Timestamp::EXT_TYPE, ExtType::Timestamp);
        assert_eq!(Interval::EXT_TYPE, ExtType::Interval);
        assert_eq!(Option::<i64>::EXT_TYPE, ExtType::Bigint);
        assert_eq!(Option::<String>::EXT_TYPE, ExtType::Varchar);

        assert_eq!(i32::RET_TYPE, ExtType::Integer);
        assert_eq!(<&'static str as IntoExtValue>::RET_TYPE, ExtType::Varchar);
        assert_eq!(<Option<i64> as IntoExtValue>::RET_TYPE, ExtType::Bigint);
        assert_eq!(Interval::RET_TYPE, ExtType::Interval);
        assert_eq!(
            <Result<Option<String>, String> as IntoExtValue>::RET_TYPE,
            ExtType::Varchar
        );
    }

    #[test]
    fn decoding_covers_every_value_and_null_policy() {
        let arg = ExtArg::Bigint(7);
        assert_eq!(i64::from_ext_arg(&arg).unwrap(), 7);
        assert_eq!(Option::<i64>::from_ext_arg(&arg).unwrap(), Some(7));

        let null = ExtArg::Null(ExtType::Bigint);
        assert_eq!(Option::<i64>::from_ext_arg(&null).unwrap(), None);
        let message = i64::from_ext_arg(&null).unwrap_err();
        assert!(message.contains("NULL"), "{message}");
        assert!(message.contains("Option<i64>"), "{message}");

        let bytes = b"h\xc3\xa9llo";
        let text = ExtArg::Varchar(bytes);
        assert_eq!(String::from_ext_arg(&text).unwrap(), "héllo");
        assert_eq!(<&str>::from_ext_arg(&text).unwrap(), "héllo");
        assert!(<&str>::from_ext_arg(&ExtArg::Integer(1)).is_err());
        assert!(
            String::from_ext_arg(&ExtArg::Varchar(&[0xff, 0xfe]))
                .unwrap_err()
                .contains("UTF-8")
        );

        assert!(bool::from_ext_arg(&ExtArg::Boolean(true)).unwrap());
        assert_eq!(f64::from_ext_arg(&ExtArg::Double(2.5)).unwrap(), 2.5);
        assert_eq!(Date::from_ext_arg(&ExtArg::Date(19)).unwrap(), Date(19));
        assert_eq!(
            Timestamp::from_ext_arg(&ExtArg::Timestamp(5)).unwrap(),
            Timestamp(5)
        );
        let iv = ExtArg::Interval {
            months: 13,
            days: -2,
            micros: 7,
        };
        assert_eq!(
            Interval::from_ext_arg(&iv).unwrap(),
            Interval {
                months: 13,
                days: -2,
                micros: 7
            }
        );
        assert!(
            Interval::from_ext_arg(&ExtArg::Null(ExtType::Interval))
                .unwrap_err()
                .contains("Option<Interval>")
        );
        // A wrong runtime tag is a clear error, never a reinterpretation.
        let message = i64::from_ext_arg(&ExtArg::Varchar(b"x")).unwrap_err();
        assert_eq!(message, "expected BIGINT, got VARCHAR");
    }

    #[test]
    fn encoding_covers_every_value_and_errors() {
        assert_eq!(true.into_ext_value().unwrap(), ExtValue::Boolean(true));
        assert_eq!(3i32.into_ext_value().unwrap(), ExtValue::Integer(3));
        assert_eq!(4i64.into_ext_value().unwrap(), ExtValue::Bigint(4));
        assert_eq!(1.5f64.into_ext_value().unwrap(), ExtValue::Double(1.5));
        assert_eq!(
            "hi".to_string().into_ext_value().unwrap(),
            ExtValue::Varchar(b"hi".to_vec())
        );
        assert_eq!(
            "lit".into_ext_value().unwrap(),
            ExtValue::Varchar(b"lit".to_vec())
        );
        assert_eq!(Date(1).into_ext_value().unwrap(), ExtValue::Date(1));
        assert_eq!(
            Timestamp(2).into_ext_value().unwrap(),
            ExtValue::Timestamp(2)
        );
        assert_eq!(
            Interval {
                months: 1,
                days: 2,
                micros: 3
            }
            .into_ext_value()
            .unwrap(),
            ExtValue::Interval {
                months: 1,
                days: 2,
                micros: 3
            }
        );
        assert_eq!(
            Option::<i64>::None.into_ext_value().unwrap(),
            ExtValue::Null
        );
        assert_eq!(Some(9i64).into_ext_value().unwrap(), ExtValue::Bigint(9));
        let err = Err::<i64, String>("nope".to_string())
            .into_ext_value()
            .unwrap_err();
        assert_eq!(err, "nope");
        let ok: Result<Option<String>, String> = Ok(Some("x".into()));
        assert_eq!(
            ok.into_ext_value().unwrap(),
            ExtValue::Varchar(b"x".to_vec())
        );
    }

    /// Wave 54: `Optional<T>` carries the three states a ranged-arity call
    /// can say about a position, and its type tag is its inner type's.
    #[test]
    fn optional_keeps_absent_null_and_value_apart() {
        let absent = Optional::<i64>::Absent;
        let null = Optional::<i64>::Null;
        let value = Optional::Value(7i64);
        assert!(absent.is_absent() && !absent.is_null());
        assert!(null.is_null() && !null.is_absent());
        assert_eq!(value.value(), Some(&7));
        assert_eq!(value.into_value(), Some(7));
        assert_eq!(absent.value(), None);
        assert_eq!(null.value(), None);
        assert_eq!(absent.into_value(), None);
        assert_eq!(null.into_value(), None);
        assert_eq!(Optional::Value(3i64).value_or(9), 3);
        assert_eq!(absent.value_or(9), 9);
        assert_eq!(null.value_or(9), 9);
        assert_eq!(Optional::<i64>::EXT_TYPE, ExtType::Bigint);
        assert_eq!(Optional::<f64>::EXT_TYPE, ExtType::Double);
        assert_eq!(Optional::<&str>::EXT_TYPE, ExtType::Varchar);
    }

    /// Wave 54: the ranged-arity decode — a plain parameter is an error when
    /// absent (defensive: the engine refuses a short call first), while
    /// `Optional<T>` reports `Absent`; NULL and values decode alike.
    #[test]
    fn from_ext_arg_at_decodes_short_calls() {
        let args = [ExtArg::Bigint(7), ExtArg::Null(ExtType::Bigint)];
        assert_eq!(
            <Optional<i64> as FromExtArgAt>::from_ext_arg_at(&args, 0).unwrap(),
            Optional::Value(7)
        );
        assert_eq!(
            <Optional<i64> as FromExtArgAt>::from_ext_arg_at(&args, 1).unwrap(),
            Optional::Null
        );
        assert_eq!(
            <Optional<i64> as FromExtArgAt>::from_ext_arg_at(&args, 2).unwrap(),
            Optional::Absent
        );
        assert!(<i64 as FromExtArgAt>::from_ext_arg_at(&args, 2).is_err());
        assert!(<Option<i64> as FromExtArgAt>::from_ext_arg_at(&args, 2).is_err());
        assert_eq!(<i64 as FromExtArgAt>::from_ext_arg_at(&args, 0).unwrap(), 7);
        // Absent is not an error for the wrapper; a *wrong* type still is.
        assert!(
            <Optional<i64> as FromExtArgAt>::from_ext_arg_at(&[ExtArg::Varchar(b"x")], 0).is_err()
        );
    }

    /// Wave 54: the variadic tail decodes every argument, with 1-based
    /// error positions, and borrowed elements keep the caller's lifetime.
    #[test]
    fn decode_arguments_covers_a_variadic_tail() {
        let args = [
            ExtArg::Bigint(1),
            ExtArg::Null(ExtType::Bigint),
            ExtArg::Bigint(3),
        ];
        assert_eq!(
            decode_arguments::<Option<i64>>(&args).unwrap(),
            vec![Some(1), None, Some(3)]
        );
        assert!(decode_arguments::<i64>(&args).is_err());
        let message = decode_arguments::<i64>(&args).unwrap_err();
        assert!(
            message.contains("argument 2") && message.contains("Option<i64>"),
            "{message}"
        );
        assert_eq!(decode_arguments::<i64>(&[]).unwrap(), Vec::<i64>::new());

        let text = [ExtArg::Varchar(b"hi"), ExtArg::Varchar(b"yo")];
        let borrowed: Vec<&str> = decode_arguments(&text).unwrap();
        assert_eq!(borrowed, vec!["hi", "yo"]);
        // The error names the 1-based position.
        let bad = [ExtArg::Varchar(b"hi"), ExtArg::Bigint(2)];
        let message = decode_arguments::<&str>(&bad).unwrap_err();
        assert!(message.contains("argument 2"), "{message}");
    }
}
