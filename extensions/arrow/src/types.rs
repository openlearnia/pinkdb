//! The physical type mapping between pinkdb engine types and Arrow types.
//!
//! ### Engine → Arrow (write)
//!
//! | Engine | Arrow |
//! |---|---|
//! | `BOOLEAN` | `Boolean` |
//! | `INTEGER` | `Int32` |
//! | `BIGINT` | `Int64` |
//! | `DOUBLE` | `Float64` |
//! | `VARCHAR` | `Utf8` |
//! | `DATE` | `Date32` (days since epoch) |
//! | `TIMESTAMP` | `Timestamp(Microsecond, no time zone)` |
//! | `DECIMAL(p,s)` | `Decimal128(p,s)` |
//! | `INTERVAL` | `Interval(MonthDayNano)` (µs → ns, exact) |
//!
//! Every field is written nullable (the engine's constraints are metadata
//! and never enforced, so claiming non-nullability would be a lie about the
//! data). Writes are uncompressed.
//!
//! ### Arrow → Engine (read)
//!
//! The write mappings above, plus the widening/narrowing legs without an
//! engine counterpart — and **only** where the conversion is exact:
//!
//! | Arrow | Engine | Rule |
//! |---|---|---|
//! | `Int8`, `Int16` | `INTEGER` | widening |
//! | `UInt8`, `UInt16` | `INTEGER` | widening (fits `i32`) |
//! | `UInt32` | `BIGINT` | widening (fits `i64`) |
//! | `Float16`, `Float32` | `DOUBLE` | widening |
//! | `Utf8`, `LargeUtf8`, `Utf8View` | `VARCHAR` | UTF-8 validated by Arrow |
//! | `Date64` | `DATE` | ms → days, **must be midnight-exact** |
//! | `Timestamp(unit, tz)` | `TIMESTAMP` | µs; s/ms scale up (overflow checked), ns truncate; a time zone is kept as the UTC instant (the engine has no TIMESTAMP WITH TIME ZONE) |
//! | `Interval(YearMonth)` / `Interval(DayTime)` | `INTERVAL` | zero-filled fields |
//! | `Decimal32`, `Decimal64` | `DECIMAL(p,s)` | widening to `i128` |
//!
//! Everything else is **refused at schema time** with a clean error naming
//! the column and the Arrow type: `Binary`/`LargeBinary`/`BinaryView`/
//! `FixedSizeBinary` (no BLOB), `Time32`/`Time64` (no TIME), `UInt64`
//! (cannot overflow-check a whole column cheaply at schema time — refused;
//! see the note below), `Float16`… all the way to `List`, `Struct`, `Map`,
//! `Union`, `Dictionary`, `RunEndEncoded`, `Duration`, `Null`, `Decimal256`
//! (precision > 38). Refusing at bind time keeps the "never a silent
//! coercion" rule: a file either reads exactly or fails loudly.
//!
//! `UInt64` note: values that fit `i64` *could* be read, but a column-wide
//! check is a scan-time decision, not a schema decision, and the engine's
//! answer would depend on the data — refused uniformly (documented follow-up
//! if a real file needs it).

use arrow_schema::{DataType as ArrowType, Field, IntervalUnit, Schema, TimeUnit};
use pinkdb::types::Result;
use pinkdb::{DataType, PinkDbError};

/// The engine type an Arrow type reads as, or a clean error naming the
/// offending column and type.
pub fn engine_type(column: &str, arrow: &ArrowType) -> Result<DataType> {
    use ArrowType::*;
    Ok(match arrow {
        Boolean => DataType::Boolean,
        Int8 | Int16 | Int32 | UInt8 | UInt16 => DataType::Integer,
        Int64 | UInt32 => DataType::Bigint,
        Float16 | Float32 | Float64 => DataType::Double,
        Utf8 | LargeUtf8 | Utf8View => DataType::Varchar,
        Date32 | Date64 => DataType::Date,
        Timestamp(_, _) => DataType::Timestamp,
        Interval(IntervalUnit::YearMonth | IntervalUnit::DayTime | IntervalUnit::MonthDayNano) => {
            DataType::Interval
        }
        Decimal32(precision, scale) | Decimal64(precision, scale) => {
            engine_decimal(column, arrow, *precision, *scale)?
        }
        Decimal128(precision, scale) => engine_decimal(column, arrow, *precision, *scale)?,
        Decimal256(_, _) => return Err(unsupported(column, arrow, "precision exceeds 38 digits")),
        UInt64 => {
            return Err(unsupported(
                column,
                arrow,
                "the engine's BIGINT is signed; values above i64::MAX are not representable",
            ));
        }
        other => return Err(unsupported(column, other, "no exact engine counterpart")),
    })
}

/// The engine's DECIMAL is `(precision 1..=38, scale 0..=precision)`: an
/// Arrow decimal outside those bounds (including Arrow's legal *negative*
/// scales, which the engine cannot represent) is refused rather than
/// rewritten.
fn engine_decimal(column: &str, arrow: &ArrowType, precision: u8, scale: i8) -> Result<DataType> {
    if scale < 0 {
        return Err(unsupported(
            column,
            arrow,
            "negative decimal scales are not representable",
        ));
    }
    if precision == 0 || precision > pinkdb::types::DECIMAL_MAX_PRECISION {
        return Err(unsupported(
            column,
            arrow,
            "the engine's DECIMAL supports 1..=38 digits",
        ));
    }
    Ok(DataType::Decimal {
        precision,
        scale: scale as u8,
    })
}

fn unsupported(column: &str, arrow: &ArrowType, why: &str) -> PinkDbError {
    PinkDbError::Type(format!(
        "column \"{column}\": arrow type {arrow} cannot be read exactly ({why}); \
         the arrow extension refuses rather than coercing silently"
    ))
}

/// The engine schema a whole Arrow schema reads as, or a clean error naming
/// the first offending column.
pub fn engine_schema(schema: &Schema) -> Result<Vec<(String, DataType)>> {
    schema
        .fields()
        .iter()
        .map(|f| Ok((f.name().clone(), engine_type(f.name(), f.data_type())?)))
        .collect()
}

/// The Arrow field one engine column writes as.
pub fn arrow_field(name: &str, dtype: DataType) -> Result<Field> {
    Ok(Field::new(name, arrow_type(dtype)?, true))
}

/// The Arrow type one engine type writes as.
pub fn arrow_type(dtype: DataType) -> Result<ArrowType> {
    Ok(match dtype {
        DataType::Boolean => ArrowType::Boolean,
        DataType::Integer => ArrowType::Int32,
        DataType::Bigint => ArrowType::Int64,
        DataType::Double => ArrowType::Float64,
        DataType::Varchar => ArrowType::Utf8,
        DataType::Date => ArrowType::Date32,
        DataType::Timestamp => ArrowType::Timestamp(TimeUnit::Microsecond, None),
        DataType::Decimal { precision, scale } => ArrowType::Decimal128(precision, scale as i8),
        DataType::Interval => ArrowType::Interval(IntervalUnit::MonthDayNano),
    })
}

/// Engine-typed boundaries for an Arrow timestamp unit, as a multiplier of
/// microseconds: `(numerator, denominator)` — seconds/milliseconds scale up
/// (checked), nanoseconds scale down (truncating, documented).
pub(crate) fn timestamp_to_micros(v: i64, unit: TimeUnit) -> Result<i64> {
    let scaled = match unit {
        TimeUnit::Second => v.checked_mul(1_000_000),
        TimeUnit::Millisecond => v.checked_mul(1_000),
        TimeUnit::Microsecond => Some(v),
        TimeUnit::Nanosecond => Some(v / 1_000),
    };
    scaled.ok_or_else(|| {
        PinkDbError::Type(format!(
            "timestamp value {v} (unit {unit:?}) does not fit the engine's microsecond range"
        ))
    })
}

/// Engine-typed interval fields for Arrow's three interval layouts.
/// `MonthDayNano`'s nanoseconds become microseconds by truncation
/// (`/ 1000`); `DayTime`'s milliseconds multiply exactly.
pub(crate) fn interval_fields(months: i32, days: i32, nanos: i64) -> (i32, i32, i64) {
    (months, days, nanos / 1_000)
}

/// `Date64` is milliseconds since the epoch at midnight; anything else is
/// malformed and refused instead of silently truncated.
pub(crate) fn date64_to_days(ms: i64) -> Result<i32> {
    const DAY_MS: i64 = 86_400_000;
    if ms % DAY_MS != 0 {
        return Err(PinkDbError::Type(format!(
            "Date64 value {ms} is not midnight-aligned (the engine's DATE is whole days)"
        )));
    }
    i32::try_from(ms / DAY_MS).map_err(|_| {
        PinkDbError::Type(format!(
            "Date64 value {ms} is outside the engine's DATE range"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn engine_write_types_round_trip_through_the_read_mapping() {
        for dtype in [
            DataType::Boolean,
            DataType::Integer,
            DataType::Bigint,
            DataType::Double,
            DataType::Varchar,
            DataType::Date,
            DataType::Timestamp,
            DataType::Decimal {
                precision: 18,
                scale: 3,
            },
            DataType::Interval,
        ] {
            let arrow = arrow_type(dtype).unwrap();
            assert_eq!(
                engine_type("c", &arrow).unwrap(),
                dtype,
                "{dtype:?} -> {arrow:?} did not read back"
            );
        }
    }

    #[test]
    fn unsupported_arrow_types_are_refused_by_name() {
        let cases: Vec<(ArrowType, &str)> = vec![
            (ArrowType::Binary, "Binary"),
            (ArrowType::LargeBinary, "LargeBinary"),
            (ArrowType::FixedSizeBinary(4), "FixedSizeBinary"),
            (ArrowType::Time32(TimeUnit::Second), "Time32"),
            (ArrowType::Time64(TimeUnit::Microsecond), "Time64"),
            (ArrowType::UInt64, "UInt64"),
            (ArrowType::Duration(TimeUnit::Second), "Duration"),
            (ArrowType::Null, "Null"),
            (ArrowType::Decimal256(40, 2), "Decimal256"),
            (
                ArrowType::List(std::sync::Arc::new(Field::new(
                    "item",
                    ArrowType::Int32,
                    true,
                ))),
                "List",
            ),
        ];
        for (arrow, needle) in cases {
            let err = engine_type("c0", &arrow).unwrap_err().to_string();
            assert!(err.contains("c0"), "{err}");
            assert!(err.contains(needle), "{err}");
            assert!(err.contains("cannot be read exactly"), "{err}");
        }
    }

    #[test]
    fn widening_legs_map_without_the_engine_counterpart() {
        assert_eq!(
            engine_type("c", &ArrowType::Int8).unwrap(),
            DataType::Integer
        );
        assert_eq!(
            engine_type("c", &ArrowType::UInt32).unwrap(),
            DataType::Bigint
        );
        assert_eq!(
            engine_type("c", &ArrowType::Float32).unwrap(),
            DataType::Double
        );
        assert_eq!(
            engine_type("c", &ArrowType::LargeUtf8).unwrap(),
            DataType::Varchar
        );
        assert_eq!(
            engine_type("c", &ArrowType::Decimal64(12, 4)).unwrap(),
            DataType::Decimal {
                precision: 12,
                scale: 4
            }
        );
    }

    #[test]
    fn timestamp_units_convert_exactly_or_refuse() {
        assert_eq!(
            timestamp_to_micros(1_500, TimeUnit::Millisecond).unwrap(),
            1_500_000
        );
        assert_eq!(timestamp_to_micros(2, TimeUnit::Second).unwrap(), 2_000_000);
        assert_eq!(timestamp_to_micros(-3, TimeUnit::Microsecond).unwrap(), -3);
        // Nanoseconds truncate toward zero (documented precision reduction).
        assert_eq!(timestamp_to_micros(1_500, TimeUnit::Nanosecond).unwrap(), 1);
        assert_eq!(
            timestamp_to_micros(-1_500, TimeUnit::Nanosecond).unwrap(),
            -1
        );
        // Seconds overflow the microsecond range.
        let err = timestamp_to_micros(i64::MAX / 2, TimeUnit::Second)
            .unwrap_err()
            .to_string();
        assert!(err.contains("microsecond range"), "{err}");
    }

    #[test]
    fn date64_must_be_midnight_aligned() {
        assert_eq!(date64_to_days(86_400_000 * 3).unwrap(), 3);
        assert_eq!(date64_to_days(-86_400_000).unwrap(), -1);
        let err = date64_to_days(1).unwrap_err().to_string();
        assert!(err.contains("midnight"), "{err}");
    }

    #[test]
    fn interval_nanos_truncate_to_micros() {
        assert_eq!(interval_fields(1, -2, 1_000), (1, -2, 1));
        assert_eq!(interval_fields(0, 0, -1_500), (0, 0, -1));
    }
}
