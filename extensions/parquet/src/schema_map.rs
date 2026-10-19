//! The Parquet → pinkdb type mapping (pinned against `duckdb`
//! v2.0.0-alpha46057) and the values the introspection functions render.
//!
//! | Parquet (physical + annotation) | pinkdb | α |
//! |---|---|---|
//! | BOOLEAN | BOOLEAN | BOOLEAN |
//! | INT32 (none / INT_8 / INT_16 / INT_32) | INTEGER | TINYINT / SMALLINT / INTEGER (narrower — widened here) |
//! | INT32 (UINT_8 / UINT_16 / UINT_32) | INTEGER | unsigned, narrower — widened here |
//! | INT32 DATE | DATE | DATE |
//! | INT32 DECIMAL(p,s) | DECIMAL(p,s) | DECIMAL(p,s) |
//! | INT64 (none / INT_64) | BIGINT | BIGINT |
//! | INT64 UINT_64 | BIGINT (values > i64::MAX are refused) | UBIGINT |
//! | INT64 TIMESTAMP_MILLIS / TIMESTAMP_MICROS | TIMESTAMP | TIMESTAMP (WITH TIME ZONE when the UTC flag is set) |
//! | INT64 DECIMAL(p,s) | DECIMAL(p,s) | DECIMAL(p,s) |
//! | INT96 | TIMESTAMP (sub-microsecond digits truncated) | TIMESTAMP |
//! | FLOAT | DOUBLE | FLOAT |
//! | DOUBLE | DOUBLE | DOUBLE |
//! | BYTE_ARRAY (none) | VARCHAR (α's `binary_as_string=true`; invalid UTF-8 refused) | BLOB |
//! | BYTE_ARRAY UTF8 / ENUM / JSON | VARCHAR | VARCHAR |
//! | FIXED_LEN_BYTE_ARRAY DECIMAL(p,s) | DECIMAL(p,s) | DECIMAL(p,s) |
//! | FIXED_LEN_BYTE_ARRAY INTERVAL (12) | INTERVAL | INTERVAL |
//! | INT64 TIME_MICROS, INT32 TIME_MILLIS, TIME(NANOS) | **refused** (no TIME type) | TIME |
//! | TIMESTAMP(NANOS) | **refused** (microsecond precision) | TIMESTAMP_NS |
//! | FIXED_LEN_BYTE_ARRAY UUID / FLOAT16, BSON, VARIANT, GEOMETRY, GEOGRAPHY | **refused** | UUID et al. |
//!
//! "Refused" is a bind-time error naming the column — never a silent
//! re-typing: a TIME column read as VARCHAR would compare wrongly in SQL.
//! Nested schemas (any group with more than one level under the schema root,
//! repeated fields, maps/lists) are refused up front.

use parquet::basic::{ConvertedType, LogicalType, TimeUnit, Type as PhysicalType};
use parquet::schema::types::Type;
use pinkdb::{DataType, PinkDbError, Result, Value};

/// One leaf column of a flat parquet file.
#[derive(Debug, Clone, PartialEq)]
pub struct FileColumn {
    pub name: String,
    /// The engine type the column reads as.
    pub ty: DataType,
    /// Leaf ordinal within the file (the column index for `get_column_reader`).
    pub leaf: usize,
    /// Whether the column may hold NULLs (a `REQUIRED` column never does).
    pub optional: bool,
    /// The physical type, kept for the decode dispatch.
    pub physical: PhysicalType,
    /// An unsigned integer column (UINT_8/16/32/64): widened into a signed
    /// engine type, so its stored statistics order is not the engine's and
    /// min/max pruning is disabled for it.
    pub unsigned: bool,
    /// `type_length` for a FIXED_LEN_BYTE_ARRAY column (`parquet_schema`).
    pub type_length: i32,
}

/// The engine type of one primitive parquet schema element, or a
/// human-readable reason why it is unsupported.
pub fn engine_type(
    physical: PhysicalType,
    converted: ConvertedType,
    logical: Option<&LogicalType>,
    precision: i32,
    scale: i32,
) -> std::result::Result<DataType, String> {
    // The logical type (2.4+) wins over the deprecated converted type when
    // both are present, exactly like parquet-rs' own conversion.
    if let Some(logical) = logical {
        return match logical {
            LogicalType::String | LogicalType::Enum | LogicalType::Json => Ok(DataType::Varchar),
            LogicalType::Decimal(d) => decimal_type(d.precision, d.scale),
            LogicalType::Date => Ok(DataType::Date),
            LogicalType::Timestamp(t) => match t.unit {
                TimeUnit::MILLIS | TimeUnit::MICROS => Ok(DataType::Timestamp),
                TimeUnit::NANOS => Err(
                    "TIMESTAMP(nanos) is not supported (pinkdb timestamps are microseconds)"
                        .to_string(),
                ),
            },
            LogicalType::Time(_) => {
                Err("TIME is not supported (pinkdb has no TIME type)".to_string())
            }
            LogicalType::Integer(i) => integer_type(i.is_signed, i.bit_width, physical),
            LogicalType::Uuid => Err("UUID is not supported".to_string()),
            LogicalType::Float16 => Err("FLOAT16 is not supported".to_string()),
            LogicalType::Bson => Err("BSON is not supported".to_string()),
            LogicalType::List | LogicalType::Map => {
                Err("nested types are not supported".to_string())
            }
            LogicalType::Variant(_) => Err("VARIANT is not supported".to_string()),
            LogicalType::Geometry(_) => Err("GEOMETRY is not supported".to_string()),
            LogicalType::Geography(_) => Err("GEOGRAPHY is not supported".to_string()),
            LogicalType::Unknown | LogicalType::_Unknown { .. } => {
                Err("an unknown logical type".to_string())
            }
        };
    }
    match converted {
        ConvertedType::NONE => match physical {
            PhysicalType::BOOLEAN => Ok(DataType::Boolean),
            PhysicalType::INT32 => Ok(DataType::Integer),
            PhysicalType::INT64 => Ok(DataType::Bigint),
            PhysicalType::INT96 => Ok(DataType::Timestamp),
            PhysicalType::FLOAT | PhysicalType::DOUBLE => Ok(DataType::Double),
            PhysicalType::BYTE_ARRAY => Ok(DataType::Varchar),
            PhysicalType::FIXED_LEN_BYTE_ARRAY => Err(
                "BINARY/FIXED_LEN_BYTE_ARRAY without a logical type is not supported".to_string(),
            ),
        },
        ConvertedType::INT_8 | ConvertedType::INT_16 | ConvertedType::INT_32 => {
            Ok(DataType::Integer)
        }
        ConvertedType::UINT_8 | ConvertedType::UINT_16 | ConvertedType::UINT_32 => {
            Ok(DataType::Integer)
        }
        ConvertedType::UINT_64 => Ok(DataType::Bigint),
        ConvertedType::INT_64 => Ok(DataType::Bigint),
        ConvertedType::DATE => Ok(DataType::Date),
        ConvertedType::TIMESTAMP_MILLIS | ConvertedType::TIMESTAMP_MICROS => {
            Ok(DataType::Timestamp)
        }
        ConvertedType::DECIMAL => decimal_type(precision, scale),
        ConvertedType::UTF8 | ConvertedType::ENUM | ConvertedType::JSON => Ok(DataType::Varchar),
        ConvertedType::INTERVAL => Ok(DataType::Interval),
        other => Err(format!("converted type {other:?} is not supported")),
    }
}

fn integer_type(
    signed: bool,
    bit_width: i8,
    physical: PhysicalType,
) -> std::result::Result<DataType, String> {
    match (physical, bit_width) {
        (PhysicalType::INT32, 8 | 16 | 32) => Ok(DataType::Integer),
        (PhysicalType::INT64, 64) => Ok(if signed {
            DataType::Bigint
        } else {
            // UINT_64: the values are range-checked while decoding.
            DataType::Bigint
        }),
        (p, w) => Err(format!(
            "integer({w}, signed={signed}) on {p:?} is not supported"
        )),
    }
}

fn decimal_type(precision: i32, scale: i32) -> std::result::Result<DataType, String> {
    if !(1..=38).contains(&precision) || scale < 0 || scale > precision {
        return Err(format!(
            "DECIMAL({precision},{scale}) is out of range (1 ≤ precision ≤ 38, 0 ≤ scale ≤ precision)"
        ));
    }
    Ok(DataType::Decimal {
        precision: precision as u8,
        scale: scale as u8,
    })
}

/// Validate a file schema for flatness and map every leaf column; `Err`
/// carries the first unsupported column's message.
///
/// The schema root's children are the columns. A child that is a group is
/// nested (refused, including LIST/MAP), and a repeated leaf is refused too
/// (one column chunk would hold many values per row).
pub fn map_columns(
    schema: &Type,
    descr: &parquet::schema::types::SchemaDescriptor,
) -> Result<Vec<FileColumn>> {
    let mut out = Vec::new();
    for field in schema.get_fields() {
        let info = field.get_basic_info();
        if field.is_group() {
            return Err(PinkDbError::Type(format!(
                "column \"{}\" is a nested group; pinkdb's parquet reader supports flat \
                 schemas only (LIST/MAP/STRUCT columns are not supported)",
                field.name()
            )));
        }
        if info.has_repetition() && info.repetition() == parquet::basic::Repetition::REPEATED {
            return Err(PinkDbError::Type(format!(
                "column \"{}\" is REPEATED; pinkdb's parquet reader supports flat, \
                 non-repeated columns only",
                field.name()
            )));
        }
        let physical = field.get_physical_type();
        let ty = engine_type(
            physical,
            info.converted_type(),
            info.logical_type_ref(),
            field.get_precision(),
            field.get_scale(),
        )
        .map_err(|why| {
            PinkDbError::Type(format!(
                "column \"{}\" has an unsupported parquet type: {why}",
                field.name()
            ))
        })?;
        let leaf = out.len();
        out.push(FileColumn {
            name: field.name().to_string(),
            ty,
            leaf,
            optional: info.has_repetition()
                && info.repetition() == parquet::basic::Repetition::OPTIONAL,
            physical,
            unsigned: is_unsigned(field),
            type_length: descr.column(leaf).type_length(),
        });
    }
    Ok(out)
}

/// Whether the schema element's annotation is an unsigned integer.
fn is_unsigned(field: &Type) -> bool {
    let info = field.get_basic_info();
    if let Some(logical) = info.logical_type_ref()
        && let LogicalType::Integer(i) = logical
    {
        return !i.is_signed;
    }
    matches!(
        info.converted_type(),
        ConvertedType::UINT_8
            | ConvertedType::UINT_16
            | ConvertedType::UINT_32
            | ConvertedType::UINT_64
    )
}

/// The `duckdb_type` column of `parquet_schema`: α's rendering of the mapped
/// type, or `None` for a type pinkdb maps differently/not at all. Only used
/// for the introspection surface; `None` is rendered as NULL like α's NULL.
pub fn alpha_type_name(ty: DataType) -> Option<String> {
    Some(match ty {
        DataType::Boolean
        | DataType::Integer
        | DataType::Bigint
        | DataType::Double
        | DataType::Varchar
        | DataType::Date
        | DataType::Timestamp
        | DataType::Interval => ty.name(),
        DataType::Decimal { precision, scale } => format!("DECIMAL({precision},{scale})"),
    })
}

/// Render a parquet min/max statistic in the engine's own display format
/// (α's rendering: dates as `YYYY-MM-DD`, timestamps as `YYYY-MM-DD HH:MM:SS`,
/// decimals with their scale, booleans `true`/`false`, and floats with a
/// trailing `.0` like α — `{:?}` rather than `Display`).
pub fn render_stat(value: &Value) -> String {
    match value {
        Value::Varchar(s) => s.clone(),
        Value::Date(d) => pinkdb::types::format_date(*d),
        Value::Timestamp(us) => pinkdb::types::format_timestamp(*us),
        Value::Decimal(v, s) => pinkdb::types::format_decimal(*v, *s),
        Value::Interval {
            months,
            days,
            micros,
        } => pinkdb::types::format_interval(*months, *days, *micros),
        Value::Double(x) => format!("{x:?}"),
        other => format!("{other}"),
    }
}

/// A fixed-length big-endian two's complement decimal (the parquet DECIMAL
/// encoding for FIXED_LEN_BYTE_ARRAY).
pub fn fixed_decimal_value(bytes: &[u8]) -> i128 {
    if bytes.is_empty() {
        return 0;
    }
    let negative = bytes[0] & 0x80 != 0;
    let mut value: i128 = if negative { -1 } else { 0 };
    for &byte in bytes {
        value = (value << 8) | (byte as i128);
    }
    value
}

/// The engine DECIMAL value of an INT32/INT64-backed parquet decimal.
pub fn int_decimal_value(unscaled: i64, scale: u8) -> Value {
    Value::Decimal(unscaled as i128, scale)
}

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::basic::TimeUnit;
    use parquet::schema::types::Type as SchemaType;

    fn leaf(logical: Option<LogicalType>, converted: ConvertedType, p: PhysicalType) -> SchemaType {
        let mut b = SchemaType::primitive_type_builder("c", p)
            .with_converted_type(converted)
            .with_repetition(parquet::basic::Repetition::REQUIRED);
        if p == PhysicalType::FIXED_LEN_BYTE_ARRAY {
            let length = match &logical {
                Some(LogicalType::Uuid) => 16,
                Some(LogicalType::Float16) => 2,
                _ => 12,
            };
            b = b.with_length(length);
        }
        if let Some(LogicalType::Decimal(d)) = &logical {
            b = b.with_precision(d.precision).with_scale(d.scale);
        }
        b = b.with_logical_type(logical);
        b.build().unwrap()
    }

    #[test]
    fn maps_the_supported_types() {
        let cases = [
            (
                leaf(None, ConvertedType::NONE, PhysicalType::BOOLEAN),
                DataType::Boolean,
            ),
            (
                leaf(None, ConvertedType::INT_32, PhysicalType::INT32),
                DataType::Integer,
            ),
            (
                leaf(None, ConvertedType::INT_8, PhysicalType::INT32),
                DataType::Integer,
            ),
            (
                leaf(None, ConvertedType::UINT_64, PhysicalType::INT64),
                DataType::Bigint,
            ),
            (
                leaf(None, ConvertedType::DATE, PhysicalType::INT32),
                DataType::Date,
            ),
            (
                leaf(None, ConvertedType::TIMESTAMP_MICROS, PhysicalType::INT64),
                DataType::Timestamp,
            ),
            (
                leaf(None, ConvertedType::NONE, PhysicalType::INT96),
                DataType::Timestamp,
            ),
            (
                leaf(None, ConvertedType::UTF8, PhysicalType::BYTE_ARRAY),
                DataType::Varchar,
            ),
            (
                leaf(None, ConvertedType::NONE, PhysicalType::BYTE_ARRAY),
                DataType::Varchar,
            ),
            (
                leaf(
                    None,
                    ConvertedType::INTERVAL,
                    PhysicalType::FIXED_LEN_BYTE_ARRAY,
                ),
                DataType::Interval,
            ),
            (
                leaf(
                    Some(LogicalType::decimal(2, 9)),
                    ConvertedType::DECIMAL,
                    PhysicalType::INT32,
                ),
                DataType::Decimal {
                    precision: 9,
                    scale: 2,
                },
            ),
        ];
        for (schema, expected) in cases {
            let info = schema.get_basic_info();
            let got = engine_type(
                schema.get_physical_type(),
                info.converted_type(),
                info.logical_type_ref(),
                schema.get_precision(),
                schema.get_scale(),
            )
            .unwrap();
            assert_eq!(got, expected, "for {schema:?}");
        }
    }

    #[test]
    fn refuses_the_unsupported_types() {
        let refused = [
            leaf(
                Some(LogicalType::timestamp(true, TimeUnit::NANOS)),
                ConvertedType::NONE,
                PhysicalType::INT64,
            ),
            leaf(
                Some(LogicalType::time(false, TimeUnit::MICROS)),
                ConvertedType::NONE,
                PhysicalType::INT64,
            ),
            leaf(
                Some(LogicalType::Uuid),
                ConvertedType::NONE,
                PhysicalType::FIXED_LEN_BYTE_ARRAY,
            ),
            leaf(
                Some(LogicalType::Float16),
                ConvertedType::NONE,
                PhysicalType::FIXED_LEN_BYTE_ARRAY,
            ),
            leaf(None, ConvertedType::BSON, PhysicalType::BYTE_ARRAY),
        ];
        for schema in refused {
            let info = schema.get_basic_info();
            assert!(
                engine_type(
                    schema.get_physical_type(),
                    info.converted_type(),
                    info.logical_type_ref(),
                    schema.get_precision(),
                    schema.get_scale(),
                )
                .is_err(),
                "expected a refusal for {schema:?}"
            );
        }
        // A nested group is refused by `map_columns`.
        let child = leaf(None, ConvertedType::INT_32, PhysicalType::INT32);
        let group = SchemaType::group_type_builder("nested")
            .with_repetition(parquet::basic::Repetition::REQUIRED)
            .with_fields(vec![std::sync::Arc::new(child)])
            .build()
            .unwrap();
        let root = SchemaType::group_type_builder("schema")
            .with_fields(vec![std::sync::Arc::new(group)])
            .build()
            .unwrap();
        let descr =
            parquet::schema::types::SchemaDescriptor::new(std::sync::Arc::new(root.clone()));
        let err = map_columns(&root, &descr).unwrap_err();
        assert!(err.to_string().contains("nested"), "{err}");
    }

    #[test]
    fn decimals_decode_from_fixed_bytes() {
        assert_eq!(fixed_decimal_value(&[0x01]), 1);
        assert_eq!(fixed_decimal_value(&[0xFF]), -1);
        assert_eq!(fixed_decimal_value(&[0x7F, 0xFF]), 32767);
        assert_eq!(fixed_decimal_value(&[0x80, 0x00]), -32768);
        assert_eq!(
            fixed_decimal_value(&[0x00, 0x00, 0x0F, 0x42, 0x40]),
            1_000_000
        );
        assert_eq!(fixed_decimal_value(&[]), 0);
    }

    #[test]
    fn stats_render_like_alpha() {
        assert_eq!(render_stat(&Value::Bigint(0)), "0");
        assert_eq!(render_stat(&Value::Date(19723)), "2024-01-01");
        assert_eq!(
            render_stat(&Value::Timestamp(1_705_279_496_000_000)),
            "2024-01-15 00:44:56"
        );
        assert_eq!(render_stat(&Value::Decimal(0, 1)), "0.0");
        assert_eq!(render_stat(&Value::Varchar("n0".to_string())), "n0");
        assert_eq!(render_stat(&Value::Boolean(true)), "true");
    }
}
