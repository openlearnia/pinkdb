//! Codec tests: frame round-trips, every value/type encoding, and the
//! malformed / truncated / oversized / unknown-type rejection paths.

use std::io::Cursor;

use pinkdb::{DataType, Value};
use pinkdb_server::protocol::{
    decode_request, decode_response, encode_request, encode_response, msg_type, read_response,
    write_response, DoneKind, ProtocolError, Request, Response, MAX_FRAME_SIZE,
    MAX_PREPARED_VALUES, MAX_ROWS_PER_FRAME, PROTOCOL_VERSION,
};

fn request_round_trip(request: Request) {
    let bytes = encode_request(&request).expect("encode request");
    let decoded = decode_request(&bytes).expect("decode request");
    assert_eq!(decoded, request);
}

fn response_round_trip(response: Response) {
    let bytes = encode_response(&response).expect("encode response");
    let decoded = decode_response(&bytes).expect("decode response");
    assert_eq!(decoded, response);
}

/// Hand-build a frame (length prefix + type + body) for malformed cases.
fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&((body.len() + 1) as u32).to_le_bytes());
    bytes.push(tag);
    bytes.extend_from_slice(body);
    bytes
}

#[test]
fn requests_round_trip() {
    request_round_trip(Request::Hello {
        protocol_version: PROTOCOL_VERSION,
    });
    request_round_trip(Request::Ping);
    request_round_trip(Request::Execute { sql: String::new() });
    request_round_trip(Request::Execute {
        sql: "SELECT 'a;b' -- ;".to_string(),
    });
    request_round_trip(Request::Execute {
        sql: "SELECT 'héllo ✓';".to_string(),
    });
    request_round_trip(Request::Prepare {
        sql: "SELECT ? + ?".to_string(),
    });
    request_round_trip(Request::ExecutePrepared {
        handle: 42,
        values: vec![
            Value::Integer(1),
            Value::Null,
            Value::Varchar("x".to_string()),
        ],
    });
}

#[test]
fn responses_round_trip() {
    response_round_trip(Response::Prepared {
        handle: 42,
        parameter_count: 3,
    });
    response_round_trip(Response::HelloOk {
        server_version: "0.1.0".to_string(),
        protocol_version: PROTOCOL_VERSION,
    });
    response_round_trip(Response::Pong);
    response_round_trip(Response::Error {
        message: "database \"x\" not found".to_string(),
    });
    response_round_trip(Response::ResultSet {
        names: vec![],
        types: vec![],
    });
    response_round_trip(Response::Rows { rows: vec![] });
    response_round_trip(Response::BatchDone { statements: 0 });
    response_round_trip(Response::BatchDone { statements: 3 });
    response_round_trip(Response::BatchDone {
        statements: u32::MAX,
    });
}

#[test]
fn result_set_round_trips_every_data_type() {
    let names = ["b", "i", "bi", "d", "v", "dt", "ts"]
        .map(String::from)
        .to_vec();
    let types = vec![
        DataType::Boolean,
        DataType::Integer,
        DataType::Bigint,
        DataType::Double,
        DataType::Varchar,
        DataType::Date,
        DataType::Timestamp,
    ];
    response_round_trip(Response::ResultSet { names, types });
}

#[test]
fn done_round_trips_every_kind() {
    let kinds = [
        DoneKind::Selected,
        DoneKind::Inserted,
        DoneKind::Updated,
        DoneKind::Deleted,
        DoneKind::Created,
        DoneKind::Began,
        DoneKind::Committed,
        DoneKind::RolledBack,
        DoneKind::CreatedView,
        DoneKind::DroppedTable,
        DoneKind::DroppedView,
        DoneKind::CreatedDatabase,
        DoneKind::DroppedDatabase,
        DoneKind::Used,
        DoneKind::LoadedExtension,
        DoneKind::AlteredTable,
        DoneKind::Called,
        DoneKind::Other,
    ];
    for kind in kinds {
        response_round_trip(Response::Done {
            kind,
            count: 7,
            name: "some name".to_string(),
        });
    }
}

#[test]
fn rows_round_trip_every_value_kind_and_extremes() {
    let rows = vec![
        vec![
            Value::Null,
            Value::Boolean(false),
            Value::Boolean(true),
            Value::Integer(i32::MIN),
            Value::Integer(i32::MAX),
            Value::Bigint(i64::MIN),
            Value::Bigint(i64::MAX),
            Value::Double(0.0),
            Value::Double(-0.5),
            Value::Varchar(String::new()),
            Value::Varchar("héllo ✓".to_string()),
            Value::Varchar("x".repeat(10_000)),
            Value::Date(i32::MIN),
            Value::Date(i32::MAX),
            Value::Timestamp(i64::MIN),
            Value::Timestamp(i64::MAX),
        ],
        vec![
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
        ],
    ];
    response_round_trip(Response::Rows { rows });
}

#[test]
fn float_bits_round_trip_exactly() {
    // NaN and -0.0 do not compare equal under `Value`'s PartialEq, so check
    // the raw bits.
    let nan = f64::from_bits(0x7ff8_0000_0000_0001);
    let response = Response::Rows {
        rows: vec![vec![Value::Double(nan), Value::Double(-0.0)]],
    };
    let bytes = encode_response(&response).unwrap();
    match decode_response(&bytes).unwrap() {
        Response::Rows { rows } => {
            let Value::Double(x) = rows[0][0] else {
                panic!("expected Double")
            };
            let Value::Double(y) = rows[0][1] else {
                panic!("expected Double")
            };
            assert_eq!(x.to_bits(), nan.to_bits());
            assert_eq!(y.to_bits(), (-0.0f64).to_bits());
        }
        other => panic!("expected Rows, got {other:?}"),
    }
}

#[test]
fn multi_result_stream_round_trips_in_order() {
    // One Execute batch: ResultSet/Rows/Done, then a non-row Done, then a
    // second ResultSet/Rows/Done, and finally the v2 BatchDone terminator —
    // decode them back off a single stream.
    let mut stream = Vec::new();
    let frames = [
        Response::ResultSet {
            names: vec!["a".to_string()],
            types: vec![DataType::Integer],
        },
        Response::Rows {
            rows: vec![vec![Value::Integer(1)], vec![Value::Integer(2)]],
        },
        Response::Done {
            kind: DoneKind::Selected,
            count: 0,
            name: String::new(),
        },
        Response::Done {
            kind: DoneKind::Inserted,
            count: 2,
            name: String::new(),
        },
        Response::ResultSet {
            names: vec![],
            types: vec![],
        },
        Response::Rows { rows: vec![] },
        Response::Done {
            kind: DoneKind::Selected,
            count: 0,
            name: String::new(),
        },
        Response::BatchDone { statements: 3 },
    ];
    for response in &frames {
        write_response(&mut stream, response).unwrap();
    }
    let mut cursor = Cursor::new(stream);
    for expected in &frames {
        assert_eq!(&read_response(&mut cursor).unwrap(), expected);
    }
}

#[test]
fn batch_done_body_is_exactly_one_u32() {
    // Truncated body.
    let err = decode_response(&frame(msg_type::BATCH_DONE, &[1, 2])).unwrap_err();
    assert!(err.to_string().contains("truncated"), "{err}");
    // Trailing bytes.
    let mut body = 2u32.to_le_bytes().to_vec();
    body.push(0);
    let err = decode_response(&frame(msg_type::BATCH_DONE, &body)).unwrap_err();
    assert!(err.to_string().contains("trailing bytes"), "{err}");
}

#[test]
fn empty_result_set_round_trips() {
    let mut stream = Vec::new();
    let frames = [
        Response::ResultSet {
            names: vec!["a".to_string()],
            types: vec![DataType::Integer],
        },
        Response::Done {
            kind: DoneKind::Selected,
            count: 0,
            name: String::new(),
        },
    ];
    for response in &frames {
        write_response(&mut stream, response).unwrap();
    }
    let mut cursor = Cursor::new(stream);
    assert_eq!(read_response(&mut cursor).unwrap(), frames[0]);
    assert_eq!(read_response(&mut cursor).unwrap(), frames[1]);
}

#[test]
fn zero_length_frame_is_rejected() {
    let err = decode_request(&[0, 0, 0, 0]).unwrap_err();
    assert!(err.to_string().contains("empty frame"), "{err}");
    let err = decode_response(&[0, 0, 0, 0]).unwrap_err();
    assert!(err.to_string().contains("empty frame"), "{err}");
}

#[test]
fn truncated_length_prefix_is_rejected() {
    assert!(decode_request(&[]).is_err());
    assert!(decode_request(&[5]).is_err());
    assert!(decode_request(&[5, 0, 0]).is_err());
}

#[test]
fn truncated_body_is_rejected() {
    // Declares a 10-byte frame, provides 5 bytes.
    let mut bytes = 10u32.to_le_bytes().to_vec();
    bytes.extend_from_slice(&[msg_type::PING, 1, 2, 3, 4]);
    let err = decode_request(&bytes).unwrap_err();
    assert!(matches!(err, ProtocolError::Truncated(_)), "{err}");
}

#[test]
fn inner_lengths_are_validated_against_remaining_bytes() {
    // Execute.sql declares 100 bytes, has 3.
    let mut body = 100u32.to_le_bytes().to_vec();
    body.extend_from_slice(b"abc");
    let err = decode_request(&frame(msg_type::EXECUTE, &body)).unwrap_err();
    assert!(err.to_string().contains("truncated"), "{err}");

    // ResultSet declares 3 columns, provides 1.
    let mut body = 3u32.to_le_bytes().to_vec();
    body.extend_from_slice(&1u32.to_le_bytes());
    body.push(b'a');
    body.push(1);
    let err = decode_response(&frame(msg_type::RESULT_SET, &body)).unwrap_err();
    assert!(err.to_string().contains("truncated"), "{err}");

    // Rows declares 5 rows x 2 columns, provides 3 value bytes.
    let mut body = 5u32.to_le_bytes().to_vec();
    body.extend_from_slice(&2u32.to_le_bytes());
    body.extend_from_slice(&[0, 0, 0]);
    let err = decode_response(&frame(msg_type::ROWS, &body)).unwrap_err();
    assert!(err.to_string().contains("truncated"), "{err}");

    // Done declares a name of 20 bytes, has 2.
    let mut body = vec![DoneKind::Inserted as u8];
    body.extend_from_slice(&1u64.to_le_bytes());
    body.extend_from_slice(&20u32.to_le_bytes());
    body.extend_from_slice(b"hi");
    let err = decode_response(&frame(msg_type::DONE, &body)).unwrap_err();
    assert!(err.to_string().contains("truncated"), "{err}");
}

#[test]
fn oversized_frames_are_rejected() {
    let mut bytes = ((MAX_FRAME_SIZE + 1) as u32).to_le_bytes().to_vec();
    bytes.push(msg_type::EXECUTE);
    let err = decode_request(&bytes).unwrap_err();
    match err {
        ProtocolError::FrameTooLarge { size, max } => {
            assert_eq!(size, MAX_FRAME_SIZE + 1);
            assert_eq!(max, MAX_FRAME_SIZE);
        }
        other => panic!("expected FrameTooLarge, got {other:?}"),
    }
    assert!(err.to_string().contains("exceeds"), "{err}");

    // The write side enforces the same maximum.
    let err = pinkdb_server::protocol::write_frame(
        &mut Vec::new(),
        msg_type::PING,
        &vec![0u8; MAX_FRAME_SIZE],
    )
    .unwrap_err();
    assert!(matches!(err, ProtocolError::FrameTooLarge { .. }), "{err}");
}

#[test]
fn an_oversized_execute_prepared_value_count_is_rejected() {
    // The frame carries one tag byte per claimed value, so the wire-size bound
    // alone would pass; the count cap must reject it before it materializes one
    // `Value` per tag.
    let count = (MAX_PREPARED_VALUES + 1) as u32;
    let mut body = 7u64.to_le_bytes().to_vec();
    body.extend_from_slice(&count.to_le_bytes());
    body.extend_from_slice(&vec![0u8; count as usize]);
    let err = decode_request(&frame(msg_type::EXECUTE_PREPARED, &body)).unwrap_err();
    assert!(err.to_string().contains("truncated"), "{err}");
}

#[test]
fn an_oversized_rows_count_is_rejected() {
    // One column, one tag byte per row: the wire-size bound alone would pass,
    // but the claimed row count exceeds the cap.
    let count = (MAX_ROWS_PER_FRAME + 1) as u32;
    let mut body = count.to_le_bytes().to_vec();
    body.extend_from_slice(&1u32.to_le_bytes());
    body.extend_from_slice(&vec![0u8; count as usize]);
    let err = decode_response(&frame(msg_type::ROWS, &body)).unwrap_err();
    assert!(err.to_string().contains("truncated"), "{err}");
}

#[test]
fn unknown_message_types_are_rejected() {
    let err = decode_request(&frame(200, &[])).unwrap_err();
    assert!(
        err.to_string().contains("unknown request message type 200"),
        "{err}"
    );
    let err = decode_response(&frame(200, &[])).unwrap_err();
    assert!(
        err.to_string()
            .contains("unknown response message type 200"),
        "{err}"
    );
    // A response tag is not a valid request, and vice versa.
    assert!(decode_request(&frame(msg_type::PONG, &[])).is_err());
    assert!(decode_response(&frame(msg_type::PING, &[])).is_err());
}

#[test]
fn unknown_value_and_type_tags_are_rejected() {
    // Rows: 1 row x 1 column, value tag 99.
    let mut body = 1u32.to_le_bytes().to_vec();
    body.extend_from_slice(&1u32.to_le_bytes());
    body.push(99);
    let err = decode_response(&frame(msg_type::ROWS, &body)).unwrap_err();
    assert!(err.to_string().contains("unknown value tag 99"), "{err}");

    // ResultSet: 1 column, name "a", type tag 99.
    let mut body = 1u32.to_le_bytes().to_vec();
    body.extend_from_slice(&1u32.to_le_bytes());
    body.push(b'a');
    body.push(99);
    let err = decode_response(&frame(msg_type::RESULT_SET, &body)).unwrap_err();
    assert!(
        err.to_string().contains("unknown data type tag 99"),
        "{err}"
    );

    // Done: kind 99.
    let err = decode_response(&frame(msg_type::DONE, &[99])).unwrap_err();
    assert!(err.to_string().contains("unknown Done kind 99"), "{err}");
}

#[test]
fn invalid_utf8_and_bad_payloads_are_rejected() {
    // Execute.sql: 2 bytes of invalid UTF-8.
    let mut body = 2u32.to_le_bytes().to_vec();
    body.extend_from_slice(&[0xff, 0xfe]);
    let err = decode_request(&frame(msg_type::EXECUTE, &body)).unwrap_err();
    assert!(err.to_string().contains("not valid UTF-8"), "{err}");

    // BOOL payload must be 0 or 1.
    let mut body = 1u32.to_le_bytes().to_vec();
    body.extend_from_slice(&1u32.to_le_bytes());
    body.extend_from_slice(&[1, 2]);
    let err = decode_response(&frame(msg_type::ROWS, &body)).unwrap_err();
    assert!(err.to_string().contains("BOOL payload"), "{err}");
}

#[test]
fn trailing_bytes_are_rejected() {
    // Hello carries exactly one u32; an extra byte is a malformed frame.
    let mut body = PROTOCOL_VERSION.to_le_bytes().to_vec();
    body.push(0);
    let err = decode_request(&frame(msg_type::HELLO, &body)).unwrap_err();
    assert!(err.to_string().contains("trailing bytes"), "{err}");

    // Ping has an empty body.
    let err = decode_request(&frame(msg_type::PING, &[1])).unwrap_err();
    assert!(err.to_string().contains("trailing bytes"), "{err}");
}

#[test]
fn rows_frame_columns_field_is_enforced() {
    // Declares 1 row x 2 columns but provides 3 value bytes: the decoder reads
    // two values and rejects the leftover byte as trailing.
    let mut body = 1u32.to_le_bytes().to_vec();
    body.extend_from_slice(&2u32.to_le_bytes());
    body.extend_from_slice(&[0, 0, 0]);
    let err = decode_response(&frame(msg_type::ROWS, &body)).unwrap_err();
    assert!(err.to_string().contains("trailing bytes"), "{err}");

    // Declares 1 row x 2 columns but stops after one value: truncated.
    let mut body = 1u32.to_le_bytes().to_vec();
    body.extend_from_slice(&2u32.to_le_bytes());
    body.push(0);
    let err = decode_response(&frame(msg_type::ROWS, &body)).unwrap_err();
    assert!(err.to_string().contains("truncated"), "{err}");
}

#[test]
fn ragged_rows_frames_do_not_encode() {
    let response = Response::Rows {
        rows: vec![
            vec![Value::Integer(1)],
            vec![Value::Integer(1), Value::Integer(2)],
        ],
    };
    let err = encode_response(&response).unwrap_err();
    assert!(err.to_string().contains("ragged"), "{err}");
}

#[test]
fn result_set_length_mismatch_does_not_encode() {
    let response = Response::ResultSet {
        names: vec!["a".to_string(), "b".to_string()],
        types: vec![DataType::Integer],
    };
    let err = encode_response(&response).unwrap_err();
    assert!(err.to_string().contains("2 names but 1 types"), "{err}");
}

#[test]
fn decimal_and_interval_values_round_trip() {
    let rows = vec![
        vec![
            Value::Decimal(0, 0),
            Value::Decimal(-123_456_789_012_345_678_901_234_567_890, 18),
            Value::Decimal(i128::MIN, 38),
            Value::Decimal(i128::MAX, 38),
            Value::Interval {
                months: i32::MIN,
                days: i32::MAX,
                micros: i64::MIN,
            },
            Value::Interval {
                months: 0,
                days: 0,
                micros: i64::MAX,
            },
        ],
        vec![
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
        ],
    ];
    response_round_trip(Response::Rows { rows });
    response_round_trip(Response::ResultSet {
        names: vec!["d".to_string(), "iv".to_string()],
        types: vec![
            DataType::Decimal {
                precision: 18,
                scale: 3,
            },
            DataType::Interval,
        ],
    });
}
