//! Codec tests: frame round-trips, every value/type encoding, and the
//! malformed / truncated / oversized / unknown-type rejection paths.

use std::io::Cursor;

use pinkdb::{ArrayElem, DataType, Value};
use pinkdb_server::protocol::{
    DoneKind, MAX_FRAME_SIZE, MAX_PREPARED_VALUES, MAX_ROWS_PER_FRAME, PROTOCOL_VERSION,
    ProtocolError, Request, Response, decode_request, decode_response, encode_request,
    encode_response, msg_type, read_response, write_response,
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

/// A VARIANT value from its JSON text (the tests spell values the way a user
/// would).
fn variant(json: &str) -> std::sync::Arc<pinkdb::variant::Variant> {
    std::sync::Arc::new(pinkdb::variant::from_json_str(json).expect(json))
}

/// An ARRAY value from its elements; `len` is the element count.
fn array(elem: ArrayElem, elements: &[Value]) -> std::sync::Arc<pinkdb::array::ArrayValue> {
    std::sync::Arc::new(
        pinkdb::array::ArrayValue::from_values(elem, elements.len() as u32, elements)
            .expect("array value"),
    )
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
    request_round_trip(Request::Login {
        user: "alice".to_string(),
        password: "hunter2".to_string(),
    });
    request_round_trip(Request::Login {
        user: "alice".to_string(),
        password: String::new(),
    });
    request_round_trip(Request::SetProfiling { enabled: true });
    request_round_trip(Request::SetProfiling { enabled: false });
}

/// Wave 48 pins the wire surface of protocol v9: the version number, the
/// `Login`/`LoggedIn` tags, and the seven RBAC `Done` kinds. Wave 84 adds the
/// v10 profiling messages; wave 101a adds the VARIANT value and dtype tags;
/// wave 95c adds the `SetVariable` `Done` kind (v12); wave 107a adds the ARRAY
/// value (tag 11) and dtype (tag 10) tags (v13); wave 139b adds the UUID value
/// (tag 12) and dtype (tag 11) tags (v15); wave 136a adds the memory limit and
/// peak to the profile frame (v16); HUGEINT step 6 adds the HUGEINT value
/// (tag 13) and dtype (tag 12) tags and the UHUGEINT value (tag 14) and dtype
/// (tag 13) tags (v17).
/// A change here is a protocol change and must bump [`PROTOCOL_VERSION`].
#[test]
fn protocol_v17_pins_the_hugeint_tags_and_keeps_the_v16_surface() {
    assert_eq!(PROTOCOL_VERSION, 17);
    assert_eq!(msg_type::LOGIN, 6);
    assert_eq!(msg_type::LOGGED_IN, 18);
    assert_eq!(msg_type::SET_PROFILING, 7);
    assert_eq!(msg_type::PROFILING_SET, 19);
    assert_eq!(msg_type::PROFILE, 20);
    for (kind, tag) in [
        (DoneKind::Selected, 0),
        (DoneKind::Used, 13),
        (DoneKind::Other, 14),
        (DoneKind::LoadedExtension, 15),
        (DoneKind::Checkpointed, 16),
        (DoneKind::AlteredTable, 17),
        (DoneKind::Called, 18),
        (DoneKind::CreatedUser, 19),
        (DoneKind::AlteredUser, 20),
        (DoneKind::DroppedUser, 21),
        (DoneKind::CreatedRole, 22),
        (DoneKind::DroppedRole, 23),
        (DoneKind::Granted, 24),
        (DoneKind::Revoked, 25),
        (DoneKind::SetVariable, 26),
        (DoneKind::CreatedSchema, 27),
        (DoneKind::DroppedSchema, 28),
    ] {
        let frame = encode_response(&Response::Done {
            kind,
            count: 0,
            name: String::new(),
        })
        .unwrap();
        assert_eq!(frame[4 + 1], tag as u8, "{kind:?} tag drifted");
        // The tag decodes back to the same kind.
        assert_eq!(
            decode_response(&frame).unwrap(),
            Response::Done {
                kind,
                count: 0,
                name: String::new(),
            }
        );
    }
    // A tag past the current surface is still rejected.
    let bytes = frame(msg_type::DONE, &[29, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
    assert!(matches!(
        decode_response(&bytes),
        Err(ProtocolError::Malformed(msg)) if msg.contains("unknown Done kind 29")
    ));
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
    response_round_trip(Response::LoggedIn {
        user: "Alice".to_string(),
    });
    response_round_trip(Response::BatchDone { statements: 0 });
    response_round_trip(Response::BatchDone { statements: 3 });
    response_round_trip(Response::BatchDone {
        statements: u32::MAX,
    });
    response_round_trip(Response::ProfilingSet { enabled: true });
    response_round_trip(Response::ProfilingSet { enabled: false });
}

#[test]
fn profile_round_trips_every_field_and_extremes() {
    response_round_trip(Response::Profile {
        parse: 0,
        bind: 1,
        optimize: 2,
        execute: 3,
        log: 4,
        total: 5,
        rows: 6,
        columns: 7,
        kind: "Selected".to_string(),
        in_transaction: false,
        memory_limit: 8,
        memory_peak: 9,
        engine_version: "0.1.0".to_string(),
    });
    response_round_trip(Response::Profile {
        parse: u64::MAX,
        bind: u64::MAX,
        optimize: u64::MAX,
        execute: u64::MAX,
        log: u64::MAX,
        total: u64::MAX,
        rows: u64::MAX,
        columns: u32::MAX,
        kind: String::new(),
        in_transaction: true,
        memory_limit: u64::MAX,
        memory_peak: u64::MAX,
        engine_version: String::new(),
    });
}

/// Hand-build a `Profile` body: seven u64s, u32 columns, the kind string, the
/// in_transaction byte, the memory limit and peak, and the engine-version
/// string.
fn profile_body(in_transaction: u8) -> Vec<u8> {
    let mut body = Vec::new();
    for n in 1..=7u64 {
        body.extend_from_slice(&n.to_le_bytes());
    }
    body.extend_from_slice(&3u32.to_le_bytes());
    body.extend_from_slice(&8u32.to_le_bytes());
    body.extend_from_slice(b"Selected");
    body.push(in_transaction);
    body.extend_from_slice(&8u64.to_le_bytes());
    body.extend_from_slice(&9u64.to_le_bytes());
    body.extend_from_slice(&5u32.to_le_bytes());
    body.extend_from_slice(b"0.1.0");
    body
}

#[test]
fn malformed_profile_frames_are_rejected() {
    // Truncated: the engine_version string claims 5 bytes, 4 arrive.
    let mut truncated = profile_body(1);
    truncated.pop();
    let err = decode_response(&frame(msg_type::PROFILE, &truncated)).unwrap_err();
    assert!(err.to_string().contains("truncated"), "{err}");

    // Trailing bytes after a complete body are rejected.
    let mut trailing = profile_body(0);
    trailing.push(0);
    let err = decode_response(&frame(msg_type::PROFILE, &trailing)).unwrap_err();
    assert!(err.to_string().contains("trailing bytes"), "{err}");

    // in_transaction is a strict 0/1 flag, not a truthiness byte.
    let err = decode_response(&frame(msg_type::PROFILE, &profile_body(2))).unwrap_err();
    assert!(
        err.to_string()
            .contains("Profile.in_transaction must be 0 or 1, got 2"),
        "{err}"
    );
}

#[test]
fn set_profiling_rejects_a_non_boolean_byte() {
    let err = decode_request(&frame(msg_type::SET_PROFILING, &[2])).unwrap_err();
    assert!(
        err.to_string()
            .contains("SetProfiling.enabled must be 0 or 1, got 2"),
        "{err}"
    );
    // The reply validates its flag the same way.
    let err = decode_response(&frame(msg_type::PROFILING_SET, &[2])).unwrap_err();
    assert!(
        err.to_string()
            .contains("ProfilingSet.enabled must be 0 or 1, got 2"),
        "{err}"
    );
}

#[test]
fn result_set_round_trips_every_data_type() {
    let names = [
        "b", "i", "bi", "d", "v", "dt", "ts", "va", "ai", "ab", "ad", "uu",
    ]
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
        DataType::Variant,
        DataType::Array {
            elem: ArrayElem::Integer,
            len: 2,
        },
        DataType::Array {
            elem: ArrayElem::Bigint,
            len: 1,
        },
        DataType::Array {
            elem: ArrayElem::Double,
            len: 768,
        },
        DataType::Uuid,
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
            Value::Variant(variant("null")),
            Value::Variant(variant("true")),
            Value::Variant(variant("-9223372036854775808")),
            Value::Variant(variant("1.5")),
            Value::Variant(variant("-1234567890123456789012345678.000001")),
            Value::Variant(variant("\"héllo 世界 🚀\"")),
            Value::Variant(variant("[]")),
            Value::Variant(variant("{}")),
            Value::Variant(variant(r#"{"a":[1,{"b":null}]}"#)),
            Value::Array(array(
                ArrayElem::Integer,
                &[Value::Integer(i32::MIN), Value::Integer(i32::MAX)],
            )),
            Value::Array(array(
                ArrayElem::Double,
                &[
                    Value::Double(-0.0),
                    Value::Double(f64::INFINITY),
                    Value::Null,
                ],
            )),
            Value::Array(array(
                ArrayElem::Bigint,
                &[
                    Value::Bigint(i64::MIN),
                    Value::Null,
                    Value::Bigint(i64::MAX),
                ],
            )),
        ],
        vec![Value::Null; 28],
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

/// Wave 101a: a VARIANT rides as `u32 byte length | encoded value` under value
/// tag 10, and its column type under dtype tag 9. The inner encoding is
/// `pinkdb::variant`'s business; this pins the framing.
#[test]
fn variant_rows_and_columns_use_tags_10_and_9() {
    let value = variant(r#"{"a":[1,2.5,"s",null]}"#);
    let mut payload = Vec::new();
    pinkdb::variant::encode(&value, &mut payload);

    let bytes = encode_response(&Response::Rows {
        rows: vec![vec![Value::Variant(std::sync::Arc::clone(&value))]],
    })
    .unwrap();
    assert_eq!(bytes[4], msg_type::ROWS);
    assert_eq!(&bytes[5..9], &1u32.to_le_bytes(), "one row");
    assert_eq!(&bytes[9..13], &1u32.to_le_bytes(), "one column");
    assert_eq!(bytes[13], 10, "VARIANT value tag");
    assert_eq!(&bytes[14..18], &(payload.len() as u32).to_le_bytes());
    assert_eq!(&bytes[18..], &payload[..]);
    assert_eq!(
        decode_response(&bytes).unwrap(),
        Response::Rows {
            rows: vec![vec![Value::Variant(value)]],
        }
    );

    let bytes = encode_response(&Response::ResultSet {
        names: vec!["v".to_string()],
        types: vec![DataType::Variant],
    })
    .unwrap();
    assert_eq!(bytes[4], msg_type::RESULT_SET);
    assert_eq!(bytes[14], 9, "VARIANT dtype tag");

    // A corrupt payload is a malformed frame, not a panic.
    let mut body = 1u32.to_le_bytes().to_vec();
    body.extend_from_slice(&1u32.to_le_bytes());
    body.push(10);
    body.extend_from_slice(&4u32.to_le_bytes());
    body.extend_from_slice(&[0x7f, 0, 0, 0]);
    let err = decode_response(&frame(msg_type::ROWS, &body)).unwrap_err();
    assert!(err.to_string().contains("bad VARIANT value"), "{err}");

    // A length past the frame end is truncated.
    let mut body = 1u32.to_le_bytes().to_vec();
    body.extend_from_slice(&1u32.to_le_bytes());
    body.push(10);
    body.extend_from_slice(&9u32.to_le_bytes());
    let err = decode_response(&frame(msg_type::ROWS, &body)).unwrap_err();
    assert!(err.to_string().contains("truncated"), "{err}");
}

/// Wave 107a: an ARRAY rides as `u32 byte length | encoded array value` under
/// value tag 11, and its column type under dtype tag 10 followed by
/// `u8 element tag | u32 length`. The inner encoding is `pinkdb::array`'s
/// business; this pins the framing.
#[test]
fn protocol_v13_pins_the_array_tags() {
    let value = array(ArrayElem::Double, &[Value::Double(1.5), Value::Null]);
    let mut payload = Vec::new();
    payload.extend_from_slice(&pinkdb::array::encode_cell(&value));

    let bytes = encode_response(&Response::Rows {
        rows: vec![vec![Value::Array(std::sync::Arc::clone(&value))]],
    })
    .unwrap();
    assert_eq!(bytes[4], msg_type::ROWS);
    assert_eq!(&bytes[5..9], &1u32.to_le_bytes(), "one row");
    assert_eq!(&bytes[9..13], &1u32.to_le_bytes(), "one column");
    assert_eq!(bytes[13], 11, "ARRAY value tag");
    assert_eq!(&bytes[14..18], &(payload.len() as u32).to_le_bytes());
    assert_eq!(&bytes[18..], &payload[..]);
    assert_eq!(
        decode_response(&bytes).unwrap(),
        Response::Rows {
            rows: vec![vec![Value::Array(value)]],
        }
    );

    let bytes = encode_response(&Response::ResultSet {
        names: vec!["v".to_string(), "f".to_string()],
        types: vec![
            DataType::Array {
                elem: ArrayElem::Integer,
                len: 3,
            },
            DataType::Array {
                elem: ArrayElem::Float,
                len: 768,
            },
        ],
    })
    .unwrap();
    assert_eq!(bytes[4], msg_type::RESULT_SET);
    // The element kind is a byte of the dtype encoding: 0 INTEGER, 1 BIGINT,
    // 2 DOUBLE, 3 FLOAT.
    assert_eq!(bytes[14], 10, "ARRAY dtype tag");
    assert_eq!(bytes[15], 0, "INTEGER element tag");
    assert_eq!(&bytes[16..20], &3u32.to_le_bytes(), "length");
    // Column 2 starts after column 1's `u32` name length, name, tag, element
    // byte and `u32` length.
    let second = 25;
    assert_eq!(bytes[second], 10, "ARRAY dtype tag");
    assert_eq!(bytes[second + 1], 3, "FLOAT element tag");
    assert_eq!(&bytes[second + 2..second + 6], &768u32.to_le_bytes());
    // A FLOAT payload is 4-byte elements: the same value round-trips.
    let bytes = encode_response(&Response::Rows {
        rows: vec![vec![Value::Array(std::sync::Arc::new(
            pinkdb::array::ArrayValue::new(
                ArrayElem::Float,
                2,
                pinkdb::array::ArrayElems::Floats32(vec![0.5, -0.25]),
                None,
            )
            .unwrap(),
        ))]],
    })
    .unwrap();
    let Response::Rows { rows } = decode_response(&bytes).unwrap() else {
        panic!("expected rows")
    };
    let Value::Array(cell) = &rows[0][0] else {
        panic!("expected an array")
    };
    assert_eq!(cell.elem, ArrayElem::Float);
    assert_eq!(cell.floats32().unwrap(), &[0.5f32, -0.25]);

    // An unknown element tag, a zero length and a corrupt payload are
    // malformed frames, not panics.
    let mut body = 1u32.to_le_bytes().to_vec();
    body.extend_from_slice(&1u32.to_le_bytes());
    body.push(11);
    body.extend_from_slice(&5u32.to_le_bytes());
    body.extend_from_slice(&[9, 0, 0, 0, 0]);
    let err = decode_response(&frame(msg_type::ROWS, &body)).unwrap_err();
    assert!(
        err.to_string().contains("bad ARRAY value") && err.to_string().contains("element kind"),
        "{err}"
    );

    let mut body = 1u32.to_le_bytes().to_vec();
    body.extend_from_slice(&1u32.to_le_bytes());
    body.push(b'v');
    body.push(10);
    body.push(2);
    body.extend_from_slice(&0u32.to_le_bytes());
    let err = decode_response(&frame(msg_type::RESULT_SET, &body)).unwrap_err();
    assert!(err.to_string().contains("bad ARRAY length"), "{err}");

    let mut body = 1u32.to_le_bytes().to_vec();
    body.extend_from_slice(&1u32.to_le_bytes());
    body.push(11);
    body.extend_from_slice(&9u32.to_le_bytes());
    let err = decode_response(&frame(msg_type::ROWS, &body)).unwrap_err();
    assert!(err.to_string().contains("truncated"), "{err}");
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
fn uuid_values_round_trip() {
    let rows = vec![
        vec![
            Value::Uuid(0),
            Value::Uuid(u128::MAX),
            Value::Uuid(0x550e_8400_e29b_41d4_a716_4466_5544_0000),
        ],
        vec![Value::Null, Value::Null, Value::Null],
    ];
    response_round_trip(Response::Rows { rows });
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
            Value::decimal(0, 0),
            Value::decimal(-123_456_789_012_345_678_901_234_567_890, 18),
            Value::decimal(i128::MIN, 38),
            Value::decimal(i128::MAX, 38),
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
