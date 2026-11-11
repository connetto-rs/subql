use super::{PgChangeEvent, PgOutputOrder, PgRowItem};
use crate::backend::{CdcEvent, RowKind, Value};
use crate::wal::{PgCellFormat, PgTypeRegistry};
use crate::ValueError;
use bytes::Bytes;
use pg_walstream::{ChangeEvent, ColumnValue, Lsn, RelationColumn, ReplicaIdentity, RowData};
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;

/// One column of every builtin family, in catalog order.
const DDL: &str = "CREATE TABLE kinds (
    id INT PRIMARY KEY, flag BOOL, small SMALLINT, big BIGINT, single REAL,
    double DOUBLE PRECISION, amount NUMERIC(10, 2), body TEXT, code VARCHAR(8),
    fixed CHAR(4), blob BYTEA, uid UUID, day DATE, at TIME, stamp TIMESTAMP,
    stamptz TIMESTAMPTZ, span INTERVAL, doc JSON, docb JSONB, acl ACLITEM
);";

const RELATION: u32 = 16_384;

/// The ordinal of `acl` in [`DDL`].
const ACL: u16 = 19;

/// A column with its type OID, its text cell, and the bytes its type's send
/// function writes for the same value.
struct Cell {
    name: &'static str,
    type_oid: u32,
    text: &'static str,
    binary: Vec<u8>,
}

const fn cell(name: &'static str, type_oid: u32, text: &'static str, binary: Vec<u8>) -> Cell {
    Cell {
        name,
        type_oid,
        text,
        binary,
    }
}

/// Every family's cell. The binary forms are written out by hand from
/// PostgreSQL's send functions, never by an encoder under test.
fn cells() -> Vec<Cell> {
    let interval = [
        &14_706_000_000_i64.to_be_bytes()[..],
        &3_i32.to_be_bytes(),
        &14_i32.to_be_bytes(),
    ]
    .concat();
    vec![
        cell("id", 23, "7", 7_i32.to_be_bytes().to_vec()),
        cell("flag", 16, "t", vec![1]),
        cell("small", 21, "-3", (-3_i16).to_be_bytes().to_vec()),
        cell(
            "big",
            20,
            "9223372036854775807",
            i64::MAX.to_be_bytes().to_vec(),
        ),
        cell("single", 700, "0.1", vec![0x3d, 0xcc, 0xcc, 0xcd]),
        cell("double", 701, "2.5", 2.5_f64.to_be_bytes().to_vec()),
        // numeric_send: ndigits 2, weight 0, sign +, dscale 2, then the
        // base-10000 digits 12 and 5000.
        cell(
            "amount",
            1700,
            "12.50",
            vec![0, 2, 0, 0, 0, 0, 0, 2, 0, 12, 0x13, 0x88],
        ),
        cell("body", 25, "héllo", "héllo".as_bytes().to_vec()),
        cell("code", 1043, "ab", b"ab".to_vec()),
        cell("fixed", 1042, "ab  ", b"ab  ".to_vec()),
        cell("blob", 17, "\\xdead", vec![0xde, 0xad]),
        cell(
            "uid",
            2950,
            "550e8400-e29b-41d4-a716-446655440000",
            vec![
                0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44,
                0x00, 0x00,
            ],
        ),
        // Days since 2000-01-01.
        cell("day", 1082, "2024-03-15", 8840_i32.to_be_bytes().to_vec()),
        // Microseconds since midnight.
        cell(
            "at",
            1083,
            "12:34:56.789",
            45_296_789_000_i64.to_be_bytes().to_vec(),
        ),
        // Microseconds since 2000-01-01 00:00:00.
        cell(
            "stamp",
            1114,
            "2024-03-15 12:34:56.789",
            763_821_296_789_000_i64.to_be_bytes().to_vec(),
        ),
        cell(
            "stamptz",
            1184,
            "2024-03-15 14:34:56.789+02",
            763_821_296_789_000_i64.to_be_bytes().to_vec(),
        ),
        // Microseconds, then days, then months.
        cell("span", 1186, "1 year 2 mons 3 days 04:05:06", interval),
        cell("doc", 114, r#"{"a": 1}"#, br#"{"a": 1}"#.to_vec()),
        // jsonb_send: version 1, then the document's text.
        cell(
            "docb",
            3802,
            r#"{"a": 1}"#,
            [&[1][..], br#"{"a": 1}"#].concat(),
        ),
    ]
}

fn relation(columns: &[(&str, u32)]) -> ChangeEvent {
    ChangeEvent::relation(
        RELATION,
        "public",
        "kinds",
        ReplicaIdentity::Default,
        columns
            .iter()
            .map(|&(name, type_id)| RelationColumn {
                name: name.into(),
                type_id,
                type_modifier: -1,
                is_key: name == "id",
            })
            .collect(),
        Lsn::new(0),
    )
}

fn begin() -> ChangeEvent {
    ChangeEvent::begin(3, Lsn::new(1500), chrono::DateTime::UNIX_EPOCH, Lsn::new(0))
}

fn insert(row: RowData, record: u64) -> ChangeEvent {
    ChangeEvent::insert("public", "kinds", RELATION, row, Lsn::new(record))
}

/// The row events `order` yields for `changes`, inside one transaction.
fn deliver_all(mut order: PgOutputOrder, changes: Vec<ChangeEvent>) -> Vec<PgChangeEvent> {
    let mut items = Vec::new();
    for change in core::iter::once(begin()).chain(changes) {
        order.apply(change, &mut items).expect("frames in place");
    }
    items
        .into_iter()
        .filter_map(|item| match item {
            PgRowItem::Event(event) => Some(event),
            PgRowItem::Relation(_) => None,
        })
        .collect()
}

/// The row event `order` yields for an insert of `row`, announced by
/// `announce`.
fn deliver(order: PgOutputOrder, announce: ChangeEvent, row: RowData) -> PgChangeEvent {
    deliver_all(order, vec![announce, insert(row, 1000)])
        .pop()
        .expect("the insert is a row event")
}

fn binary_order() -> PgOutputOrder {
    PgOutputOrder::binary(PgTypeRegistry::builtin())
}

fn kinds() -> ParserDB {
    ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL")
}

fn int4(value: i32) -> ColumnValue {
    ColumnValue::binary_bytes(Bytes::copy_from_slice(&value.to_be_bytes()))
}

fn text_row(cells: &[Cell]) -> RowData {
    RowData::from_pairs(
        cells
            .iter()
            .map(|c| (c.name, ColumnValue::text(c.text)))
            .collect(),
    )
}

fn binary_row(cells: &[Cell]) -> RowData {
    RowData::from_pairs(
        cells
            .iter()
            .map(|c| {
                (
                    c.name,
                    ColumnValue::binary_bytes(Bytes::copy_from_slice(&c.binary)),
                )
            })
            .collect(),
    )
}

fn types(cells: &[Cell]) -> Vec<(&'static str, u32)> {
    cells.iter().map(|c| (c.name, c.type_oid)).collect()
}

#[test]
fn every_family_reads_the_same_value_from_text_and_binary() {
    let db = kinds();
    let cells = cells();
    let text = deliver(
        PgOutputOrder::new(),
        relation(&types(&cells)),
        text_row(&cells),
    );
    let binary = deliver(binary_order(), relation(&types(&cells)), binary_row(&cells));
    for (col, cell) in (0_u16..).zip(&cells) {
        let from_text = text.value_at(&db, RowKind::New, col).expect("text decodes");
        let from_binary = binary
            .value_at(&db, RowKind::New, col)
            .expect("binary decodes");
        assert!(!from_text.is_missing(), "{} decodes from text", cell.name);
        assert_eq!(
            from_binary, from_text,
            "{} reads alike in both formats",
            cell.name
        );
        if let (Value::Decimal(a), Value::Decimal(b)) = (&from_text, &from_binary) {
            assert_eq!(
                a.to_string(),
                b.to_string(),
                "{} keeps its scale",
                cell.name
            );
        }
    }
}

#[test]
fn a_text_cell_in_a_column_whose_type_sends_binary_is_an_error() {
    let db = kinds();
    let event = deliver(
        binary_order(),
        relation(&[("id", 23), ("big", 20)]),
        RowData::from_pairs(vec![("id", int4(7)), ("big", ColumnValue::text("7"))]),
    );
    assert_eq!(event.value_at(&db, RowKind::New, 0), Ok(Value::Int(7)));
    assert_eq!(
        event.value_at(&db, RowKind::New, 3),
        Err(ValueError::CellFormat {
            column: 3,
            expected: PgCellFormat::Binary
        })
    );
}

#[test]
fn a_binary_cell_on_a_text_stream_is_an_error() {
    let db = kinds();
    let event = deliver(
        PgOutputOrder::new(),
        relation(&[("id", 23)]),
        RowData::from_pairs(vec![("id", int4(7))]),
    );
    assert_eq!(
        event.value_at(&db, RowKind::New, 0),
        Err(ValueError::CellFormat {
            column: 0,
            expected: PgCellFormat::Text
        })
    );
}

/// `aclitem` has no send function, so a binary stream still sends it as
/// text, and the column reads as it does on a text stream.
#[test]
fn a_type_without_a_send_function_stays_text_on_a_binary_stream() {
    let db = kinds();
    let columns = [("id", 23), ("acl", 1033)];
    let acl_text = || ("acl", ColumnValue::text("=r/postgres"));
    let binary = deliver(
        binary_order(),
        relation(&columns),
        RowData::from_pairs(vec![("id", int4(7)), acl_text()]),
    );
    let text = deliver(
        PgOutputOrder::new(),
        relation(&columns),
        RowData::from_pairs(vec![("id", ColumnValue::text("7")), acl_text()]),
    );
    assert_eq!(
        binary.value_at(&db, RowKind::New, ACL),
        text.value_at(&db, RowKind::New, ACL)
    );
    assert_eq!(binary.value_at(&db, RowKind::New, 0), Ok(Value::Int(7)));

    let refused = deliver(
        binary_order(),
        relation(&columns),
        RowData::from_pairs(vec![
            ("id", int4(7)),
            (
                "acl",
                ColumnValue::binary_bytes(Bytes::from_static(b"=r/postgres")),
            ),
        ]),
    );
    assert_eq!(
        refused.value_at(&db, RowKind::New, ACL),
        Err(ValueError::CellFormat {
            column: ACL,
            expected: PgCellFormat::Text
        })
    );
}

/// A domain over `int4` sends what `int4` sends, and reads through it.
#[test]
fn a_registered_domain_reads_through_its_base_type() {
    let db = kinds();
    let domain = 16_500;
    let event = deliver(
        PgOutputOrder::binary(PgTypeRegistry::builtin().with_user_type(domain, true, Some(23))),
        relation(&[("id", domain)]),
        RowData::from_pairs(vec![("id", int4(7))]),
    );
    assert_eq!(event.value_at(&db, RowKind::New, 0), Ok(Value::Int(7)));
}

#[test]
fn a_registered_user_type_without_a_send_function_stays_text() {
    let db = kinds();
    let user = 16_600;
    let event = deliver(
        PgOutputOrder::binary(PgTypeRegistry::builtin().with_user_type(user, false, None)),
        relation(&[("body", user)]),
        RowData::from_pairs(vec![("body", ColumnValue::text("plain"))]),
    );
    assert_eq!(
        event.value_at(&db, RowKind::New, 7),
        Ok(Value::String("plain".into()))
    );
}

/// A type created after the stream read its registry has no known format,
/// so neither form of its cell is read.
#[test]
fn a_user_type_the_registry_does_not_know_is_refused() {
    let db = kinds();
    let unknown = 16_700;
    for cell in [ColumnValue::text("7"), int4(7)] {
        let event = deliver(
            binary_order(),
            relation(&[("id", unknown)]),
            RowData::from_pairs(vec![("id", cell)]),
        );
        assert_eq!(
            event.value_at(&db, RowKind::New, 0),
            Err(ValueError::UnresolvedFormat {
                column: 0,
                type_oid: Some(unknown)
            })
        );
    }
}

/// A relation announced again after `ALTER TABLE` replans its columns.
#[test]
fn a_reannounced_relation_replans_its_columns() {
    let db = kinds();
    let id = || RowData::from_pairs(vec![("id", int4(7))]);
    let rows = deliver_all(
        binary_order(),
        vec![
            relation(&[("id", 23)]),
            insert(id(), 1000),
            relation(&[("id", 1033)]),
            insert(id(), 1100),
        ],
    );
    assert_eq!(rows[0].value_at(&db, RowKind::New, 0), Ok(Value::Int(7)));
    assert_eq!(
        rows[1].value_at(&db, RowKind::New, 0),
        Err(ValueError::CellFormat {
            column: 0,
            expected: PgCellFormat::Text
        })
    );
}

#[test]
fn a_row_of_an_unannounced_relation_is_refused_on_a_binary_stream() {
    let db = kinds();
    let rows = deliver_all(
        binary_order(),
        vec![insert(
            RowData::from_pairs(vec![("id", ColumnValue::text("7"))]),
            1000,
        )],
    );
    assert_eq!(
        rows[0].value_at(&db, RowKind::New, 0),
        Err(ValueError::UnresolvedFormat {
            column: 0,
            type_oid: None
        })
    );
}

/// A NULL carries no format byte to disagree with.
#[test]
fn a_null_reads_as_null_whatever_the_column_format() {
    let db = kinds();
    let event = deliver(
        binary_order(),
        relation(&[("id", 23), ("big", 20), ("acl", 1033)]),
        RowData::from_pairs(vec![
            ("id", int4(7)),
            ("big", ColumnValue::Null),
            ("acl", ColumnValue::Null),
        ]),
    );
    assert_eq!(event.value_at(&db, RowKind::New, 3), Ok(Value::Null));
    assert_eq!(event.value_at(&db, RowKind::New, ACL), Ok(Value::Null));
}

/// A `float8` cell in a column the catalog declares `real` is held on the
/// float4 grid, as its text would be parsed.
#[test]
fn a_double_sent_to_a_real_column_reads_at_float4() {
    let db = kinds();
    let single = 4;
    let binary = deliver(
        binary_order(),
        relation(&[("single", 701)]),
        RowData::from_pairs(vec![(
            "single",
            ColumnValue::binary_bytes(Bytes::copy_from_slice(&0.1_f64.to_be_bytes())),
        )]),
    );
    let text = deliver(
        PgOutputOrder::new(),
        relation(&[("single", 701)]),
        RowData::from_pairs(vec![("single", ColumnValue::text("0.1"))]),
    );
    assert_eq!(
        binary.value_at(&db, RowKind::New, single),
        text.value_at(&db, RowKind::New, single)
    );
}

/// Bytes sent by a type outside the declared family are not a value of the
/// column, whatever they decode to.
#[test]
fn a_cell_of_another_family_than_the_column_declares_is_refused() {
    let db = kinds();
    let body = 7;
    let event = deliver(
        binary_order(),
        relation(&[("body", 23)]),
        RowData::from_pairs(vec![("body", int4(7))]),
    );
    assert_eq!(
        event.value_at(&db, RowKind::New, body),
        Err(ValueError::Builtin {
            column: body,
            kind: crate::backend::ScalarFamily::String
        })
    );
}
