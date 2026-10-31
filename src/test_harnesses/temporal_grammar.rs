//! Generated filters over PostgreSQL's date, time and interval arithmetic.
//!
//! One fixed table holds a value of every kind the arithmetic reads, and the
//! filters move, scale and subtract them before comparing. Only PostgreSQL
//! is a reference, since MySQL and SQLite hold none of these kinds as it
//! does. A quoted literal is drawn untyped half the time, so the sweep also
//! compares the type subql gives one beside each operator. One operation in
//! six reads two operands of any kinds, which PostgreSQL mostly has no
//! operator for, so subql serving one of them shows as well.

use alloc::boxed::Box;
use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;

use arbitrary::Unstructured;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use sql_traits::structs::ParserDB;

use super::predicate_grammar::{
    float_literal, int_literal, maybe, pick, served_verdict, Engine, Op, COMPARISONS,
};
use crate::backend::{Interval, Postgres, Value};

/// The table every case reads.
pub const DDL: &str = "CREATE TABLE tt (id INT PRIMARY KEY, ts TIMESTAMP, tz TIMESTAMPTZ, \
                       d DATE, tm TIME, span INTERVAL, n BIGINT, x DOUBLE PRECISION, q NUMERIC)";

/// Timestamps with month ends, a leap day, a fraction, the first year and
/// one a thousand years short of what `chrono` holds.
const TIMESTAMPS: [&str; 6] = [
    "2024-01-31 10:00:00",
    "2024-02-29 23:59:59.999999",
    "2023-03-31 00:00:00",
    "1999-12-31 12:00:00.5",
    "0001-01-01 00:00:00",
    "262000-12-31 00:00:00",
];
/// Instants on both of 2024's daylight saving changes in Europe.
const TIMESTAMPTZS: [&str; 4] = [
    "2024-03-31 00:30:00+00",
    "2024-10-27 00:30:00+00",
    "2024-01-31 23:00:00+00",
    "1999-12-31 23:59:59.999999+00",
];
const DATES: [&str; 4] = ["2024-01-31", "2024-02-29", "2023-12-31", "0001-01-01"];
const TIMES: [&str; 4] = ["00:00:00", "23:59:59.999999", "12:30:00.5", "06:00:00"];
/// Intervals as PostgreSQL prints them, with mixed signs, fields that
/// overflow into the next, and each field's extreme.
const INTERVALS: [&str; 13] = [
    "00:00:00",
    "1 mon 1 day 00:00:01",
    "-1 mons +2 days -00:00:03",
    "1 year 2 mons",
    "25:00:00",
    "-00:00:00.000001",
    "1 day",
    "01:30:00",
    "30 days",
    "1000 years",
    "178000000 years",
    "-2147483648 days",
    "3 days 04:05:06.789",
];
const INTS: [i64; 8] = [0, 1, -1, 2, 3, 7, -100, 1 << 40];
const FLOATS: [f64; 7] = [0.0, 0.5, -1.5, 2.0, 0.1, 1e10, 1e300];
const DECIMALS: [&str; 5] = ["0", "0.3333", "2", "-1.5", "100000000000000000000"];

/// The kind of a value, each held by one column of the table.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// `timestamp`, held by `ts`.
    Timestamp,
    /// `timestamptz`, held by `tz`.
    TimestampTz,
    /// `date`, held by `d`.
    Date,
    /// `time`, held by `tm`.
    Time,
    /// `interval`, held by `span`.
    Interval,
    /// `bigint`, held by `n`.
    Int,
    /// `double precision`, held by `x`.
    Float,
    /// `numeric`, held by `q`.
    Decimal,
}

const KINDS: [Kind; 8] = [
    Kind::Timestamp,
    Kind::TimestampTz,
    Kind::Date,
    Kind::Time,
    Kind::Interval,
    Kind::Int,
    Kind::Float,
    Kind::Decimal,
];

/// The kinds a comparison is drawn over, intervals and timestamps twice.
const COMPARED: [Kind; 7] = [
    Kind::Interval,
    Kind::Interval,
    Kind::Timestamp,
    Kind::Timestamp,
    Kind::TimestampTz,
    Kind::Time,
    Kind::Date,
];

const NUMBERS: [Kind; 3] = [Kind::Int, Kind::Float, Kind::Decimal];

impl Kind {
    const fn column(self) -> &'static str {
        match self {
            Self::Timestamp => "ts",
            Self::TimestampTz => "tz",
            Self::Date => "d",
            Self::Time => "tm",
            Self::Interval => "span",
            Self::Int => "n",
            Self::Float => "x",
            Self::Decimal => "q",
        }
    }

    /// The keyword a typed literal of this kind is written after.
    const fn keyword(self) -> Option<&'static str> {
        match self {
            Self::Timestamp => Some("timestamp"),
            Self::TimestampTz => Some("timestamptz"),
            Self::Date => Some("date"),
            Self::Time => Some("time"),
            Self::Interval => Some("interval"),
            Self::Int | Self::Float | Self::Decimal => None,
        }
    }
}

/// A value of one kind, as the row stores it and a literal writes it.
#[derive(Clone, Copy, Debug)]
pub enum Literal {
    /// A date, time or interval, in the text PostgreSQL prints.
    Text(Kind, &'static str),
    /// A `bigint`.
    Int(i64),
    /// A `double precision`.
    Float(f64),
    /// A `numeric`, in its text.
    Decimal(&'static str),
}

impl Literal {
    fn arbitrary(u: &mut Unstructured<'_>, kind: Kind) -> arbitrary::Result<Self> {
        Ok(match kind {
            Kind::Timestamp => Self::Text(kind, pick(u, &TIMESTAMPS)?),
            Kind::TimestampTz => Self::Text(kind, pick(u, &TIMESTAMPTZS)?),
            Kind::Date => Self::Text(kind, pick(u, &DATES)?),
            Kind::Time => Self::Text(kind, pick(u, &TIMES)?),
            Kind::Interval => Self::Text(kind, pick(u, &INTERVALS)?),
            Kind::Int => Self::Int(pick(u, &INTS)?),
            Kind::Float => Self::Float(pick(u, &FLOATS)?),
            Kind::Decimal => Self::Decimal(pick(u, &DECIMALS)?),
        })
    }

    /// The literal, a quoted one after its kind's keyword when `typed`.
    fn render(self, typed: bool) -> String {
        match self {
            Self::Text(kind, text) => kind.keyword().filter(|_| typed).map_or_else(
                || format!("'{text}'"),
                |keyword| format!("{keyword} '{text}'"),
            ),
            Self::Int(n) => int_literal(n, Engine::Postgres),
            Self::Float(x) => float_literal(x),
            Self::Decimal(text) if text.starts_with('-') => format!("({text})"),
            Self::Decimal(text) => text.into(),
        }
    }

    /// The value a change event carries for this literal.
    ///
    /// # Panics
    ///
    /// When a pool holds text its kind does not read.
    #[must_use]
    pub fn cell(self) -> Value<Postgres> {
        match self {
            Self::Text(Kind::Timestamp, text) => Value::Timestamp(timestamp(text)),
            Self::Text(Kind::TimestampTz, text) => Value::TimestampTz(
                DateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f%#z")
                    .expect("the pool's timestamptz parses")
                    .with_timezone(&Utc),
            ),
            Self::Text(Kind::Date, text) => Value::Date(
                NaiveDate::parse_from_str(text, "%Y-%m-%d").expect("the pool's date parses"),
            ),
            Self::Text(Kind::Time, text) => Value::Time(
                NaiveTime::parse_from_str(text, "%H:%M:%S%.f").expect("the pool's time parses"),
            ),
            Self::Text(_, text) => {
                Value::Interval(Interval::parse(text).expect("the pool's interval parses"))
            }
            Self::Int(n) => Value::Int(n),
            Self::Float(x) => Value::Float(x),
            Self::Decimal(text) => Value::Decimal(text.parse().expect("the pool's numeric parses")),
        }
    }
}

/// `chrono` reads a year past 9999 only after a sign.
fn timestamp(text: &str) -> NaiveDateTime {
    const FORMAT: &str = "%Y-%m-%d %H:%M:%S%.f";
    NaiveDateTime::parse_from_str(text, FORMAT)
        .or_else(|_| NaiveDateTime::parse_from_str(&format!("+{text}"), FORMAT))
        .expect("the pool's timestamp parses")
}

/// One row of the table, each column possibly `NULL`.
#[derive(Clone, Debug)]
pub struct Row(Vec<Option<Literal>>);

impl Row {
    fn arbitrary(u: &mut Unstructured<'_>) -> arbitrary::Result<Self> {
        KINDS
            .iter()
            .map(|&kind| maybe(u, |u| Literal::arbitrary(u, kind)))
            .collect::<arbitrary::Result<_>>()
            .map(Self)
    }

    /// The statement that stores the row with `id = 1`.
    #[must_use]
    pub fn insert_sql(&self) -> String {
        let columns: Vec<&str> = KINDS.iter().map(|kind| kind.column()).collect();
        let values: Vec<String> = self
            .0
            .iter()
            .map(|cell| cell.map_or_else(|| "NULL".into(), |literal| literal.render(false)))
            .collect();
        format!(
            "INSERT INTO tt (id, {}) VALUES (1, {})",
            columns.join(", "),
            values.join(", ")
        )
    }

    /// The row as a change event carries it, in column order, with `id = 1`.
    #[must_use]
    pub fn cells(&self) -> Vec<Value<Postgres>> {
        core::iter::once(Value::Int(1))
            .chain(
                self.0
                    .iter()
                    .map(|cell| cell.map_or(Value::Null, Literal::cell)),
            )
            .collect()
    }
}

/// An expression tree, rendered fully parenthesised.
#[derive(Clone, Debug)]
pub enum Expr {
    /// The column holding a kind.
    Column(Kind),
    /// A literal, typed or quoted alone.
    Literal(Literal, bool),
    /// `NULL`.
    Null,
    /// `-e`.
    Neg(Box<Self>),
    /// `NOT e`.
    Not(Box<Self>),
    /// A binary operator.
    Binary(Op, Box<Self>, Box<Self>),
    /// `e IS [NOT] NULL`.
    IsNull(Box<Self>, bool),
    /// `e [NOT] BETWEEN low AND high`.
    Between(Box<Self>, Box<Self>, Box<Self>, bool),
    /// `e [NOT] IN (list)`.
    In(Box<Self>, Vec<Self>, bool),
}

impl Expr {
    fn condition(u: &mut Unstructured<'_>, depth: u8) -> arbitrary::Result<Self> {
        let below = depth.saturating_sub(1);
        if depth == 0 || u.ratio(1u8, 3u8)? {
            return Self::comparison(u, below);
        }
        let condition = |u: &mut Unstructured<'_>| Self::condition(u, below).map(Box::new);
        Ok(match u.int_in_range(0u8..=5)? {
            0 => Self::Not(condition(u)?),
            1 | 2 => Self::Binary(pick(u, &[Op::And, Op::Or])?, condition(u)?, condition(u)?),
            3 => {
                let kind = pick(u, &COMPARED)?;
                Self::IsNull(Box::new(Self::value(u, below, kind)?), u.arbitrary()?)
            }
            4 => {
                let kind = pick(u, &COMPARED)?;
                Self::Between(
                    Box::new(Self::value(u, below, kind)?),
                    Box::new(Self::value(u, below, kind)?),
                    Box::new(Self::value(u, below, kind)?),
                    u.arbitrary()?,
                )
            }
            _ => {
                let kind = pick(u, &COMPARED)?;
                let list = (0..u.int_in_range(1usize..=3)?)
                    .map(|_| Ok(Self::Literal(Literal::arbitrary(u, kind)?, u.arbitrary()?)))
                    .collect::<arbitrary::Result<_>>()?;
                Self::In(Box::new(Self::value(u, below, kind)?), list, u.arbitrary()?)
            }
        })
    }

    fn comparison(u: &mut Unstructured<'_>, depth: u8) -> arbitrary::Result<Self> {
        let kind = pick(u, &COMPARED)?;
        Ok(Self::Binary(
            pick(u, &COMPARISONS)?,
            Box::new(Self::value(u, depth, kind)?),
            Box::new(Self::value(u, depth, kind)?),
        ))
    }

    /// A value of `kind`, its operands of the kinds PostgreSQL's operator
    /// reads, except one operation in six over operands of any kinds.
    fn value(u: &mut Unstructured<'_>, depth: u8, kind: Kind) -> arbitrary::Result<Self> {
        if depth == 0 || u.ratio(1u8, 3u8)? {
            return Self::leaf(u, kind);
        }
        let below = depth - 1;
        let of = |u: &mut Unstructured<'_>, kind| Self::value(u, below, kind).map(Box::new);
        if u.ratio(1u8, 6u8)? {
            let op = pick(u, &[Op::Add, Op::Sub, Op::Mul, Op::Div])?;
            let (left, right) = (pick(u, &KINDS)?, pick(u, &KINDS)?);
            return Ok(Self::Binary(op, of(u, left)?, of(u, right)?));
        }
        let additive = |u: &mut Unstructured<'_>| pick(u, &[Op::Add, Op::Sub]);
        Ok(match kind {
            Kind::Interval => match u.int_in_range(0u8..=5)? {
                0 => Self::Binary(additive(u)?, of(u, kind)?, of(u, kind)?),
                1 => Self::Neg(of(u, kind)?),
                2 => {
                    let number = pick(u, &NUMBERS)?;
                    let number = of(u, number)?;
                    if u.arbitrary()? {
                        Self::Binary(Op::Mul, of(u, kind)?, number)
                    } else {
                        Self::Binary(Op::Mul, number, of(u, kind)?)
                    }
                }
                3 => {
                    let number = pick(u, &NUMBERS)?;
                    Self::Binary(Op::Div, of(u, kind)?, of(u, number)?)
                }
                4 => Self::Binary(Op::Sub, of(u, Kind::Timestamp)?, of(u, Kind::Timestamp)?),
                _ => Self::Binary(
                    Op::Sub,
                    of(u, Kind::TimestampTz)?,
                    of(u, Kind::TimestampTz)?,
                ),
            },
            Kind::Timestamp => match u.int_in_range(0u8..=2)? {
                0 => Self::Binary(additive(u)?, of(u, kind)?, of(u, Kind::Interval)?),
                1 => Self::Binary(Op::Add, of(u, Kind::Interval)?, of(u, kind)?),
                _ => Self::Binary(additive(u)?, of(u, Kind::Date)?, of(u, Kind::Interval)?),
            },
            Kind::TimestampTz | Kind::Time => {
                if u.arbitrary()? {
                    Self::Binary(additive(u)?, of(u, kind)?, of(u, Kind::Interval)?)
                } else {
                    Self::Binary(Op::Add, of(u, Kind::Interval)?, of(u, kind)?)
                }
            }
            Kind::Int | Kind::Float | Kind::Decimal => Self::Neg(of(u, kind)?),
            Kind::Date => Self::leaf(u, kind)?,
        })
    }

    /// The column of `kind` two times in three, otherwise a literal of it or
    /// now and then `NULL`.
    fn leaf(u: &mut Unstructured<'_>, kind: Kind) -> arbitrary::Result<Self> {
        if u.ratio(1u8, 12u8)? {
            return Ok(Self::Null);
        }
        if u.ratio(1u8, 3u8)? {
            Ok(Self::Literal(Literal::arbitrary(u, kind)?, u.arbitrary()?))
        } else {
            Ok(Self::Column(kind))
        }
    }

    /// The expression as PostgreSQL spells it.
    #[must_use]
    pub fn render(&self) -> String {
        let not = |negated: bool| if negated { "NOT " } else { "" };
        match self {
            Self::Column(kind) => kind.column().into(),
            Self::Literal(literal, typed) => literal.render(*typed),
            Self::Null => "NULL".into(),
            Self::Neg(inner) => format!("(- {})", inner.render()),
            Self::Not(inner) => format!("(NOT {})", inner.render()),
            Self::Binary(op, left, right) => {
                format!("({} {} {})", left.render(), op.sql(), right.render())
            }
            Self::IsNull(inner, negated) => {
                format!("({} IS {}NULL)", inner.render(), not(*negated))
            }
            Self::Between(tested, low, high, negated) => format!(
                "({} {}BETWEEN {} AND {})",
                tested.render(),
                not(*negated),
                low.render(),
                high.render()
            ),
            Self::In(tested, list, negated) => {
                let list: Vec<String> = list.iter().map(Self::render).collect();
                format!(
                    "({} {}IN ({}))",
                    tested.render(),
                    not(*negated),
                    list.join(", ")
                )
            }
        }
    }
}

/// One generated case, a row of the table and a filter over it.
#[derive(Clone, Debug)]
pub struct Case {
    /// The row stored in the table.
    pub row: Row,
    /// The filter.
    pub filter: Expr,
}

impl Case {
    /// Draw a case from fuzzer-controlled bytes.
    ///
    /// # Errors
    ///
    /// When the bytes run out before the case is complete.
    pub fn arbitrary(u: &mut Unstructured<'_>) -> arbitrary::Result<Self> {
        Ok(Self {
            row: Row::arbitrary(u)?,
            filter: Expr::condition(u, 3)?,
        })
    }

    /// DDL, insert and filter, which is what a divergence has to carry.
    #[must_use]
    pub fn reproduction(&self) -> String {
        format!(
            "{DDL};\n{};\nSELECT COUNT(*) FROM tt WHERE {};",
            self.row.insert_sql(),
            self.filter.render()
        )
    }
}

/// The catalog of [`DDL`].
///
/// # Panics
///
/// If the fixed DDL stops parsing.
#[must_use]
pub fn catalog() -> ParserDB {
    ParserDB::parse::<sqlparser::dialect::PostgreSqlDialect>(DDL).expect("the case table parses")
}

/// Whether subql, serving the filter in process on PostgreSQL, delivers the
/// row, `None` when it does not answer in process.
///
/// `database` is [`catalog`], taken by value so a caller judging many cases
/// parses it once and clones it.
#[must_use]
pub fn subql_selects(case: &Case, database: ParserDB) -> Option<bool> {
    served_verdict::<Postgres>(
        database,
        "tt",
        &case.filter.render(),
        case.row.cells(),
        || case.reproduction(),
    )
}
