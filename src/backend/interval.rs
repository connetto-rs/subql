//! The PostgreSQL `interval` a row carries.

use core::cmp::Ordering;
use core::fmt;

use sql_values::PgInterval;

/// A PostgreSQL `interval` as its months, days and microseconds.
///
/// Two values are equal and hash alike only when every field is, so
/// `1 mon` and `30 days` stay distinct cells, as PostgreSQL prints them
/// differently. SQL compares them by span, a month as 30 days and a day as
/// 24 hours, through [`Interval::sql_cmp`], under which they are equal.
/// Arithmetic runs on the [`PgInterval`] that [`Interval::span`] answers.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct Interval {
    months: i32,
    days: i32,
    microseconds: i64,
}

impl Interval {
    /// Read the text PostgreSQL's `interval_in` accepts, or `None` where
    /// PostgreSQL raises an error.
    #[must_use]
    pub fn parse(text: &str) -> Option<Self> {
        sql_values::parse_interval(text).ok().map(Self::from)
    }

    /// The months, twelve of which print as a year.
    #[must_use]
    pub const fn months(self) -> i32 {
        self.months
    }

    /// The days, never folded into months.
    #[must_use]
    pub const fn days(self) -> i32 {
        self.days
    }

    /// The microseconds, never folded into days.
    #[must_use]
    pub const fn microseconds(self) -> i64 {
        self.microseconds
    }

    /// PostgreSQL's `interval_cmp`, which orders by span.
    #[must_use]
    pub fn sql_cmp(&self, other: &Self) -> Ordering {
        self.span().cmp(&other.span())
    }

    /// The value as PostgreSQL compares, hashes and computes it, equal for
    /// every spelling of one span.
    #[must_use]
    pub const fn span(self) -> PgInterval {
        PgInterval {
            months: self.months,
            days: self.days,
            microseconds: self.microseconds,
        }
    }
}

impl From<PgInterval> for Interval {
    fn from(interval: PgInterval) -> Self {
        Self {
            months: interval.months,
            days: interval.days,
            microseconds: interval.microseconds,
        }
    }
}

/// The text PostgreSQL prints under its default `IntervalStyle`.
impl fmt::Display for Interval {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.span(), f)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::Interval;
    use alloc::string::ToString;
    use core::cmp::Ordering;

    /// `1 mon` and `30 days` are one span to SQL and two cells to a row.
    #[test]
    fn spellings_of_one_span_compare_equal_and_stay_distinct() {
        let month = Interval::parse("1 mon").unwrap();
        let thirty_days = Interval::parse("30 days").unwrap();
        assert_ne!(month, thirty_days);
        assert_eq!(month.sql_cmp(&thirty_days), Ordering::Equal);
        assert_eq!(month.span(), thirty_days.span());
        assert_eq!(month.to_string(), "1 mon");
        assert_eq!(
            Interval::parse("-1 days +02:00:00")
                .unwrap()
                .sql_cmp(&month),
            Ordering::Less
        );
    }
}
