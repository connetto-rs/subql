//! The PostgreSQL `interval` a row carries.

use core::cmp::Ordering;
use core::fmt;

use sql_scalar_text::PgInterval;

/// A PostgreSQL `interval` as its months, days and microseconds.
///
/// Two values are equal and hash alike only when every field is, so
/// `1 mon` and `30 days` stay distinct cells, as PostgreSQL prints them
/// differently. SQL compares them by span, a month as 30 days and a day as
/// 24 hours, through [`Interval::sql_cmp`], under which they are equal.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
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
        sql_scalar_text::parse_interval(text).map(Self::from)
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

    /// The span in microseconds, a month as 30 days and a day as 24 hours,
    /// which is `interval_cmp_value` and so one key per span.
    #[must_use]
    pub fn span_microseconds(self) -> i128 {
        const MICROSECONDS_PER_DAY: i128 = 86_400_000_000;
        let days = i128::from(self.months) * 30 + i128::from(self.days);
        days * MICROSECONDS_PER_DAY + i128::from(self.microseconds)
    }

    /// The value as PostgreSQL compares and hashes it, equal for every
    /// spelling of one span.
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

    /// The index key orders and equates exactly as PostgreSQL compares,
    /// infinities included.
    #[test]
    fn the_span_key_agrees_with_sql_comparison() {
        let texts = [
            "-infinity",
            "-178000000 years",
            "-1 days +02:00:00",
            "00:00:00",
            "24:00:00",
            "1 day",
            "29 days 23:59:59.999999",
            "1 mon",
            "30 days",
            "1 year -1 mons",
            "infinity",
        ];
        for left in texts {
            for right in texts {
                let (left, right) = (
                    Interval::parse(left).unwrap(),
                    Interval::parse(right).unwrap(),
                );
                assert_eq!(
                    left.span_microseconds().cmp(&right.span_microseconds()),
                    left.sql_cmp(&right),
                    "{left} against {right}"
                );
            }
        }
    }
}
