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

    /// Whether the value is neither `infinity` nor `-infinity`.
    #[must_use]
    pub const fn is_finite(self) -> bool {
        self.span().is_finite()
    }

    /// PostgreSQL's `interval_div` of a finite interval by a nonzero finite
    /// `factor`, or `None` where it raises `interval out of range`.
    ///
    /// Months and days divide on their own, and what a month leaves over
    /// cascades into days as 30 and what a day leaves over into the clock,
    /// each rounded to the microsecond in double precision as PostgreSQL
    /// rounds it.
    #[must_use]
    #[expect(
        clippy::cast_possible_truncation,
        reason = "C's `(int)` casts of remainders the debug assertions bound"
    )]
    #[expect(
        clippy::suboptimal_flops,
        reason = "PostgreSQL rounds the product and the sum apart, which a fused multiply-add would not"
    )]
    pub fn divided_by(self, factor: f64) -> Option<Self> {
        const DAYS_PER_MONTH: f64 = 30.0;
        const SECS_PER_DAY: f64 = 86_400.0;
        const USECS_PER_SEC: f64 = 1_000_000.0;
        debug_assert!(self.is_finite() && factor.is_finite() && factor != 0.0);
        let months = f64::from(self.months) / factor;
        let days = f64::from(self.days) / factor;
        let month = fits_i32(months)?;
        let mut day = fits_i32(days)?;
        let month_remainder_days =
            ts_round((f64::from(self.months) / factor - f64::from(month)) * DAYS_PER_MONTH);
        debug_assert!(month_remainder_days.abs() < DAYS_PER_MONTH + 1.0);
        // C's `(int)` cast, truncating toward zero, of a value under 31 in magnitude.
        let whole_remainder_days = month_remainder_days as i32;
        let mut sec_remainder = ts_round(
            (f64::from(self.days) / factor - f64::from(day) + month_remainder_days
                - f64::from(whole_remainder_days))
                * SECS_PER_DAY,
        );
        if sec_remainder.abs() >= SECS_PER_DAY {
            debug_assert!(sec_remainder.abs() < 4.0 * SECS_PER_DAY);
            // Truncating toward zero as C does, of a value under four in magnitude.
            let whole_days = (sec_remainder / SECS_PER_DAY) as i32;
            day = day.checked_add(whole_days)?;
            sec_remainder -= f64::from(whole_days) * SECS_PER_DAY;
        }
        day = day.checked_add(whole_remainder_days)?;
        // The clock is an `int64` C divides as a double, as here.
        #[allow(clippy::cast_precision_loss)]
        let microseconds = self.microseconds as f64 / factor + sec_remainder * USECS_PER_SEC;
        let result = Self {
            months: month,
            days: day,
            microseconds: fits_i64(rint(microseconds))?,
        };
        result.is_finite().then_some(result)
    }
}

/// C's `rint` under the default rounding mode, ties to even, with no libm:
/// adding and removing 2^52 leaves only the rounded integer part.
fn rint(value: f64) -> f64 {
    const TWO_52: f64 = 4_503_599_627_370_496.0;
    if value.is_nan() || value.abs() >= TWO_52 {
        return value;
    }
    if value.is_sign_negative() {
        (value - TWO_52) + TWO_52
    } else {
        (value + TWO_52) - TWO_52
    }
}

/// PostgreSQL's `TSROUND`, rounding to the microsecond.
fn ts_round(value: f64) -> f64 {
    const TS_PREC_INV: f64 = 1_000_000.0;
    rint(value * TS_PREC_INV) / TS_PREC_INV
}

/// `FLOAT8_FITS_IN_INT32` and the truncating cast it guards.
#[expect(
    clippy::cast_possible_truncation,
    reason = "C's truncating cast of a value the range check bounds"
)]
fn fits_i32(value: f64) -> Option<i32> {
    (value >= f64::from(i32::MIN) && value < -f64::from(i32::MIN)).then_some(value as i32)
}

/// `FLOAT8_FITS_IN_INT64` and the truncating cast it guards.
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    reason = "-2^63 is exact as a double, and the cast of a value the check bounds truncates as C's"
)]
fn fits_i64(value: f64) -> Option<i64> {
    let min = i64::MIN as f64;
    (value >= min && value < -min).then_some(value as i64)
}

/// The running state of PostgreSQL's `sum(interval)` and `avg(interval)`.
///
/// The finite values are summed exactly per field and the infinities are
/// counted, as PostgreSQL 17 counts them. PostgreSQL adds in scan order and
/// raises on a partial sum leaving a field's range, so the same rows can sum
/// or raise by the order they are read in. Whenever a field's net total
/// fits, some order of the rows never leaves the range, and its answer is
/// that net total, which is the one reported here.
///
/// The counts are signed so that a row leaving is the same type as one
/// arriving, and folding either in is componentwise addition.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct IntervalSum {
    finite: i64,
    positive_infinities: i64,
    negative_infinities: i64,
    months: i128,
    days: i128,
    microseconds: i128,
}

impl IntervalSum {
    /// One row's interval contributing with `weight`, `1` for a row
    /// arriving and `-1` for one leaving.
    #[must_use]
    pub fn of(value: Interval, weight: i64) -> Self {
        if value == Interval::from(PgInterval::INFINITY) {
            return Self {
                positive_infinities: weight,
                ..Self::default()
            };
        }
        if value == Interval::from(PgInterval::NEG_INFINITY) {
            return Self {
                negative_infinities: weight,
                ..Self::default()
            };
        }
        let weight_wide = i128::from(weight);
        Self {
            finite: weight,
            positive_infinities: 0,
            negative_infinities: 0,
            months: i128::from(value.months) * weight_wide,
            days: i128::from(value.days) * weight_wide,
            microseconds: i128::from(value.microseconds) * weight_wide,
        }
    }

    /// The state a database reports for some rows: the sum of the finite
    /// ones, how many there are, and how many are each infinity.
    #[must_use]
    pub fn seeded(
        finite_sum: Option<Interval>,
        finite: i64,
        positive_infinities: i64,
        negative_infinities: i64,
    ) -> Self {
        let sum = finite_sum.unwrap_or_default();
        Self {
            finite,
            positive_infinities,
            negative_infinities,
            months: i128::from(sum.months),
            days: i128::from(sum.days),
            microseconds: i128::from(sum.microseconds),
        }
    }

    /// Fold `other` in, componentwise.
    pub const fn merge(&mut self, other: Self) {
        self.finite += other.finite;
        self.positive_infinities += other.positive_infinities;
        self.negative_infinities += other.negative_infinities;
        self.months += other.months;
        self.days += other.days;
        self.microseconds += other.microseconds;
    }

    /// Whether this moves nothing.
    #[must_use]
    pub fn is_zero(&self) -> bool {
        *self == Self::default()
    }

    /// The rows folded in.
    #[must_use]
    pub const fn count(&self) -> i64 {
        self.finite + self.positive_infinities + self.negative_infinities
    }

    /// PostgreSQL's `sum(interval)`: `Ok(None)` over no rows, and `Err` where
    /// every scan order raises `interval out of range`.
    ///
    /// # Errors
    ///
    /// [`IntervalOutOfRange`] for infinities of both signs and for a field
    /// whose net total leaves its range.
    pub fn sum(&self) -> Result<Option<Interval>, IntervalOutOfRange> {
        if self.count() <= 0 {
            return Ok(None);
        }
        match (self.positive_infinities > 0, self.negative_infinities > 0) {
            (true, true) => return Err(IntervalOutOfRange),
            (true, false) => return Ok(Some(Interval::from(PgInterval::INFINITY))),
            (false, true) => return Ok(Some(Interval::from(PgInterval::NEG_INFINITY))),
            (false, false) => {}
        }
        let total = Interval {
            months: i32::try_from(self.months).map_err(|_| IntervalOutOfRange)?,
            days: i32::try_from(self.days).map_err(|_| IntervalOutOfRange)?,
            microseconds: i64::try_from(self.microseconds).map_err(|_| IntervalOutOfRange)?,
        };
        // A finite total equal to an infinity's fields is out of range too.
        if total.is_finite() {
            Ok(Some(total))
        } else {
            Err(IntervalOutOfRange)
        }
    }

    /// PostgreSQL's `avg(interval)`, the sum divided by the finite count as
    /// `interval_div` divides, with the same `Ok(None)` and `Err` as
    /// [`IntervalSum::sum`].
    ///
    /// # Errors
    ///
    /// [`IntervalOutOfRange`] where the sum is, or the quotient leaves the range.
    pub fn mean(&self) -> Result<Option<Interval>, IntervalOutOfRange> {
        let Some(sum) = self.sum()? else {
            return Ok(None);
        };
        if !sum.is_finite() {
            return Ok(Some(sum));
        }
        // PostgreSQL divides by the count as a double too.
        #[allow(clippy::cast_precision_loss)]
        let count = self.finite as f64;
        sum.divided_by(count).map(Some).ok_or(IntervalOutOfRange)
    }
}

/// PostgreSQL's `interval out of range` error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IntervalOutOfRange;

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
    use super::{Interval, IntervalOutOfRange, IntervalSum};
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

    fn interval(text: &str) -> Interval {
        Interval::parse(text).unwrap()
    }

    /// `interval / n` as PostgreSQL 16 and 18 both print it, the cascade of
    /// a month's remainder into days and a day's into the clock included.
    #[test]
    fn division_cascades_as_postgres_divides() {
        for (dividend, factor, quotient) in [
            ("1 mon 1 day 00:00:01", 7.0, "4 days 10:17:08.6896"),
            ("-1 mon 5 days -00:00:00.000001", 2.0, "-13 days +12:00:00"),
            ("1 mon", 3.0, "10 days"),
            (
                "7 mons 3 days 00:00:00.000005",
                3.0,
                "2 mons 11 days 00:00:00.000002",
            ),
            ("-7 mons -3 days", 3.0, "-2 mons -11 days"),
            ("1 year 1 day 23:59:59.999999", 4.0, "3 mons 12:00:00"),
            (
                "11 mons 29 days 23:59:59.999999",
                3.0,
                "3 mons 29 days 24:00:00",
            ),
            ("-1 mons", 7.0, "-4 days -06:51:25.6896"),
            ("00:00:00.5", 3.0, "00:00:00.166667"),
        ] {
            assert_eq!(
                interval(dividend).divided_by(factor).unwrap().to_string(),
                quotient,
                "{dividend} / {factor}"
            );
        }
        assert_eq!(interval("2147483647 days").divided_by(0.5), None);
    }

    fn fold(sum: &mut IntervalSum, text: &str, weight: i64) {
        sum.merge(IntervalSum::of(interval(text), weight));
    }

    /// The net total is answered whenever it fits, which is what PostgreSQL
    /// answers for 2e9, -2e9, 2e9 months read in that order, and refused
    /// when it does not, as every order raises then.
    #[test]
    fn a_sum_is_its_net_total_when_that_fits() {
        let mut sum = IntervalSum::default();
        assert_eq!(sum.sum(), Ok(None));
        for text in ["1 mon", "1 day", "1 sec"] {
            fold(&mut sum, text, 1);
        }
        assert_eq!(sum.sum(), Ok(Some(interval("1 mon 1 day 00:00:01"))));
        assert_eq!(
            sum.mean().unwrap().unwrap().to_string(),
            "10 days 08:00:00.333333"
        );
        fold(&mut sum, "1 day", -1);
        assert_eq!(sum.sum(), Ok(Some(interval("1 mon 00:00:01"))));

        let mut wide = IntervalSum::default();
        for text in ["2000000000 mons", "-2000000000 mons", "2000000000 mons"] {
            fold(&mut wide, text, 1);
        }
        assert_eq!(wide.sum(), Ok(Some(interval("2000000000 mons"))));
        fold(&mut wide, "-2000000000 mons", -1);
        assert_eq!(wide.sum(), Err(IntervalOutOfRange));
        assert_eq!(
            IntervalSum::seeded(None, 0, 0, 0).sum(),
            Ok(None),
            "a seed over no rows answers NULL"
        );
        let mut seeded = IntervalSum::seeded(Some(interval("1 mon 1 day")), 2, 1, 0);
        assert_eq!(seeded.sum(), Ok(Some(interval("infinity"))));
        fold(&mut seeded, "infinity", -1);
        assert_eq!(
            seeded.mean().unwrap().unwrap().to_string(),
            "15 days 12:00:00"
        );
    }

    /// A row added and taken back nets to nothing, so a batch of both moves
    /// no total.
    #[test]
    fn a_row_and_its_removal_cancel() {
        let mut delta = IntervalSum::of(interval("-1 days +02:00:00"), 1);
        assert!(!delta.is_zero());
        delta.merge(IntervalSum::of(interval("-1 days +02:00:00"), -1));
        assert!(delta.is_zero());
    }

    /// Infinities of one sign answer that infinity, and of both raise, as on
    /// PostgreSQL 17 and later.
    #[test]
    fn infinities_decide_the_sum_and_the_mean() {
        let mut sum = IntervalSum::default();
        fold(&mut sum, "1 day", 1);
        fold(&mut sum, "infinity", 1);
        assert_eq!(sum.sum(), Ok(Some(interval("infinity"))));
        assert_eq!(sum.mean(), Ok(Some(interval("infinity"))));
        assert_eq!(sum.count(), 2);
        fold(&mut sum, "-infinity", 1);
        assert_eq!(sum.sum(), Err(IntervalOutOfRange));
        fold(&mut sum, "infinity", -1);
        assert_eq!(sum.mean(), Ok(Some(interval("-infinity"))));
    }
}
