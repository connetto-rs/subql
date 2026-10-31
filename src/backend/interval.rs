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

    /// PostgreSQL 17's `interval_pl`, or `None` where it raises `interval
    /// out of range`: a field overflows, or the infinities cancel.
    #[must_use]
    pub fn checked_add(self, other: Self) -> Option<Self> {
        match (self.infinity(), other.infinity()) {
            (Some(left), Some(right)) => (left == right).then_some(self),
            (Some(_), None) => Some(self),
            (None, Some(_)) => Some(other),
            (None, None) => Self {
                months: self.months.checked_add(other.months)?,
                days: self.days.checked_add(other.days)?,
                microseconds: self.microseconds.checked_add(other.microseconds)?,
            }
            .finite(),
        }
    }

    /// PostgreSQL 17's `interval_mi`, with [`Interval::checked_add`]'s `None`.
    #[must_use]
    pub fn checked_sub(self, other: Self) -> Option<Self> {
        match (self.infinity(), other.infinity()) {
            (Some(left), Some(right)) => (left != right).then_some(self),
            (Some(_), None) => Some(self),
            (None, Some(_)) => other.checked_neg(),
            (None, None) => Self {
                months: self.months.checked_sub(other.months)?,
                days: self.days.checked_sub(other.days)?,
                microseconds: self.microseconds.checked_sub(other.microseconds)?,
            }
            .finite(),
        }
    }

    /// PostgreSQL 17's `interval_um`, or `None` for a field at its minimum.
    #[must_use]
    pub fn checked_neg(self) -> Option<Self> {
        match self.infinity() {
            Some(Ordering::Greater) => Some(Self::from(PgInterval::NEG_INFINITY)),
            Some(_) => Some(Self::from(PgInterval::INFINITY)),
            None => Self {
                months: self.months.checked_neg()?,
                days: self.days.checked_neg()?,
                microseconds: self.microseconds.checked_neg()?,
            }
            .finite(),
        }
    }

    /// `Greater` for `infinity`, `Less` for `-infinity`, `None` when finite.
    fn infinity(self) -> Option<Ordering> {
        if self == Self::from(PgInterval::INFINITY) {
            Some(Ordering::Greater)
        } else if self == Self::from(PgInterval::NEG_INFINITY) {
            Some(Ordering::Less)
        } else {
            None
        }
    }

    /// `self` when finite: a finite result spelling an infinity's fields is
    /// out of range.
    fn finite(self) -> Option<Self> {
        self.is_finite().then_some(self)
    }

    /// PostgreSQL 17's `interval_mul`, or `None` where it raises `interval
    /// out of range`.
    ///
    /// An infinite interval keeps its sign or flips it with the factor, an
    /// infinite factor gives the infinity of the product's sign, and zero
    /// against an infinity or a `NaN` factor has no answer.
    #[must_use]
    pub fn times(self, factor: f64) -> Option<Self> {
        if factor.is_nan() {
            return None;
        }
        if !self.is_finite() {
            return match factor.partial_cmp(&0.0)? {
                Ordering::Equal => None,
                Ordering::Less => self.checked_neg(),
                Ordering::Greater => Some(self),
            };
        }
        if factor.is_infinite() {
            let sign = self.span_microseconds().cmp(&0);
            return match (sign, factor > 0.0) {
                (Ordering::Equal, _) => None,
                (Ordering::Greater, true) | (Ordering::Less, false) => {
                    Some(Self::from(PgInterval::INFINITY))
                }
                _ => Some(Self::from(PgInterval::NEG_INFINITY)),
            };
        }
        self.cascade(|field| field * factor)
    }

    /// PostgreSQL 17's `interval_div` by a nonzero `factor`, or `None` where
    /// it raises `interval out of range`.
    ///
    /// An infinite interval keeps its sign or flips it with the factor, and
    /// against an infinite factor or a `NaN` one has no answer. A finite
    /// interval divided by an infinity is zero.
    #[must_use]
    pub fn divided_by(self, factor: f64) -> Option<Self> {
        debug_assert!(factor != 0.0, "division by zero is the caller's error");
        if factor.is_nan() {
            return None;
        }
        if !self.is_finite() {
            return match (factor.is_infinite(), factor < 0.0) {
                (true, _) => None,
                (false, true) => self.checked_neg(),
                (false, false) => Some(self),
            };
        }
        self.cascade(|field| field / factor)
    }

    /// `interval_mul` and `interval_div` once the infinities are handled:
    /// months and days scale on their own, and what a month leaves over
    /// cascades into days as 30 and what a day leaves over into the clock,
    /// each rounded to the microsecond in double precision as PostgreSQL
    /// rounds it.
    #[expect(
        clippy::cast_possible_truncation,
        reason = "C's `(int)` casts of remainders the debug assertions bound"
    )]
    #[expect(
        clippy::suboptimal_flops,
        reason = "PostgreSQL rounds the product and the sum apart, which a fused multiply-add would not"
    )]
    fn cascade(self, scale: impl Fn(f64) -> f64) -> Option<Self> {
        const DAYS_PER_MONTH: f64 = 30.0;
        const SECS_PER_DAY: f64 = 86_400.0;
        const USECS_PER_SEC: f64 = 1_000_000.0;
        let month = fits_i32(scale(f64::from(self.months)))?;
        let mut day = fits_i32(scale(f64::from(self.days)))?;
        let month_remainder_days =
            ts_round((scale(f64::from(self.months)) - f64::from(month)) * DAYS_PER_MONTH);
        debug_assert!(month_remainder_days.abs() < DAYS_PER_MONTH + 1.0);
        let whole_remainder_days = month_remainder_days as i32;
        let mut sec_remainder = ts_round(
            (scale(f64::from(self.days)) - f64::from(day) + month_remainder_days
                - f64::from(whole_remainder_days))
                * SECS_PER_DAY,
        );
        if sec_remainder.abs() >= SECS_PER_DAY {
            debug_assert!(sec_remainder.abs() < 4.0 * SECS_PER_DAY);
            let whole_days = (sec_remainder / SECS_PER_DAY) as i32;
            day = day.checked_add(whole_days)?;
            sec_remainder -= f64::from(whole_days) * SECS_PER_DAY;
        }
        day = day.checked_add(whole_remainder_days)?;
        // The clock is an `int64` C converts to a double before scaling, as here.
        #[allow(clippy::cast_precision_loss)]
        let microseconds = scale(self.microseconds as f64) + sec_remainder * USECS_PER_SEC;
        Self {
            months: month,
            days: day,
            microseconds: fits_i64(rint(microseconds))?,
        }
        .finite()
    }
}

/// Microseconds in a day.
const USECS_PER_DAY: i64 = 86_400_000_000;

/// The first and the past-the-end PostgreSQL `timestamp`, as microseconds
/// from its 2000-01-01 epoch: 4714-11-24 BC and 294277-01-01.
const MIN_TIMESTAMP: i64 = -211_813_488_000_000_000;
const END_TIMESTAMP: i64 = 9_223_371_331_200_000_000;

/// The Julian day of 0001-01-01, which `chrono` counts as day 1 of the era.
const JULIAN_OF_DAY_ZERO_CE: i32 = 1_721_425;

/// The past-the-end Julian day of a PostgreSQL `timestamp`, 294277-01-01.
const TIMESTAMP_END_JULIAN: i32 = 109_203_528;

fn pg_epoch() -> chrono::NaiveDateTime {
    chrono::NaiveDate::from_ymd_opt(2000, 1, 1)
        .unwrap_or_default()
        .and_time(chrono::NaiveTime::MIN)
}

/// A timestamp as PostgreSQL holds it, microseconds from its epoch, or
/// `None` outside what PostgreSQL holds.
fn pg_timestamp(at: chrono::NaiveDateTime) -> Option<i64> {
    (at - pg_epoch())
        .num_microseconds()
        .filter(|micros| (MIN_TIMESTAMP..END_TIMESTAMP).contains(micros))
}

/// Why a date or time result has no in-process value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimestampShift {
    /// PostgreSQL raises, `timestamp out of range` or `interval out of range`.
    OutOfRange,
    /// PostgreSQL answers a value `chrono` cannot hold: an infinite timestamp,
    /// or one past the year 262142.
    Unrepresentable,
}

/// PostgreSQL 17's `timestamp_pl_interval`: months added to the calendar
/// month, the day of the month clamped to the month's last, then days added
/// to the calendar day, then the clock.
///
/// # Errors
///
/// [`TimestampShift`] where PostgreSQL raises or answers a value `chrono`
/// cannot hold.
pub fn shift_timestamp(
    at: chrono::NaiveDateTime,
    span: Interval,
) -> Result<chrono::NaiveDateTime, TimestampShift> {
    use chrono::Datelike;

    if !span.is_finite() {
        return Err(TimestampShift::Unrepresentable);
    }
    // A date `chrono` cannot build lies past its year 262142, which
    // PostgreSQL holds until 294276 and then raises on.
    let past_chrono = |inside_postgres: bool| {
        if inside_postgres {
            TimestampShift::Unrepresentable
        } else {
            TimestampShift::OutOfRange
        }
    };
    let mut at = at;
    if span.months != 0 {
        let mut month = i32::try_from(at.month())
            .ok()
            .and_then(|month| month.checked_add(span.months))
            .ok_or(TimestampShift::OutOfRange)?;
        let mut year = at.year();
        // C's truncating division and remainder, as `timestamp_pl_interval` normalises.
        if month > 12 {
            year += (month - 1) / 12;
            month = (month - 1) % 12 + 1;
        } else if month < 1 {
            year += month / 12 - 1;
            month = month % 12 + 12;
        }
        let month = u32::try_from(month).map_err(|_| TimestampShift::OutOfRange)?;
        let last = days_in_month(year, month);
        let date = chrono::NaiveDate::from_ymd_opt(year, month, at.day().min(last))
            .ok_or_else(|| past_chrono((1..=294_276).contains(&year)))?;
        at = date.and_time(at.time());
        pg_timestamp(at).ok_or(TimestampShift::OutOfRange)?;
    }
    if span.days != 0 {
        let julian = at
            .date()
            .num_days_from_ce()
            .checked_add(JULIAN_OF_DAY_ZERO_CE)
            .and_then(|julian| julian.checked_add(span.days))
            .filter(|julian| *julian >= 0)
            .ok_or(TimestampShift::OutOfRange)?;
        let date = chrono::NaiveDate::from_num_days_from_ce_opt(julian - JULIAN_OF_DAY_ZERO_CE)
            .ok_or_else(|| past_chrono(julian < TIMESTAMP_END_JULIAN))?;
        at = date.and_time(at.time());
        pg_timestamp(at).ok_or(TimestampShift::OutOfRange)?;
    }
    let micros = pg_timestamp(at)
        .ok_or(TimestampShift::OutOfRange)?
        .checked_add(span.microseconds)
        .filter(|micros| (MIN_TIMESTAMP..END_TIMESTAMP).contains(micros))
        .ok_or(TimestampShift::OutOfRange)?;
    pg_epoch()
        .checked_add_signed(chrono::TimeDelta::microseconds(micros))
        .ok_or(TimestampShift::Unrepresentable)
}

/// Days in `month` of the proleptic Gregorian `year`, `0` being 1 BC.
const fn days_in_month(year: i32, month: u32) -> u32 {
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    match month {
        2 if leap => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

/// PostgreSQL's `timestamp_mi` of two finite timestamps, the difference in
/// microseconds with whole days carried into the days field as
/// `interval_justify_hours` carries them, or `None` where it raises.
#[must_use]
pub fn timestamp_difference(
    later: chrono::NaiveDateTime,
    earlier: chrono::NaiveDateTime,
) -> Option<Interval> {
    let time = pg_timestamp(later)?.checked_sub(pg_timestamp(earlier)?)?;
    let whole_days = time / USECS_PER_DAY;
    let mut time = time - whole_days * USECS_PER_DAY;
    let mut days = i32::try_from(whole_days).ok()?;
    if days > 0 && time < 0 {
        time += USECS_PER_DAY;
        days -= 1;
    } else if days < 0 && time > 0 {
        time -= USECS_PER_DAY;
        days += 1;
    }
    Some(Interval {
        months: 0,
        days,
        microseconds: time,
    })
}

/// PostgreSQL's `time_pl_interval` and `time_mi_interval`, the clock moved
/// by the interval's microseconds and wrapped within the day.
///
/// The interval's months and days are ignored. `None` for an infinite
/// interval, which PostgreSQL 17 refuses.
#[must_use]
pub fn shift_time(
    at: chrono::NaiveTime,
    span: Interval,
    subtract: bool,
) -> Option<chrono::NaiveTime> {
    use chrono::Timelike;

    if !span.is_finite() {
        return None;
    }
    let clock = i128::from(at.num_seconds_from_midnight()) * 1_000_000
        + i128::from(at.nanosecond() / 1_000);
    let moved = if subtract {
        clock - i128::from(span.microseconds)
    } else {
        clock + i128::from(span.microseconds)
    };
    let micros = moved.rem_euclid(i128::from(USECS_PER_DAY));
    let seconds = u32::try_from(micros / 1_000_000).ok()?;
    let nanos = u32::try_from(micros % 1_000_000).ok()? * 1_000;
    chrono::NaiveTime::from_num_seconds_from_midnight_opt(seconds, nanos)
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
    use super::{
        shift_time, shift_timestamp, timestamp_difference, Interval, IntervalOutOfRange,
        IntervalSum, TimestampShift,
    };
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
            // The month's remainder and the day's both push the clock's past the day, so the cascade carries a whole day into the clock.
            ("1 mon 6 days", 7.0, "5 days 03:25:42.832457"),
            ("-1 mon -6 days", 7.0, "-5 days -03:25:42.832457"),
        ] {
            assert_eq!(
                interval(dividend).divided_by(factor).unwrap().to_string(),
                quotient,
                "{dividend} / {factor}"
            );
        }
        // Each quotient leaves a field's range, and PostgreSQL 16 and 18 raise `interval out of range` there.
        assert_eq!(interval("2147483647 days").divided_by(0.5), None);
        assert_eq!(interval("178956970 years 7 months").divided_by(0.5), None);
        let clock = Interval {
            months: 0,
            days: 0,
            microseconds: i64::MAX,
        };
        assert_eq!(clock.divided_by(0.5), None);
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

    /// Totals past a field's range and a total holding the infinity's own
    /// fields are refused, while the months at the field's ceiling answer,
    /// as PostgreSQL 16 and 18 do.
    #[test]
    fn a_sum_that_leaves_a_field_range_is_refused() {
        let mut months_ceiling = IntervalSum::default();
        fold(&mut months_ceiling, "178956969 years 11 months", 1);
        fold(&mut months_ceiling, "8 months", 1);
        assert_eq!(
            months_ceiling.sum(),
            Ok(Some(interval("178956970 years 7 mons")))
        );
        let mut months_overflow = IntervalSum::default();
        fold(&mut months_overflow, "2147483647 mons", 1);
        assert!(
            months_overflow.sum().is_ok(),
            "the months' ceiling stays in range"
        );
        fold(&mut months_overflow, "1 mons", 1);
        assert_eq!(months_overflow.sum(), Err(IntervalOutOfRange));
        let mut days = IntervalSum::default();
        fold(&mut days, "1500000000 days", 1);
        fold(&mut days, "1500000000 days", 1);
        assert_eq!(days.sum(), Err(IntervalOutOfRange));
        // The total leaves the clock's range, which PostgreSQL 16 and 18 answer differently, so the stop serves the database's answer.
        let mut clock = IntervalSum::default();
        fold(&mut clock, "9223372036854775807 microseconds", 1);
        assert!(
            clock.sum().is_ok(),
            "one of the ceiling's value stays in range"
        );
        fold(&mut clock, "9223372036854775807 microseconds", 1);
        assert_eq!(clock.sum(), Err(IntervalOutOfRange));
        // Each row keeps the total in range until it holds the infinity's own fields, which no answer can carry.
        let mut sentinel = IntervalSum::default();
        fold(&mut sentinel, "178956970 years 7 mons", 1);
        assert!(sentinel.sum().is_ok(), "the months' ceiling stays in range");
        fold(&mut sentinel, "2147483647 days", 1);
        assert!(sentinel.sum().is_ok(), "the days' ceiling stays in range");
        fold(&mut sentinel, "9223372036854775807 microseconds", 1);
        assert_eq!(sentinel.sum(), Err(IntervalOutOfRange));
        // The same fields read from their text are the infinity's own span.
        assert_eq!(
            interval("178956970 years 7 mons 2147483647 days 9223372036854775807 microseconds")
                .span(),
            sql_scalar_text::PgInterval::INFINITY
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

    /// A timestamp as the vectors spell it, minutes or seconds optional, and
    /// a year past 9999 written without the sign `chrono` asks for.
    fn timestamp(text: &str) -> chrono::NaiveDateTime {
        let signed = alloc::format!("+{text}");
        let text = if text.split('-').next().is_some_and(|year| year.len() > 4) {
            signed.as_str()
        } else {
            text
        };
        ["%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%d %H:%M"]
            .iter()
            .find_map(|format| chrono::NaiveDateTime::parse_from_str(text, format).ok())
            .or_else(|| {
                chrono::NaiveDate::parse_from_str(text, "%Y-%m-%d")
                    .ok()
                    .map(|date| date.and_time(chrono::NaiveTime::MIN))
            })
            .unwrap()
    }

    /// `interval * factor` as PostgreSQL 16 and 18 both print it, and the
    /// infinities as 18 answers them.
    #[test]
    fn multiplication_cascades_as_postgres_multiplies() {
        for (interval_text, factor, product) in [
            (
                "1 mon 1 day 00:00:01",
                1.5,
                Some("1 mon 16 days 12:00:01.5"),
            ),
            ("1 mon", 0.1, Some("3 days")),
            ("-1 mon -1 day", 0.3333, Some("-10 days -07:58:30.72")),
            (
                "7 mons 3 days 00:00:00.000005",
                3.0,
                Some("1 year 9 mons 9 days 00:00:00.000015"),
            ),
            ("1 day", 1e10, None),
            ("01:00:00", -0.5, Some("-00:30:00")),
            ("1 mon", f64::NAN, None),
            ("1 day", f64::INFINITY, Some("infinity")),
            ("infinity", -2.0, Some("-infinity")),
            ("infinity", 0.0, None),
            ("0", f64::INFINITY, None),
        ] {
            assert_eq!(
                interval(interval_text)
                    .times(factor)
                    .map(|i| i.to_string())
                    .as_deref(),
                product,
                "{interval_text} * {factor}"
            );
        }
    }

    /// `+`, `-` and unary `-` as PostgreSQL answers them, overflow and
    /// cancelling infinities raising.
    #[test]
    fn addition_and_negation_follow_postgres() {
        assert_eq!(
            interval("1 day 02:00")
                .checked_add(interval("-3 days 1 mon"))
                .unwrap()
                .to_string(),
            "1 mon -2 days +02:00:00"
        );
        assert_eq!(
            interval("2147483647 days").checked_add(interval("1 day")),
            None
        );
        assert_eq!(
            interval("1 mon -2 days 03:00")
                .checked_neg()
                .unwrap()
                .to_string(),
            "-1 mons +2 days -03:00:00"
        );
        assert_eq!(
            interval("infinity").checked_add(interval("-infinity")),
            None
        );
        assert_eq!(interval("infinity").checked_sub(interval("infinity")), None);
        assert_eq!(
            interval("1 day").checked_sub(interval("-infinity")),
            Some(interval("infinity"))
        );
        assert_eq!(
            interval("1 day")
                .checked_sub(interval("02:00"))
                .unwrap()
                .to_string(),
            "1 day -02:00:00"
        );
    }

    /// `timestamp + interval` as PostgreSQL 16 and 18 both print it: the
    /// month clamped to its last day, then days, then the clock.
    #[test]
    fn a_timestamp_moves_as_postgres_moves_it() {
        for (start, span, end) in [
            (
                "2024-01-31 10:00:00.5",
                "1 mon",
                Ok("2024-02-29 10:00:00.500"),
            ),
            ("2023-01-31 10:00", "1 mon", Ok("2023-02-28 10:00:00")),
            ("2024-03-31 00:00", "-1 mon", Ok("2024-02-29 00:00:00")),
            (
                "2024-01-31 10:00",
                "13 mons 1 day -01:00:00",
                Ok("2025-03-01 09:00:00"),
            ),
            (
                "2024-01-01 00:00",
                "-25 mons -1 day",
                Ok("2021-11-30 00:00:00"),
            ),
            ("2024-02-29 12:00", "1 year", Ok("2025-02-28 12:00:00")),
            ("0001-01-01 00:00", "-1 day", Ok("0000-12-31 00:00:00")),
            (
                "262142-12-31 23:59:59",
                "00:00:01",
                Err(TimestampShift::Unrepresentable),
            ),
            (
                "2024-01-31 10:00",
                "2147483647 mons",
                Err(TimestampShift::OutOfRange),
            ),
            (
                "2024-01-31 10:00",
                "infinity",
                Err(TimestampShift::Unrepresentable),
            ),
            (
                "262142-12-31 00:00",
                "1 day",
                Err(TimestampShift::Unrepresentable),
            ),
            (
                "262142-12-31 00:00",
                "1 mon",
                Err(TimestampShift::Unrepresentable),
            ),
            (
                "262142-12-31 00:00",
                "33000 years",
                Err(TimestampShift::OutOfRange),
            ),
        ] {
            let format = |at: chrono::NaiveDateTime| at.format("%Y-%m-%d %H:%M:%S%.f").to_string();
            assert_eq!(
                shift_timestamp(timestamp(start), interval(span)).map(format),
                end.map(|text| format(
                    chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f").unwrap()
                )),
                "{start} + {span}"
            );
        }
        // 4713-11-24 BC less a microsecond, as PostgreSQL 16 and 18 answer it.
        let bc = |year, day| {
            chrono::NaiveDate::from_ymd_opt(year, 11, day)
                .unwrap()
                .and_time(chrono::NaiveTime::MIN)
        };
        assert_eq!(
            shift_timestamp(bc(-4712, 24), interval("-00:00:00.000001")),
            Ok(bc(-4712, 23) + chrono::TimeDelta::microseconds(86_399_999_999))
        );
        let before_the_first = chrono::NaiveDate::from_ymd_opt(-4713, 11, 24)
            .unwrap()
            .and_time(chrono::NaiveTime::MIN);
        assert_eq!(
            shift_timestamp(before_the_first, interval("-00:00:00.000001")),
            Err(TimestampShift::OutOfRange),
            "4714-11-24 BC is PostgreSQL's first timestamp"
        );
    }

    /// `timestamp - timestamp` carries whole days into the days field, and a
    /// time wraps within its day ignoring months and days.
    #[test]
    fn differences_and_clocks_follow_postgres() {
        let between = |later: &str, earlier: &str| {
            timestamp_difference(timestamp(later), timestamp(earlier)).map(|i| i.to_string())
        };
        assert_eq!(
            between("2024-03-01 00:00", "2024-01-31 12:00:00.25").as_deref(),
            Some("29 days 11:59:59.75")
        );
        assert_eq!(
            between("2024-01-01", "2024-03-01 00:00:01").as_deref(),
            Some("-60 days -00:00:01")
        );
        let clock = |at: &str, span: &str, subtract: bool| {
            shift_time(
                chrono::NaiveTime::parse_from_str(at, "%H:%M").unwrap(),
                interval(span),
                subtract,
            )
            .map(|t| t.to_string())
        };
        assert_eq!(
            clock("23:30", "1 day 01:00", false).as_deref(),
            Some("00:30:00")
        );
        assert_eq!(clock("00:30", "50:00", true).as_deref(), Some("22:30:00"));
        assert_eq!(
            clock("10:00", "-1 mon 25:00", false).as_deref(),
            Some("11:00:00")
        );
        assert_eq!(clock("10:00", "infinity", false), None);
    }
}
