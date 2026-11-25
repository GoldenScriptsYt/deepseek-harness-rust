use crate::{PlanningError, PlanningResult, MAX_SAFE_SESSION_INTEGER};
use chrono::{Datelike, NaiveDate, NaiveDateTime, TimeZone, Timelike};
use chrono_tz::Tz;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::str::FromStr;

const MAX_RRULE_COUNT: u64 = 1_000;
const MAX_PERIOD_CANDIDATES: usize = 10_080;
const MAX_SET_POSITIONS: usize = 31;
const MIN_RRULE_YEAR: u32 = 100;
const WEEKDAYS: [&str; 7] = ["MO", "TU", "WE", "TH", "FR", "SA", "SU"];

#[derive(Debug, Clone, Copy)]
struct CronSpec {
    min: u32,
    max: u32,
    canonical_max: u32,
}

const CRON_FIELDS: [CronSpec; 5] = [
    CronSpec {
        min: 0,
        max: 59,
        canonical_max: 59,
    },
    CronSpec {
        min: 0,
        max: 23,
        canonical_max: 23,
    },
    CronSpec {
        min: 1,
        max: 31,
        canonical_max: 31,
    },
    CronSpec {
        min: 1,
        max: 12,
        canonical_max: 12,
    },
    CronSpec {
        min: 0,
        max: 7,
        canonical_max: 6,
    },
];

#[derive(Debug)]
struct CronField {
    canonical: String,
}

fn digits(text: &str, start: usize, length: usize) -> Option<u32> {
    let bytes = text.as_bytes();
    if start.checked_add(length)? > bytes.len() {
        return None;
    }
    let mut value = 0u32;
    for byte in &bytes[start..start + length] {
        if !byte.is_ascii_digit() {
            return None;
        }
        value = value * 10 + u32::from(byte - b'0');
    }
    Some(value)
}

fn unsigned(text: &str) -> Option<u64> {
    if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    text.parse().ok()
}

fn signed(text: &str) -> Option<i64> {
    let body = text
        .strip_prefix('+')
        .or_else(|| text.strip_prefix('-'))
        .unwrap_or(text);
    if body.is_empty() || !body.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    text.parse().ok()
}

pub(crate) fn leap_year(year: u32) -> bool {
    year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400))
}

fn valid_date(year: u32, month: u32, day: u32) -> bool {
    if year == 0 || !(1..=12).contains(&month) {
        return false;
    }
    let days = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap_year(year) => 29,
        2 => 28,
        _ => return false,
    };
    day >= 1 && day <= days
}

pub(crate) fn canonical_instant(value: &str) -> PlanningResult<()> {
    let bytes = value.as_bytes();
    if bytes.len() != 24
        || bytes.get(4) != Some(&b'-')
        || bytes.get(7) != Some(&b'-')
        || bytes.get(10) != Some(&b'T')
        || bytes.get(13) != Some(&b':')
        || bytes.get(16) != Some(&b':')
        || bytes.get(19) != Some(&b'.')
        || bytes.get(23) != Some(&b'Z')
    {
        return Err(invalid(
            "schedule.scheduledAt",
            "must be a canonical four-digit-year RFC 3339 UTC instant",
        ));
    }
    let year = digits(value, 0, 4).unwrap_or(0);
    let month = digits(value, 5, 2).unwrap_or(0);
    let day = digits(value, 8, 2).unwrap_or(0);
    let hour = digits(value, 11, 2).unwrap_or(99);
    let minute = digits(value, 14, 2).unwrap_or(99);
    let second = digits(value, 17, 2).unwrap_or(99);
    let millis = digits(value, 20, 3).unwrap_or(1_000);
    if !valid_date(year, month, day) || hour > 23 || minute > 59 || second > 59 || millis > 999 {
        return Err(invalid(
            "schedule.scheduledAt",
            "is not a real canonical UTC calendar instant",
        ));
    }
    Ok(())
}

pub(crate) fn canonical_local_time(value: &str) -> PlanningResult<()> {
    let bytes = value.as_bytes();
    if bytes.len() != 12
        || bytes.get(2) != Some(&b':')
        || bytes.get(5) != Some(&b':')
        || bytes.get(8) != Some(&b'.')
    {
        return Err(invalid(
            "schedule.time",
            "must be normalized to HH:mm:ss.SSS",
        ));
    }
    let hour = digits(value, 0, 2).unwrap_or(99);
    let minute = digits(value, 3, 2).unwrap_or(99);
    let second = digits(value, 6, 2).unwrap_or(99);
    let millis = digits(value, 9, 3).unwrap_or(1_000);
    if hour > 23 || minute > 59 || second > 59 || millis > 999 {
        return Err(invalid("schedule.time", "is not a valid local clock time"));
    }
    Ok(())
}

fn zone_shape(value: &str) -> bool {
    if value == "UTC" {
        return true;
    }
    let mut parts = value.split('/');
    let Some(first) = parts.next() else {
        return false;
    };
    let first = first.as_bytes();
    if first.is_empty()
        || !first[0].is_ascii_alphabetic()
        || !first
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(*byte, b'_' | b'+' | b'-' | b'.'))
    {
        return false;
    }
    let mut count = 0;
    for part in parts {
        let bytes = part.as_bytes();
        if bytes.is_empty()
            || !bytes.iter().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(*byte, b'_' | b'+' | b'-' | b'.')
            })
        {
            return false;
        }
        count += 1;
    }
    count > 0
}

pub(crate) fn time_zone(value: &str) -> PlanningResult<Tz> {
    if value.is_empty() || value.trim() != value || !zone_shape(value) {
        return Err(invalid(
            "schedule.timeZone",
            "must be UTC or a valid IANA Area/Location name",
        ));
    }
    let zone = Tz::from_str(value).map_err(|_| {
        invalid(
            "schedule.timeZone",
            "must be UTC or a supported IANA Area/Location name",
        )
    })?;
    let name = zone.name();
    if !zone_shape(name) {
        return Err(invalid(
            "schedule.timeZone",
            "does not resolve to a canonical IANA zone",
        ));
    }
    Ok(zone)
}

fn cron_number(text: &str, spec: CronSpec, name: &str) -> PlanningResult<u32> {
    let value = unsigned(text)
        .ok_or_else(|| invalid("schedule.cron", format!("{name} value is not an integer")))?;
    if value < u64::from(spec.min) || value > u64::from(spec.max) {
        return Err(invalid(
            "schedule.cron",
            format!("{name} value is outside {}-{}", spec.min, spec.max),
        ));
    }
    Ok(value as u32)
}

fn cron_step(text: &str, name: &str) -> PlanningResult<u32> {
    let value = unsigned(text)
        .ok_or_else(|| invalid("schedule.cron", format!("{name} step is not an integer")))?;
    if value == 0 || value > u64::from(u32::MAX) {
        return Err(invalid(
            "schedule.cron",
            format!("{name} step must be positive"),
        ));
    }
    Ok(value as u32)
}

fn fold_cron(value: u32, spec: CronSpec) -> u32 {
    if spec.canonical_max != spec.max && value == spec.max {
        spec.min
    } else {
        value
    }
}

fn star_walk(spec: CronSpec, step: u32) -> Vec<u32> {
    let mut values = BTreeSet::new();
    let mut value = spec.min;
    while value <= spec.max {
        values.insert(fold_cron(value, spec));
        match value.checked_add(step) {
            Some(next) => value = next,
            None => break,
        }
    }
    values.into_iter().collect()
}

fn encode_cron_values(values: &[u32]) -> String {
    if values.len() == 1 {
        return values[0].to_string();
    }
    let step = values[1] - values[0];
    if values.windows(2).all(|pair| pair[1] - pair[0] == step) {
        return if step == 1 {
            format!("{}-{}", values[0], values[values.len() - 1])
        } else {
            format!("{}-{}/{step}", values[0], values[values.len() - 1])
        };
    }
    let mut parts = Vec::new();
    let mut start = values[0];
    for index in 1..values.len() {
        if values[index] == values[index - 1] + 1 {
            continue;
        }
        let previous = values[index - 1];
        parts.push(if start == previous {
            start.to_string()
        } else {
            format!("{start}-{previous}")
        });
        start = values[index];
    }
    let last = values[values.len() - 1];
    parts.push(if start == last {
        start.to_string()
    } else {
        format!("{start}-{last}")
    });
    parts.join(",")
}

fn encode_star_cron(values: &[u32], spec: CronSpec) -> String {
    if values.len() == usize::try_from(spec.canonical_max - spec.min + 1).unwrap_or(usize::MAX) {
        return "*".into();
    }
    let present: BTreeSet<u32> = values.iter().copied().collect();
    let mut best_step = 1u32;
    let mut best = Vec::new();
    for step in 1..=spec.max - spec.min + 1 {
        let walk = star_walk(spec, step);
        if walk.len() > best.len() && walk.iter().all(|value| present.contains(value)) {
            best_step = step;
            best = walk;
        }
    }
    let walked: BTreeSet<u32> = best.into_iter().collect();
    let remaining: Vec<u32> = values
        .iter()
        .copied()
        .filter(|value| !walked.contains(value))
        .collect();
    if remaining.is_empty() {
        format!("*/{best_step}")
    } else {
        format!("*/{best_step},{}", encode_cron_values(&remaining))
    }
}

fn parse_cron_field(raw: &str, spec: CronSpec) -> PlanningResult<CronField> {
    let mut matched = BTreeSet::new();
    for element in raw.split(',') {
        if element.is_empty() {
            return Err(invalid("schedule.cron", "contains an empty list element"));
        }
        if element == "*" {
            matched.extend((spec.min..=spec.max).map(|value| fold_cron(value, spec)));
            continue;
        }
        if let Some(step_text) = element.strip_prefix("*/") {
            let step = cron_step(step_text, "cron")?;
            matched.extend(star_walk(spec, step));
            continue;
        }
        if element.starts_with('*') {
            return Err(invalid("schedule.cron", "contains an invalid star element"));
        }
        let mut pieces = element.split('/');
        let base = pieces.next().unwrap_or_default();
        let step_text = pieces.next();
        if pieces.next().is_some() {
            return Err(invalid(
                "schedule.cron",
                "contains an invalid range element",
            ));
        }
        let (start, end) = if let Some((start, end)) = base.split_once('-') {
            if end.contains('-') {
                return Err(invalid("schedule.cron", "contains an invalid range"));
            }
            (
                cron_number(start, spec, "cron")?,
                cron_number(end, spec, "cron")?,
            )
        } else {
            if step_text.is_some() {
                return Err(invalid(
                    "schedule.cron",
                    "a single value cannot carry a step",
                ));
            }
            let value = cron_number(base, spec, "cron")?;
            matched.insert(fold_cron(value, spec));
            continue;
        };
        if start > end {
            return Err(invalid("schedule.cron", "contains an inverted range"));
        }
        let step = step_text
            .map(|text| cron_step(text, "cron"))
            .transpose()?
            .unwrap_or(1);
        let mut value = start;
        while value <= end {
            matched.insert(fold_cron(value, spec));
            match value.checked_add(step) {
                Some(next) => value = next,
                None => break,
            }
        }
    }
    let values: Vec<u32> = matched.into_iter().collect();
    let star = raw.starts_with('*');
    let canonical = if star {
        encode_star_cron(&values, spec)
    } else {
        encode_cron_values(&values)
    };
    Ok(CronField { canonical })
}

pub(crate) fn canonical_cron(value: &str) -> PlanningResult<()> {
    if value.is_empty() || value.trim() != value {
        return Err(invalid(
            "schedule.cron.expression",
            "must be a non-empty trimmed string",
        ));
    }
    let raw: Vec<&str> = value.split_whitespace().collect();
    if raw.len() != 5 {
        return Err(invalid(
            "schedule.cron.expression",
            "must contain exactly five fields",
        ));
    }
    let fields: Vec<CronField> = raw
        .iter()
        .zip(CRON_FIELDS)
        .map(|(raw, spec)| parse_cron_field(raw, spec))
        .collect::<PlanningResult<_>>()?;
    let canonical = fields
        .iter()
        .map(|field| field.canonical.as_str())
        .collect::<Vec<_>>()
        .join(" ");
    if canonical != value {
        return Err(invalid("schedule.cron.expression", "must be canonical"));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
struct ListRange {
    min: i64,
    max: i64,
}

const LIST_RANGES: [(&str, ListRange); 8] = [
    ("BYSECOND", ListRange { min: 0, max: 59 }),
    ("BYMINUTE", ListRange { min: 0, max: 59 }),
    ("BYHOUR", ListRange { min: 0, max: 23 }),
    ("BYMONTHDAY", ListRange { min: -31, max: 31 }),
    (
        "BYYEARDAY",
        ListRange {
            min: -366,
            max: 366,
        },
    ),
    ("BYWEEKNO", ListRange { min: -53, max: 53 }),
    ("BYMONTH", ListRange { min: 1, max: 12 }),
    (
        "BYSETPOS",
        ListRange {
            min: -366,
            max: 366,
        },
    ),
];

#[derive(Debug)]
struct ByDay {
    entries: usize,
    ordinals: usize,
}

#[derive(Debug)]
struct RuleValues {
    frequency: String,
    lists: BTreeMap<String, Vec<i64>>,
    by_day: Option<ByDay>,
}

fn basic_time(field: &'static str, text: &str) -> PlanningResult<NaiveDateTime> {
    if text.len() != 15 || text.as_bytes().get(8) != Some(&b'T') {
        return Err(invalid(field, "must be a basic YYYYMMDDTHHMMSS date-time"));
    }
    let year = digits(text, 0, 4).unwrap_or(0);
    let month = digits(text, 4, 2).unwrap_or(0);
    let day = digits(text, 6, 2).unwrap_or(0);
    let hour = digits(text, 9, 2).unwrap_or(99);
    let minute = digits(text, 11, 2).unwrap_or(99);
    let second = digits(text, 13, 2).unwrap_or(99);
    if year < MIN_RRULE_YEAR
        || !valid_date(year, month, day)
        || hour > 23
        || minute > 59
        || second > 59
    {
        return Err(invalid(
            field,
            "must be a real calendar date-time in years 0100 through 9999",
        ));
    }
    NaiveDate::from_ymd_opt(year as i32, month, day)
        .and_then(|date| date.and_hms_opt(hour, minute, second))
        .ok_or_else(|| invalid(field, "must be a real calendar date-time"))
}

fn exists_in_zone(local: NaiveDateTime, zone: Tz) -> bool {
    zone.from_local_datetime(&local).single().is_some()
        || zone.from_local_datetime(&local).earliest().is_some()
}

fn recurrence_lines(value: &str) -> PlanningResult<(String, String)> {
    let lines: Vec<String> = value
        .replace("\r\n", "\n")
        .trim()
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_owned)
        .collect();
    if lines.len() != 2 || !lines[0].starts_with("DTSTART") || !lines[1].starts_with("RRULE:") {
        return Err(invalid(
            "schedule.rrule",
            "must contain one DTSTART line followed by one RRULE line",
        ));
    }
    Ok((lines[0].clone(), lines[1].clone()))
}

fn parse_start(line: &str) -> PlanningResult<(Tz, NaiveDateTime)> {
    if let Some(text) = line.strip_prefix("DTSTART:") {
        let text = text
            .strip_suffix('Z')
            .ok_or_else(|| invalid("schedule.rrule.DTSTART", "must use UTC with Z"))?;
        let local = basic_time("schedule.rrule.DTSTART", text)?;
        return Ok((time_zone("UTC")?, local));
    }
    let rest = line.strip_prefix("DTSTART;TZID=").ok_or_else(|| {
        invalid(
            "schedule.rrule.DTSTART",
            "must use UTC with Z or an explicit TZID",
        )
    })?;
    if rest.len() < 16 || rest.as_bytes().get(rest.len() - 16) != Some(&b':') {
        return Err(invalid(
            "schedule.rrule.DTSTART",
            "must use a local basic date-time with TZID",
        ));
    }
    let split = rest.len() - 16;
    let zone = time_zone(&rest[..split])?;
    let local = basic_time("schedule.rrule.DTSTART", &rest[split + 1..])?;
    if !exists_in_zone(local, zone) {
        return Err(invalid(
            "schedule.rrule.DTSTART",
            "must be a real local date-time in its time zone",
        ));
    }
    Ok((zone, local))
}

fn rule_parts(line: &str) -> PlanningResult<BTreeMap<String, String>> {
    let body = line
        .strip_prefix("RRULE:")
        .ok_or_else(|| invalid("schedule.rrule", "must start with RRULE:"))?;
    let mut parts = BTreeMap::new();
    for component in body.split(';') {
        let Some((key, text)) = component.split_once('=') else {
            return Err(invalid(
                "schedule.rrule",
                "fields must be non-empty and unique",
            ));
        };
        if key.is_empty() || text.is_empty() || parts.contains_key(key) {
            return Err(invalid(
                "schedule.rrule",
                "fields must be non-empty and unique",
            ));
        }
        const SUPPORTED: [&str; 14] = [
            "FREQ",
            "INTERVAL",
            "COUNT",
            "UNTIL",
            "WKST",
            "BYSECOND",
            "BYMINUTE",
            "BYHOUR",
            "BYDAY",
            "BYMONTHDAY",
            "BYYEARDAY",
            "BYWEEKNO",
            "BYMONTH",
            "BYSETPOS",
        ];
        if !SUPPORTED.contains(&key) {
            return Err(invalid(
                "schedule.rrule",
                format!("unsupported field {key}"),
            ));
        }
        parts.insert(key.to_owned(), text.to_owned());
    }
    Ok(parts)
}

fn positive_value(
    text: Option<&String>,
    max: u64,
    field: &'static str,
) -> PlanningResult<Option<u64>> {
    text.map(|text| {
        let value = unsigned(text).ok_or_else(|| invalid(field, "must be a positive integer"))?;
        if value == 0 || value > max {
            return Err(invalid(field, "is outside the supported range"));
        }
        Ok(value)
    })
    .transpose()
}

fn list_values(key: &'static str, text: &str, range: ListRange) -> PlanningResult<Vec<i64>> {
    let mut values = Vec::new();
    let mut seen = BTreeSet::new();
    for item in text.split(',') {
        let value = if range.min < 0 {
            signed(item)
        } else {
            unsigned(item).and_then(|value| i64::try_from(value).ok())
        }
        .ok_or_else(|| invalid(key, "must contain integers"))?;
        if value < range.min
            || value > range.max
            || (range.min < 0 && value == 0)
            || !seen.insert(value)
        {
            return Err(invalid(key, "values must be unique and in range"));
        }
        values.push(value);
    }
    Ok(values)
}

fn by_day(text: Option<&String>) -> PlanningResult<Option<ByDay>> {
    let Some(text) = text else { return Ok(None) };
    let mut seen = BTreeSet::new();
    let mut ordinals = 0usize;
    let mut entries = 0usize;
    for selector in text.split(',') {
        if selector.len() < 2 {
            return Err(invalid("BYDAY", "contains an invalid weekday selector"));
        }
        let (ordinal, weekday) = selector.split_at(selector.len() - 2);
        if !WEEKDAYS.contains(&weekday) {
            return Err(invalid("BYDAY", "contains an invalid weekday selector"));
        }
        let normalized_ordinal = if ordinal.is_empty() {
            None
        } else {
            let digits = ordinal
                .strip_prefix('+')
                .or_else(|| ordinal.strip_prefix('-'))
                .unwrap_or(ordinal);
            let value = if digits.len() > 1 && digits.starts_with('0') {
                None
            } else {
                signed(ordinal)
            }
            .ok_or_else(|| invalid("BYDAY", "contains an invalid ordinal"))?;
            if value == 0 || !(-53..=53).contains(&value) {
                return Err(invalid(
                    "BYDAY",
                    "ordinals must be from -53 through 53 excluding zero",
                ));
            }
            ordinals += 1;
            Some(value)
        };
        let normalized = normalized_ordinal
            .map_or_else(|| weekday.to_owned(), |value| format!("{value}{weekday}"));
        if !seen.insert(normalized) {
            return Err(invalid("BYDAY", "values must be unique"));
        }
        entries += 1;
    }
    Ok(Some(ByDay { entries, ordinals }))
}

fn rule_values(parts: &BTreeMap<String, String>) -> PlanningResult<RuleValues> {
    let frequency = parts
        .get("FREQ")
        .ok_or_else(|| invalid("schedule.rrule", "must contain FREQ"))?
        .clone();
    if frequency == "SECONDLY" {
        return Err(invalid(
            "schedule.rrule.FREQ",
            "occurrences must be at least 60 seconds apart",
        ));
    }
    if !["MINUTELY", "HOURLY", "DAILY", "WEEKLY", "MONTHLY", "YEARLY"].contains(&frequency.as_str())
    {
        return Err(invalid("schedule.rrule.FREQ", "is invalid"));
    }
    positive_value(parts.get("INTERVAL"), MAX_SAFE_SESSION_INTEGER, "INTERVAL")?;
    let count = positive_value(parts.get("COUNT"), MAX_RRULE_COUNT, "COUNT")?;
    let until = parts
        .get("UNTIL")
        .map(|text| {
            let text = text
                .strip_suffix('Z')
                .ok_or_else(|| invalid("UNTIL", "must be an absolute UTC date-time ending in Z"))?;
            basic_time("UNTIL", text)
        })
        .transpose()?;
    if count.is_some() && until.is_some() {
        return Err(invalid(
            "schedule.rrule",
            "must not contain both COUNT and UNTIL",
        ));
    }
    if let Some(wkst) = parts.get("WKST") {
        if !WEEKDAYS.contains(&wkst.as_str()) {
            return Err(invalid("WKST", "must be MO, TU, WE, TH, FR, SA, or SU"));
        }
    }
    let mut lists = BTreeMap::new();
    for (key, range) in LIST_RANGES {
        if let Some(text) = parts.get(key) {
            lists.insert(key.to_owned(), list_values(key, text, range)?);
        }
    }
    let by_day = by_day(parts.get("BYDAY"))?;
    if lists.get("BYSECOND").is_some_and(|values| values.len() > 1) {
        return Err(invalid("BYSECOND", "must contain one value"));
    }
    if matches!(frequency.as_str(), "MINUTELY" | "HOURLY")
        && (!lists.is_empty() || by_day.is_some())
    {
        return Err(invalid(
            "schedule.rrule",
            "MINUTELY and HOURLY do not support BY rule parts",
        ));
    }
    for key in ["BYWEEKNO", "BYYEARDAY"] {
        if lists.contains_key(key) && frequency != "YEARLY" {
            return Err(invalid(key, "requires FREQ=YEARLY"));
        }
    }
    if lists.contains_key("BYMONTHDAY") && !matches!(frequency.as_str(), "MONTHLY" | "YEARLY") {
        return Err(invalid(
            "BYMONTHDAY",
            "requires FREQ=MONTHLY or FREQ=YEARLY",
        ));
    }
    if by_day.as_ref().is_some_and(|by_day| by_day.ordinals > 0)
        && !matches!(frequency.as_str(), "MONTHLY" | "YEARLY")
    {
        return Err(invalid(
            "BYDAY",
            "ordinals require FREQ=MONTHLY or FREQ=YEARLY",
        ));
    }
    if by_day.as_ref().is_some_and(|by_day| by_day.ordinals > 0) && lists.contains_key("BYWEEKNO") {
        return Err(invalid(
            "schedule.rrule",
            "BYDAY ordinals cannot be combined with BYWEEKNO",
        ));
    }
    if lists.contains_key("BYSETPOS") && lists.len() == 1 && by_day.is_none() {
        return Err(invalid("BYSETPOS", "requires another BY rule part"));
    }
    Ok(RuleValues {
        frequency,
        lists,
        by_day,
    })
}

fn list_len(values: &RuleValues, key: &str) -> usize {
    values.lists.get(key).map_or(usize::MAX, Vec::len)
}

fn by_day_days(by_day: Option<&ByDay>, per_weekday: usize) -> usize {
    let Some(by_day) = by_day else {
        return usize::MAX;
    };
    by_day
        .ordinals
        .saturating_add(per_weekday.saturating_mul(by_day.entries - by_day.ordinals))
}

fn period_days(values: &RuleValues) -> usize {
    let months = values.lists.get("BYMONTH").map(Vec::len);
    match values.frequency.as_str() {
        "MINUTELY" | "HOURLY" | "DAILY" => 1,
        "WEEKLY" => 7.min(by_day_days(values.by_day.as_ref(), 1)),
        "MONTHLY" => 31
            .min(list_len(values, "BYMONTHDAY"))
            .min(by_day_days(values.by_day.as_ref(), 5)),
        "YEARLY" => {
            if let Some(months) = months {
                [
                    months.saturating_mul(31),
                    list_len(values, "BYYEARDAY"),
                    months.saturating_mul(list_len(values, "BYMONTHDAY")),
                    list_len(values, "BYWEEKNO").saturating_mul(9),
                    months.saturating_mul(by_day_days(values.by_day.as_ref(), 5)),
                ]
                .into_iter()
                .min()
                .unwrap_or(usize::MAX)
            } else {
                [
                    366,
                    list_len(values, "BYYEARDAY"),
                    list_len(values, "BYMONTHDAY").saturating_mul(12),
                    list_len(values, "BYWEEKNO").saturating_mul(9),
                    by_day_days(values.by_day.as_ref(), 53),
                ]
                .into_iter()
                .min()
                .unwrap_or(usize::MAX)
            }
        }
        _ => usize::MAX,
    }
}

fn check_cost(values: &RuleValues) -> PlanningResult<()> {
    let times = ["BYHOUR", "BYMINUTE", "BYSECOND"]
        .into_iter()
        .fold(1usize, |product, key| {
            product.saturating_mul(values.lists.get(key).map_or(1, Vec::len))
        });
    let candidates = period_days(values).saturating_mul(times);
    if candidates > MAX_PERIOD_CANDIDATES {
        return Err(invalid(
            "schedule.rrule",
            "one FREQ period exceeds the occurrence candidate limit",
        ));
    }
    let positions = values
        .lists
        .get("BYSETPOS")
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    if positions.len() > MAX_SET_POSITIONS {
        return Err(invalid("BYSETPOS", "contains too many values"));
    }
    if positions
        .iter()
        .any(|position| candidates == 1 || position.unsigned_abs() as usize > candidates)
    {
        return Err(invalid(
            "BYSETPOS",
            "contains a position outside one FREQ period",
        ));
    }
    Ok(())
}

fn add_start_defaults(
    parts: &mut BTreeMap<String, String>,
    values: &RuleValues,
    start: NaiveDateTime,
) {
    if matches!(values.frequency.as_str(), "MINUTELY" | "HOURLY") {
        return;
    }
    for (key, value) in [
        ("BYHOUR", start.hour()),
        ("BYMINUTE", start.minute()),
        ("BYSECOND", start.second()),
    ] {
        parts
            .entry(key.to_owned())
            .or_insert_with(|| value.to_string());
    }
    if values.by_day.is_none()
        && !values.lists.contains_key("BYMONTHDAY")
        && !values.lists.contains_key("BYYEARDAY")
        && !values.lists.contains_key("BYWEEKNO")
    {
        match values.frequency.as_str() {
            "WEEKLY" => {
                let weekday = WEEKDAYS[start.weekday().num_days_from_monday() as usize];
                parts
                    .entry("BYDAY".into())
                    .or_insert_with(|| weekday.into());
            }
            "MONTHLY" | "YEARLY" => {
                parts
                    .entry("BYMONTHDAY".into())
                    .or_insert_with(|| start.day().to_string());
            }
            _ => {}
        }
    }
    if values.frequency == "YEARLY" && !values.lists.contains_key("BYMONTH") {
        parts
            .entry("BYMONTH".into())
            .or_insert_with(|| start.month().to_string());
    }
}

pub(crate) fn canonical_rrule(value: &str) -> PlanningResult<()> {
    let (start_line, rule_line) = recurrence_lines(value)?;
    let (_zone, start) = parse_start(&start_line)?;
    let parts = rule_parts(&rule_line)?;
    let values = rule_values(&parts)?;
    let mut with_defaults = parts;
    add_start_defaults(&mut with_defaults, &values, start);
    let defaults = rule_values(&with_defaults)?;
    check_cost(&defaults)?;
    let normalized = format!("{start_line}\n{rule_line}");
    if normalized != value {
        return Err(invalid("schedule.rrule", "must be normalized"));
    }
    Ok(())
}

fn invalid(field: &'static str, message: impl Into<String>) -> PlanningError {
    PlanningError::invalid(field, message)
}
