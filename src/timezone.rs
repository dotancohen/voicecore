//! The timezone an action happened in.
//!
//! Every timestamp is stored as a Unix epoch second, which is an instant and
//! says nothing about the clock on the wall where the action happened. Next to
//! the user-visible ones we store two more values: the offset from UTC that
//! was in force on the device at that moment, in seconds east of UTC, and the
//! IANA name of its timezone when the platform supplied one.
//!
//! That is what lets a note recorded at 15:20 in Jerusalem still read 15:20
//! after the user flies to New York: the offset renders the wall clock, and
//! the name is kept so a later feature can say where it was recorded.
//!
//! The offset cannot be recovered afterwards from the instant alone, which is
//! why it has to be written at the same time as the timestamp.

use std::sync::RwLock;

use chrono::{DateTime, FixedOffset, Local, Utc};

/// The timezone of the device performing an action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalZone {
    /// Seconds east of UTC at that moment (Jerusalem in summer is 10800).
    pub offset_seconds: i32,
    /// IANA name such as "Asia/Jerusalem", when the platform knows it.
    pub name: Option<String>,
}

static LOCAL_ZONE: RwLock<Option<LocalZone>> = RwLock::new(None);

/// Tell the core which timezone this device is in.
///
/// Applications call this at start and whenever the device's timezone
/// changes. The platform knows this better than a library can: on Android the
/// zone lives in the framework, not in the environment a native library sees.
/// Until it is called, the offset is read from the operating system and no
/// name is recorded.
pub fn set_local_timezone(offset_seconds: i32, name: Option<String>) {
    if let Ok(mut zone) = LOCAL_ZONE.write() {
        *zone = Some(LocalZone { offset_seconds, name });
    }
}

/// Forget what the application set, for tests.
pub fn clear_local_timezone() {
    if let Ok(mut zone) = LOCAL_ZONE.write() {
        *zone = None;
    }
}

/// The timezone to stamp on an action performed right now.
pub fn local_zone() -> LocalZone {
    if let Some(zone) = LOCAL_ZONE.read().ok().and_then(|z| z.clone()) {
        return zone;
    }
    LocalZone {
        offset_seconds: Local::now().offset().local_minus_utc(),
        name: None,
    }
}

/// Offset to store beside a timestamp for an action happening here now.
pub fn stamp_offset() -> Option<i32> {
    Some(local_zone().offset_seconds)
}

/// Zone name to store beside a timestamp for an action happening here now.
pub fn stamp_zone() -> Option<String> {
    local_zone().name
}

/// Instant, offset and zone name for an action happening now.
pub fn stamp_now() -> (i64, i32, Option<String>) {
    let zone = local_zone();
    (Utc::now().timestamp(), zone.offset_seconds, zone.name)
}

/// Offset and zone name for an action happening now, for the many writes that
/// take the instant from SQLite's own clock.
pub fn zone_now() -> (i32, Option<String>) {
    let zone = local_zone();
    (zone.offset_seconds, zone.name)
}

/// Render an instant as the clock read where the action happened.
///
/// With no offset recorded (a row written before the timezone fields, or by a
/// device that never reported one) it falls back to this device's timezone,
/// which is what every reader did before.
pub fn format_at_offset(ts: i64, offset_seconds: Option<i32>) -> String {
    let offset = offset_seconds
        .and_then(FixedOffset::east_opt)
        .unwrap_or_else(|| *Local::now().offset());
    DateTime::from_timestamp(ts, 0)
        .map(|dt| dt.with_timezone(&offset).format("%Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_else(|| "Unknown".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 2026-09-08 12:20:00 UTC is 15:20 in Jerusalem and 08:20 in New York,
    /// and a note carrying the Jerusalem offset reads 15:20 in both places.
    #[test]
    fn an_instant_is_rendered_at_the_offset_it_was_written_in() {
        let instant = 1_788_870_000; // 2026-09-08 12:20:00 UTC
        assert_eq!(format_at_offset(instant, Some(10800)), "2026-09-08 15:20:00");
        assert_eq!(format_at_offset(instant, Some(-14400)), "2026-09-08 08:20:00");
    }

    #[test]
    fn set_and_read_back_a_zone() {
        set_local_timezone(10800, Some("Asia/Jerusalem".to_string()));
        let zone = local_zone();
        assert_eq!(zone.offset_seconds, 10800);
        assert_eq!(zone.name.as_deref(), Some("Asia/Jerusalem"));

        // New York in winter
        set_local_timezone(-18000, Some("America/New_York".to_string()));
        assert_eq!(local_zone().offset_seconds, -18000);

        clear_local_timezone();
        // Without the application saying anything we still get an offset
        assert!(local_zone().name.is_none());
    }
}
