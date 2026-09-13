//! Corner cases of the timezone a timestamp was written in.
//!
//! Every timestamp is an instant; beside it the core keeps the offset from UTC
//! that was in force where the action happened, so a note written at 15:20 in
//! Jerusalem still reads 15:20 from New York. These tests cover the awkward
//! parts: offsets that are not whole hours, UTC itself (which must not be
//! mistaken for "nothing recorded"), the two hours a year that either happen
//! twice or not at all, travel between devices, and what a peer that has never
//! heard of any of this can and cannot overwrite.

use std::sync::Mutex;

use serde_json::json;

use crate::database::Database;
use crate::models::SyncChange;
use crate::sync_apply::apply_changes;
use crate::timezone::{clear_local_timezone, format_at_offset, set_local_timezone};
use crate::versions::{ENTITY_NOTE, FIELD_CONTENT};

/// The device's timezone is process-wide, so these tests take turns.
static ONE_AT_A_TIME: Mutex<()> = Mutex::new(());

fn lock() -> std::sync::MutexGuard<'static, ()> {
    ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner())
}

/// Offsets used throughout, in seconds east of UTC.
const JERUSALEM_SUMMER: i32 = 3 * 3600; // UTC+3, from spring to autumn
const JERUSALEM_WINTER: i32 = 2 * 3600; // UTC+2
const NEW_YORK_SUMMER: i32 = -4 * 3600; // UTC-4
const NEW_YORK_WINTER: i32 = -5 * 3600; // UTC-5
const INDIA: i32 = 5 * 3600 + 1800; // UTC+5:30
const NEPAL: i32 = 5 * 3600 + 2700; // UTC+5:45
const CHATHAM: i32 = 12 * 3600 + 2700; // UTC+12:45
const KIRITIMATI: i32 = 14 * 3600; // the earliest clock on earth
const BAKER_ISLAND: i32 = -12 * 3600; // the latest
const JERUSALEM_MEAN_TIME: i32 = 8454; // +02:20:54, before zones were tidy

/// The offset and zone stored beside one timestamp.
fn zone_of(db: &Database, table: &str, id_hex: &str, stamp: &str) -> (Option<i64>, Option<String>) {
    let id = uuid::Uuid::parse_str(id_hex).unwrap().as_bytes().to_vec();
    let sql = format!("SELECT {stamp}_offset, {stamp}_zone FROM {table} WHERE id = ?");
    db.connection()
        .query_row(&sql, rusqlite::params![id], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
}

/// One timestamp of a note.
fn stamp_of(db: &Database, id_hex: &str, stamp: &str) -> Option<i64> {
    let id = uuid::Uuid::parse_str(id_hex).unwrap().as_bytes().to_vec();
    let sql = format!("SELECT {stamp} FROM notes WHERE id = ?");
    db.connection()
        .query_row(&sql, rusqlite::params![id], |row| row.get(0))
        .unwrap()
}

/// How a note's timestamp reads, at the clock it was written on.
fn shown(db: &Database, id_hex: &str, stamp: &str) -> String {
    let at = stamp_of(db, id_hex, stamp).expect("timestamp");
    let (offset, _) = zone_of(db, "notes", id_hex, stamp);
    format_at_offset(at, offset.map(|o| o as i32))
}

fn feed(db: &Database) -> Vec<SyncChange> {
    db.get_changes_after_seq_as_sync_changes(0, None, 10_000).unwrap().0
}

fn deliver(to: &Database, changes: &[SyncChange]) {
    apply_changes(to, changes, "01a07848cc607813973baa00457b79db", Some("Peer"), 1).unwrap();
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

/// 2026-09-08 12:20:00 UTC, a summer afternoon in Jerusalem.
const NOON: i64 = 1_788_870_000;

#[test]
fn utc_is_a_real_answer_and_not_a_missing_one() {
    // An offset of zero is a device in London in winter, not a device that
    // never said where it was. Anything that treats 0 as absent would show
    // those notes in the reader's timezone instead.
    assert_eq!(format_at_offset(NOON, Some(0)), "2026-09-08 12:20:00");
    assert_ne!(format_at_offset(NOON, Some(0)), format_at_offset(NOON, Some(JERUSALEM_SUMMER)));
}

#[test]
fn offsets_that_are_not_whole_hours() {
    assert_eq!(format_at_offset(NOON, Some(INDIA)), "2026-09-08 17:50:00");
    assert_eq!(format_at_offset(NOON, Some(NEPAL)), "2026-09-08 18:05:00");
    assert_eq!(format_at_offset(NOON, Some(CHATHAM)), "2026-09-09 01:05:00");
    assert_eq!(format_at_offset(NOON, Some(-3 * 3600 - 1800)), "2026-09-08 08:50:00"); // Newfoundland
}

#[test]
fn an_offset_measured_in_seconds() {
    // Jerusalem ran on its own mean solar time until 1880: +02:20:54
    assert_eq!(format_at_offset(NOON, Some(JERUSALEM_MEAN_TIME)), "2026-09-08 14:40:54");
}

#[test]
fn the_two_ends_of_the_map_are_a_day_apart() {
    // The same instant, on the earliest and latest clocks in use
    assert_eq!(format_at_offset(NOON, Some(KIRITIMATI)), "2026-09-09 02:20:00");
    assert_eq!(format_at_offset(NOON, Some(BAKER_ISLAND)), "2026-09-08 00:20:00");
}

#[test]
fn instants_before_the_epoch_still_render() {
    // An old recording imported with its own date: 1969-07-20 20:17:40 UTC
    let moon = -14_182_940;
    assert_eq!(format_at_offset(moon, Some(0)), "1969-07-20 20:17:40");
    assert_eq!(format_at_offset(moon, Some(JERUSALEM_WINTER)), "1969-07-20 22:17:40");
}

// ---------------------------------------------------------------------------
// The hours that happen twice, or never
// ---------------------------------------------------------------------------

#[test]
fn the_hour_that_happens_twice() {
    // Jerusalem, the night the clocks go back: 22:00 UTC is 01:00 at +03:00,
    // and an hour later 22:00 UTC + 1h is 01:00 again at +02:00. Two notes an
    // hour apart show the same clock, and only the instant tells them apart.
    let first = 1_792_879_200; // 2026-10-24 22:00:00 UTC
    let second = first + 3600;
    assert_eq!(format_at_offset(first, Some(JERUSALEM_SUMMER)), "2026-10-25 01:00:00");
    assert_eq!(format_at_offset(second, Some(JERUSALEM_WINTER)), "2026-10-25 01:00:00");
    assert!(second > first, "the instants stay in order even though the clocks agree");
}

#[test]
fn the_hour_that_never_happens() {
    // The night the clocks go forward, 02:00 to 03:00 does not exist in
    // Jerusalem. A stored instant is never inside it, because each side of the
    // jump carries its own offset.
    let before = 1_774_645_200; // 2026-03-27 21:00:00 UTC, so 23:00 at +02:00
    let after = before + 4 * 3600; // four hours later
    assert_eq!(format_at_offset(before, Some(JERUSALEM_WINTER)), "2026-03-27 23:00:00");
    let rendered = format_at_offset(after, Some(JERUSALEM_SUMMER));
    assert_eq!(rendered, "2026-03-28 04:00:00");
    let hour: u32 = rendered[11..13].parse().unwrap();
    assert!(hour != 2, "no rendered time may land in the hour that was skipped");
}

// ---------------------------------------------------------------------------
// What the database keeps
// ---------------------------------------------------------------------------

#[test]
fn a_device_in_utc_records_zero_rather_than_nothing() {
    let _guard = lock();
    set_local_timezone(0, Some("Etc/UTC".to_string()));
    let db = Database::new_in_memory().unwrap();
    let note = db.create_note("פתק מלונדון").unwrap();

    let (offset, zone) = zone_of(&db, "notes", &note, "created_at");
    assert_eq!(offset, Some(0), "zero is a recorded offset, not a missing one");
    assert_eq!(zone.as_deref(), Some("Etc/UTC"));
    clear_local_timezone();
}

#[test]
fn a_device_may_report_an_offset_without_a_name() {
    let _guard = lock();
    set_local_timezone(NEPAL, None);
    let db = Database::new_in_memory().unwrap();
    let note = db.create_note("פתק מקטמנדו").unwrap();

    let (offset, zone) = zone_of(&db, "notes", &note, "created_at");
    assert_eq!(offset, Some(NEPAL as i64));
    assert_eq!(zone, None, "the name is optional; the offset is what draws the clock");
    // and it still renders at the right clock
    assert!(shown(&db, &note, "created_at").len() == 19);
    clear_local_timezone();
}

#[test]
fn travelling_does_not_disturb_what_was_already_written() {
    let _guard = lock();
    set_local_timezone(JERUSALEM_SUMMER, Some("Asia/Jerusalem".to_string()));
    let db = Database::new_in_memory().unwrap();
    let in_jerusalem = db.create_note("פגישה בירושלים").unwrap();
    let before = shown(&db, &in_jerusalem, "created_at");

    // The user flies; the application reports the new zone
    set_local_timezone(NEW_YORK_SUMMER, Some("America/New_York".to_string()));
    let in_new_york = db.create_note("Meeting in New York").unwrap();

    assert_eq!(shown(&db, &in_jerusalem, "created_at"), before, "the old note is untouched");
    assert_eq!(zone_of(&db, "notes", &in_jerusalem, "created_at").0, Some(JERUSALEM_SUMMER as i64));
    assert_eq!(zone_of(&db, "notes", &in_new_york, "created_at").0, Some(NEW_YORK_SUMMER as i64));
    clear_local_timezone();
}

#[test]
fn an_edit_can_read_earlier_than_the_note_it_changes() {
    // The case the user asked for: write a note in Jerusalem, fly to New York,
    // edit it there. The edit happened later, but its clock reads earlier.
    let _guard = lock();
    set_local_timezone(JERUSALEM_SUMMER, Some("Asia/Jerusalem".to_string()));
    let db = Database::new_in_memory().unwrap();
    let note = db.create_note("רשימת קניות").unwrap();

    set_local_timezone(NEW_YORK_SUMMER, Some("America/New_York".to_string()));
    db.update_note(&note, "רשימת קניות מעודכנת").unwrap();

    let created_at = stamp_of(&db, &note, "created_at").unwrap();
    let modified_at = stamp_of(&db, &note, "modified_at").unwrap();
    assert!(modified_at >= created_at, "the edit happened later");

    let created_shown = shown(&db, &note, "created_at");
    let modified_shown = shown(&db, &note, "modified_at");
    assert!(
        modified_shown < created_shown,
        "seven hours west, so the edit reads earlier: created {created_shown}, modified {modified_shown}"
    );
    assert_eq!(zone_of(&db, "notes", &note, "created_at").0, Some(JERUSALEM_SUMMER as i64));
    assert_eq!(zone_of(&db, "notes", &note, "modified_at").0, Some(NEW_YORK_SUMMER as i64));
    clear_local_timezone();
}

#[test]
fn an_edit_after_the_clocks_change_records_the_new_offset() {
    let _guard = lock();
    set_local_timezone(JERUSALEM_WINTER, Some("Asia/Jerusalem".to_string()));
    let db = Database::new_in_memory().unwrap();
    let note = db.create_note("תזכורת מהחורף").unwrap();

    // Spring arrives while the note sits there
    set_local_timezone(JERUSALEM_SUMMER, Some("Asia/Jerusalem".to_string()));
    db.update_note(&note, "תזכורת מעודכנת בקיץ").unwrap();

    assert_eq!(zone_of(&db, "notes", &note, "created_at").0, Some(JERUSALEM_WINTER as i64));
    assert_eq!(zone_of(&db, "notes", &note, "modified_at").0, Some(JERUSALEM_SUMMER as i64));
    // Same zone name on both, which is how a later feature can tell that the
    // difference is daylight saving rather than travel
    assert_eq!(zone_of(&db, "notes", &note, "created_at").1.as_deref(), Some("Asia/Jerusalem"));
    assert_eq!(zone_of(&db, "notes", &note, "modified_at").1.as_deref(), Some("Asia/Jerusalem"));
    clear_local_timezone();
}

#[test]
fn a_deletion_records_where_it_happened_and_a_restore_forgets_it() {
    let _guard = lock();
    set_local_timezone(JERUSALEM_SUMMER, Some("Asia/Jerusalem".to_string()));
    let db = Database::new_in_memory().unwrap();
    let note = db.create_note("פתק למחיקה").unwrap();

    set_local_timezone(NEW_YORK_WINTER, Some("America/New_York".to_string()));
    db.delete_note(&note).unwrap();
    assert_eq!(zone_of(&db, "notes", &note, "deleted_at").0, Some(NEW_YORK_WINTER as i64));

    // Restoring clears the deletion, and its timezone goes with it
    db.set_field(ENTITY_NOTE, &note, "deleted", "0", None).unwrap();
    assert_eq!(stamp_of(&db, &note, "deleted_at"), None);
    assert_eq!(zone_of(&db, "notes", &note, "deleted_at"), (None, None));
    clear_local_timezone();
}

#[test]
fn every_time_an_import_records_is_stamped() {
    let _guard = lock();
    set_local_timezone(INDIA, Some("Asia/Kolkata".to_string()));
    let db = Database::new_in_memory().unwrap();
    let (note, audio) = db
        .import_audio_file("הקלטה 2026-09-08 14-53-14.ogg", Some(NOON), Some(42), None)
        .unwrap();

    assert_eq!(zone_of(&db, "notes", &note, "created_at").0, Some(INDIA as i64));
    assert_eq!(zone_of(&db, "audio_files", &audio, "imported_at").0, Some(INDIA as i64));
    assert_eq!(
        zone_of(&db, "audio_files", &audio, "file_created_at").0,
        Some(INDIA as i64),
        "the best this device can say about the file's own date"
    );
    clear_local_timezone();
}

#[test]
fn history_keeps_every_version_in_its_own_zone() {
    let _guard = lock();
    let db = Database::new_in_memory().unwrap();
    set_local_timezone(JERUSALEM_SUMMER, Some("Asia/Jerusalem".to_string()));
    let note = db.create_note("גרסה ראשונה").unwrap();
    set_local_timezone(NEW_YORK_SUMMER, Some("America/New_York".to_string()));
    db.update_note(&note, "גרסה שנייה").unwrap();
    set_local_timezone(CHATHAM, Some("Pacific/Chatham".to_string()));
    db.update_note(&note, "גרסה שלישית").unwrap();

    let history = db.get_field_history(ENTITY_NOTE, &note, FIELD_CONTENT).unwrap();
    let offsets: Vec<Option<i32>> = history.iter().map(|v| v.created_at_offset).collect();
    assert!(offsets.contains(&Some(JERUSALEM_SUMMER)), "{offsets:?}");
    assert!(offsets.contains(&Some(NEW_YORK_SUMMER)), "{offsets:?}");
    assert!(offsets.contains(&Some(CHATHAM)), "{offsets:?}");
    clear_local_timezone();
}

#[test]
fn the_list_cache_shows_the_note_s_own_clock() {
    let _guard = lock();
    set_local_timezone(JERUSALEM_SUMMER, Some("Asia/Jerusalem".to_string()));
    let db = Database::new_in_memory().unwrap();
    let note = db.create_note("פתק לרשימה").unwrap();
    let expected = shown(&db, &note, "created_at");

    // The reader has since travelled; the cache must not follow it
    set_local_timezone(BAKER_ISLAND, None);
    db.rebuild_note_list_cache(&note).unwrap();
    let row = db.get_note(&note).unwrap().unwrap();
    let cache: serde_json::Value = serde_json::from_str(row.list_display_cache.as_deref().unwrap()).unwrap();
    assert_eq!(cache["date"].as_str().unwrap(), expected);
    clear_local_timezone();
}

// ---------------------------------------------------------------------------
// Between devices
// ---------------------------------------------------------------------------

#[test]
fn a_note_carries_its_clock_to_the_other_side_of_the_world() {
    let _guard = lock();
    set_local_timezone(JERUSALEM_SUMMER, Some("Asia/Jerusalem".to_string()));
    let jerusalem = Database::new_in_memory().unwrap();
    let note = jerusalem.create_note("פגישה בירושלים").unwrap();
    let expected = shown(&jerusalem, &note, "created_at");
    let changes = feed(&jerusalem);

    set_local_timezone(NEW_YORK_SUMMER, Some("America/New_York".to_string()));
    let new_york = Database::new_in_memory().unwrap();
    deliver(&new_york, &changes);

    assert_eq!(shown(&new_york, &note, "created_at"), expected);
    assert_eq!(
        zone_of(&new_york, "notes", &note, "created_at").1.as_deref(),
        Some("Asia/Jerusalem")
    );
    clear_local_timezone();
}

#[test]
fn a_relay_in_a_third_zone_changes_nothing() {
    let _guard = lock();
    set_local_timezone(JERUSALEM_SUMMER, Some("Asia/Jerusalem".to_string()));
    let phone = Database::new_in_memory().unwrap();
    let note = phone.create_note("פתק שעובר דרך שרת").unwrap();
    let expected = shown(&phone, &note, "created_at");

    // A server that keeps its own clock in UTC passes it on
    set_local_timezone(0, Some("Etc/UTC".to_string()));
    let server = Database::new_in_memory().unwrap();
    deliver(&server, &feed(&phone));
    assert_eq!(shown(&server, &note, "created_at"), expected);

    set_local_timezone(NEW_YORK_WINTER, Some("America/New_York".to_string()));
    let desktop = Database::new_in_memory().unwrap();
    deliver(&desktop, &feed(&server));
    assert_eq!(shown(&desktop, &note, "created_at"), expected, "the relay did not repaint it");
    assert_eq!(
        zone_of(&desktop, "notes", &note, "created_at").1.as_deref(),
        Some("Asia/Jerusalem")
    );
    clear_local_timezone();
}

#[test]
fn a_peer_that_has_never_heard_of_zones_erases_nothing() {
    let _guard = lock();
    set_local_timezone(JERUSALEM_SUMMER, Some("Asia/Jerusalem".to_string()));
    let db = Database::new_in_memory().unwrap();
    let note = db.create_note("פתק עם אזור זמן").unwrap();
    let before = zone_of(&db, "notes", &note, "created_at");
    assert_eq!(before.0, Some(JERUSALEM_SUMMER as i64));

    // An older device sends the same row with no timezone fields at all
    let mut changes = feed(&db);
    for change in changes.iter_mut() {
        if let Some(object) = change.data.as_object_mut() {
            object.retain(|key, _| !key.ends_with("_offset") && !key.ends_with("_zone"));
        }
    }
    deliver(&db, &changes);

    assert_eq!(zone_of(&db, "notes", &note, "created_at"), before, "nothing was erased");
    clear_local_timezone();
}

#[test]
fn a_zone_is_only_kept_for_the_timestamp_it_arrived_with() {
    let _guard = lock();
    set_local_timezone(JERUSALEM_SUMMER, Some("Asia/Jerusalem".to_string()));
    let db = Database::new_in_memory().unwrap();
    let note = db.create_note("פתק שנערך כאן").unwrap();
    db.update_note(&note, "נערך כאן, ולא שם").unwrap();
    let ours = zone_of(&db, "notes", &note, "modified_at");
    let our_modified = stamp_of(&db, &note, "modified_at").unwrap();

    // A peer sends an older edit of the same note, from Nepal. The newer
    // modified_at wins, so the Nepali offset must not be recorded against it.
    let older = our_modified - 3600;
    let change = SyncChange {
        entity_type: "note".to_string(),
        entity_id: note.clone(),
        operation: "update".to_string(),
        data: json!({
            "id": note,
            "created_at": stamp_of(&db, &note, "created_at").unwrap(),
            "content": "עריכה ישנה",
            "modified_at": older,
            "modified_at_offset": NEPAL,
            "modified_at_zone": "Asia/Kathmandu",
            "deleted_at": serde_json::Value::Null,
        }),
        timestamp: older,
        device_id: "01a07848cc607813973baa00457b79db".to_string(),
        device_name: Some("Kathmandu".to_string()),
    };
    deliver(&db, &[change]);

    assert_eq!(stamp_of(&db, &note, "modified_at"), Some(our_modified), "our edit stands");
    assert_eq!(zone_of(&db, "notes", &note, "modified_at"), ours, "and so does its zone");
    clear_local_timezone();
}

#[test]
fn delivering_the_same_change_twice_changes_nothing() {
    let _guard = lock();
    set_local_timezone(CHATHAM, Some("Pacific/Chatham".to_string()));
    let source = Database::new_in_memory().unwrap();
    let note = source.create_note("פתק מצ'טהם").unwrap();
    let changes = feed(&source);

    set_local_timezone(BAKER_ISLAND, Some("Pacific/Midway".to_string()));
    let target = Database::new_in_memory().unwrap();
    deliver(&target, &changes);
    let after_first = zone_of(&target, "notes", &note, "created_at");
    deliver(&target, &changes);
    let after_second = zone_of(&target, "notes", &note, "created_at");

    assert_eq!(after_first.0, Some(CHATHAM as i64));
    assert_eq!(after_first, after_second, "an echo leaves the zone alone");
    clear_local_timezone();
}

#[test]
fn notes_stay_in_order_even_when_their_clocks_go_backwards() {
    // Ten hours in the air, three hours on the clock: the newer note shows the
    // earlier time, and the order is still by the instant.
    let _guard = lock();
    set_local_timezone(JERUSALEM_SUMMER, Some("Asia/Jerusalem".to_string()));
    let db = Database::new_in_memory().unwrap();
    let first = db.create_note("לפני הטיסה").unwrap();

    set_local_timezone(NEW_YORK_SUMMER, Some("America/New_York".to_string()));
    let second = db.create_note("After the flight").unwrap();

    let first_at = stamp_of(&db, &first, "created_at").unwrap();
    let second_at = stamp_of(&db, &second, "created_at").unwrap();
    assert!(second_at >= first_at, "the second note is later");
    assert!(
        shown(&db, &second, "created_at") < shown(&db, &first, "created_at"),
        "and reads earlier"
    );

    let notes = db.get_all_notes().unwrap();
    let order: Vec<&str> = notes.iter().map(|n| n.id.as_str()).collect();
    assert_eq!(order.first(), Some(&second.as_str()), "newest by instant is first");
    clear_local_timezone();
}


// ---------------------------------------------------------------------------
// The first two tests written for this feature, kept here so that every test
// touching the process-wide timezone shares one lock.
// ---------------------------------------------------------------------------

/// A note written in Jerusalem keeps the clock its author was reading, and
/// that clock survives the journey to another device.
#[test]
fn the_zone_of_a_note_is_stored_and_synced() {
    let _guard = lock();
    set_local_timezone(JERUSALEM_SUMMER, Some("Asia/Jerusalem".to_string()));
    let db = Database::new_in_memory().unwrap();
    let note_id = db.create_note("פגישה בירושלים").unwrap();

    let (offset, zone) = zone_of(&db, "notes", &note_id, "created_at");
    assert_eq!(offset, Some(JERUSALEM_SUMMER as i64));
    assert_eq!(zone.as_deref(), Some("Asia/Jerusalem"));

    // The version behind the note carries it too
    let history = db.get_field_history(ENTITY_NOTE, &note_id, FIELD_CONTENT).unwrap();
    assert_eq!(history[0].created_at_offset, Some(JERUSALEM_SUMMER));
    assert_eq!(history[0].created_at_zone.as_deref(), Some("Asia/Jerusalem"));

    // Now the same note reaches a device that is in New York
    let changes = feed(&db);
    set_local_timezone(NEW_YORK_SUMMER, Some("America/New_York".to_string()));
    let other = Database::new_in_memory().unwrap();
    deliver(&other, &changes);

    let (offset, zone) = zone_of(&other, "notes", &note_id, "created_at");
    assert_eq!(offset, Some(JERUSALEM_SUMMER as i64), "written in Jerusalem, wherever it is read");
    assert_eq!(zone.as_deref(), Some("Asia/Jerusalem"));
    let there = other.get_field_history(ENTITY_NOTE, &note_id, FIELD_CONTENT).unwrap();
    assert_eq!(there[0].created_at_offset, Some(JERUSALEM_SUMMER));

    // A note written on the second device gets its own zone
    let local_note = other.create_note("Meeting in New York").unwrap();
    let (offset, zone) = zone_of(&other, "notes", &local_note, "created_at");
    assert_eq!(offset, Some(NEW_YORK_SUMMER as i64));
    assert_eq!(zone.as_deref(), Some("America/New_York"));
    clear_local_timezone();
}

/// An edit made after travelling records where the edit happened, while the
/// note keeps where it was created.
#[test]
fn editing_elsewhere_records_the_other_zone() {
    let _guard = lock();
    set_local_timezone(JERUSALEM_SUMMER, Some("Asia/Jerusalem".to_string()));
    let db = Database::new_in_memory().unwrap();
    let note_id = db.create_note("רשימת קניות").unwrap();

    set_local_timezone(NEW_YORK_SUMMER, Some("America/New_York".to_string()));
    db.update_note(&note_id, "רשימת קניות מעודכנת").unwrap();

    let (created_offset, created_zone) = zone_of(&db, "notes", &note_id, "created_at");
    assert_eq!(created_offset, Some(JERUSALEM_SUMMER as i64), "created in Jerusalem");
    assert_eq!(created_zone.as_deref(), Some("Asia/Jerusalem"));

    let (modified_offset, modified_zone) = zone_of(&db, "notes", &note_id, "modified_at");
    assert_eq!(modified_offset, Some(NEW_YORK_SUMMER as i64), "edited in New York");
    assert_eq!(modified_zone.as_deref(), Some("America/New_York"));
    clear_local_timezone();
}

// ---------------------------------------------------------------------------
// Two devices editing at once, in different zones
// ---------------------------------------------------------------------------

#[test]
fn a_conflict_between_two_zones_keeps_both_sides_clocks() {
    let _guard = lock();
    // The note starts in Jerusalem and reaches a second device
    set_local_timezone(JERUSALEM_SUMMER, Some("Asia/Jerusalem".to_string()));
    let jerusalem = Database::new_in_memory().unwrap();
    let note = jerusalem.create_note("סדר יום").unwrap();
    let start = feed(&jerusalem);

    set_local_timezone(NEW_YORK_SUMMER, Some("America/New_York".to_string()));
    let new_york = Database::new_in_memory().unwrap();
    deliver(&new_york, &start);

    // Both edit it before either hears from the other
    new_york.update_note(&note, "Agenda, New York version").unwrap();
    let from_new_york = feed(&new_york);
    set_local_timezone(JERUSALEM_SUMMER, Some("Asia/Jerusalem".to_string()));
    jerusalem.update_note(&note, "סדר יום, גרסת ירושלים").unwrap();

    // Jerusalem hears about the other edit and merges
    deliver(&jerusalem, &from_new_york);

    let history = jerusalem.get_field_history(ENTITY_NOTE, &note, FIELD_CONTENT).unwrap();
    let authored: Vec<Option<i32>> = history
        .iter()
        .filter(|v| v.device_id.is_some())
        .map(|v| v.created_at_offset)
        .collect();
    assert!(
        authored.contains(&Some(JERUSALEM_SUMMER)) && authored.contains(&Some(NEW_YORK_SUMMER)),
        "each side keeps the clock it was written on: {authored:?}"
    );

    // The merge itself was computed here, so it carries this device's clock
    let merged: Vec<Option<i32>> = history
        .iter()
        .filter(|v| v.device_id.is_none() && v.merge_parent_id.is_some())
        .map(|v| v.created_at_offset)
        .collect();
    if !merged.is_empty() {
        assert!(
            merged.iter().all(|o| *o == Some(JERUSALEM_SUMMER)),
            "a merge is made where the merging happens: {merged:?}"
        );
    }

    // And the note's own modified_at follows whichever version is the head
    let (offset, _) = zone_of(&jerusalem, "notes", &note, "modified_at");
    assert!(offset.is_some(), "the head's clock is on the row");
    clear_local_timezone();
}

#[test]
fn a_transcription_keeps_the_clock_of_the_device_that_made_it() {
    let _guard = lock();
    set_local_timezone(NEPAL, Some("Asia/Kathmandu".to_string()));
    let phone = Database::new_in_memory().unwrap();
    let (_note, audio) = phone.import_audio_file("הקלטה.ogg", Some(NOON), Some(9), None).unwrap();
    let transcription = phone
        .create_transcription(&audio, "זהו תמלול", None, "local_whisper", None, None, None)
        .unwrap();
    let (offset, zone) = zone_of(&phone, "transcriptions", &transcription, "created_at");
    assert_eq!(offset, Some(NEPAL as i64));
    assert_eq!(zone.as_deref(), Some("Asia/Kathmandu"));

    // It reads the same on a desktop on the other side of the world
    set_local_timezone(NEW_YORK_WINTER, Some("America/New_York".to_string()));
    let desktop = Database::new_in_memory().unwrap();
    deliver(&desktop, &feed(&phone));
    assert_eq!(
        zone_of(&desktop, "transcriptions", &transcription, "created_at").0,
        Some(NEPAL as i64)
    );
    clear_local_timezone();
}

// ---------------------------------------------------------------------------
// Rows that were written before any of this existed
// ---------------------------------------------------------------------------

#[test]
fn an_old_row_is_never_given_a_zone_it_did_not_have() {
    let _guard = lock();
    set_local_timezone(JERUSALEM_SUMMER, Some("Asia/Jerusalem".to_string()));
    let db = Database::new_in_memory().unwrap();
    let note = db.create_note("פתק ותיק").unwrap();

    // Pretend this note predates the timezone columns
    let id = uuid::Uuid::parse_str(&note).unwrap().as_bytes().to_vec();
    db.connection()
        .execute(
            "UPDATE notes SET created_at_offset = NULL, created_at_zone = NULL WHERE id = ?",
            rusqlite::params![id],
        )
        .unwrap();

    // Editing it today must not invent a clock for the day it was written
    set_local_timezone(NEW_YORK_SUMMER, Some("America/New_York".to_string()));
    db.update_note(&note, "פתק ותיק, נערך היום").unwrap();

    assert_eq!(
        zone_of(&db, "notes", &note, "created_at"),
        (None, None),
        "we do not know where it was written, and must not guess"
    );
    assert_eq!(
        zone_of(&db, "notes", &note, "modified_at").0,
        Some(NEW_YORK_SUMMER as i64),
        "but we do know where it was edited"
    );
    clear_local_timezone();
}

#[test]
fn a_row_without_a_zone_is_shown_on_the_reader_s_clock() {
    let _guard = lock();
    let db = Database::new_in_memory().unwrap();
    set_local_timezone(JERUSALEM_SUMMER, Some("Asia/Jerusalem".to_string()));
    let note = db.create_note("פתק בלי אזור זמן").unwrap();
    let id = uuid::Uuid::parse_str(&note).unwrap().as_bytes().to_vec();
    db.connection()
        .execute(
            "UPDATE notes SET created_at_offset = NULL, created_at_zone = NULL WHERE id = ?",
            rusqlite::params![id],
        )
        .unwrap();

    let at = stamp_of(&db, &note, "created_at").unwrap();
    // What the reader sees is its own timezone, which is what every reader did
    // before any of this was recorded
    let fallback = format_at_offset(at, None);
    let local = chrono::DateTime::from_timestamp(at, 0)
        .unwrap()
        .with_timezone(&chrono::Local)
        .format("%Y-%m-%d %H:%M:%S")
        .to_string();
    assert_eq!(fallback, local);
    clear_local_timezone();
}

#[test]
fn the_zone_of_a_note_survives_a_round_trip_through_a_peer_and_back() {
    // A note goes out, comes back, and is not repainted by whoever returned it
    let _guard = lock();
    set_local_timezone(CHATHAM, Some("Pacific/Chatham".to_string()));
    let home = Database::new_in_memory().unwrap();
    let note = home.create_note("פתק שחוזר הביתה").unwrap();
    let before = zone_of(&home, "notes", &note, "created_at");

    set_local_timezone(KIRITIMATI, Some("Pacific/Kiritimati".to_string()));
    let away = Database::new_in_memory().unwrap();
    deliver(&away, &feed(&home));
    deliver(&home, &feed(&away));

    assert_eq!(zone_of(&home, "notes", &note, "created_at"), before);
    clear_local_timezone();
}

// ---------------------------------------------------------------------------
// Values that should not be there at all
// ---------------------------------------------------------------------------

#[test]
fn an_impossible_offset_falls_back_instead_of_lying() {
    // No zone on earth is a hundred hours from UTC. A corrupted row must not
    // panic, and must not draw a clock nobody has ever read.
    let absurd = 100 * 3600;
    let shown = format_at_offset(NOON, Some(absurd));
    let local = chrono::DateTime::from_timestamp(NOON, 0)
        .unwrap()
        .with_timezone(&chrono::Local)
        .format("%Y-%m-%d %H:%M:%S")
        .to_string();
    assert_eq!(shown, local, "an offset out of range is treated as unknown");
    assert_eq!(format_at_offset(NOON, Some(-absurd)), local);
}

#[test]
fn an_offset_too_large_for_the_column_is_read_as_unknown() {
    let _guard = lock();
    set_local_timezone(JERUSALEM_SUMMER, Some("Asia/Jerusalem".to_string()));
    let db = Database::new_in_memory().unwrap();
    let note = db.create_note("פתק עם נתון פגום").unwrap();

    // Something has written a number that cannot be an offset
    let id = uuid::Uuid::parse_str(&note).unwrap().as_bytes().to_vec();
    db.connection()
        .execute(
            "UPDATE notes SET created_at_offset = ? WHERE id = ?",
            rusqlite::params![5_000_000_000i64, id],
        )
        .unwrap();

    // Reading it must not wrap it into a plausible-looking wrong offset
    let row = db.get_note(&note).unwrap().unwrap();
    assert_eq!(row.created_at_offset, None, "a value that cannot fit is unknown, not wrapped");
    clear_local_timezone();
}

#[test]
fn without_a_report_the_device_still_records_its_own_offset() {
    let _guard = lock();
    clear_local_timezone();
    let db = Database::new_in_memory().unwrap();
    let note = db.create_note("פתק בלי דיווח אזור זמן").unwrap();

    let (offset, zone) = zone_of(&db, "notes", &note, "created_at");
    let expected = chrono::Local::now().offset().local_minus_utc() as i64;
    assert_eq!(offset, Some(expected), "the operating system is asked when nobody says otherwise");
    assert_eq!(zone, None, "but only the platform can name the zone");
}

#[test]
fn daylight_saving_that_moves_by_half_an_hour() {
    // Lord Howe Island shifts by thirty minutes, not an hour
    let winter = 10 * 3600 + 1800; // +10:30
    let summer = 11 * 3600; // +11:00
    assert_eq!(format_at_offset(NOON, Some(winter)), "2026-09-08 22:50:00");
    assert_eq!(format_at_offset(NOON, Some(summer)), "2026-09-08 23:20:00");
}
