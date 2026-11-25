mod common;

use common::*;
use dsh_planning::{
    AfterScheduleRecord, AtScheduleRecord, CronScheduleRecord, DailyScheduleRecord,
    DeliveryReceipt, EveryScheduleRecord, PlanningError, PlanningEvent, PlanningLimits,
    PlanningPayload, PlanningState, RRuleScheduleRecord, ScheduleChange, ScheduleCreateSpec,
    ScheduleEntry, ScheduleKind, ScheduleRecord, ScheduleStatus, ScheduleTarget,
    WeeklyScheduleRecord,
};
use serde_json::{json, Value};

const INSTANT: &str = "2026-11-10T09:00:00.000Z";

fn all_records() -> Vec<ScheduleRecord> {
    vec![
        ScheduleRecord::After(AfterScheduleRecord {
            id: dsh_planning::ScheduleId::new("after").unwrap(),
            kind: ScheduleKind::After,
            title: "After".into(),
            prompt: "After prompt.".into(),
            after_seconds: 60,
            scheduled_at: INSTANT.into(),
        }),
        ScheduleRecord::At(AtScheduleRecord {
            id: dsh_planning::ScheduleId::new("at").unwrap(),
            kind: ScheduleKind::At,
            title: "At".into(),
            prompt: "At prompt.".into(),
            scheduled_at: INSTANT.into(),
        }),
        ScheduleRecord::Every(EveryScheduleRecord {
            id: dsh_planning::ScheduleId::new("every").unwrap(),
            kind: ScheduleKind::Every,
            title: "Every".into(),
            prompt: "Every prompt.".into(),
            every_seconds: 60,
            scheduled_at: INSTANT.into(),
        }),
        ScheduleRecord::Daily(DailyScheduleRecord {
            id: dsh_planning::ScheduleId::new("daily").unwrap(),
            kind: ScheduleKind::Daily,
            title: "Daily".into(),
            prompt: "Daily prompt.".into(),
            time: "09:00:00.000".into(),
            time_zone: "America/New_York".into(),
            scheduled_at: INSTANT.into(),
        }),
        ScheduleRecord::Weekly(WeeklyScheduleRecord {
            id: dsh_planning::ScheduleId::new("weekly").unwrap(),
            kind: ScheduleKind::Weekly,
            title: "Weekly".into(),
            prompt: "Weekly prompt.".into(),
            time: "09:00:00.000".into(),
            time_zone: "Europe/Paris".into(),
            weekdays: vec![1, 3, 7],
            scheduled_at: INSTANT.into(),
        }),
        ScheduleRecord::Cron(CronScheduleRecord {
            id: dsh_planning::ScheduleId::new("cron").unwrap(),
            kind: ScheduleKind::Cron,
            title: "Cron".into(),
            prompt: "Cron prompt.".into(),
            expression: "0 9 * * 1-5".into(),
            time_zone: "UTC".into(),
            scheduled_at: INSTANT.into(),
        }),
        ScheduleRecord::Rrule(RRuleScheduleRecord {
            id: dsh_planning::ScheduleId::new("rrule").unwrap(),
            kind: ScheduleKind::Rrule,
            title: "RRule".into(),
            prompt: "RRule prompt.".into(),
            rrule: "DTSTART:20261110T090000Z\nRRULE:FREQ=DAILY;COUNT=2".into(),
            scheduled_at: INSTANT.into(),
        }),
    ]
}

fn entry_for(record: ScheduleRecord) -> ScheduleEntry {
    ScheduleEntry {
        record,
        target: ScheduleTarget::Session {
            session_id: dsh_planning::SessionId::new("session-1").unwrap(),
        },
        involved_session_ids: vec![dsh_planning::SessionId::new("session-1").unwrap()],
        status: ScheduleStatus::Active,
        created_at: Some("2026-11-01T00:00:00.000Z".into()),
        updated_at: Some("2026-11-01T00:00:00.000Z".into()),
        last_delivery: None,
    }
}

fn payload_value(payload: PlanningPayload) -> Value {
    event(payload, 0, 0).data
}

fn assert_invalid_payload(payload: PlanningPayload) {
    let error = PlanningEvent::new(payload, 0, 0).expect_err("invalid payload accepted");
    assert_eq!(error.code(), "PLANNING_INVALID");
}

fn assert_invalid_entry(mutate: impl FnOnce(&mut ScheduleEntry)) {
    let mut entry = at_entry("schedule-1", ScheduleStatus::Active);
    mutate(&mut entry);
    assert_invalid_payload(put(entry));
}

fn assert_invalid_data(mutate: impl FnOnce(&mut Value)) {
    let mut data = payload_value(put(at_entry("schedule-1", ScheduleStatus::Active)));
    mutate(&mut data);
    assert!(
        PlanningEvent::from_session_event(&raw_event("schedule/entry/change", data, 0, 0,))
            .is_err()
    );
}

#[test]
fn all_seven_schedule_record_kinds_round_trip_with_stable_ids() {
    let mut state = PlanningState::default();
    for (seq, record) in all_records().into_iter().enumerate() {
        let id = record.id().clone();
        let kind = record.kind();
        apply(&mut state, put(entry_for(record)), seq as u64, seq as u64);
        let stored = state.schedule_entries().get(&id).unwrap();
        assert_eq!(stored.record.kind(), kind);
        assert_eq!(stored.record.id(), &id);
    }
    assert_eq!(state.schedule_entries().len(), 7);

    let restored =
        PlanningState::from_json(&state.to_json().unwrap(), PlanningLimits::default()).unwrap();
    assert_eq!(restored, state);
}

#[test]
fn schedule_upsert_and_delete_are_idempotent_and_keep_other_ids() {
    let mut state = PlanningState::default();
    apply(
        &mut state,
        put(at_entry("one", ScheduleStatus::Active)),
        0,
        0,
    );
    apply(
        &mut state,
        put(at_entry("two", ScheduleStatus::Paused)),
        1,
        1,
    );
    apply(
        &mut state,
        put(at_entry("one", ScheduleStatus::Ended)),
        2,
        2,
    );
    assert_eq!(state.schedule_entries().len(), 2);
    assert_eq!(
        state.schedule_entries().values().next().unwrap().status,
        ScheduleStatus::Ended
    );

    let delete = |id: &str| {
        PlanningPayload::ScheduleEntryChange(ScheduleChange::Delete(
            dsh_planning::ScheduleId::new(id).unwrap(),
        ))
    };
    apply(&mut state, delete("missing"), 3, 3);
    apply(&mut state, delete("one"), 4, 4);
    let before = state.snapshot();
    apply(&mut state, delete("one"), 5, 5);
    assert_eq!(state.schedule_entries().len(), 1);
    assert!(state
        .schedule_entries()
        .contains_key(&dsh_planning::ScheduleId::new("two").unwrap()));
    assert_eq!(state.schedule().entries, before.schedule.entries);
}

#[test]
fn schedule_entry_and_involved_session_bounds_are_configurable() {
    let mut state = PlanningState::new(PlanningLimits {
        max_schedule_entries: 1,
        max_involved_session_ids: 1,
        ..PlanningLimits::default()
    });
    apply(
        &mut state,
        put(at_entry("one", ScheduleStatus::Active)),
        0,
        0,
    );
    let before = state.snapshot();
    let error = state
        .replay(&event(put(at_entry("two", ScheduleStatus::Active)), 1, 1))
        .unwrap_err();
    assert!(matches!(
        error,
        PlanningError::LimitExceeded { limit: 1, .. }
    ));
    assert_eq!(state.snapshot(), before);

    let mut involved = at_entry("one", ScheduleStatus::Active);
    involved
        .involved_session_ids
        .push(dsh_planning::SessionId::new("session-2").unwrap());
    let error = state.replay(&event(put(involved), 1, 1)).unwrap_err();
    assert!(matches!(
        error,
        PlanningError::LimitExceeded { limit: 1, .. }
    ));
    assert_eq!(state.snapshot(), before);
}

#[test]
fn schedule_targets_use_authoritative_camel_case_wire_shape() {
    let targets = [
        ScheduleTarget::Session {
            session_id: dsh_planning::SessionId::new("session-1").unwrap(),
        },
        ScheduleTarget::NewOnce {
            session_id: dsh_planning::SessionId::new("reserved-1").unwrap(),
            create: ScheduleCreateSpec {
                cwd: Some("C:/work".into()),
                agent_preset: Some("default".into()),
            },
        },
        ScheduleTarget::New {
            create: ScheduleCreateSpec {
                cwd: None,
                agent_preset: None,
            },
        },
    ];
    for target in targets {
        let mut entry = at_entry("schedule-1", ScheduleStatus::Active);
        entry.target = target;
        let data = payload_value(put(entry));
        let text = data.to_string();
        assert!(text.contains("\"sessionId\"") || data["entry"]["target"]["kind"] == "new");
        assert!(!text.contains("session_id"));
        assert!(PlanningEvent::from_session_event(
            &raw_event("schedule/entry/change", data, 0, 0,)
        )
        .is_ok());
    }
}

#[test]
fn schedule_accepts_latest_delivery_receipt_without_runtime_history() {
    let mut entry = at_entry("schedule-1", ScheduleStatus::Ended);
    entry.last_delivery = Some(DeliveryReceipt {
        scheduled_at: INSTANT.into(),
        delivered_at: "2026-11-10T09:00:01.000Z".into(),
        message_id: dsh_planning::MessageId::new("message-1").unwrap(),
        target_session_id: dsh_planning::SessionId::new("session-2").unwrap(),
    });
    let mut state = PlanningState::default();
    apply(&mut state, put(entry), 0, 0);
    let value = state.to_snapshot_value().unwrap();
    assert!(value["schedule"]["entries"].is_object());
    let text = value.to_string();
    assert!(!text.contains("deliveryHistory"));
    assert!(!text.contains("pendingDeliveries"));
    assert!(!text.contains("timer"));
}

#[test]
fn schedule_rejects_record_discriminator_mismatches_and_bad_headers() {
    let mut wrong_kind = all_records()[0].clone();
    if let ScheduleRecord::After(record) = &mut wrong_kind {
        record.kind = ScheduleKind::At;
    }
    assert_invalid_payload(put(entry_for(wrong_kind)));

    assert_invalid_entry(|entry| match &mut entry.record {
        ScheduleRecord::At(record) => record.title = " padded ".into(),
        _ => unreachable!(),
    });
    assert_invalid_entry(|entry| match &mut entry.record {
        ScheduleRecord::At(record) => record.prompt = " ".into(),
        _ => unreachable!(),
    });
    assert_invalid_entry(|entry| match &mut entry.record {
        ScheduleRecord::At(record) => record.scheduled_at = "2026-02-30T09:00:00.000Z".into(),
        _ => unreachable!(),
    });
    assert_invalid_entry(|entry| match &mut entry.record {
        ScheduleRecord::At(record) => record.scheduled_at = "2026-11-10T09:00:00Z".into(),
        _ => unreachable!(),
    });
    assert_invalid_entry(|entry| match &mut entry.record {
        ScheduleRecord::At(record) => record.scheduled_at = "2026-11-10T09:00:00.000+00:00".into(),
        _ => unreachable!(),
    });
}

#[test]
fn schedule_title_limit_counts_javascript_utf16_units() {
    let mut valid = at_entry("schedule-1", ScheduleStatus::Active);
    if let ScheduleRecord::At(record) = &mut valid.record {
        record.title = "😀".repeat(60);
    }
    assert!(PlanningEvent::new(put(valid), 0, 0).is_ok());

    let mut invalid = at_entry("schedule-1", ScheduleStatus::Active);
    if let ScheduleRecord::At(record) = &mut invalid.record {
        record.title = "😀".repeat(61);
    }
    assert_invalid_payload(put(invalid));
}

#[test]
fn schedule_timing_fields_enforce_duration_local_time_zone_and_weekday_rules() {
    let mut after = all_records()[0].clone();
    if let ScheduleRecord::After(record) = &mut after {
        record.after_seconds = 0;
    }
    assert_invalid_payload(put(entry_for(after)));

    let mut every = all_records()[2].clone();
    if let ScheduleRecord::Every(record) = &mut every {
        record.every_seconds = 59;
    }
    assert_invalid_payload(put(entry_for(every)));

    let mut every = all_records()[2].clone();
    if let ScheduleRecord::Every(record) = &mut every {
        record.every_seconds = dsh_planning::MAX_SAFE_SESSION_INTEGER / 1_000 + 1;
    }
    assert_invalid_payload(put(entry_for(every)));

    let mut daily = all_records()[3].clone();
    if let ScheduleRecord::Daily(record) = &mut daily {
        record.time = "09:00:00".into();
    }
    assert_invalid_payload(put(entry_for(daily)));

    let mut daily = all_records()[3].clone();
    if let ScheduleRecord::Daily(record) = &mut daily {
        record.time_zone = "EST".into();
    }
    assert_invalid_payload(put(entry_for(daily)));

    let mut daily = all_records()[3].clone();
    if let ScheduleRecord::Daily(record) = &mut daily {
        record.time_zone = "Not/AZone".into();
    }
    assert_invalid_payload(put(entry_for(daily)));

    let mut weekly = all_records()[4].clone();
    if let ScheduleRecord::Weekly(record) = &mut weekly {
        record.weekdays = vec![3, 1];
    }
    assert_invalid_payload(put(entry_for(weekly)));

    let mut weekly = all_records()[4].clone();
    if let ScheduleRecord::Weekly(record) = &mut weekly {
        record.weekdays = vec![1, 1];
    }
    assert_invalid_payload(put(entry_for(weekly)));
}

#[test]
fn cron_requires_canonical_vixie_syntax() {
    let valid = ["0 9 * * 1-5", "*/15 0-22/2 * * 0", "0 0 1 * *"];
    for expression in valid {
        let mut entry = at_entry("schedule-1", ScheduleStatus::Active);
        entry.record = ScheduleRecord::Cron(CronScheduleRecord {
            id: dsh_planning::ScheduleId::new("schedule-1").unwrap(),
            kind: ScheduleKind::Cron,
            title: "Cron".into(),
            prompt: "Prompt.".into(),
            expression: expression.into(),
            time_zone: "UTC".into(),
            scheduled_at: INSTANT.into(),
        });
        assert!(PlanningEvent::new(put(entry), 0, 0).is_ok(), "{expression}");
    }

    let invalid = [
        "0 9 * *",
        "60 9 * * *",
        "*/0 9 * * *",
        "0 9 * * 8",
        "0 9 * * 5-1",
        "0  9 * * *",
        "00 09 * * *",
    ];
    for expression in invalid {
        let mut entry = at_entry("schedule-1", ScheduleStatus::Active);
        entry.record = ScheduleRecord::Cron(CronScheduleRecord {
            id: dsh_planning::ScheduleId::new("schedule-1").unwrap(),
            kind: ScheduleKind::Cron,
            title: "Cron".into(),
            prompt: "Prompt.".into(),
            expression: expression.into(),
            time_zone: "UTC".into(),
            scheduled_at: INSTANT.into(),
        });
        assert_invalid_payload(put(entry));
    }
}

#[test]
fn rrule_enforces_canonical_lines_numeric_grammar_combinations_and_cost_bounds() {
    let valid = [
        "DTSTART:20261110T090000Z\nRRULE:FREQ=DAILY;COUNT=2",
        "DTSTART;TZID=America/New_York:20261110T090000\nRRULE:FREQ=MONTHLY;BYDAY=+2TU,TH",
        "DTSTART:20261110T090000Z\nRRULE:FREQ=MONTHLY;BYDAY=MO,TU,WE,TH,FR;BYSETPOS=-1",
    ];
    for rrule in valid {
        let mut entry = at_entry("schedule-1", ScheduleStatus::Active);
        entry.record = ScheduleRecord::Rrule(RRuleScheduleRecord {
            id: dsh_planning::ScheduleId::new("schedule-1").unwrap(),
            kind: ScheduleKind::Rrule,
            title: "RRule".into(),
            prompt: "Prompt.".into(),
            rrule: rrule.into(),
            scheduled_at: INSTANT.into(),
        });
        assert!(PlanningEvent::new(put(entry), 0, 0).is_ok(), "{rrule}");
    }

    let invalid = [
        " DTSTART:20261110T090000Z\nRRULE:FREQ=DAILY",
        "DTSTART:20261110T090000Z\nRRULE:FREQ=SECONDLY",
        "DTSTART:20261110T090000Z\nRRULE:COUNT=2",
        "DTSTART:20261110T090000Z\nRRULE:FREQ=DAILY;COUNT=1001",
        "DTSTART:20261110T090000Z\nRRULE:FREQ=DAILY;INTERVAL=0",
        "DTSTART:20261110T090000Z\nRRULE:FREQ=DAILY;COUNT=2;UNTIL=20261112T090000Z",
        "DTSTART:20261110T090000Z\nRRULE:FREQ=DAILY;BYMONTH=+1",
        "DTSTART:20261110T090000Z\nRRULE:FREQ=MONTHLY;BYDAY=01MO",
        "DTSTART:20261110T090000Z\nRRULE:FREQ=MONTHLY;BYDAY=+01MO",
        "DTSTART:20261110T090000Z\nRRULE:FREQ=MONTHLY;BYDAY=+2TU,2TU",
        "DTSTART:20261110T090000Z\nRRULE:FREQ=WEEKLY;BYDAY=2TU",
        "DTSTART:20261110T090000Z\nRRULE:FREQ=MONTHLY;BYSETPOS=1",
        "DTSTART:20261110T090000Z\nRRULE:FREQ=YEARLY;BYMONTH=1;BYMONTHDAY=1;BYSETPOS=1",
        "DTSTART:20261110T090000Z\nRRULE:FREQ=YEARLY;BYMONTHDAY=1;BYSETPOS=32",
        "DTSTART:20261110T090000Z\nRRULE:FREQ=MONTHLY;BYDAY=MO,TU,WE,TH,FR,SA,SU;BYHOUR=0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23;BYMINUTE=0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59;BYSECOND=0",
        "DTSTART;TZID=America/New_York:20260308T023000\nRRULE:FREQ=DAILY",
    ];
    for rrule in invalid {
        let mut entry = at_entry("schedule-1", ScheduleStatus::Active);
        entry.record = ScheduleRecord::Rrule(RRuleScheduleRecord {
            id: dsh_planning::ScheduleId::new("schedule-1").unwrap(),
            kind: ScheduleKind::Rrule,
            title: "RRule".into(),
            prompt: "Prompt.".into(),
            rrule: rrule.into(),
            scheduled_at: INSTANT.into(),
        });
        assert_invalid_payload(put(entry));
    }
}

#[test]
fn schedule_wire_decoder_rejects_unknown_keys_explicit_null_and_wrong_versions() {
    assert_invalid_data(|data| data["entry"]["createdAt"] = Value::Null);
    assert_invalid_data(|data| data["entry"]["extra"] = json!(true));
    assert_invalid_data(|data| data["entry"]["record"]["extra"] = json!(true));
    assert_invalid_data(|data| data["entry"]["target"]["extra"] = json!(true));
    assert_invalid_data(|data| data["entry"]["target"]["session_id"] = json!("session-1"));
    assert_invalid_data(|data| data["version"] = json!(2));
    assert_invalid_data(|data| data["operation"] = json!("dispatch"));
    assert_invalid_data(|data| data["entry"]["status"] = json!("queued"));
    assert_invalid_data(|data| data["entry"]["lastDelivery"] = Value::Null);
}

#[test]
fn schedule_entry_timestamp_order_and_delivery_fields_are_validated() {
    assert_invalid_entry(|entry| {
        entry.created_at = Some("2026-11-02T00:00:00.000Z".into());
        entry.updated_at = Some("2026-11-01T00:00:00.000Z".into());
    });
    assert_invalid_entry(|entry| {
        entry.last_delivery = Some(DeliveryReceipt {
            scheduled_at: "not-an-instant".into(),
            delivered_at: INSTANT.into(),
            message_id: dsh_planning::MessageId::new("message-1").unwrap(),
            target_session_id: dsh_planning::SessionId::new("session-1").unwrap(),
        });
    });
    assert_invalid_entry(|entry| {
        entry.target = ScheduleTarget::New {
            create: ScheduleCreateSpec {
                cwd: Some(String::new()),
                agent_preset: None,
            },
        };
    });
}

#[test]
fn malformed_schedule_suffix_does_not_partially_apply() {
    let mut state = PlanningState::default();
    apply(
        &mut state,
        put(at_entry("one", ScheduleStatus::Active)),
        0,
        0,
    );
    let before = state.snapshot();
    let mut malformed = event(put(at_entry("two", ScheduleStatus::Active)), 1, 1);
    malformed.data["entry"]["record"]["scheduledAt"] = json!("invalid");
    assert!(state.replay(&malformed).is_err());
    assert_eq!(state.snapshot(), before);
}
