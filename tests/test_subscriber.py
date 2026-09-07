"""Regresi subscriber: mapping kanonis v2.1, dedup, legacy-compat (P2.4-6). Tanpa AWS."""
import sys

sys.path.insert(0, ".")

import subscriber_aws as sub

TOPIC = "astraea/v1/intersections/SIMPANG_TALUN_01/controllers/ESP32_TRAFFIC_01/telemetry"


def canon_payload():
    return {
        "schema_version": 1,
        "intersection_id": "SIMPANG_TALUN_01",
        "controller_id": "ESP32_TRAFFIC_01",
        "timestamp": "2026-09-07T00:00:00Z",
        "mode": {"auto": True, "adaptive": True, "fallback": False, "degraded": True},
        "vision": {"state": "DEGRADED", "fresh_lane_count": 2,
                   "north_fresh": True, "south_fresh": True, "east_fresh": False},
        "approaches": {
            "north": {"light": "green", "camera_vehicle_count": 8, "camera_queue_count": 6,
                      "vehicle_count_valid": True, "vehicle_count_source": "camera",
                      "sensor_level": 2, "ir_occupied": True, "ultrasonic_occupied": True,
                      "distance_cm": 4, "recommended_green_s": 42, "effective_green_s": 40},
            "south": {"light": "red", "camera_vehicle_count": 0, "camera_queue_count": 0,
                      "vehicle_count_valid": False, "vehicle_count_source": "vision_stale",
                      "sensor_level": 1, "ir_occupied": True, "ultrasonic_occupied": False,
                      "distance_cm": 0, "recommended_green_s": 25, "effective_green_s": 20},
        },
        "wifi_rssi": -52, "uptime_s": 100, "config_version": 7,
        "firmware_version": "2.1.0", "sig_state": "ACTIVE_GREEN", "active_lane": "north",
    }


def test_vision_state_mapping():
    flat = sub.normalize_canonical_telemetry(TOPIC, canon_payload())
    assert flat["vision_state"] == "DEGRADED"
    assert flat["vision_fresh_lanes"] == 2
    assert flat["vision_fresh"] is True
    assert flat["mode_degraded"] is True
    assert flat["north_vision_fresh"] is True
    assert flat["east_vision_fresh"] is False  # tidak ada di payload -> False


def test_queue_vehicles_not_cm():
    flat = sub.normalize_canonical_telemetry(TOPIC, canon_payload())
    assert flat["north_queue_vehicles"] == 6
    assert flat["north_queue_estimate_cm"] == 0  # jangan karang cm
    assert flat["north_count_valid"] is True
    assert flat["north_count_source"] == "camera"
    assert flat["south_count_valid"] is False
    assert flat["south_count_source"] == "vision_stale"


def test_recommended_vs_effective():
    flat = sub.normalize_canonical_telemetry(TOPIC, canon_payload())
    assert flat["north_recommended_green_s"] == 42
    assert flat["north_effective_green_s"] == 40
    assert flat["north_green_duration_s"] == 40  # dashboard = aktual


def test_distance_preserved():
    flat = sub.normalize_canonical_telemetry(TOPIC, canon_payload())
    assert flat["north_distance_cm"] == 4


def test_build_document_keeps_extras():
    doc = sub.build_document(sub.normalize_canonical_telemetry(TOPIC, canon_payload()))
    assert doc["vision_state"] == "DEGRADED"
    assert doc["firmware_version"] == "2.1.0"
    assert doc["active_lane"] == "north"
    assert doc["north_count_source"] == "camera"
    assert doc["config_version"] == 7


def test_build_document_legacy_unchanged():
    doc = sub.build_document({"device_id": "X", "intersection_id": "I"})
    assert doc["vision_state"] == "UNKNOWN"
    assert doc["north_count_valid"] is True  # asumsi lama dipertahankan
    assert "north_recommended_green_s" not in doc


def test_dedup_prefers_canonical():
    sub._canon_last_seen.clear()
    sub.mark_canonical_seen("I", "C", 1000.0)
    assert sub.should_drop_legacy("I", "C", 1003.0) is True
    assert sub.should_drop_legacy("I", "C", 1010.0) is False
    assert sub.should_drop_legacy("I", "OTHER", 1003.0) is False
    assert sub.should_drop_legacy("J", "C", 1003.0) is False


def _payload_all_fresh():
    p = canon_payload()
    p["vision"] = {"state": "NORMAL", "fresh_lane_count": 3,
                   "north_fresh": True, "south_fresh": True, "east_fresh": True}
    p["approaches"]["south"] = dict(p["approaches"]["north"])
    p["approaches"]["east"] = dict(p["approaches"]["north"])
    return p


def _payload_none_fresh():
    p = canon_payload()
    p["vision"] = {"state": "FALLBACK", "fresh_lane_count": 0,
                   "north_fresh": False, "south_fresh": False, "east_fresh": False}
    return p


def test_source_all_fresh_camera():
    flat = sub.normalize_canonical_telemetry(TOPIC, _payload_all_fresh())
    assert flat["vehicle_count_source"] == "camera"


def test_source_partial_mixed():
    flat = sub.normalize_canonical_telemetry(TOPIC, canon_payload())
    assert flat["vehicle_count_source"] == "mixed"


def test_source_none_stale():
    flat = sub.normalize_canonical_telemetry(TOPIC, _payload_none_fresh())
    assert flat["vehicle_count_source"] == "vision_stale"
