import json
import os
import time
import uuid
import urllib.request
import urllib.parse
import smtplib

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import boto3
import paho.mqtt.client as mqtt
from botocore.exceptions import ClientError
from dotenv import load_dotenv


load_dotenv()

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USER = os.getenv("MQTT_USER", "")
MQTT_PASS = os.getenv("MQTT_PASS", "")
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "traffic/+/data")
# Topik kanonis PRD §25 (selalu disubscribe bersama legacy §26)
MQTT_TOPIC_V1_TELEMETRY = os.getenv(
    "MQTT_TOPIC_V1_TELEMETRY", "astraea/v1/intersections/+/controllers/+/telemetry"
)
MQTT_TOPIC_V1_VISION = os.getenv(
    "MQTT_TOPIC_V1_VISION", "astraea/v1/intersections/+/vision/metrics"
)
# Persistensi vision sampled (§74): simpan bila berubah ATAU tiap N detik
VISION_SAMPLE_SECONDS = int(os.getenv("VISION_SAMPLE_SECONDS", "60"))

AWS_REGION = os.getenv("AWS_REGION", "ap-southeast-2")

DYNAMODB_TABLE = os.getenv("DYNAMODB_TABLE", "TrafficTelemetry")
DYNAMODB_DEVICE_STATUS_TABLE = os.getenv("DYNAMODB_DEVICE_STATUS_TABLE", "DeviceStatus")
DYNAMODB_NOTIFICATIONS_TABLE = os.getenv("DYNAMODB_NOTIFICATIONS_TABLE", "Notifications")
DYNAMODB_USERS_TABLE = os.getenv("DYNAMODB_USERS_TABLE", "Users")

S3_BUCKET = os.getenv("S3_BUCKET", "")
S3_BASE_PREFIX = os.getenv("S3_BASE_PREFIX", "traffic/raw")

DEFAULT_NOTIFICATION_USER_ID = os.getenv("DEFAULT_NOTIFICATION_USER_ID", "all")
NOTIFICATION_COOLDOWN_SECONDS = int(os.getenv("NOTIFICATION_COOLDOWN_SECONDS", "300"))
WEAK_WIFI_RSSI = int(os.getenv("WEAK_WIFI_RSSI", "-75"))

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

SMTP_HOST = os.getenv("SMTP_HOST", "")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")
SMTP_FROM = os.getenv("SMTP_FROM", SMTP_USER)

_notification_cache: Dict[str, float] = {}

_user_cache: Dict[str, Any] = {
    "expired_at": 0,
    "users": [],
}

dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)

traffic_table = dynamodb.Table(DYNAMODB_TABLE)
device_status_table = dynamodb.Table(DYNAMODB_DEVICE_STATUS_TABLE)
notifications_table = dynamodb.Table(DYNAMODB_NOTIFICATIONS_TABLE)
users_table = dynamodb.Table(DYNAMODB_USERS_TABLE)

s3 = boto3.client("s3", region_name=AWS_REGION)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def convert_float_to_decimal(obj: Any) -> Any:
    if isinstance(obj, float):
        return Decimal(str(obj))

    if isinstance(obj, dict):
        return {key: convert_float_to_decimal(value) for key, value in obj.items()}

    if isinstance(obj, list):
        return [convert_float_to_decimal(value) for value in obj]

    return obj


def normalize_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value

    if isinstance(value, str):
        lower = value.strip().lower()
        if lower in ["true", "1", "on", "yes", "enabled"]:
            return True
        if lower in ["false", "0", "off", "no", "disabled"]:
            return False

    if isinstance(value, (int, float)):
        return value != 0

    return default


def normalize_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(value)
    except Exception:
        return default


def normalize_light(value: Any) -> str:
    text = str(value or "red").strip().lower()
    if text in ["red", "yellow", "green"]:
        return text
    return "red"


def build_document(payload: Dict[str, Any]) -> Dict[str, Any]:
    now = utc_now()

    timestamp = (
        payload.get("timestamp")
        or payload.get("received_at_utc")
        or now.isoformat()
    )

    intersection_id = (
        payload.get("intersection_id")
        or payload.get("intersectionId")
        or "SIMPANG_TALUN_01"
    )

    device_id = (
        payload.get("device_id")
        or payload.get("deviceId")
        or payload.get("device")
        or "ESP32_TRAFFIC_01"
    )

    doc_id = payload.get("id") or (
        f"{intersection_id}_{device_id}_{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}"
    )

    document: Dict[str, Any] = {
        "id": doc_id,
        "intersection_id": intersection_id,
        "device_id": device_id,
        "device": payload.get("device") or device_id,
        "timestamp": timestamp,
        "received_at_utc": now.isoformat(),
        "source": "mqtt_mosquitto_aws_ec2",
        "wifi_rssi": normalize_int(payload.get("wifi_rssi"), 0),
        "uptime_s": normalize_int(payload.get("uptime_s"), 0),
        "dummy_mode": normalize_bool(payload.get("dummy_mode"), False),
        "sensor_mode": normalize_bool(payload.get("sensor_mode"), False),
        "auto_mode": normalize_bool(payload.get("auto_mode"), True),
        "adaptive_mode": normalize_bool(payload.get("adaptive_mode"), True),
        "green_time_s": normalize_int(payload.get("green_time_s"), 10),
        "yellow_time_s": normalize_int(payload.get("yellow_time_s"), 3),
        "density_level_0_green_s": normalize_int(payload.get("density_level_0_green_s"), 10),
        "density_level_1_green_s": normalize_int(payload.get("density_level_1_green_s"), 20),
        "density_level_2_green_s": normalize_int(payload.get("density_level_2_green_s"), 30),
    }

    for lane in ["north", "south", "east"]:
        density_level = normalize_int(payload.get(f"{lane}_density_level"), 0)
        distance_cm = normalize_int(payload.get(f"{lane}_distance_cm"), 0)

        vehicle_detected = normalize_bool(
            payload.get(f"{lane}_vehicle_detected"),
            density_level >= 1,
        )

        queue_detected = normalize_bool(
            payload.get(f"{lane}_queue_detected"),
            density_level >= 2,
        )

        document[f"{lane}_vehicle_count"] = normalize_int(payload.get(f"{lane}_vehicle_count"), 0)
        document[f"{lane}_vehicle_detected"] = vehicle_detected
        document[f"{lane}_distance_cm"] = distance_cm
        document[f"{lane}_ultrasonic_detected"] = normalize_bool(
            payload.get(f"{lane}_ultrasonic_detected"),
            0 < distance_cm <= 5,
        )
        document[f"{lane}_density_level"] = density_level
        document[f"{lane}_queue_detected"] = queue_detected
        document[f"{lane}_queue_estimate_cm"] = normalize_int(payload.get(f"{lane}_queue_estimate_cm"), 0)
        document[f"{lane}_queue_vehicles"] = normalize_int(payload.get(f"{lane}_queue_vehicles"), 0)
        document[f"{lane}_light"] = normalize_light(payload.get(f"{lane}_light"))
        document[f"{lane}_green_duration_s"] = normalize_int(
            payload.get(f"{lane}_green_duration_s"),
            document["green_time_s"],
        )
        # Extra v2.1 (backward-compatible: payload legacy tidak punya → default lama).
        # count_valid default True = asumsi lama "count selalu valid".
        document[f"{lane}_count_valid"] = normalize_bool(
            payload.get(f"{lane}_count_valid"), True
        )
        document[f"{lane}_count_source"] = str(payload.get(f"{lane}_count_source") or "")
        document[f"{lane}_vision_fresh"] = normalize_bool(
            payload.get(f"{lane}_vision_fresh"), False
        )
        if payload.get(f"{lane}_recommended_green_s") is not None:
            try:
                document[f"{lane}_recommended_green_s"] = float(
                    payload.get(f"{lane}_recommended_green_s")
                )
            except (TypeError, ValueError):
                pass
        if payload.get(f"{lane}_effective_green_s") is not None:
            try:
                document[f"{lane}_effective_green_s"] = float(
                    payload.get(f"{lane}_effective_green_s")
                )
            except (TypeError, ValueError):
                pass

    # Extra top-level v2.1 (aman untuk legacy: default netral).
    document["mode_degraded"] = normalize_bool(payload.get("mode_degraded"), False)
    document["vision_state"] = str(payload.get("vision_state") or "UNKNOWN")
    document["vision_fresh_lanes"] = normalize_int(payload.get("vision_fresh_lanes"), 0)
    document["vision_fresh"] = normalize_bool(payload.get("vision_fresh"), False)
    document["firmware_version"] = str(payload.get("firmware_version") or "")
    document["active_lane"] = str(payload.get("active_lane") or "")
    if payload.get("sig_state") is not None:
        document["sig_state"] = payload.get("sig_state")
    document["config_version"] = normalize_int(payload.get("config_version"), 0)

    return document


def save_to_dynamodb(document: Dict[str, Any]) -> None:
    traffic_table.put_item(Item=convert_float_to_decimal(document))


def build_device_status(document: Dict[str, Any]) -> Dict[str, Any]:
    device_id = document.get("device_id", "ESP32_TRAFFIC_01")
    intersection_id = document.get("intersection_id", "SIMPANG_TALUN_01")

    status_item: Dict[str, Any] = {
        "device_id": device_id,
        "intersection_id": intersection_id,
        "device": document.get("device", device_id),
        "status": "online",
        "last_seen": document.get("received_at_utc", utc_now_iso()),
        "timestamp": document.get("timestamp", utc_now_iso()),
        "updated_at": utc_now_iso(),
        "wifi_rssi": document.get("wifi_rssi", 0),
        "uptime_s": document.get("uptime_s", 0),
        "dummy_mode": document.get("dummy_mode", False),
        "sensor_mode": document.get("sensor_mode", False),
        "auto_mode": document.get("auto_mode", True),
        "adaptive_mode": document.get("adaptive_mode", True),
        "green_time_s": document.get("green_time_s", 10),
        "yellow_time_s": document.get("yellow_time_s", 3),
        "density_level_0_green_s": document.get("density_level_0_green_s", 10),
        "density_level_1_green_s": document.get("density_level_1_green_s", 20),
        "density_level_2_green_s": document.get("density_level_2_green_s", 30),
        "source": "mqtt_mosquitto_aws_ec2",
    }

    for lane in ["north", "south", "east"]:
        status_item[f"{lane}_vehicle_count"] = document.get(f"{lane}_vehicle_count", 0)
        status_item[f"{lane}_vehicle_detected"] = document.get(f"{lane}_vehicle_detected", False)
        status_item[f"{lane}_distance_cm"] = document.get(f"{lane}_distance_cm", 0)
        status_item[f"{lane}_ultrasonic_detected"] = document.get(f"{lane}_ultrasonic_detected", False)
        status_item[f"{lane}_density_level"] = document.get(f"{lane}_density_level", 0)
        status_item[f"{lane}_queue_detected"] = document.get(f"{lane}_queue_detected", False)
        status_item[f"{lane}_queue_estimate_cm"] = document.get(f"{lane}_queue_estimate_cm", 0)
        status_item[f"{lane}_queue_vehicles"] = document.get(f"{lane}_queue_vehicles", 0)
        status_item[f"{lane}_light"] = document.get(f"{lane}_light", "red")
        status_item[f"{lane}_green_duration_s"] = document.get(f"{lane}_green_duration_s", 0)

    return status_item


def save_device_status(document: Dict[str, Any]) -> None:
    device_status_table.put_item(Item=convert_float_to_decimal(build_device_status(document)))


def should_send_notification(cache_key: str) -> bool:
    now = time.time()
    last_sent = _notification_cache.get(cache_key)

    if last_sent and now - last_sent < NOTIFICATION_COOLDOWN_SECONDS:
        return False

    _notification_cache[cache_key] = now
    return True


def get_active_users() -> List[Dict[str, Any]]:
    now = time.time()

    if _user_cache["users"] and now < _user_cache["expired_at"]:
        return _user_cache["users"]

    try:
        items: List[Dict[str, Any]] = []
        exclusive_start_key = None

        while True:
            params: Dict[str, Any] = {
                "ProjectionExpression": "id, email, #status, appSettings",
                "ExpressionAttributeNames": {
                    "#status": "status",
                },
            }

            if exclusive_start_key:
                params["ExclusiveStartKey"] = exclusive_start_key

            result = users_table.scan(**params)
            items.extend(result.get("Items", []))

            exclusive_start_key = result.get("LastEvaluatedKey")
            if not exclusive_start_key:
                break

        users = [
            item
            for item in items
            if item.get("id")
            and str(item.get("status", "active")).lower() == "active"
        ]

        if not users:
            users = [
                {
                    "id": "user-001",
                    "email": "",
                    "appSettings": {},
                }
            ]

        _user_cache["users"] = users
        _user_cache["expired_at"] = now + 60

        return users

    except Exception as exc:
        print("Failed to fetch active users:", exc)

        return [
            {
                "id": "user-001",
                "email": "",
                "appSettings": {},
            }
        ]


def get_notification_target_users() -> List[Dict[str, Any]]:
    if DEFAULT_NOTIFICATION_USER_ID.strip().lower() == "all":
        return get_active_users()

    return [
        {
            "id": DEFAULT_NOTIFICATION_USER_ID.strip() or "user-001",
            "email": "",
            "appSettings": {},
        }
    ]


def send_telegram_message(
    bot_token: str,
    chat_id: str,
    text: str,
) -> bool:
    if not bot_token or not chat_id:
        return False

    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"

        data = urllib.parse.urlencode(
            {
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": "true",
            }
        ).encode("utf-8")

        request = urllib.request.Request(
            url,
            data=data,
            method="POST",
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )

        with urllib.request.urlopen(request, timeout=8) as response:
            if response.status == 200:
                return True

            print(f"Telegram failed with status: {response.status}")
            return False

    except Exception as exc:
        print("Telegram send error:", exc)
        return False


def should_send_telegram(user: Dict[str, Any]) -> bool:
    app_settings = user.get("appSettings") or {}
    return bool(app_settings.get("telegramNotification", False))


def get_user_telegram_config(user: Dict[str, Any]) -> Dict[str, str]:
    app_settings = user.get("appSettings") or {}

    bot_token = str(
        app_settings.get("telegramBotToken")
        or TELEGRAM_BOT_TOKEN
        or ""
    ).strip()

    chat_id = str(
        app_settings.get("telegramChatId")
        or TELEGRAM_CHAT_ID
        or ""
    ).strip()

    return {
        "bot_token": bot_token,
        "chat_id": chat_id,
    }


NOTIFICATION_TRANSLATIONS: Dict[str, Dict[str, Dict[str, str]]] = {
    "id": {
        "dummyMode": {
            "title": "Mode Dummy Masih Aktif",
            "message": (
                "Perangkat {deviceId} pada {intersection} masih mengirim data dummy. "
                "Data belum berasal dari sensor asli."
            ),
        },
        "sensorModeFalse": {
            "title": "Sensor Mode Tidak Aktif",
            "message": "Perangkat {deviceId} pada {intersection} belum menggunakan mode sensor asli.",
        },
        "weakWifi": {
            "title": "Sinyal WiFi ESP32 Lemah",
            "message": (
                "Sinyal WiFi perangkat {deviceId} lemah ({rssi} dBm). "
                "Periksa jarak router atau kualitas jaringan."
            ),
        },
        "queueLevel2": {
            "title": "Antrean Padat Jalur {lane}",
            "message": (
                "Jalur {lane} pada {intersection} mencapai Queue Level {level}. "
                "Estimasi antrean {queueEstimateCm} cm dan sekitar {queueVehicles} kendaraan."
            ),
        },
    },
    "en": {
        "dummyMode": {
            "title": "Dummy Mode Still Active",
            "message": (
                "Device {deviceId} at {intersection} is still sending dummy data. "
                "The data is not from real sensors yet."
            ),
        },
        "sensorModeFalse": {
            "title": "Sensor Mode Is Not Active",
            "message": "Device {deviceId} at {intersection} is not using real sensor mode yet.",
        },
        "weakWifi": {
            "title": "Weak ESP32 WiFi Signal",
            "message": (
                "Device {deviceId} has a weak WiFi signal ({rssi} dBm). "
                "Check the router distance or network quality."
            ),
        },
        "queueLevel2": {
            "title": "Heavy Queue on {lane} Lane",
            "message": (
                "The {lane} lane at {intersection} reached Queue Level {level}. "
                "Estimated queue length is {queueEstimateCm} cm with around {queueVehicles} vehicles."
            ),
        },
    },
}

NOTIFICATION_CODE_BY_TYPE = {
    "dummy_mode": "dummyMode",
    "sensor_mode_false": "sensorModeFalse",
    "weak_wifi": "weakWifi",
    "queue_level_2": "queueLevel2",
}

LANE_LABELS = {
    "id": {
        "north": "Utara",
        "south": "Selatan",
        "east": "Timur",
        "west": "Barat",
    },
    "en": {
        "north": "North",
        "south": "South",
        "east": "East",
        "west": "West",
    },
}


def get_user_locale(user: Dict[str, Any]) -> str:
    app_settings = user.get("appSettings") or {}
    locale = str(
        app_settings.get("language")
        or app_settings.get("locale")
        or app_settings.get("appLanguage")
        or "id"
    ).lower()

    if locale not in ["id", "en"]:
        return "id"

    return locale


def get_notification_code(notif_type: str) -> str:
    return NOTIFICATION_CODE_BY_TYPE.get(notif_type, notif_type)


def build_notification_params(
    document: Dict[str, Any],
    lane: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
    locale: str = "id",
) -> Dict[str, Any]:
    meta = metadata or {}
    raw_lane = lane or str(meta.get("lane") or "-")
    lane_label = LANE_LABELS.get(locale, LANE_LABELS["id"]).get(raw_lane, raw_lane)

    return {
        "intersection": document.get("intersection_id", "-"),
        "intersectionId": document.get("intersection_id", "-"),
        "deviceId": document.get("device_id", "-"),
        "device_id": document.get("device_id", "-"),
        "lane": lane_label,
        "laneRaw": raw_lane,
        "level": meta.get("queue_level", meta.get("queueLevel", "-")),
        "queueEstimateCm": meta.get(
            "queue_estimate_cm",
            meta.get("queueEstimateCm", "-"),
        ),
        "queueVehicles": meta.get(
            "queue_vehicles",
            meta.get("queueVehicles", "-"),
        ),
        "rssi": meta.get("wifi_rssi", meta.get("wifiRssi", "-")),
        "threshold": meta.get("threshold", "-"),
        "light": meta.get("light", "-"),
    }


def format_template(template: str, params: Dict[str, Any]) -> str:
    text = template

    for key, value in params.items():
        text = text.replace("{" + key + "}", str(value))

    return text


def render_notification_text(
    notif_type: str,
    field: str,
    params: Dict[str, Any],
    locale: str = "id",
    fallback: str = "",
) -> str:
    code = get_notification_code(notif_type)
    translations = NOTIFICATION_TRANSLATIONS.get(locale) or NOTIFICATION_TRANSLATIONS["id"]
    template = (translations.get(code) or {}).get(field)

    if not template:
        return fallback

    return format_template(template, params)


def build_telegram_alert_text(
    title: str,
    message: str,
    severity: str,
    category: str,
    document: Dict[str, Any],
    lane: Optional[str] = None,
    locale: str = "id",
) -> str:
    intersection_id = document.get("intersection_id", "-")
    device_id = document.get("device_id", "-")
    lane_text = (
        LANE_LABELS.get(locale, LANE_LABELS["id"]).get(lane, lane)
        if lane
        else "-"
    )

    label_category = "Category" if locale == "en" else "Kategori"
    label_intersection = "Intersection" if locale == "en" else "Persimpangan"
    label_lane = "Lane" if locale == "en" else "Jalur"
    label_time = "UTC Time" if locale == "en" else "Waktu UTC"

    return (
        f"🚦 <b>ASTRAEA Alert</b>\n\n"
        f"<b>{title}</b>\n"
        f"{message}\n\n"
        f"Severity: <b>{severity.upper()}</b>\n"
        f"{label_category}: {category}\n"
        f"{label_intersection}: {intersection_id}\n"
        f"Device: {device_id}\n"
        f"{label_lane}: {lane_text}\n"
        f"{label_time}: {utc_now_iso()}"
    )


def should_send_email(user: Dict[str, Any]) -> bool:
    app_settings = user.get("appSettings") or {}
    return bool(app_settings.get("emailNotification", False))


def send_email_message(to_email: str, subject: str, html: str) -> bool:
    if not SMTP_HOST or not SMTP_USER or not SMTP_PASS or not SMTP_FROM:
        print("Email skipped: SMTP not configured")
        return False

    if not to_email:
        print("Email skipped: user email empty")
        return False

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = SMTP_FROM
        msg["To"] = to_email

        msg.attach(MIMEText(html, "html"))

        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=10) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASS)
            server.sendmail(SMTP_FROM, [to_email], msg.as_string())

        return True

    except Exception as exc:
        print("Email send error:", exc)
        return False


def build_email_alert_html(
    title: str,
    message: str,
    severity: str,
    category: str,
    document: Dict[str, Any],
    lane: Optional[str] = None,
    locale: str = "id",
) -> str:
    intersection_id = document.get("intersection_id", "-")
    device_id = document.get("device_id", "-")
    lane_text = (
        LANE_LABELS.get(locale, LANE_LABELS["id"]).get(lane, lane)
        if lane
        else "-"
    )

    label_category = "Category" if locale == "en" else "Kategori"
    label_intersection = "Intersection" if locale == "en" else "Persimpangan"
    label_lane = "Lane" if locale == "en" else "Jalur"
    label_time = "UTC Time" if locale == "en" else "Waktu UTC"

    return f"""
    <div style="font-family:Arial,sans-serif;line-height:1.6;color:#0f172a">
      <h2>🚦 ASTRAEA Alert</h2>
      <h3>{title}</h3>
      <p>{message}</p>

      <hr />

      <p><b>Severity:</b> {severity.upper()}</p>
      <p><b>{label_category}:</b> {category}</p>
      <p><b>{label_intersection}:</b> {intersection_id}</p>
      <p><b>Device:</b> {device_id}</p>
      <p><b>{label_lane}:</b> {lane_text}</p>
      <p><b>{label_time}:</b> {utc_now_iso()}</p>
    </div>
    """


def save_notification(
    document: Dict[str, Any],
    notif_type: str,
    severity: str,
    title: str,
    message: str,
    category: str = "system",
    lane: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
    action_url: str = "/dashboard",
) -> None:
    created_at = utc_now_iso()
    target_users = get_notification_target_users()

    notification_code = get_notification_code(notif_type)
    title_key = f"notifications.items.{notification_code}.title"
    message_key = f"notifications.items.{notification_code}.message"

    base_metadata = {
        **(metadata or {}),
        "notificationType": notif_type,
        "code": notification_code,
    }

    params_id = build_notification_params(
        document=document,
        lane=lane,
        metadata=base_metadata,
        locale="id",
    )

    for user in target_users:
        user_id = str(user.get("id") or "user-001")
        notification_id = f"notif_{uuid.uuid4().hex[:12]}"
        locale = get_user_locale(user)

        user_params = build_notification_params(
            document=document,
            lane=lane,
            metadata=base_metadata,
            locale=locale,
        )

        localized_title = render_notification_text(
            notif_type=notif_type,
            field="title",
            params=user_params,
            locale=locale,
            fallback=title,
        )
        localized_message = render_notification_text(
            notif_type=notif_type,
            field="message",
            params=user_params,
            locale=locale,
            fallback=message,
        )

        item = {
            "user_id": user_id,
            "created_at": created_at,
            "notification_id": notification_id,
            "id": notification_id,
            "type": severity,
            "severity": severity,
            "category": category,

            # Fallback text for old UI / direct DB checks.
            "title": title,
            "message": message,

            # I18n fields for dashboard rendering.
            "titleKey": title_key,
            "messageKey": message_key,
            "params": params_id,
            "notificationType": notif_type,
            "code": notification_code,

            "read": False,
            "actionUrl": action_url,
            "relatedTo": document.get("id"),
            "intersection_id": document.get("intersection_id"),
            "device_id": document.get("device_id"),
            "lane": lane,
            "metadata": base_metadata,
            "createdAt": created_at,
            "updatedAt": created_at,
        }

        notifications_table.put_item(Item=convert_float_to_decimal(item))

        print("Saved to DynamoDB Notifications:")
        print(f"  user_id={user_id}")
        print(f"  notification_id={notification_id}")
        print(f"  title={title}")
        print(f"  titleKey={title_key}")

        if should_send_telegram(user):
            config = get_user_telegram_config(user)

            sent = send_telegram_message(
                bot_token=config["bot_token"],
                chat_id=config["chat_id"],
                text=build_telegram_alert_text(
                    title=localized_title,
                    message=localized_message,
                    severity=severity,
                    category=category,
                    document=document,
                    lane=lane,
                    locale=locale,
                ),
            )

            if sent:
                print("Telegram alert sent:")
                print(f"  user_id={user_id}")
            else:
                print("Telegram alert skipped/failed:")
                print(f"  user_id={user_id}")

        if should_send_email(user):
            email_sent = send_email_message(
                to_email=str(user.get("email") or ""),
                subject=f"[ASTRAEA] {localized_title}",
                html=build_email_alert_html(
                    title=localized_title,
                    message=localized_message,
                    severity=severity,
                    category=category,
                    document=document,
                    lane=lane,
                    locale=locale,
                ),
            )

            if email_sent:
                print("Email alert sent:")
                print(f"  user_id={user_id}")
                print(f"  email={user.get('email')}")
            else:
                print("Email alert skipped/failed:")
                print(f"  user_id={user_id}")


def create_notifications_from_telemetry(document: Dict[str, Any]) -> None:
    intersection_id = document.get("intersection_id", "SIMPANG_TALUN_01")
    device_id = document.get("device_id", "ESP32_TRAFFIC_01")

    if document.get("dummy_mode") is True:
        cache_key = f"{device_id}:dummy_mode"

        if should_send_notification(cache_key):
            save_notification(
                document=document,
                notif_type="dummy_mode",
                severity="warning",
                category="system",
                title="Mode Dummy Masih Aktif",
                message=(
                    f"Perangkat {device_id} pada {intersection_id} masih mengirim data dummy. "
                    "Data belum berasal dari sensor asli."
                ),
                metadata={
                    "dummy_mode": True,
                    "sensor_mode": document.get("sensor_mode"),
                },
                action_url="/iot-config",
            )

    if document.get("sensor_mode") is False:
        cache_key = f"{device_id}:sensor_mode_false"

        if should_send_notification(cache_key):
            save_notification(
                document=document,
                notif_type="sensor_mode_false",
                severity="warning",
                category="system",
                title="Sensor Mode Tidak Aktif",
                message=f"Perangkat {device_id} pada {intersection_id} belum menggunakan mode sensor asli.",
                metadata={
                    "sensor_mode": False,
                },
                action_url="/iot-config",
            )

    wifi_rssi = normalize_int(document.get("wifi_rssi"), 0)

    if wifi_rssi <= WEAK_WIFI_RSSI:
        cache_key = f"{device_id}:weak_wifi"

        if should_send_notification(cache_key):
            save_notification(
                document=document,
                notif_type="weak_wifi",
                severity="warning",
                category="system",
                title="Sinyal WiFi ESP32 Lemah",
                message=(
                    f"Sinyal WiFi perangkat {device_id} lemah ({wifi_rssi} dBm). "
                    "Periksa jarak router atau kualitas jaringan."
                ),
                metadata={
                    "wifi_rssi": wifi_rssi,
                    "threshold": WEAK_WIFI_RSSI,
                },
                action_url="/iot-config",
            )

    lane_labels = {
        "north": "Utara",
        "south": "Selatan",
        "east": "Timur",
    }

    for lane in ["north", "south", "east"]:
        level = normalize_int(document.get(f"{lane}_density_level"), 0)
        queue_detected = bool(document.get(f"{lane}_queue_detected", False))
        queue_estimate_cm = normalize_int(document.get(f"{lane}_queue_estimate_cm"), 0)
        queue_vehicles = normalize_int(document.get(f"{lane}_queue_vehicles"), 0)

        if level >= 2 or queue_detected:
            cache_key = f"{device_id}:{lane}:queue_level_2"

            if should_send_notification(cache_key):
                if queue_estimate_cm == 0:
                    msg_text = f"Jalur {lane_labels[lane]} pada {intersection_id} mencapai Queue Level {level}. Sekitar {queue_vehicles} kendaraan."
                else:
                    msg_text = (
                        f"Jalur {lane_labels[lane]} pada {intersection_id} mencapai Queue Level {level}. "
                        f"Estimasi antrean {queue_estimate_cm} cm dan sekitar {queue_vehicles} kendaraan."
                    )
                save_notification(
                    document=document,
                    notif_type="queue_level_2",
                    severity="critical",
                    category="traffic",
                    lane=lane,
                    title=f"Antrean Padat Jalur {lane_labels[lane]}",
                    message=msg_text,
                    metadata={
                        "queue_level": level,
                        "queue_detected": queue_detected,
                        "queue_estimate_cm": queue_estimate_cm,
                        "queue_vehicles": queue_vehicles,
                        "light": document.get(f"{lane}_light"),
                    },
                    action_url="/dashboard",
                )


def save_to_s3(document: Dict[str, Any]) -> Optional[str]:
    if not S3_BUCKET:
        return None

    now = utc_now()

    key = (
        f"{S3_BASE_PREFIX}/"
        f"year={now.strftime('%Y')}/"
        f"month={now.strftime('%m')}/"
        f"day={now.strftime('%d')}/"
        f"hour={now.strftime('%H')}/"
        f"events/{document.get('id')}.json"
    )

    body = json.dumps(document, ensure_ascii=False) + "\n"

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
    )

    return key


def extract_device_id_from_topic(topic: str) -> Optional[str]:
    parts = topic.split("/")

    if len(parts) >= 3 and parts[0] == "traffic" and parts[2] == "data":
        return parts[1]

    return None


def is_canonical_telemetry_topic(topic: str) -> bool:
    parts = topic.split("/")
    return (
        len(parts) >= 7
        and parts[0] == "astraea"
        and parts[1] == "v1"
        and parts[2] == "intersections"
        and parts[4] == "controllers"
        and parts[6] == "telemetry"
    )


def is_canonical_vision_topic(topic: str) -> bool:
    parts = topic.split("/")
    return (
        len(parts) >= 6
        and parts[0] == "astraea"
        and parts[1] == "v1"
        and parts[2] == "intersections"
        and parts[4] == "vision"
        and parts[5] == "metrics"
    )


def normalize_canonical_telemetry(topic: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """Skema kanonis v2.1 (§27/F17) -> bentuk flat agar pipeline sama.

    Aturan semantik (F3):
    - camera_queue_count (KENDARAAN) -> queue_vehicles. TIDAK PERNAH ke cm.
    - queue_estimate_cm kanonis tidak ada -> 0, jangan mengarang.
    - recommended_green_s (fuzzy) vs effective_green_s (aktual) dipisah;
      green_duration_s = effective (kompat dashboard lama = durasi aktual).
    - vehicle_count_valid/source dipertahankan; stale -> source vision_stale.
    - distance_cm = ultrasonic, dipertahankan.
    - vision: state + fresh_lane_count + {lane}_fresh (BUKAN vision[fresh]).
    """
    parts = topic.split("/")
    intersection_id = parts[3] if len(parts) > 3 else payload.get("intersection_id")
    controller_id = parts[5] if len(parts) > 5 else payload.get("controller_id")
    mode = payload.get("mode") or {}
    vision = payload.get("vision") or {}
    fresh_lanes = normalize_int(vision.get("fresh_lane_count"), 0)
    lane_fresh = {
        "north": normalize_bool(vision.get("north_fresh"), False),
        "south": normalize_bool(vision.get("south_fresh"), False),
        "east": normalize_bool(vision.get("east_fresh"), False),
    }
    flat: Dict[str, Any] = {
        "schema_version": payload.get("schema_version", 1),
        "intersection_id": intersection_id,
        "device_id": controller_id,
        "device": controller_id,
        "timestamp": payload.get("timestamp"),
        "wifi_rssi": payload.get("wifi_rssi", 0),
        "uptime_s": payload.get("uptime_s", 0),
        "config_version": payload.get("config_version", 0),
        "sig_state": payload.get("sig_state", ""),
        "firmware_version": payload.get("firmware_version", ""),
        "active_lane": payload.get("active_lane", ""),
        "vehicle_count_source": "camera",
        "sensor_mode": True,
        "dummy_mode": False,
        "auto_mode": normalize_bool(mode.get("auto"), True),
        "adaptive_mode": normalize_bool(mode.get("adaptive"), True),
        "mode_degraded": normalize_bool(mode.get("degraded"), False),
        "vision_state": str(vision.get("state") or "UNKNOWN"),
        "vision_fresh_lanes": fresh_lanes,
        "vision_fresh": fresh_lanes > 0,
    }
    approaches = payload.get("approaches") or {}
    for lane in ("north", "south", "east"):
        st = approaches.get(lane)
        if not isinstance(st, dict):
            st = {}
        flat[f"{lane}_vehicle_count"] = st.get("camera_vehicle_count", 0)
        flat[f"{lane}_queue_vehicles"] = st.get("camera_queue_count", 0)
        flat[f"{lane}_queue_estimate_cm"] = 0  # kanonis tak punya cm; jangan karang
        flat[f"{lane}_count_valid"] = st.get("vehicle_count_valid", False)
        flat[f"{lane}_count_source"] = st.get("vehicle_count_source", "")
        flat[f"{lane}_vision_fresh"] = lane_fresh.get(lane, False)
        flat[f"{lane}_vehicle_detected"] = st.get("ir_occupied", False)
        flat[f"{lane}_distance_cm"] = st.get("distance_cm", 0)
        flat[f"{lane}_density_level"] = st.get("sensor_level", 0)
        flat[f"{lane}_queue_detected"] = st.get("sensor_level", 0) >= 2
        flat[f"{lane}_light"] = st.get("light", "red")
        flat[f"{lane}_ultrasonic_detected"] = st.get("ultrasonic_occupied", False)
        if st.get("recommended_green_s") is not None:
            flat[f"{lane}_recommended_green_s"] = st.get("recommended_green_s")
        eff = st.get("effective_green_s", st.get("green_duration_s"))
        if eff is not None:
            flat[f"{lane}_green_duration_s"] = eff
            flat[f"{lane}_effective_green_s"] = eff
    return flat


_vision_last_saved: Dict[str, Any] = {}
# E3: controller v2 mengirim kanonis + legacy untuk heartbeat yang sama.
# Prefer kanonis; buang legacy bila kanonis controller tsb terlihat <= 6 dtk.
_canon_last_seen: Dict[str, float] = {}
DEDUP_WINDOW_S = 6.0


def mark_canonical_seen(intersection_id: str, device_id: str, now: float) -> None:
    _canon_last_seen[f"{intersection_id}|{device_id}"] = now


def should_drop_legacy(intersection_id: str, device_id: str, now: float) -> bool:
    """E3: buang legacy bila kanonis controller yang sama terlihat dalam window."""
    return now - _canon_last_seen.get(f"{intersection_id}|{device_id}", 0) <= DEDUP_WINDOW_S


def should_persist_vision(intersection_id: str, payload: Dict[str, Any]) -> bool:
    """Persistensi sampled §74: berubah bermakna ATAU tiap VISION_SAMPLE_SECONDS."""
    import time as _time

    now = time.time()
    key = str(intersection_id)
    prev = _vision_last_saved.get(key)
    summary = json.dumps(payload.get("approaches", {}), sort_keys=True)
    if prev is None:
        _vision_last_saved[key] = {"at": now, "summary": summary}
        return True
    if summary != prev["summary"]:
        _vision_last_saved[key] = {"at": now, "summary": summary}
        return True
    if now - prev["at"] >= VISION_SAMPLE_SECONDS:
        _vision_last_saved[key] = {"at": now, "summary": summary}
        return True
    return False


def process_vision_metrics(topic: str, payload: Dict[str, Any]) -> None:
    parts = topic.split("/")
    intersection_id = parts[3] if len(parts) > 3 else payload.get("intersection_id")
    if not should_persist_vision(str(intersection_id), payload):
        return
    document = build_document({
        "device_id": f"VISION_{intersection_id}",
        "device": f"VISION_{intersection_id}",
        "intersection_id": intersection_id,
        "timestamp": payload.get("generated_at"),
        "source": "astraea-vision-sampled",
        "vision_metrics": payload.get("approaches", {}),
    })
    try:
        s3_key = save_to_s3(document)
        print(f"Sampled vision persisted: {intersection_id} -> {s3_key}")
    except Exception as exc:
        print("ERROR persisting vision sample:", exc)


def process_payload(payload_text: str, topic: str = "") -> None:
    print("\nMQTT message received:")
    if topic:
        print(f"MQTT topic: {topic}")
    print(payload_text)

    try:
        payload = json.loads(payload_text)
    except json.JSONDecodeError as exc:
        print("JSON decode error:", exc)
        return

    if not isinstance(payload, dict):
        print("Payload bukan JSON object.")
        return

    device_id_from_topic = extract_device_id_from_topic(topic)

    if device_id_from_topic:
        key = f"{payload.get('intersection_id', '')}|{device_id_from_topic}"
        if should_drop_legacy(
            str(payload.get("intersection_id", "")), device_id_from_topic, time.time()
        ):
            print(f"Dedup: legacy {device_id_from_topic} dibuang (kanonis fresh).")
            return
        payload["device_id"] = payload.get("device_id") or device_id_from_topic
        payload["device"] = payload.get("device") or device_id_from_topic
    elif is_canonical_telemetry_topic(topic):
        print("Canonical telemetry -> normalisasi ke pipeline legacy.")
        payload = normalize_canonical_telemetry(topic, payload)
        mark_canonical_seen(
            str(payload.get("intersection_id", "")),
            str(payload.get("device_id", "")),
            time.time(),
        )
    elif is_canonical_vision_topic(topic):
        process_vision_metrics(topic, payload)
        return

    try:
        document = build_document(payload)

        save_to_dynamodb(document)
        print("Saved to DynamoDB TrafficTelemetry:")
        print(f"  id={document.get('id')}")
        print(f"  intersection_id={document.get('intersection_id')}")
        print(f"  device_id={document.get('device_id')}")
        print(f"  timestamp={document.get('timestamp')}")

        save_device_status(document)
        print("Saved to DynamoDB DeviceStatus:")
        print(f"  device_id={document.get('device_id')}")
        print(f"  last_seen={document.get('received_at_utc')}")

        create_notifications_from_telemetry(document)

        s3_key = save_to_s3(document)
        if s3_key:
            print("Saved to S3:")
            print(f"  {s3_key}")

    except ClientError as exc:
        print("AWS ClientError:", exc)

    except Exception as exc:
        print("ERROR while processing payload:", exc)


def on_connect(client, userdata, flags, reason_code, properties=None):
    print("MQTT connected")
    print(f"Reason code: {reason_code}")
    print(f"Subscribing to topic: {MQTT_TOPIC}")
    print(f"Subscribing to topic: {MQTT_TOPIC_V1_TELEMETRY}")
    print(f"Subscribing to topic: {MQTT_TOPIC_V1_VISION}")

    client.subscribe(MQTT_TOPIC, qos=0)
    client.subscribe(MQTT_TOPIC_V1_TELEMETRY, qos=0)
    client.subscribe(MQTT_TOPIC_V1_VISION, qos=0)


def on_disconnect(client, userdata, disconnect_flags, reason_code, properties=None):
    print("MQTT disconnected")
    print(f"Reason code: {reason_code}")


def on_message(client, userdata, msg):
    try:
        payload_text = msg.payload.decode("utf-8", errors="replace")
        process_payload(payload_text, msg.topic)
    except Exception as exc:
        print("ERROR while processing MQTT message:", exc)


def main():
    print("=======================================")
    print("Traffic MQTT Subscriber AWS")
    print("MQTT -> DynamoDB TrafficTelemetry")
    print("MQTT -> DynamoDB DeviceStatus")
    print("MQTT -> DynamoDB Notifications")
    print("MQTT -> Telegram Alert")
    print("MQTT -> Email Alert")
    print("MQTT -> S3 Data Lake")
    print("=======================================")
    print(f"MQTT_HOST={MQTT_HOST}")
    print(f"MQTT_PORT={MQTT_PORT}")
    print(f"MQTT_TOPIC={MQTT_TOPIC}")
    print(f"MQTT_TOPIC_V1_TELEMETRY={MQTT_TOPIC_V1_TELEMETRY}")
    print(f"MQTT_TOPIC_V1_VISION={MQTT_TOPIC_V1_VISION} (sampled {VISION_SAMPLE_SECONDS}s)")
    print(f"AWS_REGION={AWS_REGION}")
    print(f"DYNAMODB_TABLE={DYNAMODB_TABLE}")
    print(f"DYNAMODB_DEVICE_STATUS_TABLE={DYNAMODB_DEVICE_STATUS_TABLE}")
    print(f"DYNAMODB_NOTIFICATIONS_TABLE={DYNAMODB_NOTIFICATIONS_TABLE}")
    print(f"DYNAMODB_USERS_TABLE={DYNAMODB_USERS_TABLE}")
    print(f"DEFAULT_NOTIFICATION_USER_ID={DEFAULT_NOTIFICATION_USER_ID}")
    print(f"TELEGRAM_BOT_TOKEN={'set' if TELEGRAM_BOT_TOKEN else 'empty'}")
    print(f"TELEGRAM_CHAT_ID={'set' if TELEGRAM_CHAT_ID else 'empty'}")
    print(f"SMTP_HOST={SMTP_HOST or 'empty'}")
    print(f"SMTP_PORT={SMTP_PORT}")
    print(f"SMTP_USER={'set' if SMTP_USER else 'empty'}")
    print(f"SMTP_PASS={'set' if SMTP_PASS else 'empty'}")
    print(f"SMTP_FROM={SMTP_FROM or 'empty'}")
    print(f"S3_BUCKET={S3_BUCKET}")
    print(f"S3_BASE_PREFIX={S3_BASE_PREFIX}")
    print("=======================================")

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

    if MQTT_USER and MQTT_PASS:
        client.username_pw_set(MQTT_USER, MQTT_PASS)

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message

    while True:
        try:
            print("Connecting to MQTT broker...")
            client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
            client.loop_forever()

        except KeyboardInterrupt:
            print("Stopped by user")
            try:
                client.disconnect()
            except Exception:
                pass
            break

        except Exception as exc:
            print("MQTT connection error:", exc)
            print("Retrying in 5 seconds...")
            time.sleep(5)


if __name__ == "__main__":
    main()
