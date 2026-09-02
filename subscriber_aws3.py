import json
import os
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

import boto3
import paho.mqtt.client as mqtt
from botocore.exceptions import ClientError
from dotenv import load_dotenv


load_dotenv()

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USER = os.getenv("MQTT_USER", "")
MQTT_PASS = os.getenv("MQTT_PASS", "")
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "traffic/data")

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

_notification_cache: Dict[str, float] = {}
_user_cache: Dict[str, Any] = {"expired_at": 0, "user_ids": []}

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


def get_active_user_ids() -> List[str]:
    now = time.time()

    if _user_cache["user_ids"] and now < _user_cache["expired_at"]:
        return _user_cache["user_ids"]

    try:
        items: List[Dict[str, Any]] = []
        exclusive_start_key = None

        while True:
            params: Dict[str, Any] = {
                "ProjectionExpression": "id, #status",
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

        user_ids = [
            str(item["id"])
            for item in items
            if item.get("id") and str(item.get("status", "active")).lower() == "active"
        ]

        if not user_ids:
            user_ids = ["user-001"]

        _user_cache["user_ids"] = user_ids
        _user_cache["expired_at"] = now + 60

        return user_ids

    except Exception as exc:
        print("Failed to fetch active users:", exc)
        return ["user-001"]


def get_notification_target_user_ids() -> List[str]:
    if DEFAULT_NOTIFICATION_USER_ID.strip().lower() == "all":
        return get_active_user_ids()

    return [DEFAULT_NOTIFICATION_USER_ID.strip() or "user-001"]


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
    target_user_ids = get_notification_target_user_ids()

    for user_id in target_user_ids:
        notification_id = f"notif_{uuid.uuid4().hex[:12]}"

        item = {
            "user_id": user_id,
            "created_at": created_at,
            "notification_id": notification_id,
            "id": notification_id,
            "type": severity,
            "severity": severity,
            "category": category,
            "title": title,
            "message": message,
            "read": False,
            "actionUrl": action_url,
            "relatedTo": document.get("id"),
            "intersection_id": document.get("intersection_id"),
            "device_id": document.get("device_id"),
            "lane": lane,
            "metadata": metadata or {},
            "createdAt": created_at,
            "updatedAt": created_at,
        }

        notifications_table.put_item(Item=convert_float_to_decimal(item))

        print("Saved to DynamoDB Notifications:")
        print(f"  user_id={user_id}")
        print(f"  notification_id={notification_id}")
        print(f"  title={title}")


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
                save_notification(
                    document=document,
                    notif_type="queue_level_2",
                    severity="critical",
                    category="traffic",
                    lane=lane,
                    title=f"Antrean Padat Jalur {lane_labels[lane]}",
                    message=(
                        f"Jalur {lane_labels[lane]} pada {intersection_id} mencapai Queue Level {level}. "
                        f"Estimasi antrean {queue_estimate_cm} cm dan sekitar {queue_vehicles} kendaraan."
                    ),
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


def process_payload(payload_text: str) -> None:
    print("\nMQTT message received:")
    print(payload_text)

    try:
        payload = json.loads(payload_text)
    except json.JSONDecodeError as exc:
        print("JSON decode error:", exc)
        return

    if not isinstance(payload, dict):
        print("Payload bukan JSON object.")
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

    client.subscribe(MQTT_TOPIC, qos=0)


def on_disconnect(client, userdata, disconnect_flags, reason_code, properties=None):
    print("MQTT disconnected")
    print(f"Reason code: {reason_code}")


def on_message(client, userdata, msg):
    try:
        payload_text = msg.payload.decode("utf-8", errors="replace")
        process_payload(payload_text)
    except Exception as exc:
        print("ERROR while processing MQTT message:", exc)


def main():
    print("=======================================")
    print("Traffic MQTT Subscriber AWS")
    print("MQTT -> DynamoDB TrafficTelemetry")
    print("MQTT -> DynamoDB DeviceStatus")
    print("MQTT -> DynamoDB Notifications")
    print("MQTT -> S3 Data Lake")
    print("=======================================")
    print(f"MQTT_HOST={MQTT_HOST}")
    print(f"MQTT_PORT={MQTT_PORT}")
    print(f"MQTT_TOPIC={MQTT_TOPIC}")
    print(f"AWS_REGION={AWS_REGION}")
    print(f"DYNAMODB_TABLE={DYNAMODB_TABLE}")
    print(f"DYNAMODB_DEVICE_STATUS_TABLE={DYNAMODB_DEVICE_STATUS_TABLE}")
    print(f"DYNAMODB_NOTIFICATIONS_TABLE={DYNAMODB_NOTIFICATIONS_TABLE}")
    print(f"DYNAMODB_USERS_TABLE={DYNAMODB_USERS_TABLE}")
    print(f"DEFAULT_NOTIFICATION_USER_ID={DEFAULT_NOTIFICATION_USER_ID}")
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
