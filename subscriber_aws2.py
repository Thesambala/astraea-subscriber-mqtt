import json
import os
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Optional

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import paho.mqtt.client as mqtt


load_dotenv()

# =========================
# MQTT CONFIG
# =========================
MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USER = os.getenv("MQTT_USER", "")
MQTT_PASS = os.getenv("MQTT_PASS", "")
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "traffic/data")

# =========================
# AWS CONFIG
# =========================
AWS_REGION = os.getenv("AWS_REGION", "ap-southeast-2")

DYNAMODB_TABLE = os.getenv("DYNAMODB_TABLE", "TrafficTelemetry")
DYNAMODB_DEVICE_STATUS_TABLE = os.getenv(
    "DYNAMODB_DEVICE_STATUS_TABLE",
    "DeviceStatus"
)

S3_BUCKET = os.getenv("S3_BUCKET", "")
S3_BASE_PREFIX = os.getenv("S3_BASE_PREFIX", "traffic/raw")

# =========================
# AWS CLIENTS
# =========================
dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
traffic_table = dynamodb.Table(DYNAMODB_TABLE)
device_status_table = dynamodb.Table(DYNAMODB_DEVICE_STATUS_TABLE)

s3 = boto3.client("s3", region_name=AWS_REGION)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def safe_get(payload: Dict[str, Any], key: str, default: Any = None) -> Any:
    value = payload.get(key)
    if value is None:
        return default
    return value


def convert_float_to_decimal(obj: Any) -> Any:
    """
    DynamoDB boto3 tidak menerima float native.
    Jadi semua float dikonversi ke Decimal.
    """
    if isinstance(obj, float):
        return Decimal(str(obj))

    if isinstance(obj, dict):
        return {k: convert_float_to_decimal(v) for k, v in obj.items()}

    if isinstance(obj, list):
        return [convert_float_to_decimal(v) for v in obj]

    return obj


def normalize_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value

    if isinstance(value, str):
        lower = value.strip().lower()
        if lower in ["true", "1", "on", "yes"]:
            return True
        if lower in ["false", "0", "off", "no"]:
            return False

    if isinstance(value, int):
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
    """
    Bentuk dokumen final untuk TrafficTelemetry.

    Partition key DynamoDB:
      intersection_id

    Sort key DynamoDB:
      timestamp
    """
    now = utc_now()
    timestamp = payload.get("timestamp") or payload.get("received_at_utc") or now.isoformat()

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
        # Primary identifiers
        "id": doc_id,
        "intersection_id": intersection_id,
        "device_id": device_id,
        "device": payload.get("device") or device_id,

        # DynamoDB sort key
        "timestamp": timestamp,
        "received_at_utc": now.isoformat(),

        # Source metadata
        "source": "mqtt_mosquitto_aws_ec2",

        # System state
        "wifi_rssi": normalize_int(payload.get("wifi_rssi"), 0),
        "uptime_s": normalize_int(payload.get("uptime_s"), 0),
        "dummy_mode": normalize_bool(payload.get("dummy_mode"), False),
        "sensor_mode": normalize_bool(payload.get("sensor_mode"), False),

        # Traffic timing config
        "green_time_s": normalize_int(payload.get("green_time_s"), 10),
        "yellow_time_s": normalize_int(payload.get("yellow_time_s"), 3),
        "density_level_0_green_s": normalize_int(
            payload.get("density_level_0_green_s"),
            10,
        ),
        "density_level_1_green_s": normalize_int(
            payload.get("density_level_1_green_s"),
            20,
        ),
        "density_level_2_green_s": normalize_int(
            payload.get("density_level_2_green_s"),
            30,
        ),
        "auto_mode": normalize_bool(payload.get("auto_mode"), True),
        "adaptive_mode": normalize_bool(payload.get("adaptive_mode"), True),
    }

    # Lane fields: north, south, east
    for lane in ["north", "south", "east"]:
        density_level = normalize_int(payload.get(f"{lane}_density_level"), 0)
        vehicle_detected = normalize_bool(
            payload.get(f"{lane}_vehicle_detected"),
            density_level >= 1,
        )
        queue_detected = normalize_bool(
            payload.get(f"{lane}_queue_detected"),
            density_level >= 2,
        )

        document[f"{lane}_vehicle_count"] = normalize_int(
            payload.get(f"{lane}_vehicle_count"),
            0,
        )
        document[f"{lane}_vehicle_detected"] = vehicle_detected
        document[f"{lane}_density_level"] = density_level
        document[f"{lane}_queue_detected"] = queue_detected
        document[f"{lane}_queue_estimate_cm"] = normalize_int(
            payload.get(f"{lane}_queue_estimate_cm"),
            0,
        )
        document[f"{lane}_light"] = normalize_light(payload.get(f"{lane}_light"))
        document[f"{lane}_green_duration_s"] = normalize_int(
            payload.get(f"{lane}_green_duration_s"),
            document["green_time_s"],
        )

    return document


def save_to_dynamodb(document: Dict[str, Any]) -> None:
    """
    Simpan full telemetry ke table TrafficTelemetry.
    """
    item = convert_float_to_decimal(document)

    traffic_table.put_item(Item=item)


def build_device_status(document: Dict[str, Any]) -> Dict[str, Any]:
    """
    Bentuk item untuk table DeviceStatus.
    Table ini hanya menyimpan status terakhir device.
    """
    device_id = document.get("device_id", "ESP32_TRAFFIC_01")
    intersection_id = document.get("intersection_id", "SIMPANG_TALUN_01")

    status_item = {
        "device_id": device_id,
        "intersection_id": intersection_id,
        "device": document.get("device", device_id),

        "status": "online",
        "last_seen": document.get("received_at_utc", utc_now_iso()),
        "timestamp": document.get("timestamp", utc_now_iso()),

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
        "updated_at": utc_now_iso(),
    }

    # Ringkasan lane terakhir
    for lane in ["north", "south", "east"]:
        status_item[f"{lane}_vehicle_count"] = document.get(
            f"{lane}_vehicle_count",
            0,
        )
        status_item[f"{lane}_density_level"] = document.get(
            f"{lane}_density_level",
            0,
        )
        status_item[f"{lane}_light"] = document.get(
            f"{lane}_light",
            "red",
        )
        status_item[f"{lane}_queue_estimate_cm"] = document.get(
            f"{lane}_queue_estimate_cm",
            0,
        )
        status_item[f"{lane}_green_duration_s"] = document.get(
            f"{lane}_green_duration_s",
            0,
        )

    return status_item


def save_device_status(document: Dict[str, Any]) -> None:
    """
    Upsert status terakhir device ke table DeviceStatus.
    """
    item = build_device_status(document)
    item = convert_float_to_decimal(item)

    device_status_table.put_item(Item=item)


def save_to_s3(document: Dict[str, Any]) -> Optional[str]:
    """
    Simpan raw JSON ke S3 sebagai data lake.

    S3 tidak bisa append file seperti filesystem biasa,
    jadi kita simpan 1 object JSON per message.

    Path:
      traffic/raw/year=2026/month=06/day=04/events/<id>.json
    """
    if not S3_BUCKET:
        return None

    now = utc_now()

    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")

    document_id = document.get("id") or f"event_{int(time.time() * 1000)}"

    key = (
        f"{S3_BASE_PREFIX}/year={year}/month={month}/day={day}/events/"
        f"{document_id}.json"
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
    """
    Proses satu pesan MQTT.
    """
    print("\nMQTT message received:")
    print(payload_text)

    try:
        payload = json.loads(payload_text)
    except json.JSONDecodeError as exc:
        print("JSON decode error:", exc)
        return

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

    s3_key = save_to_s3(document)
    if s3_key:
        print("Saved to S3:")
        print(f"  {s3_key}")


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
    print("MQTT → DynamoDB TrafficTelemetry")
    print("MQTT → DynamoDB DeviceStatus")
    print("MQTT → S3 Data Lake")
    print("=======================================")
    print(f"MQTT_HOST={MQTT_HOST}")
    print(f"MQTT_PORT={MQTT_PORT}")
    print(f"MQTT_TOPIC={MQTT_TOPIC}")
    print(f"AWS_REGION={AWS_REGION}")
    print(f"DYNAMODB_TABLE={DYNAMODB_TABLE}")
    print(f"DYNAMODB_DEVICE_STATUS_TABLE={DYNAMODB_DEVICE_STATUS_TABLE}")
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
