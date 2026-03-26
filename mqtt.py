"""
    Multi-Machine MQTT Publisher / Subscriber
    ==================================================================================================
    Author  :   Yogesh S Nanda
    Date    :   21/06/2023  (updated 2026-03-25)
    Version :   4.0
    Notes   :   Publishes fixed-value data for 10 machine types (ASRS × 10, CNC, IMM, LC, MR,
                PM, QI, RA, SM, VI).  Templates loaded from Machines/ sub-folders.
                Flow: publish → machine/<ID>/<subtopic>
                      subscribe → forward to dashboard/<ID>/<subtopic>
    Licence:   Nanda Dynamics Pvt.Ltd
    ==================================================================================================
"""

import copy
import json
import pathlib
import time
from dataclasses import dataclass, field
from threading import Lock
from typing import Dict, List, Optional, Tuple
from uuid import uuid4

import paho.mqtt.client as mqtt

# ---------------------------------------------------------------------------
# Root folder that contains all machine sub-folders
# ---------------------------------------------------------------------------
MACHINES_ROOT = pathlib.Path(__file__).parent / "Machines "

# ---------------------------------------------------------------------------
# Machine configurations
# Each entry: machine_type → {folder, id_field, num_machines, subtopics}
#   subtopics: {json_file_stem → mqtt_subtopic_name}
# ---------------------------------------------------------------------------
MACHINE_CONFIGS: Dict[str, dict] = {

    "ASRS": {
        "folder":       "ASRS",
        "id_field":     "asrsId",
        "num_machines": 1,
        "subtopics": {
            "ASRS_Telemetry":                        "telemetry",
            "ASRS_Alarm":                            "alarm",
            "ASRS_Health & Maintenance":             "health",
            "ASRS_Task Execution":                   "task",
            "ASRS_Storage & Inventory Data":         "storage",
            "ASRS_Throughput & Performance Metrics": "performance",
            "ASRS_Order Fulfillment & Traceability": "order",
            "ASRS_Integration":                      "integration",
        },
    },

    "CNC": {
        "folder":       "CNC",
        "id_field":     "machineId",
        "num_machines": 1,
        "subtopics": {
            "cnc01_alarm":     "alarm",
            "cnc01_health":    "health",
            "cnc01_telemetry": "telemetry",
        },
    },

    "IMM": {
        "folder":       "Injection Moulding",
        "id_field":     "machineId",
        "num_machines": 1,
        "subtopics": {
            "IMM_Alarm":                      "alarm",
            "IMM_Cycle Breakdown":            "cycle_breakdown",
            "IMM_HealthHealth & Maintenance": "health",
            "IMM_Integration":                "integration",
            "IMM_Production & Quality Data":  "production",
            "IMM_Telemetry":                  "telemetry",
        },
    },

    "LC": {
        "folder":       "Laser Cutter",
        "id_field":     "machineId",
        "num_machines": 1,
        "subtopics": {
            "LC_Alarm":                   "alarm",
            "LC_Cutting Path & Job Data": "cutting",
            "LC_Health & Maintenance":    "health",
            "LC_Integration":             "integration",
            "LC_Quality Metrics":         "quality",
            "LC_Telemetry":               "telemetry",
        },
    },

    "MR": {
        "folder":       "Mobile Robot",
        "id_field":     "robotId",
        "num_machines": 1,
        "subtopics": {
            "MR_Alarm":                       "alarm",
            "MR_Fleet & Charging Management": "fleet",
            "MR_Integration":                 "integration",
            "MR_Navigation & Path Planning":  "navigation",
            "MR_Sensor Data":                 "sensor",
            "MR_Task & Material Transport":   "task",
            "MR_Telemetry":                   "telemetry",
        },
    },

    "PM": {
        "folder":       "Packaging Machine",
        "id_field":     "machineId",
        "num_machines": 1,
        "subtopics": {
            "PM_Alarm":                        "alarm",
            "PM_Batch & Traceability":         "batch",
            "PM_Machine Health & Maintenance": "health",
            "PM_Packaging Quality Metrics":    "quality",
            "PM_Production & Efficiency Data": "production",
            "PM_Telemetry":                    "telemetry",
            "PM_Workflow Integration":         "integration",
        },
    },

    "QI": {
        "folder":       "Quality Inspection",
        "id_field":     "inspectionId",
        "num_machines": 1,
        "subtopics": {
            "QI_Defect Detection":         "defect",
            "QI_Feedback Loop":            "feedback",
            "QI_Integration":              "integration",
            "QI_Measurement Data":         "measurement",
            "QI_Quality Metrics & Trends": "quality",
            "QI_Telemetry":                "telemetry",
        },
    },

    "RA": {
        "folder":       "Robotic Arm",
        "id_field":     "robotId",
        "num_machines": 1,
        "subtopics": {
            "RA_Alarm":                    "alarm",
            "RA_Health & Maintenance":     "health",
            "RA_Integration":              "integration",
            "RA_Path & Motion Data":       "path",
            "RA_Task & Material Handling": "task",
            "RA_Telemetry":                "telemetry",
        },
    },

    "SM": {
        "folder":       "Strapping Machine",
        "id_field":     "machineId",
        "num_machines": 1,
        "subtopics": {
            "SM_Alarm":                        "alarm",
            "SM_Machine Health & Maintenance": "health",
            "SM_Packaging & Dispatch Context": "dispatch",
            "SM_Production & Throughput Data": "production",
            "SM_Telemetry":                    "telemetry",
            "SM_Workflow Integration":         "integration",
            "SM_strap Quality & Validation":   "quality",
        },
    },

    "VI": {
        "folder":       "Vision Inspection",
        "id_field":     "inspectionId",
        "num_machines": 1,
        "subtopics": {
            "VI_AI Inference Result":       "ai_inference",
            "VI_Defect Detection":          "defect",
            "VI_Feedback Loop":             "feedback",
            "VI_Image & Traceability Data": "image",
            "VI_Integration":               "integration",
            "VI_Quality Analytics":         "quality",
            "VI_Telemetry":                 "telemetry",
        },
    },
}


# ---------------------------------------------------------------------------
# Fixed values for ASRS machines (index 1-10)
# All other machine types use their JSON template values as-is (already fixed)
# ---------------------------------------------------------------------------
ASRS_FIXED_VALUES = {
    1: {"x": 5, "y": 3, "z": 4, "kw": 22.5, "volt": 415.0, "amp": 32.0, "mode": "AUTOMATIC", "health": 92, "crane_h": 90, "shuttle_h": 94, "sensor_h": 91, "rem_hrs": 300, "store_r": 120, "retr_r": 140, "cycle": 22, "util": 78, "occupied": 720, "progress": 65, "pickup": True, "alarm_sev": "LOW"},
}


# ---------------------------------------------------------------------------
# Template loading
# ---------------------------------------------------------------------------

def load_all_templates() -> Dict[str, Dict[str, dict]]:
    """Load JSON templates for all machine types.
    Returns {machine_type: {subtopic: template_dict}}.
    """
    all_templates: Dict[str, Dict[str, dict]] = {}
    for mtype, cfg in MACHINE_CONFIGS.items():
        folder = MACHINES_ROOT / cfg["folder"]
        templates: Dict[str, dict] = {}
        for stem, subtopic in cfg["subtopics"].items():
            path = folder / f"{stem}.json"
            with open(path, encoding="utf-8") as fh:
                templates[subtopic] = json.load(fh)
        all_templates[mtype] = templates
        print(f"  [{mtype}] loaded {len(templates)} subtopics: {list(templates.keys())}")
    return all_templates


# ---------------------------------------------------------------------------
# Payload builder — applies fixed values and updates id + timestamp
# ---------------------------------------------------------------------------

def _build_payload(base: dict, machine_type: str, id_field: str,
                   machine_id: str, machine_num: int) -> dict:
    """Deep-copy template and stamp with correct machine ID and timestamp.
    For ASRS, also applies the per-machine FIXED_VALUES overrides.
    All other types use the template values as-is (already fixed).
    """
    d = copy.deepcopy(base)
    d[id_field]    = machine_id
    d["timestamp"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    if machine_type == "ASRS":
        fv = ASRS_FIXED_VALUES[machine_num]

        if "crane" in d:
            d["crane"]["craneId"]       = f"CRANE_{machine_num:02d}"
            d["crane"]["position"]["x"] = fv["x"]
            d["crane"]["position"]["y"] = fv["y"]
            d["crane"]["position"]["z"] = fv["z"]
        if "energy" in d:
            d["energy"]["consumptionKW"] = fv["kw"]
            d["energy"]["voltage"]       = fv["volt"]
            d["energy"]["current"]       = fv["amp"]
        if "operation" in d:
            d["operation"]["mode"] = fv["mode"]
        if "health" in d:
            d["health"].update({k: fv[v] for k, v in {
                "overallHealthScore": "health",
                "craneHealth":        "crane_h",
                "shuttleHealth":      "shuttle_h",
                "sensorHealth":       "sensor_h",
            }.items() if k in d["health"]})
        if "maintenance" in d and "predictedFailure" in d["maintenance"]:
            d["maintenance"]["predictedFailure"]["remainingHours"] = fv["rem_hrs"]
        if "performance" in d:
            d["performance"]["storageRatePerHour"]   = fv["store_r"]
            d["performance"]["retrievalRatePerHour"] = fv["retr_r"]
            d["performance"]["averageCycleTimeSec"]  = fv["cycle"]
            d["performance"]["utilizationPercent"]   = fv["util"]
        if "capacity" in d:
            total = d["capacity"].get("totalBins", 1000)
            d["capacity"]["occupiedBins"]  = fv["occupied"]
            d["capacity"]["availableBins"] = total - fv["occupied"]
        if "taskProgress" in d:
            d["taskProgress"]["progressPercent"] = fv["progress"]
            d["taskProgress"]["pickupCompleted"]  = fv["pickup"]
        if "alarm" in d:
            d["alarm"]["severity"] = fv["alarm_sev"]

    return d


# ---------------------------------------------------------------------------
# MqttClient dataclass
# ---------------------------------------------------------------------------

@dataclass
class MqttClient:
    broker: str
    port: int
    client_id: str = field(default_factory=lambda: str(uuid4()))
    username: str = ""
    password: str = ""
    connected: bool = False
    message: str = "None"
    client: Optional[mqtt.Client] = None
    rcv_msg_time: float = field(default_factory=time.time)
    rcv_msg_timeout: float = 1.0
    subscribed_topics: List[str] = field(default_factory=list)
    _lock: Lock = field(default_factory=Lock, repr=False)

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print(f"{self.client_id}: Connected to MQTT Server!")
            self.connected = True
        else:
            print(f"{self.client_id}: Failed to connect, return code {rc}")
            self.connected = False

    def on_disconnect(self, client, userdata, rc):
        self.connected = False
        print(f"{self.client_id}: Disconnected from MQTT Server!")

    def on_message(self, client, userdata, msg):
        with self._lock:
            self.message = msg.payload.decode()
            self.rcv_msg_time = time.time()
        # Forward machine/<ID>/subtopic → dashboard/<ID>/subtopic
        dashboard_topic = msg.topic.replace("machine/", "dashboard/", 1)
        client.publish(dashboard_topic, msg.payload, qos=1)

    def connect(self):
        try:
            self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id=self.client_id)
        except AttributeError:
            self.client = mqtt.Client(client_id=self.client_id)
        self.client.username_pw_set(self.username, self.password)
        self.client.on_connect    = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message    = self.on_message
        self.client.connect(self.broker, self.port, keepalive=60)

    def disconnect(self):
        if self.client:
            self.client.disconnect()
            print(f"{self.client_id}: Disconnect request sent")

    def service_start(self):
        if self.client:
            self.client.loop_start()

    def service_stop(self):
        if self.client:
            self.client.loop_stop()

    def publish(self, topic, message, qos=1):
        if not self.connected or not self.client:
            return
        self.client.publish(topic, message, qos=qos)

    def subscribe(self, topic, qos=1):
        if not self.connected or not self.client:
            return "None"
        self.client.subscribe(topic, qos=qos)
        with self._lock:
            age = time.time() - self.rcv_msg_time
            if age > self.rcv_msg_timeout:
                return "None"
            return self.message


# ---------------------------------------------------------------------------
# Client factory — creates one client per machine instance across all types
# Returns a flat list of (machine_type, machine_id, machine_num, subtopics, client)
# ---------------------------------------------------------------------------

MachineEntry = Tuple[str, str, int, List[str], MqttClient]

def create_all_clients(broker: str, port: int) -> List[MachineEntry]:
    """Create one MQTT client per machine instance for every configured type."""
    entries: List[MachineEntry] = []

    for mtype, cfg in MACHINE_CONFIGS.items():
        subtopic_names = list(cfg["subtopics"].values())
        num            = cfg["num_machines"]

        for i in range(1, num + 1):
            machine_id = f"{mtype}_{i:02d}"
            client     = MqttClient(broker=broker, port=port)
            client.connect()
            client.service_start()

            topics = [f"machine/{machine_id}/{sub}" for sub in subtopic_names]
            client.subscribed_topics = topics
            for topic in topics:
                client.subscribe(topic)

            entries.append((mtype, machine_id, i, subtopic_names, client))

    return entries


# ---------------------------------------------------------------------------
# Publisher — one round of fixed-value payloads for all machines / subtopics
# ---------------------------------------------------------------------------

def publish_all_data(entries: List[MachineEntry],
                     all_templates: Dict[str, Dict[str, dict]]):
    total = 0
    for mtype, machine_id, machine_num, subtopics, client in entries:
        if not client.connected:
            print(f"[{machine_id}] skipped (not connected)")
            continue
        cfg       = MACHINE_CONFIGS[mtype]
        id_field  = cfg["id_field"]
        templates = all_templates[mtype]

        for subtopic in subtopics:
            payload = _build_payload(templates[subtopic], mtype, id_field,
                                     machine_id, machine_num)
            topic   = f"machine/{machine_id}/{subtopic}"
            client.publish(topic, json.dumps(payload), qos=1)
            total  += 1

    print(f"[publish] {total} messages sent across {len(entries)} machine instances")


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run_loop(entries: List[MachineEntry],
             all_templates: Dict[str, Dict[str, dict]],
             interval: float = 2.0):
    try:
        while True:
            publish_all_data(entries, all_templates)
            time.sleep(interval)
            for _, machine_id, _, _, client in entries:
                with client._lock:
                    if client.message != "None":
                        try:
                            msg = json.loads(client.message)
                            ts  = msg.get("timestamp", "")[-8:]
                            print(f"[{machine_id}] fwd → dashboard/{machine_id}/... | ts={ts}")
                        except Exception:
                            print(f"[{machine_id}] raw: {client.message[:80]}")
                        client.message = "None"
    except KeyboardInterrupt:
        print("\nInterrupted by user")


def stop_clients(entries: List[MachineEntry]):
    for _, _, _, _, client in entries:
        client.service_stop()
        client.disconnect()


def main():
    broker = '127.0.0.1'
    port   = 1883

    print("Loading JSON templates for all machine types...")
    all_templates = load_all_templates()

    total_instances = sum(cfg["num_machines"] for cfg in MACHINE_CONFIGS.values())
    print(f"\nCreating {total_instances} MQTT clients...")
    entries = create_all_clients(broker, port)
    print(f"Started {len(entries)} clients across {len(MACHINE_CONFIGS)} machine types.\n")

    run_loop(entries, all_templates)
    stop_clients(entries)


if __name__ == "__main__":
    main()
