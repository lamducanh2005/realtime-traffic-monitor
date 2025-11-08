from PyQt6.QtWidgets import QWidget, QVBoxLayout, QScrollArea
from PyQt6.QtCore import pyqtSignal
import cv2
import numpy as np
import threading
import base64
import json
from kafka import KafkaConsumer
from .event_item import EventItem


CAMERA_EVENT_TOPIC = "cam_event"
BOOTSTRAP_SERVER = [f"10.11.7.180:{i}" for i in range(9092, 9092 + 12)]


class EventStack(QWidget):
    event_received = pyqtSignal(object)

    def __init__(self, camera_id: str):
        super().__init__()
        self.camera_id = camera_id
        self.event_thread = None
        self.event_running = False
        self.consumer = None

        self.main_layout = QVBoxLayout()
        self.main_layout.setContentsMargins(0, 0, 0, 0)
        self.setLayout(self.main_layout)

        # scroll area (widget chứa các EventItem)
        self.scroll = QScrollArea()
        self.scroll.setWidgetResizable(True)
        self.scroll.setStyleSheet("border: none; background: transparent")

        # connect signal -> handler BEFORE starting consumer
        self.event_received.connect(self._on_event_received)

        self.setup_ui()
        self.start_kafka_consumer()

    def setup_ui(self):
        container = QWidget()
        self.container_layout = QVBoxLayout()
        self.container_layout.setContentsMargins(6, 6, 6, 6)
        self.container_layout.setSpacing(8)
        container.setLayout(self.container_layout)

        # stretch ở cuối để push items lên trên
        self.container_layout.addStretch()

        # gắn container vào scroll và thêm scroll vào layout
        self.scroll.setWidget(container)
        self.main_layout.addWidget(self.scroll)

    def add_event(self, event_item: EventItem):
        # Insert at top (index 0 before the stretch)
        self.container_layout.insertWidget(0, event_item)

    def _on_event_received(self, payload: dict):
        try:
            plate = payload.get('plate') or '--'
            vehicle_type = payload.get('vehicle_type') or '--'
            timestamp = payload.get('timestamp') or ''
            thumbnail = payload.get('thumbnail', None)
            item = EventItem(plate=plate, vehicle_type=vehicle_type, timestamp=timestamp, thumbnail=thumbnail)
            self.add_event(item)
        except Exception:
            pass

    def start_kafka_consumer(self):
        self.event_running = True
        self.event_thread = threading.Thread(target=self._consume_events, daemon=True)
        self.event_thread.start()

    def _consume_events(self):
        try:
            self.consumer = KafkaConsumer(
                CAMERA_EVENT_TOPIC,
                bootstrap_servers=BOOTSTRAP_SERVER,
                auto_offset_reset='latest',
                enable_auto_commit=True,
                fetch_min_bytes=1,
                fetch_max_wait_ms=100
            )

            for msg in self.consumer:
                if not self.event_running:
                    break
                try:
                    data = json.loads(msg.value.decode('utf-8'))
                except Exception:
                    continue

                if data.get('camera_id') != self.camera_id:
                    continue

                # If message contains event info (plate, raw_frame, timestamp)
                plate = data.get('plate') or data.get('plate_number') or '--'
                vehicle_type = data.get('vehicle_type') or '--'
                timestamp = data.get('timestamp') or ''
                frame_b64 = data.get('raw_frame') or data.get('frame')
                thumbnail = None
                if frame_b64:
                    try:
                        b = base64.b64decode(frame_b64)
                        arr = np.frombuffer(b, dtype=np.uint8)
                        frame = cv2.imdecode(arr, cv2.IMREAD_COLOR)
                        # create small thumbnail crop/resize
                        if frame is not None:
                            thumbnail = cv2.resize(frame, (160, 120))
                    except Exception:
                        thumbnail = None

                payload = {
                    'plate': plate,
                    'vehicle_type': vehicle_type,
                    'timestamp': timestamp,
                    'thumbnail': thumbnail,
                }

                # emit signal (thread-safe, queued) to update GUI
                try:
                    self.event_received.emit(payload)
                except Exception:
                    # last-resort fallback to direct add (shouldn't be needed)
                    try:
                        self.add_event(EventItem(plate=plate, vehicle_type=vehicle_type, timestamp=timestamp, thumbnail=thumbnail))
                    except Exception:
                        pass

        except Exception as e:
            print(f"Event consumer error ({self.camera_id}): {e}")


    def close(self):
        self.event_running = False
        if self.event_thread:
            try:
                self.event_thread.join(timeout=1)
            except Exception:
                pass
