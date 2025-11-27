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
BOOTSTRAP_SERVER = [f"192.168.0.106:{i}" for i in range(9092, 9092 + 6)]


class EventStack(QWidget):
    event_received = pyqtSignal(object)

    def __init__(self, camera_id: str):
        super().__init__()
        self.camera_id = camera_id
        self.event_thread = None
        self.event_running = False
        self.consumer = None
        
        # THÊM: Queue để lưu events chưa hiển thị
        self.pending_events = []  # List of (timestamp, payload)
        self.current_video_timestamp = None  # Timestamp hiện tại của video

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
        """Handler khi nhận event từ Kafka"""
        try:
            timestamp = payload.get('timestamp') or ''
            
            # THÊM: Thêm vào pending queue
            self.pending_events.append((timestamp, payload))
            
            # Sort theo timestamp (đảm bảo thứ tự)
            self.pending_events.sort(key=lambda x: x[0])
            
            # Kiểm tra và hiển thị events đã đến lúc
            self._check_and_display_pending_events()
        except Exception:
            pass
    
    def on_video_frame_displayed(self, timestamp: str):
        """
        Callback khi video hiển thị frame mới
        Được gọi từ VideoPanel.frame_displayed signal
        """
        self.current_video_timestamp = timestamp
        self._check_and_display_pending_events()
    
    def _check_and_display_pending_events(self):
        """Kiểm tra và hiển thị events đã đến timestamp"""
        if not self.current_video_timestamp:
            return  # Chưa có frame nào hiển thị
        
        # Hiển thị tất cả events có timestamp <= current_video_timestamp
        events_to_display = []
        remaining_events = []
        
        for timestamp, payload in self.pending_events:
            if timestamp <= self.current_video_timestamp:
                events_to_display.append(payload)
            else:
                remaining_events.append((timestamp, payload))
        
        # Update pending list
        self.pending_events = remaining_events
        
        # Hiển thị events
        for payload in events_to_display:
            num_plate = payload.get('num_plate') or '--'
            timestamp = payload.get('timestamp') or ''
            plate_frame = payload.get('plate_frame', None)
            obj_frame = payload.get('obj_frame', None)
            warning = payload.get('warning', '') or ''
            speed = payload.get('speed', '--')
            vehicle_type = payload.get('type', '--')

            item = EventItem(
                num_plate=num_plate, 
                timestamp=timestamp, 
                warning=warning,
                speed=speed,
                vehicle_type=vehicle_type,
                plate_frame=plate_frame,
                obj_frame=obj_frame
            )
            self.add_event(item)

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
                fetch_max_wait_ms=10,
                max_poll_records=100
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
                
                # print("New message!")
                # Extract data from new message structure
                num_plate = data.get('num_plate') or '--'
                timestamp = data.get('timestamp') or ''
                plate_frame_b64 = data.get('plate_frame')
                obj_frame_b64 = data.get('obj_frame')
                plate_frame = None
                obj_frame = None

                # Decode plate frame
                if plate_frame_b64:
                    try:
                        b = base64.b64decode(plate_frame_b64)
                        arr = np.frombuffer(b, dtype=np.uint8)
                        frame = cv2.imdecode(arr, cv2.IMREAD_COLOR)
                        if frame is not None:
                            plate_frame = frame
                    except Exception:
                        plate_frame = None

                # Decode obj frame
                if obj_frame_b64:
                    try:
                        b = base64.b64decode(obj_frame_b64)
                        arr = np.frombuffer(b, dtype=np.uint8)
                        frame = cv2.imdecode(arr, cv2.IMREAD_COLOR)
                        if frame is not None:
                            obj_frame = frame
                    except Exception:
                        obj_frame = None

                payload = {
                    'num_plate': num_plate,
                    'timestamp': timestamp,
                    'warning': data.get('warning', ''),
                    'speed': data.get('speed', '--'),
                    'type': data.get('type', '--'),
                    'plate_frame': plate_frame,
                    'obj_frame': obj_frame,
                }

                # emit signal (thread-safe, queued) to update GUI
                try:
                    self.event_received.emit(payload)
                except Exception:
                    # last-resort fallback to direct add (shouldn't be needed)
                    try:
                        self.add_event(EventItem(
                            num_plate=num_plate,
                            timestamp=timestamp,
                            warning=data.get('warning', ''),
                            speed=data.get('speed', '--'),
                            vehicle_type=data.get('type', '--'),
                            plate_frame=plate_frame,
                            obj_frame=obj_frame
                        ))
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
