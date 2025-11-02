from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QLabel, QVBoxLayout, QHBoxLayout,
    QTabWidget, QScrollArea, QSizePolicy, QFrame
)
from PyQt6.QtGui import QImage, QPixmap
from PyQt6.QtCore import Qt, pyqtSignal, QEvent
import sys
import cv2
import numpy as np
import threading
import base64
import json
from kafka import KafkaConsumer

CAMERA_TRACK_TOPIC = "camera_track"
BOOTSTRAP_SERVER = "localhost:9092"

class EventItem(QWidget):
    def __init__(self, plate: str = "--", vehicle_type: str = "--", timestamp: str = "--", thumbnail: np.ndarray = None):
        super().__init__()
        self.plate = plate
        self.vehicle_type = vehicle_type
        self.timestamp = timestamp
        self.thumbnail = thumbnail

        self.setStyleSheet("background-color: #2b2b2b; border:1px solid #3e3e42; border-radius:6px; padding:6px")
        self.main_layout = QHBoxLayout()
        self.main_layout.setContentsMargins(6, 6, 6, 6)
        self.setLayout(self.main_layout)

        self.setup_ui()

    def setup_ui(self):
        # Thumbnail
        self.thumb_label = QLabel()
        self.thumb_label.setFixedSize(120, 80)
        self.thumb_label.setStyleSheet("background-color: #000; border-radius:4px")
        self.main_layout.addWidget(self.thumb_label)

        # Meta
        meta_layout = QVBoxLayout()
        self.plate_label = QLabel(f"Biển số: {self.plate}")
        self.plate_label.setStyleSheet("color: #ffffff; font-weight: bold")
        self.type_label = QLabel(f"Loại: {self.vehicle_type}")
        self.type_label.setStyleSheet("color: #cccccc; font-size:11px")
        self.time_label = QLabel(f"{self.timestamp}")
        self.time_label.setStyleSheet("color: #999999; font-size:11px")

        meta_layout.addWidget(self.plate_label)
        meta_layout.addWidget(self.type_label)
        meta_layout.addWidget(self.time_label)
        meta_layout.addStretch()

        self.main_layout.addLayout(meta_layout)

        if self.thumbnail is not None:
            self.set_thumbnail(self.thumbnail)
    

    def set_thumbnail(self, img: np.ndarray):
        try:
            rgb = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
            h, w, ch = rgb.shape
            bytes_per_line = ch * w
            qimg = QImage(rgb.data, w, h, bytes_per_line, QImage.Format.Format_RGB888)
            pix = QPixmap.fromImage(qimg).scaled(self.thumb_label.size(), Qt.AspectRatioMode.KeepAspectRatio, Qt.TransformationMode.SmoothTransformation)
            self.thumb_label.setPixmap(pix)
        except Exception:
            pass


class StatisticPanel(QWidget):
    def __init__(self):
        super().__init__()

        self.setStyleSheet("background-color: #252526; border:1px solid #3e3e42; border-radius:6px; padding:8px")
        self.main_layout = QVBoxLayout()
        self.main_layout.setContentsMargins(10, 6, 10, 6)
        self.setLayout(self.main_layout)

        self.setup_ui()

    def setup_ui(self):
        title = QLabel("📊 Thống kê đường")
        title.setStyleSheet("color: #ffffff; font-weight:bold")
        self.main_layout.addWidget(title)

        self.info_label = QLabel("Tốc độ TB: -- km/h | Xe phát hiện: -- | Lưu lượng: -- xe/phút")
        self.info_label.setStyleSheet("color: #cccccc; font-size:12px")
        self.main_layout.addWidget(self.info_label)

class VideoPanel(QWidget):
    frame_updated = pyqtSignal(object)

    def __init__(self, camera_id: str, topic: str = CAMERA_TRACK_TOPIC):
        super().__init__()
        self.camera_id = camera_id
        self.topic = topic
        self.consumer = None
        self.consumer_thread = None
        self.is_running = False
        
        self.setStyleSheet("background-color: #1e1e1e")
        self.main_layout = QVBoxLayout()
        self.main_layout.setContentsMargins(10, 10, 10, 10)
        self.setLayout(self.main_layout)
        
        self.setup_ui()
        self.start_kafka_consumer()

    def setup_ui(self):
        self.video_label = QLabel()
        self.video_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.video_label.setStyleSheet("QLabel { background-color: #000000; border: 2px solid #3e3e42; border-radius: 5px; }")
        self.video_label.setMinimumSize(640, 480)
        self.main_layout.addWidget(self.video_label)

         # connect signal
        self.frame_updated.connect(self.update_frame)

        # show placeholder
        self.show_placeholder()

    def show_placeholder(self):
        placeholder = np.zeros((480, 640, 3), dtype=np.uint8)
        cv2.putText(placeholder, f"Cam {self.camera_id} No Signal", (20, 240), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (150, 150, 150), 2)
        self.update_frame(placeholder)

    def start_kafka_consumer(self):
        self.is_running = True
        self.consumer_thread = threading.Thread(target=self._consume_frames, daemon=True)
        self.consumer_thread.start()

    def _consume_frames(self):
        try:
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=BOOTSTRAP_SERVER,
                auto_offset_reset='latest',
                enable_auto_commit=True,
                fetch_min_bytes=1,
                fetch_max_wait_ms=100
            )

            for msg in self.consumer:
                if not self.is_running:
                    break
                try:
                    data = json.loads(msg.value.decode('utf-8'))
                except Exception:
                    continue

                # filter by camera id (expecting camera_id like 'cam1')
                if data.get('camera_id') != self.camera_id:
                    continue

                frame_b64 = data.get('raw_frame') or data.get('frame')
                if not frame_b64:
                    continue

                try:
                    frame_bytes = base64.b64decode(frame_b64)
                    arr = np.frombuffer(frame_bytes, dtype=np.uint8)
                    frame = cv2.imdecode(arr, cv2.IMREAD_COLOR)
                except Exception:
                    frame = None

                if frame is not None:
                    self.frame_updated.emit(frame)

        except Exception as e:
            print(f"Kafka consumer error ({self.camera_id}): {e}")

    def update_frame(self, frame):
        if frame is None:
            return
        try:
            rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
            h, w, ch = rgb.shape
            bytes_per_line = ch * w
            qimg = QImage(rgb.data, w, h, bytes_per_line, QImage.Format.Format_RGB888)
            pix = QPixmap.fromImage(qimg).scaled(self.video_label.size(), Qt.AspectRatioMode.KeepAspectRatio, Qt.TransformationMode.SmoothTransformation)
            self.video_label.setPixmap(pix)
        except Exception:
            pass

    def close(self):
        self.is_running = False
        if self.consumer:
            try:
                self.consumer.close()
            except Exception:
                pass
        if self.consumer_thread:
            try:
                self.consumer_thread.join(timeout=1)
            except Exception:
                pass

    
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
                CAMERA_TRACK_TOPIC,
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


class MonitorTab(QWidget):
    def __init__(self, camera_id: str):
        super().__init__()
        self.camera_id = camera_id

        self.main_layout = QVBoxLayout()
        self.main_layout.setContentsMargins(6, 6, 6, 6)
        self.main_layout.setSpacing(6)
        self.setLayout(self.main_layout)

        self.setup_ui()
    
    def setup_ui(self):

        # Video + event stack
        content = QHBoxLayout()
        content.setSpacing(5)

        # Video
        self.video_panel = VideoPanel(self.camera_id)
        content.addWidget(self.video_panel, stretch=3)

        # Event stack
        self.event_stack = EventStack(self.camera_id)
        self.event_stack.setMinimumWidth(240)
        content.addWidget(self.event_stack, stretch=1)

        self.main_layout.addLayout(content, stretch=8)

        # Stats bar
        self.stat_panel = StatisticPanel()
        self.main_layout.addWidget(self.stat_panel, stretch=0)

    def closeEvent(self, event):
        """Called automatically when widget is closed"""
        try:
            self.video_panel.close()
        except Exception:
            pass
        try:
            self.event_stack.close()
        except Exception:
            pass
        if event:
            event.accept()