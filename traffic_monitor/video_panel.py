from PyQt6.QtWidgets import QWidget, QLabel, QVBoxLayout
from PyQt6.QtGui import QImage, QPixmap
from PyQt6.QtCore import Qt, pyqtSignal
import cv2
import numpy as np
import threading
import base64
import json
from kafka import KafkaConsumer

CAMERA_STREAM_TOPIC = "cam_streaming"
BOOTSTRAP_SERVER = "localhost:9092"

class VideoPanel(QWidget):
    frame_updated = pyqtSignal(object)

    def __init__(self, camera_id: str, topic: str = CAMERA_STREAM_TOPIC):
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

   