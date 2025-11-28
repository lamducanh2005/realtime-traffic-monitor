from PyQt6.QtWidgets import QWidget, QLabel, QVBoxLayout
from PyQt6.QtGui import QImage, QPixmap
from PyQt6.QtCore import Qt, pyqtSignal, QTimer
import cv2
import numpy as np
import base64
import json
from kafka import KafkaConsumer
from collections import deque
import time
from multiprocessing import Process, Queue
import os
import signal
from central_processor.utils import *

CAMERA_STREAM_TOPIC = "cam_tracking"
BOOTSTRAP_SERVER = [f"10.11.193.172:{p}" for p in range(9092, 9092 + 6)]


def kafka_consumer_worker(camera_id: str, frame_queue: Queue, topic: str, bootstrap_servers: list):
    """
    Process riêng biệt chạy Kafka consumer
    Nhận frame từ Kafka và đẩy vào Queue
    """
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            fetch_min_bytes=1024,
            fetch_max_wait_ms=100,
            max_poll_records=50
        )

        for msg in consumer:
            try:
                data = json.loads(msg.value.decode('utf-8'))
            except Exception:
                continue

            # Filter by camera id
            if data.get('camera_id') != camera_id:
                continue

            
            no, name = data.get('no'), data.get('name')
            frame_b64 = MongoCamService().get_frame(name, no)
            timestamp = data.get('timestamp', '')
            
            if not frame_b64:
                continue

            try:
                frame_bytes = base64.b64decode(frame_b64)
                arr = np.frombuffer(frame_bytes, dtype=np.uint8)
                frame = cv2.imdecode(arr, cv2.IMREAD_COLOR)
            except Exception:
                frame = None

            if frame is not None:
                # Đẩy vào Queue (thread-safe) kèm timestamp
                try:
                    frame_queue.put((frame, timestamp), block=False)  # THÊM: Tuple (frame, timestamp)
                except:
                    pass  # Queue full, bỏ qua frame

    except Exception as e:
        print(f"Kafka consumer error ({camera_id}): {e}")
    finally:
        consumer.close()


class VideoPanel(QWidget):
    frame_updated = pyqtSignal(object)
    frame_displayed = pyqtSignal(str)  # THÊM: Signal emit timestamp khi frame được hiển thị

    def __init__(self, camera_id: str, topic: str = CAMERA_STREAM_TOPIC):
        super().__init__()
        self.camera_id = camera_id
        self.topic = topic
        
        # Queue để communicate giữa Kafka process và UI process
        self.frame_queue = Queue(maxsize=5000)
        self.consumer_process = None
        self.is_running = False
        
        # Frame Buffer (FIFO queue) để chống giật lag
        self.frame_buffer = deque(maxlen=5000)
        self.buffer_ready = False
        self.target_buffer_size = 300
        
        # Timing control
        self.target_fps = 20  # FPS mục tiêu
        self.frame_interval = 1.0 / self.target_fps  # 0.05s = 50ms
        self.last_display_time = time.time()
        
        # Adaptive playback speed
        self.playback_speed = 1.0  # Tốc độ phát (1.0 = normal)
        
        self.setStyleSheet("background-color: #1e1e1e")
        self.main_layout = QVBoxLayout()
        self.main_layout.setContentsMargins(10, 10, 10, 10)
        self.setLayout(self.main_layout)
        
        self.setup_ui()
        self.start_kafka_consumer_process()
        self.start_display_timer()

    def setup_ui(self):
        self.video_label = QLabel()
        self.video_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.video_label.setStyleSheet("QLabel { background-color: #000000; border: 2px solid #3e3e42; border-radius: 5px; }")
        self.video_label.setMinimumSize(640, 480)
        self.main_layout.addWidget(self.video_label)

        # Connect signal
        self.frame_updated.connect(self.update_frame)

        # Show placeholder
        self.show_placeholder()

    def show_placeholder(self):
        placeholder = np.zeros((480, 640, 3), dtype=np.uint8)
        cv2.putText(placeholder, f"Cam {self.camera_id} Connecting...", (20, 240), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (150, 150, 150), 2)
        self.update_frame(placeholder)

    def start_kafka_consumer_process(self):
        """Khởi động Kafka consumer trong process riêng biệt"""
        self.is_running = True
        self.consumer_process = Process(
            target=kafka_consumer_worker,
            args=(self.camera_id, self.frame_queue, "cam_raw", BOOTSTRAP_SERVER),
            daemon=True
        )
        self.consumer_process.start()
        print(f"[{self.camera_id}] Kafka consumer process started (PID: {self.consumer_process.pid})")

    def start_display_timer(self):
        """Timer: Lấy frame từ Queue và hiển thị đều đặn"""
        self.display_timer = QTimer()
        self.display_timer.timeout.connect(self._pull_and_display_frame)
        self.display_timer.start(20)  # ~60fps check, nhưng chỉ hiển thị 20fps

    def _pull_and_display_frame(self):
        """Pull frame từ Queue sang buffer"""
        current_time = time.time()
        
        # Lấy hết frame từ Queue vào buffer
        while not self.frame_queue.empty():
            try:
                frame_data = self.frame_queue.get(block=False)
                
                # Unpack frame và timestamp
                if isinstance(frame_data, tuple):
                    frame, timestamp = frame_data
                else:
                    frame = frame_data
                    timestamp = None
                
                self.frame_buffer.append((frame, timestamp))
                
                if not self.buffer_ready and len(self.frame_buffer) >= self.target_buffer_size:
                    self.buffer_ready = True
                    print(f"[{self.camera_id}] Buffer ready! ({len(self.frame_buffer)} frames)")
            except:
                break

        # Hiển thị frame từ buffer
        if not self.buffer_ready:
            if len(self.frame_buffer) > 0:
                placeholder = np.zeros((480, 640, 3), dtype=np.uint8)
                progress = len(self.frame_buffer) / self.target_buffer_size * 100
                cv2.putText(placeholder, f"Cam {self.camera_id} Buffering... {progress:.0f}%", 
                           (20, 240), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 255, 255), 2)
                self.update_frame(placeholder, None)
            return

        # Adaptive playback speed dựa trên buffer size
        buffer_size = len(self.frame_buffer)
        print(f'{buffer_size}/600')
        
        if buffer_size < self.target_buffer_size * 0.9:
            self.playback_speed = 0.9
        elif buffer_size > self.target_buffer_size * 1.5:
            self.playback_speed = 1.1
        else:
            self.playback_speed = 1.0
        
        # Chỉ hiển thị frame khi đã đủ thời gian
        adjusted_interval = self.frame_interval / self.playback_speed
        elapsed = current_time - self.last_display_time
        
        if elapsed < adjusted_interval:
            return  # Chưa đến lúc hiển thị frame tiếp theo
        
        # Hiển thị frame
        if len(self.frame_buffer) > 0:
            frame_data = self.frame_buffer.popleft()
            
            if isinstance(frame_data, tuple):
                frame, timestamp = frame_data
                cv2.putText(frame, f"Cam {self.camera_id} - {timestamp}", 
                       (10, 60), cv2.FONT_HERSHEY_SIMPLEX, 0.6, (0, 255, 0), 2)
            else:
                frame = frame_data
                timestamp = None
            
            self.frame_updated.emit(frame)
            
            if timestamp:
                self.frame_displayed.emit(timestamp)
            
            self.last_display_time = current_time
        else:
            self.buffer_ready = False
            print(f"[{self.camera_id}] Buffer underrun! Re-buffering...")

    def update_frame(self, frame, timestamp=None):
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

    def closeEvent(self, event):
        """Cleanup khi close widget"""
        self.is_running = False
        
        if hasattr(self, 'display_timer'):
            self.display_timer.stop()
        
        if self.consumer_process and self.consumer_process.is_alive():
            self.consumer_process.terminate()
            self.consumer_process.join(timeout=2)
            if self.consumer_process.is_alive():
                self.consumer_process.kill()
            print(f"[{self.camera_id}] Kafka consumer process terminated")
        
        event.accept()

    def close(self):
        """Alias cho closeEvent"""
        self.closeEvent(None)