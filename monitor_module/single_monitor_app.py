from PyQt6.QtWidgets import *
from PyQt6.QtGui import *
from PyQt6.QtCore import *
import sys
import cv2
import numpy as np
from kafka import KafkaConsumer
import threading
import base64
import json

CAMERA_TRACK_TOPIC = "camera_raw"
BOOTSTRAP_SERVER = [
    '192.168.0.106:9092',
    '192.168.0.106:9093',
    '192.168.0.106:9094',
    '192.168.0.106:9095',
    '192.168.0.106:9096',
    '192.168.0.106:9097',
    '192.168.0.106:9098',
    '192.168.0.106:9099',
    '192.168.0.106:9100',
    '192.168.0.106:9101',
    '192.168.0.106:9102',
    '192.168.0.106:9103'
]

class HeaderBar(QWidget):
    def __init__(self):
        super().__init__()
        
        self.setFixedHeight(80)
        self.setStyleSheet("background-color: #393E46; padding: 15px 20px")

        self.main_layout = QHBoxLayout()
        self.main_layout.setContentsMargins(0, 0, 0, 0)
        self.setLayout(self.main_layout)

        self.setup_ui()

    def setup_ui(self):
        title_label = QLabel("Màn hình giám sát giao thông - Camera 1")
        title_label.setStyleSheet("color: #DFD0B8; border: none; font-size: 20px; font-weight:bold")
        self.main_layout.addWidget(title_label)

class VideoPanel(QWidget):
    frame_updated = pyqtSignal(np.ndarray, list)

    def __init__(self):
        super().__init__()
        self.setStyleSheet("background-color: #1e1e1e")

        self.main_layout = QVBoxLayout(self)
        self.main_layout.setContentsMargins(10, 10, 10, 10)
        self.main_layout.setSpacing(10)
        self.setLayout(self.main_layout)

        # Kafka consumer
        self.consumer = None
        self.consumer_thread = None
        self.is_running = False

        self.setup_ui()
        self.start_kafka_consumer()

    def setup_ui(self):
        # Video display
        self.video_label = QLabel()
        self.video_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.video_label.setStyleSheet("""
            QLabel {
                background-color: #000000;
                border: 2px solid #3e3e42;
                border-radius: 5px;
            }
        """)
        self.video_label.setMinimumSize(800, 600)
        self.main_layout.addWidget(self.video_label, stretch=7)

        # Statistics panel
        stats_panel = StatisticPanel()
        self.main_layout.addWidget(stats_panel, stretch=2)
        
        # Connect signal
        self.frame_updated.connect(self.update_frame)
        
        # Show placeholder
        self.show_placeholder()

    def start_kafka_consumer(self):
        """Khởi động Kafka consumer trong thread riêng"""
        self.is_running = True
        self.consumer_thread = threading.Thread(target=self._consume_frames, daemon=True)
        self.consumer_thread.start()

    def _consume_frames(self):
        """Nhận frame từ Kafka topic"""
        try:
            self.consumer = KafkaConsumer(
                CAMERA_TRACK_TOPIC,
                bootstrap_servers=BOOTSTRAP_SERVER,
            )
            
            for message in self.consumer:
                if not self.is_running:
                    break
                
                try:
                    data = json.loads(message.value.decode('utf-8'))
                    
                    # Lấy frame từ raw_frame
                    frame_base64 = data.get('raw_frame', '')
                    frame_bytes = base64.b64decode(frame_base64)
                    frame_array = np.frombuffer(frame_bytes, dtype=np.uint8)
                    frame = cv2.imdecode(frame_array, cv2.IMREAD_COLOR)
                    
                    # Lấy detections
                    detections = data.get('detections', [])
                    
                    if frame is not None:
                        # Emit signal với frame và detections
                        self.frame_updated.emit(frame, detections)
                except Exception as e:
                    print(f"Error decoding frame: {e}")
        except Exception as e:
            print(f"Kafka consumer error: {e}")

    def show_placeholder(self):
        placeholder = np.zeros((600, 800, 3), dtype=np.uint8)
        cv2.putText(placeholder, "Dang cho ket noi Kafka...", 
                   (200, 300), cv2.FONT_HERSHEY_SIMPLEX, 1, (100, 100, 100), 2)
        self.update_frame(placeholder, [])

    def update_frame(self, frame, detections):
        """Cập nhật video frame"""
        if frame is None:
            return
        
        annotated_frame = frame.copy()
        for bbox in detections:
            if len(bbox) == 4:  # [x1, y1, x2, y2]
                x1, y1, x2, y2 = map(int, bbox)
                # Vẽ rectangle
                cv2.rectangle(annotated_frame, (x1, y1), (x2, y2), (0, 255, 0), 2)
                # Vẽ label (optional)
                cv2.putText(annotated_frame, "Vehicle", (x1, y1-10), 
                           cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 255, 0), 2)
        
        # Convert BGR to RGB
        rgb_frame = cv2.cvtColor(annotated_frame, cv2.COLOR_BGR2RGB)
        h, w, ch = rgb_frame.shape
        bytes_per_line = ch * w
        
        # Create QImage
        qt_image = QImage(rgb_frame.data, w, h, bytes_per_line, QImage.Format.Format_RGB888)
        
        # Scale to fit label
        scaled_pixmap = QPixmap.fromImage(qt_image).scaled(
            self.video_label.size(),
            Qt.AspectRatioMode.KeepAspectRatio,
            Qt.TransformationMode.SmoothTransformation
        )
        
        self.video_label.setPixmap(scaled_pixmap)
    
    def closeEvent(self, event):
        """Dừng consumer khi đóng ứng dụng"""
        self.is_running = False
        if self.consumer:
            self.consumer.close()
        super().closeEvent(event)

class StatisticPanel(QWidget):
    def __init__(self):
        super().__init__()
        self.setFixedHeight(150)
        self.setStyleSheet("""
            QWidget {
                background-color: #252526;
                border: 1px solid #3e3e42;
                border-radius: 5px;
            }
        """)

        self.main_layout = QVBoxLayout()
        self.main_layout.setContentsMargins(15, 10, 15, 10)
        self.setLayout(self.main_layout)

        self.setup_ui()

    def setup_ui(self):
        # Title
        title = QLabel("📊 Thông tin đường")
        title.setStyleSheet("color: #ffffff; border: none; font-size: 12px; font-weight: bold;")
        self.main_layout.addWidget(title)
        
        # Info text
        self.info_label = QLabel(
            "Trạng thái: Hoạt động\n"
            "Tốc độ TB: -- km/h | Xe phát hiện: -- | Lưu lượng: -- xe/phút"
        )
        self.info_label.setStyleSheet("color: #cccccc; border: none; font-size: 11px;")
        self.info_label.setWordWrap(True)
        self.main_layout.addWidget(self.info_label)

class MonitorAppContainer(QWidget):
    def __init__(self):
        super().__init__()

        self.main_layout = QVBoxLayout()
        self.main_layout.setContentsMargins(0, 0, 0, 0)
        self.main_layout.setSpacing(0)
        self.setLayout(self.main_layout)

        self.setup_ui()

    def setup_ui(self):
        # Header Bar
        self.header = HeaderBar()
        self.main_layout.addWidget(self.header)

        # Video Panel
        self.video_panel = VideoPanel()
        self.main_layout.addWidget(self.video_panel)

class MonitorApp(QMainWindow):
    def __init__(self):
        super().__init__()

        self.setWindowTitle("Màn hình giám sát giao thông")
        self.setGeometry(100, 100, 1000, 850)

        self.container = MonitorAppContainer()
        self.setCentralWidget(self.container)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyle('Fusion')
    
    window = MonitorApp()
    window.show()

    sys.exit(app.exec())