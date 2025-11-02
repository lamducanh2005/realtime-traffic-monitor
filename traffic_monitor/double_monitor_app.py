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

CAMERA_TRACK_TOPIC = "camera_track"
BOOTSTRAP_SERVER = "localhost:9092"

class HeaderBar(QWidget):
    def __init__(self, title_text: str):
        super().__init__()
        self.setFixedHeight(80)
        self.setStyleSheet("background-color: #393E46; padding: 15px 20px")

        self.main_layout = QHBoxLayout()
        self.main_layout.setContentsMargins(0, 0, 0, 0)
        self.setLayout(self.main_layout)

        self.setup_ui(title_text)

    def setup_ui(self, title_text: str):
        title_label = QLabel(title_text)
        title_label.setStyleSheet("color: #DFD0B8; border: none; font-size: 20px; font-weight:bold")
        self.main_layout.addWidget(title_label)

class VideoPanel(QWidget):
    frame_updated = pyqtSignal(np.ndarray)

    def __init__(self, camera_id: str, show_stats: bool = True):
        super().__init__()
        self.camera_id = camera_id
        self.setStyleSheet("background-color: #1e1e1e")

        self.main_layout = QVBoxLayout(self)
        self.main_layout.setContentsMargins(10, 10, 10, 10)
        self.main_layout.setSpacing(10)
        self.setLayout(self.main_layout)

        # Kafka consumer
        self.consumer = None
        self.consumer_thread = None
        self.is_running = False

        self.setup_ui(show_stats)
        self.start_kafka_consumer()

    def setup_ui(self, show_stats: bool):
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
        self.video_label.setMinimumSize(640, 480)
        self.main_layout.addWidget(self.video_label, stretch=7)

        # Statistics panel (optional)
        if show_stats:
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
        """Nhận frame từ Kafka topic và lọc theo camera_id"""
        try:
            # Do not set group_id so each consumer gets all messages (then we filter locally)
            self.consumer = KafkaConsumer(
                CAMERA_TRACK_TOPIC,
                bootstrap_servers=BOOTSTRAP_SERVER,
                fetch_min_bytes=1,
                fetch_max_wait_ms=100,
                max_poll_records=1,
                auto_offset_reset='latest',
                enable_auto_commit=True
            )

            for message in self.consumer:
                if not self.is_running:
                    break

                try:
                    data = json.loads(message.value.decode('utf-8'))

                    # Lọc theo camera_id
                    msg_cam = data.get('camera_id', '')
                    if msg_cam != self.camera_id:
                        continue

                    # Lấy frame từ raw_frame
                    frame_base64 = data.get('raw_frame', '')
                    if not frame_base64:
                        continue

                    frame_bytes = base64.b64decode(frame_base64)
                    frame_array = np.frombuffer(frame_bytes, dtype=np.uint8)
                    frame = cv2.imdecode(frame_array, cv2.IMREAD_COLOR)

                    if frame is not None:
                        # Emit signal với frame
                        self.frame_updated.emit(frame)
                except Exception as e:
                    print(f"Error decoding frame for {self.camera_id}: {e}")
        except Exception as e:
            print(f"Kafka consumer error ({self.camera_id}): {e}")

    def show_placeholder(self):
        placeholder = np.zeros((480, 640, 3), dtype=np.uint8)
        cv2.putText(placeholder, f"Dang cho ket noi Kafka... ({self.camera_id})", 
                   (20, 240), cv2.FONT_HERSHEY_SIMPLEX, 0.8, (100, 100, 100), 2)
        self.update_frame(placeholder)

    def update_frame(self, frame):
        """Cập nhật video frame - KHÔNG CẦN VẼ NỮA"""
        if frame is None:
            return

        # Bỏ phần vẽ bbox vì đã vẽ sẵn ở Flink
        # Convert BGR to RGB
        rgb_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
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

    def close(self):
        """Dừng consumer khi panel bị đóng"""
        self.is_running = False
        if self.consumer:
            try:
                self.consumer.close()
            except Exception:
                pass

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

class MonitorTab(QWidget):
    def __init__(self, camera_id: str, title_text: str):
        super().__init__()
        self.main_layout = QVBoxLayout()
        self.main_layout.setContentsMargins(0, 0, 0, 0)
        self.setLayout(self.main_layout)

        # Header and video panel
        self.header = HeaderBar(title_text)
        self.video_panel = VideoPanel(camera_id)

        self.main_layout.addWidget(self.header)
        self.main_layout.addWidget(self.video_panel)

    def close(self):
        try:
            self.video_panel.close()
        except Exception:
            pass

class DoubleMonitorApp(QMainWindow):
    def __init__(self):
        super().__init__()

        self.setWindowTitle("Màn hình giám sát giao thông - 2 Cameras")
        self.setGeometry(100, 100, 1200, 900)

        # Central widget
        self.central = QWidget()
        self.central_layout = QVBoxLayout()
        self.central.setLayout(self.central_layout)
        self.setCentralWidget(self.central)

        # Tab widget for switching between cameras
        self.tab_widget = QTabWidget()
        self.tab_widget.setTabPosition(QTabWidget.TabPosition.North)

        # Create two tabs for Camera 1 and Camera 2
        self.cam1_tab = MonitorTab('cam1', 'Màn hình giám sát - Camera 1')
        self.cam2_tab = MonitorTab('cam2', 'Màn hình giám sát - Camera 2')

        self.tab_widget.addTab(self.cam1_tab, "Camera 1")
        self.tab_widget.addTab(self.cam2_tab, "Camera 2")

        self.central_layout.addWidget(self.tab_widget)

    def closeEvent(self, event):
        # Close consumer threads
        try:
            self.cam1_tab.close()
        except Exception:
            pass
        try:
            self.cam2_tab.close()
        except Exception:
            pass
        super().closeEvent(event)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyle('Fusion')
    window = DoubleMonitorApp()
    window.show()
    sys.exit(app.exec())
