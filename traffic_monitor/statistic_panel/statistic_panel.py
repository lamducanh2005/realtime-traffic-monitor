from PyQt6.QtWidgets import QWidget, QLabel, QVBoxLayout
from PyQt6.QtCore import pyqtSignal
import json
import threading
from kafka import KafkaConsumer


CAMERA_STATS_TOPIC = "cam_stats"
BOOTSTRAP_SERVER = [f"192.168.0.106:{i}" for i in range(9092, 9092 + 6)]


class StatisticPanel(QWidget):
    stats_received = pyqtSignal(object)

    def __init__(self, camera_id: str):
        super().__init__()
        self.camera_id = camera_id
        self.stats_thread = None
        self.stats_running = False
        self.consumer = None

        # Queue để lưu stats chưa hiển thị
        self.pending_stats = []  # List of (timestamp, payload)
        self.current_video_timestamp = None  # Timestamp hiện tại của video

        self.setStyleSheet("background-color: #252526; border:1px solid #3e3e42; border-radius:6px; padding:8px")
        self.main_layout = QVBoxLayout()
        self.main_layout.setContentsMargins(10, 6, 10, 6)
        self.setLayout(self.main_layout)

        # Connect signal to handler
        self.stats_received.connect(self._on_stats_received)

        self.setup_ui()
        self.start_kafka_consumer()

    def setup_ui(self):
        title = QLabel("📊 Thống kê giao thông")
        title.setStyleSheet("color: #ffffff; font-weight:bold")
        self.main_layout.addWidget(title)

        self.info_label = QLabel("Tốc độ TB: -- km/h | Xe phát hiện: -- | Lưu lượng: -- xe/phút")
        self.info_label.setStyleSheet("color: #cccccc; font-size:12px")
        self.main_layout.addWidget(self.info_label)

    def _on_stats_received(self, payload: dict):
        """Handler khi nhận stats từ Kafka"""
        try:
            # Kiểm tra camera_id
            if payload.get("camera_id") != self.camera_id:
                return

            timestamp = payload.get('timestamp', '')
            
            # Thêm vào pending queue
            self.pending_stats.append((timestamp, payload))
            
            # Sort theo timestamp
            self.pending_stats.sort(key=lambda x: x[0])
            
            # Kiểm tra và hiển thị stats đã đến lúc
            self._check_and_display_pending_stats()
        except Exception as e:
            print(f"Error processing stats: {e}")

    def on_video_frame_displayed(self, timestamp: str):
        """
        Callback từ VideoPanel khi một frame được hiển thị
        Cập nhật timestamp hiện tại và kiểm tra pending stats
        """
        self.current_video_timestamp = timestamp
        self._check_and_display_pending_stats()

    def _check_and_display_pending_stats(self):
        """Kiểm tra và hiển thị stats đã đến timestamp"""
        if not self.current_video_timestamp or not self.pending_stats:
            return

        # Hiển thị tất cả stats có timestamp <= video timestamp
        stats_to_display = []
        remaining_stats = []

        for ts, payload in self.pending_stats:
            if ts <= self.current_video_timestamp:
                stats_to_display.append(payload)
            else:
                remaining_stats.append((ts, payload))

        # Cập nhật pending queue
        self.pending_stats = remaining_stats

        # Hiển thị stats mới nhất (nếu có)
        if stats_to_display:
            latest_stats = stats_to_display[-1]  # Lấy stats mới nhất
            self._display_stats(latest_stats)

    def _display_stats(self, payload: dict):
        """Hiển thị thống kê lên UI"""
        try:
            avg_speed = payload.get("avg_speed", 0)
            vehicle_count = payload.get("vehicle_count_60s", 0)
            vehicles_per_minute = payload.get("vehicles_per_minute", 0)

            # Cập nhật label
            self.info_label.setText(
                f"Tốc độ TB: {avg_speed:.1f} km/h | "
                f"Xe phát hiện: {vehicle_count} | "
                f"Lưu lượng: {vehicles_per_minute} xe/phút"
            )
        except Exception as e:
            print(f"Error displaying stats: {e}")

    def start_kafka_consumer(self):
        """Khởi động Kafka consumer trong thread riêng"""
        if self.stats_thread is not None:
            return

        self.stats_running = True
        self.stats_thread = threading.Thread(
            target=self._kafka_consumer_thread,
            daemon=True
        )
        self.stats_thread.start()
        print(f"[{self.camera_id}] Stats Kafka consumer started")

    def _kafka_consumer_thread(self):
        """Thread chạy Kafka consumer"""
        try:
            self.consumer = KafkaConsumer(
                CAMERA_STATS_TOPIC,
                bootstrap_servers=BOOTSTRAP_SERVER,
                auto_offset_reset='latest',
                enable_auto_commit=True,
                group_id=f'traffic_monitor_stats_{self.camera_id}',
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )

            for message in self.consumer:
                if not self.stats_running:
                    break

                try:
                    payload = message.value
                    # Emit signal để cập nhật UI trong main thread
                    self.stats_received.emit(payload)
                except Exception as e:
                    print(f"Error processing stats message: {e}")

        except Exception as e:
            print(f"Kafka stats consumer error ({self.camera_id}): {e}")
        finally:
            if self.consumer:
                self.consumer.close()
            print(f"[{self.camera_id}] Stats consumer stopped")

    def close(self):
        """Đóng Kafka consumer"""
        self.stats_running = False
        if self.stats_thread:
            self.stats_thread.join(timeout=2)
        if self.consumer:
            try:
                self.consumer.close()
            except Exception:
                pass
