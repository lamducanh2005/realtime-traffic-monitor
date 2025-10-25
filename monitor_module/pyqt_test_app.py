"""
Traffic Monitoring Dashboard - PyQt6 Version
Real-time vehicle tracking and monitoring system
"""

import sys
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QPushButton, QListWidget, QFrame, QScrollArea, QTableWidget,
    QTableWidgetItem, QHeaderView, QSplitter
)
from PyQt6.QtCore import Qt, QTimer, QThread, pyqtSignal, QSize
from PyQt6.QtGui import QImage, QPixmap, QFont, QPalette, QColor
import cv2
import numpy as np
import json
from datetime import datetime

# Uncomment khi có Kafka
# from kafka import KafkaConsumer


class HeaderBar(QWidget):
    """Header bar với title và status indicator"""
    
    def __init__(self, title="Traffic Monitoring Dashboard"):
        super().__init__()
        self.setFixedHeight(70)
        self.setStyleSheet("""
            QWidget {
                background-color: #1e1e1e;
                border-bottom: 2px solid #007acc;
            }
        """)
        
        layout = QHBoxLayout(self)
        layout.setContentsMargins(20, 10, 20, 10)
        
        # Title
        title_label = QLabel(title)
        title_label.setFont(QFont("Segoe UI", 20, QFont.Weight.Bold))
        title_label.setStyleSheet("color: #ffffff; border: none;")
        layout.addWidget(title_label)
        
        layout.addStretch()
        
        # Status indicator
        self.status_dot = QLabel("●")
        self.status_dot.setFont(QFont("Segoe UI", 16))
        self.status_dot.setStyleSheet("color: #f44336; border: none;")
        
        self.status_label = QLabel("Disconnected")
        self.status_label.setFont(QFont("Segoe UI", 12))
        self.status_label.setStyleSheet("color: #ffffff; border: none;")
        
        layout.addWidget(self.status_dot)
        layout.addWidget(self.status_label)
    
    def set_status(self, text, color):
        """Update connection status"""
        self.status_label.setText(text)
        self.status_dot.setStyleSheet(f"color: {color}; border: none;")


class CameraSidebar(QWidget):
    """Sidebar for camera selection"""
    
    camera_selected = pyqtSignal(str, int)  # camera_name, camera_index
    
    def __init__(self):
        super().__init__()
        self.setFixedWidth(280)
        self.setStyleSheet("""
            QWidget {
                background-color: #252526;
            }
        """)
        
        layout = QVBoxLayout(self)
        layout.setContentsMargins(15, 15, 15, 15)
        layout.setSpacing(10)
        
        # Title
        title = QLabel("📹 Cameras")
        title.setFont(QFont("Segoe UI", 14, QFont.Weight.Bold))
        title.setStyleSheet("color: #ffffff; padding: 5px;")
        layout.addWidget(title)
        
        # Camera list
        self.camera_list = QListWidget()
        self.camera_list.setStyleSheet("""
            QListWidget {
                background-color: #1e1e1e;
                color: #ffffff;
                border: 1px solid #3e3e42;
                border-radius: 5px;
                padding: 5px;
                font-size: 13px;
            }
            QListWidget::item {
                padding: 12px;
                border-radius: 3px;
                margin: 2px;
            }
            QListWidget::item:hover {
                background-color: #2a2d2e;
            }
            QListWidget::item:selected {
                background-color: #007acc;
                color: #ffffff;
            }
        """)
        
        # Add sample cameras
        cameras = [
            "Camera 1 - Highway A1",
            "Camera 2 - Downtown Junction",
            "Camera 3 - Airport Road",
            "Camera 4 - Industrial Zone",
            "Camera 5 - City Center",
            "Camera 6 - Harbor Bridge"
        ]
        
        for cam in cameras:
            self.camera_list.addItem(cam)
        
        self.camera_list.currentRowChanged.connect(self._on_camera_selected)
        layout.addWidget(self.camera_list)
        
        # Info box
        info_frame = QFrame()
        info_frame.setStyleSheet("""
            QFrame {
                background-color: #1e1e1e;
                border: 1px solid #3e3e42;
                border-radius: 5px;
                padding: 10px;
            }
        """)
        info_layout = QVBoxLayout(info_frame)
        
        info_title = QLabel("ℹ️ Information")
        info_title.setFont(QFont("Segoe UI", 11, QFont.Weight.Bold))
        info_title.setStyleSheet("color: #ffffff; border: none;")
        
        self.info_text = QLabel("Select a camera to\nview live stream")
        self.info_text.setStyleSheet("color: #cccccc; border: none; font-size: 11px;")
        self.info_text.setWordWrap(True)
        
        info_layout.addWidget(info_title)
        info_layout.addWidget(self.info_text)
        
        layout.addWidget(info_frame)
    
    def _on_camera_selected(self, index):
        """Handle camera selection"""
        if index >= 0:
            camera_name = self.camera_list.item(index).text()
            self.camera_selected.emit(camera_name, index)
            self.info_text.setText(f"Viewing: {camera_name}\nStatus: Active")


class VideoPanel(QWidget):
    """Main video display panel"""
    
    def __init__(self):
        super().__init__()
        self.setStyleSheet("""
            QWidget {
                background-color: #1e1e1e;
            }
        """)
        
        layout = QVBoxLayout(self)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(10)
        
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
        layout.addWidget(self.video_label, stretch=7)
        
        # Route information panel (phần dưới video)
        self.route_panel = QFrame()
        self.route_panel.setStyleSheet("""
            QFrame {
                background-color: #252526;
                border: 1px solid #3e3e42;
                border-radius: 5px;
            }
        """)
        self.route_panel.setFixedHeight(150)
        
        route_layout = QVBoxLayout(self.route_panel)
        route_layout.setContentsMargins(15, 10, 15, 10)
        
        route_title = QLabel("📊 Route Information")
        route_title.setFont(QFont("Segoe UI", 12, QFont.Weight.Bold))
        route_title.setStyleSheet("color: #ffffff; border: none;")
        
        self.route_info = QLabel("Select a camera to view route details.\nTraffic flow, speed, and congestion data will appear here.")
        self.route_info.setStyleSheet("color: #cccccc; border: none; font-size: 11px;")
        self.route_info.setWordWrap(True)
        
        route_layout.addWidget(route_title)
        route_layout.addWidget(self.route_info)
        
        layout.addWidget(self.route_panel, stretch=2)
        
        # Show placeholder
        self.show_placeholder()
    
    def show_placeholder(self):
        """Show placeholder when no camera selected"""
        placeholder = np.zeros((480, 640, 3), dtype=np.uint8)
        cv2.putText(placeholder, "Select a camera to view stream", 
                   (80, 240), cv2.FONT_HERSHEY_SIMPLEX, 0.8, (100, 100, 100), 2)
        cv2.putText(placeholder, "Waiting for Kafka stream...", 
                   (140, 280), cv2.FONT_HERSHEY_SIMPLEX, 0.6, (80, 80, 80), 1)
        self.update_frame(placeholder)
    
    def update_frame(self, frame):
        """Update video frame"""
        if frame is None:
            return
        
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
    
    def update_route_info(self, camera_name):
        """Update route information"""
        self.route_info.setText(
            f"Camera: {camera_name}\n"
            f"Route Status: Active\n"
            f"Average Speed: -- km/h | Vehicles Detected: -- | Flow Rate: -- vehicles/min"
        )


class VehicleLogPanel(QWidget):
    """Vehicle detection log panel"""
    
    def __init__(self):
        super().__init__()
        self.setFixedWidth(400)
        self.setStyleSheet("""
            QWidget {
                background-color: #252526;
            }
        """)
        
        layout = QVBoxLayout(self)
        layout.setContentsMargins(15, 15, 15, 15)
        layout.setSpacing(10)
        
        # Title
        title = QLabel("📋 Vehicle Log")
        title.setFont(QFont("Segoe UI", 14, QFont.Weight.Bold))
        title.setStyleSheet("color: #ffffff;")
        layout.addWidget(title)
        
        # Table
        self.table = QTableWidget()
        self.table.setColumnCount(4)
        self.table.setHorizontalHeaderLabels(["Camera", "Class", "Track ID", "Time"])
        
        self.table.setStyleSheet("""
            QTableWidget {
                background-color: #1e1e1e;
                color: #ffffff;
                border: 1px solid #3e3e42;
                border-radius: 5px;
                gridline-color: #3e3e42;
            }
            QTableWidget::item {
                padding: 8px;
            }
            QTableWidget::item:selected {
                background-color: #007acc;
            }
            QHeaderView::section {
                background-color: #2d2d30;
                color: #ffffff;
                padding: 8px;
                border: none;
                border-bottom: 2px solid #007acc;
                font-weight: bold;
            }
        """)
        
        # Column widths
        header = self.table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(3, QHeaderView.ResizeMode.Stretch)
        
        self.table.verticalHeader().setVisible(False)
        self.table.setAlternatingRowColors(True)
        
        layout.addWidget(self.table)
        
        # Stats
        stats_frame = QFrame()
        stats_frame.setStyleSheet("""
            QFrame {
                background-color: #1e1e1e;
                border: 1px solid #3e3e42;
                border-radius: 5px;
                padding: 10px;
            }
        """)
        stats_layout = QVBoxLayout(stats_frame)
        
        self.stats_label = QLabel("Total Vehicles: 0")
        self.stats_label.setStyleSheet("color: #ffffff; font-size: 12px; border: none;")
        stats_layout.addWidget(self.stats_label)
        
        layout.addWidget(stats_frame)
    
    def add_vehicle(self, camera_id, vehicle_class, track_id, timestamp):
        """Add vehicle entry to log"""
        row_position = self.table.rowCount()
        self.table.insertRow(row_position)
        
        self.table.setItem(row_position, 0, QTableWidgetItem(camera_id))
        self.table.setItem(row_position, 1, QTableWidgetItem(vehicle_class))
        self.table.setItem(row_position, 2, QTableWidgetItem(str(track_id)))
        self.table.setItem(row_position, 3, QTableWidgetItem(timestamp))
        
        # Auto scroll to bottom
        self.table.scrollToBottom()
        
        # Update stats
        self.stats_label.setText(f"Total Vehicles: {row_position + 1}")
    
    def clear_log(self):
        """Clear all entries"""
        self.table.setRowCount(0)
        self.stats_label.setText("Total Vehicles: 0")


class KafkaConsumerThread(QThread):
    """Thread for consuming Kafka messages"""
    
    frame_received = pyqtSignal(np.ndarray)
    vehicle_detected = pyqtSignal(dict)
    connection_status = pyqtSignal(str, str)  # status_text, color
    
    def __init__(self, brokers, topic):
        super().__init__()
        self.brokers = brokers
        self.topic = topic
        self.running = False
    
    def run(self):
        """Run Kafka consumer (placeholder - uncomment when ready)"""
        self.running = True
        
        # Uncomment khi có Kafka
        """
        try:
            consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.brokers,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="latest"
            )
            
            self.connection_status.emit("Connected", "#4caf50")
            
            for message in consumer:
                if not self.running:
                    break
                
                payload = message.value
                
                # Decode frame
                if "processed_image" in payload or "base64_image" in payload:
                    import base64
                    image_b64 = payload.get("processed_image") or payload.get("base64_image")
                    data = base64.b64decode(image_b64)
                    array = np.frombuffer(data, dtype=np.uint8)
                    frame = cv2.imdecode(array, cv2.IMREAD_COLOR)
                    if frame is not None:
                        self.frame_received.emit(frame)
                
                # Vehicle data
                self.vehicle_detected.emit(payload)
                
        except Exception as e:
            self.connection_status.emit(f"Error: {str(e)}", "#f44336")
        """
        
        # Demo mode - generate sample data
        self.connection_status.emit("Demo Mode (No Kafka)", "#ff9800")
        
    def stop(self):
        """Stop consumer"""
        self.running = False


class MonitorApp(QMainWindow):
    """Main application window"""
    
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Traffic Monitoring Dashboard - PyQt6")
        self.setGeometry(100, 100, 1400, 900)
        
        # Kafka settings
        self.brokers = ["localhost:9092"]
        self.topic = "vehicle_tracking_data"
        
        self.setup_ui()
        self.setup_demo_timer()
    
    def setup_ui(self):
        """Setup user interface"""
        # Main widget
        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        main_widget.setStyleSheet("background-color: #1e1e1e;")
        
        # Main layout
        main_layout = QVBoxLayout(main_widget)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)
        
        # Header
        self.header = HeaderBar()
        main_layout.addWidget(self.header)
        
        # Content area
        content_layout = QHBoxLayout()
        content_layout.setContentsMargins(0, 0, 0, 0)
        content_layout.setSpacing(0)
        
        # Sidebar
        self.sidebar = CameraSidebar()
        self.sidebar.camera_selected.connect(self.on_camera_selected)
        content_layout.addWidget(self.sidebar)
        
        # Video panel
        self.video_panel = VideoPanel()
        content_layout.addWidget(self.video_panel, stretch=1)
        
        # Vehicle log panel
        self.log_panel = VehicleLogPanel()
        content_layout.addWidget(self.log_panel)
        
        main_layout.addLayout(content_layout)
        
        # Set demo status
        self.header.set_status("Demo Mode", "#ff9800")
    
    def setup_demo_timer(self):
        """Setup timer for demo mode"""
        self.demo_timer = QTimer()
        self.demo_timer.timeout.connect(self.generate_demo_data)
        self.demo_timer.start(2000)  # Generate demo data every 2 seconds
        
        self.demo_vehicle_counter = 0
    
    def on_camera_selected(self, camera_name, camera_index):
        """Handle camera selection"""
        self.video_panel.update_route_info(camera_name)
        
        # Generate demo frame for selected camera
        frame = self.generate_demo_frame(camera_name)
        self.video_panel.update_frame(frame)
    
    def generate_demo_frame(self, camera_name):
        """Generate demo video frame"""
        # Create a colored frame with camera info
        frame = np.random.randint(20, 60, (480, 640, 3), dtype=np.uint8)
        
        # Add camera name
        cv2.putText(frame, camera_name, (20, 40),
                   cv2.FONT_HERSHEY_SIMPLEX, 0.8, (255, 255, 255), 2)
        
        # Add timestamp
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cv2.putText(frame, timestamp, (20, 460),
                   cv2.FONT_HERSHEY_SIMPLEX, 0.6, (200, 200, 200), 1)
        
        # Add "DEMO MODE" watermark
        cv2.putText(frame, "DEMO MODE", (200, 240),
                   cv2.FONT_HERSHEY_SIMPLEX, 1.5, (0, 165, 255), 3)
        
        return frame
    
    def generate_demo_data(self):
        """Generate demo vehicle detection data"""
        vehicle_classes = ["Car", "Truck", "Bus", "Motorcycle", "Van"]
        camera_ids = ["Cam-1", "Cam-2", "Cam-3", "Cam-4"]
        
        self.demo_vehicle_counter += 1
        
        self.log_panel.add_vehicle(
            camera_id=np.random.choice(camera_ids),
            vehicle_class=np.random.choice(vehicle_classes),
            track_id=self.demo_vehicle_counter,
            timestamp=datetime.now().strftime("%H:%M:%S")
        )
    
    def closeEvent(self, event):
        """Handle window close"""
        # Stop demo timer
        if hasattr(self, 'demo_timer'):
            self.demo_timer.stop()
        
        # Stop Kafka consumer if running
        # if hasattr(self, 'kafka_thread'):
        #     self.kafka_thread.stop()
        #     self.kafka_thread.wait()
        
        event.accept()


def main():
    """Main entry point"""
    app = QApplication(sys.argv)
    
    # Set application style
    app.setStyle('Fusion')
    
    # Dark palette
    # palette = QPalette()
    # palette.setColor(QPalette.ColorRole.Window, QColor(30, 30, 30))
    # palette.setColor(QPalette.ColorRole.WindowText, QColor(255, 255, 255))
    # palette.setColor(QPalette.ColorRole.Base, QColor(25, 25, 25))
    # palette.setColor(QPalette.ColorRole.AlternateBase, QColor(35, 35, 35))
    # palette.setColor(QPalette.ColorRole.Text, QColor(255, 255, 255))
    # palette.setColor(QPalette.ColorRole.Button, QColor(45, 45, 45))
    # palette.setColor(QPalette.ColorRole.ButtonText, QColor(255, 255, 255))
    # palette.setColor(QPalette.ColorRole.Highlight, QColor(0, 122, 204))
    # palette.setColor(QPalette.ColorRole.HighlightedText, QColor(255, 255, 255))
    # app.setPalette(palette)
    
    # Create and show main window
    window = MonitorApp()
    window.show()
    
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
