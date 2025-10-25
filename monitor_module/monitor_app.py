from PyQt6.QtWidgets import *
from PyQt6.QtGui import *
from PyQt6.QtCore import *
import sys
import cv2
import numpy as np

CAMERA_TRACK_TOPIC = "camera_track"
BOOTSTRAP_SERVER = "localhost:9092"

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
        """Xếp các children vào layout"""

        title_label = QLabel("Màn hình giám sát giao thông")
        # title_label.setFont(QFont("Arial", 20, QFont.Weight.Bold))
        title_label.setStyleSheet("color: #DFD0B8; border: none; font-size: 20px; font-weight:bold")
        self.main_layout.addWidget(title_label)

class SideBar(QWidget):
    camera_selected = pyqtSignal(str, int)

    def __init__(self):
        super().__init__()

        self.setMinimumWidth(120)
        self.setStyleSheet("""
            QWidget {
                background-color: #252526;
            }
        """)

        self.main_layout = QVBoxLayout()
        self.main_layout.setContentsMargins(15, 15, 15, 15)
        self.main_layout.setSpacing(10)
        self.setLayout(self.main_layout)

        self.setup_ui()

    def setup_ui(self):

        # Title
        title = QLabel("Các camera")
        title.setStyleSheet("color: #fff; padding: 5px; font-size: 14px")
        self.main_layout.addWidget(title)


        # Danh sách camera
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
                color: #ffffff;
            }
        """)

        cameras = ['Cam 1', 'Cam 2']

        for cam in cameras:
            self.camera_list.addItem(cam)
        
        self.camera_list.currentRowChanged.connect(self._on_camera_selected)
        self.main_layout.addWidget(self.camera_list)

    def _on_camera_selected(self, index):

        if index >= 0:
            camera_name = self.camera_list.item(index).text()
            self.camera_selected.emit(camera_name, index)


class VideoPanel(QWidget):

    frame_updated = pyqtSignal(np.ndarray)

    def __init__(self):
        super().__init__()
        self.setStyleSheet("background-color: #1e1e1e")

        self.main_layout = QVBoxLayout(self)
        self.main_layout.setContentsMargins(10, 10, 10, 10)
        self.main_layout.setSpacing(10)
        self.setLayout(self.main_layout)

        self.setup_ui()

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
        self.video_label.setMinimumSize(640, 480)
        self.main_layout.addWidget(self.video_label, stretch=7)

        # Route information panel
        route_panel = StatisticPanel()
        self.main_layout.addWidget(route_panel, stretch=2)
        
        # Show placeholder
        self.show_placeholder()

    def show_placeholder(self):
        placeholder = np.zeros((480, 640, 3), dtype=np.uint8)
        cv2.putText(placeholder, "Chon camera de xem video", 
                   (120, 240), cv2.FONT_HERSHEY_SIMPLEX, 0.8, (100, 100, 100), 2)
        cv2.putText(placeholder, "Dang cho ket noi Kafka...", 
                   (140, 280), cv2.FONT_HERSHEY_SIMPLEX, 0.6, (80, 80, 80), 1)
        self.update_frame(placeholder)

    def update_frame(self, frame):
        """Cập nhật video frame"""
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


class VehicleLog(QWidget):
    def __init__(self):
        super().__init__()
        self.setFixedWidth(400)
        self.setStyleSheet("""
            QWidget {
                background-color: #252526;
            }
        """)

        self.main_layout = QVBoxLayout()
        self.main_layout.setContentsMargins(15, 15, 15, 15)
        self.main_layout.setSpacing(10)
        self.setLayout(self.main_layout)

        self.setup_ui()

    def setup_ui(self):
        """Xếp các children vào layout"""
        
        # Title
        title = QLabel("📋 Nhật ký phương tiện")
        title.setStyleSheet("color: #ffffff; font-size: 14px; font-weight: bold;")
        self.main_layout.addWidget(title)
        
        # Table
        self.table = QTableWidget()
        self.table.setColumnCount(4)
        self.table.setHorizontalHeaderLabels(["Camera", "Loại xe", "Track ID", "Thời gian"])

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
        
        self.main_layout.addWidget(self.table)

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
        stats_layout = QVBoxLayout()
        stats_frame.setLayout(stats_layout)
        
        self.stats_label = QLabel("Tổng số xe: 0")
        self.stats_label.setStyleSheet("color: #ffffff; font-size: 12px; border: none;")
        stats_layout.addWidget(self.stats_label)
        
        self.main_layout.addWidget(stats_frame)

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
        """Xếp các children vào layout"""
        
        # Title
        title = QLabel("📊 Thông tin đường")
        title.setStyleSheet("color: #ffffff; border: none; font-size: 12px; font-weight: bold;")
        self.main_layout.addWidget(title)
        
        # Info text
        self.info_label = QLabel(
            "Chọn camera để xem chi tiết.\n"
            "Thông tin lưu lượng, tốc độ và mật độ giao thông sẽ hiển thị tại đây."
        )
        self.info_label.setStyleSheet("color: #cccccc; border: none; font-size: 11px;")
        self.info_label.setWordWrap(True)
        self.main_layout.addWidget(self.info_label)

    def update_info(self, camera_name):
        """Cập nhật thông tin route"""
        self.info_label.setText(
            f"Camera: {camera_name}\n"
            f"Trạng thái: Hoạt động\n"
            f"Tốc độ TB: -- km/h | Xe phát hiện: -- | Lưu lượng: -- xe/phút"
        )

class ContentArea(QWidget):
    def __init__(self):
        super().__init__()

        self.main_layout = QHBoxLayout()
        self.main_layout.setContentsMargins(0, 0, 0, 0)
        self.setLayout(self.main_layout)

        self.setup_ui()
    
    def setup_ui(self):

        sidebar = SideBar()
        self.main_layout.addWidget(sidebar)

        video_panel = VideoPanel()
        self.main_layout.addWidget(video_panel)

        vehicle_log = VehicleLog()
        self.main_layout.addWidget(vehicle_log)
        


class MonitorAppContainer(QWidget):
    def __init__(self):
        super().__init__()

        # Thiết đặt Layout
        self.main_layout = QVBoxLayout()
        self.main_layout.setContentsMargins(0, 0, 0, 0)
        self.main_layout.setSpacing(0)
        self.setLayout(self.main_layout)

        self.setup_ui()

    def setup_ui(self):
        """ Xếp các children vào layout """
        
        # Header Bar
        self.header = HeaderBar()
        self.main_layout.addWidget(self.header)

        # Content Area
        self.content = ContentArea()
        self.main_layout.addWidget(self.content)



class MonitorApp(QMainWindow):
    def __init__(self):
        super().__init__()

        self.setWindowTitle("Màn hình giám sát giao thông")
        self.setGeometry(100, 100, 1400, 900)

        self.container = MonitorAppContainer()
        self.setCentralWidget(self.container)
    

app = QApplication(sys.argv)
app.setStyle('Fusion')
    
window = MonitorApp()
window.show()

sys.exit(app.exec())

