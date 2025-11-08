import cv2
from PyQt6.QtWidgets import QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton, QFileDialog
from PyQt6.QtCore import Qt
from PyQt6.QtGui import QImage, QPixmap
import numpy as np
from .cam_thread import CameraThread

class VideoDisplay(QWidget):
    """Widget hiển thị video"""
    
    def __init__(self):
        super().__init__()
        
        layout = QVBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)
        self.setLayout(layout)
        
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
        
        layout.addWidget(self.video_label)
    
    def show_placeholder(self, camera_id):
        """Hiển thị placeholder"""
        placeholder = np.zeros((600, 800, 3), dtype=np.uint8)
        cv2.putText(
            placeholder, f"Camera {camera_id}", 
                   (200, 250), cv2.FONT_HERSHEY_SIMPLEX, 1.5, (100, 100, 100), 2)
        
        cv2.putText(placeholder, "Tai len video de bat dau", 
                   (150, 350), cv2.FONT_HERSHEY_SIMPLEX, 1, (100, 100, 100), 2)
        
        self.update_frame(placeholder)
    
    def update_frame(self, frame):
        """
            Thực hiện cập nhật frame

            Args:
                frame: Khung hình màu BGR theo thứ tự (H, W, 3).
            
            Returns:
                None
        """

        if frame is None:
            return
        
        rgb_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
        h, w, ch = rgb_frame.shape
        bytes_per_line = ch * w
        
        qt_image = QImage(rgb_frame.data, w, h, bytes_per_line, QImage.Format.Format_RGB888)
        
        scaled_pixmap = QPixmap.fromImage(qt_image).scaled(
            self.video_label.size(),
            Qt.AspectRatioMode.KeepAspectRatio,
            Qt.TransformationMode.SmoothTransformation
        )
        
        self.video_label.setPixmap(scaled_pixmap)

class ControlPanel(QWidget):
    """Panel điều khiển camera"""
    
    def __init__(self, camera_id, on_load, on_toggle):
        super().__init__()
        self.camera_id = camera_id
        self.setStyleSheet("""
            QWidget {
                background-color: #252526;
                border-radius: 8px;
            }
            QLabel {
                color: #cccccc;
                font-size: 20px;
                background: transparent;
            }
            QPushButton {
                background-color: #3a3f44;
                color: #f0f0f0;
                border: 1px solid #4b4f54;
                border-radius: 6px;
                padding: 8px 12px;
                font-size: 14px;
            }
            QPushButton:hover {
                background-color: #50565b;
            }
            
            QPushButton:pressed {
                background-color: #2f3336;
                padding-top: 10px; /* subtle pressed effect */
                padding-bottom: 6px;
            }
            QPushButton:disabled {
                background-color: #2b2b2b;
                color: #777;
                border-color: #2b2b2b;
            }
            QPushButton#control_btn {
                background-color: #2b8de9;
                border: 1px solid #2166b2;
                font-weight: 600;
            }
            QPushButton#control_btn:hover {
                background-color: #3a99ef;
            }
            QPushButton#control_btn:pressed {
                background-color: #1f5fb0;
            }
        """)

        self.setFixedWidth(200)
        
        layout = QVBoxLayout()
        layout.setContentsMargins(15, 10, 15, 10)
        layout.setSpacing(10)
        self.setLayout(layout)
        
        # File info
        self.info_label = QLabel(f"Camera {camera_id}")
        self.info_label.setWordWrap(True)
        layout.addWidget(self.info_label)
        
        # Load button
        self.load_btn = QPushButton("📁 Tải video")
        self.load_btn.setMinimumHeight(40)
        self.load_btn.setCursor(Qt.CursorShape.PointingHandCursor)

        self.load_btn.clicked.connect(on_load)
        layout.addWidget(self.load_btn)
        
        # Control button
        self.control_btn = QPushButton("▶️ Start")
        self.control_btn.setMinimumHeight(40)
        self.control_btn.clicked.connect(on_toggle)
        self.control_btn.setEnabled(False)
        layout.addWidget(self.control_btn)
        
        # Stretch để đẩy các button lên trên
        layout.addStretch()
    
    def set_loaded(self, filename):
        """Cập nhật khi tải video"""
        # self.info_label.setText(f"Camera {self.camera_id}: {filename}")
        self.control_btn.setEnabled(True)
    
    def set_running(self, is_running):
        """Cập nhật trạng thái running"""
        if is_running:
            self.control_btn.setText("⏹️ Dừng")
            self.load_btn.setEnabled(False)
        else:
            self.control_btn.setText("▶️ Bắt đầu")
            self.load_btn.setEnabled(True)


class CameraPanel(QWidget):
    """Panel cho một camera - gồm header, video display, control"""
    
    def __init__(self, camera_id):
        super().__init__()
        self.camera_id = camera_id
        self.thread = None
        self.video_path = None
        
        self.main_layout = QHBoxLayout()
        self.main_layout.setContentsMargins(10, 10, 10, 10)
        self.main_layout.setSpacing(10)
        self.setLayout(self.main_layout)
        
        self.setup_ui()

    def setup_ui(self):
        
        # Video Display
        self.video_display = VideoDisplay()
        self.video_display.show_placeholder(self.camera_id)
        self.main_layout.addWidget(self.video_display, stretch=1)

        # Control Panel
        self.control_panel = ControlPanel(
            self.camera_id,
            on_load=self.load_video,
            on_toggle=self.toggle_camera
        )
        self.main_layout.addWidget(self.control_panel, stretch=0)

    def load_video(self):
        """Chọn video từ một đường dẫn"""
        
        video_path, _ = QFileDialog.getOpenFileName(
            parent=self, 
            caption="Chọn video", 
            directory="", 
            filter="Video Files (*.mp4 *.avi *.mov)"
        )
        
        if video_path:
            self.video_path = video_path
            filename = video_path.split('/')[-1]
            self.control_panel.set_loaded(filename)

    def start_camera(self):
        """Khởi động camera"""
        
        if not self.video_path:
            return
        
        model_path = "resources/models/yolo11n.pt"
        
        self.thread = CameraThread(self.camera_id, self.video_path, model_path)
        self.thread.frame_ready.connect(self.video_display.update_frame)
        self.thread.start()
        
        self.control_panel.set_running(True)

    def toggle_camera(self):
        """Bật/tắt camera"""
        if self.thread is None or not self.thread.running:
            self.start_camera()
        else:
            self.stop_camera()
    
    def stop_camera(self):
        """Dừng camera"""
        
        if self.thread:
            self.thread.stop()
            self.control_panel.set_running(False)

    def close_camera(self):
        """Đóng camera khi tab bị đóng"""

        if self.thread and self.thread.running:
            self.stop_camera()
