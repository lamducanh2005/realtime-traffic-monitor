from PyQt6.QtWidgets import QWidget, QLabel, QVBoxLayout, QHBoxLayout
from PyQt6.QtGui import QImage, QPixmap
from PyQt6.QtCore import Qt
import cv2
import numpy as np


class EventItem(QWidget):
    def __init__(
            self, 
            num_plate: str = "--", 
            timestamp: str = "--",
            warning: str = "",
            plate_frame: np.ndarray = None, 
            obj_frame: np.ndarray = None
        ):
        super().__init__()
        self.num_plate = num_plate
        self.warning = warning
        self.timestamp = timestamp
        self.plate_frame = plate_frame
        self.obj_frame = obj_frame

        self.setStyleSheet("""
            EventItem {
                background-color: #2b2b2b;
                border: 1px solid #555555;
                border-radius: 8px;
                padding: 8px;
            }
        """)
        self.setGraphicsEffect(self.create_shadow())
        
        self.main_layout = QVBoxLayout()
        self.main_layout.setContentsMargins(8, 8, 8, 8)
        self.main_layout.setSpacing(8)
        self.setLayout(self.main_layout)

        self.setup_ui()

    def create_shadow(self):
        """Tạo hiệu ứng đổ bóng"""
        from PyQt6.QtWidgets import QGraphicsDropShadowEffect
        from PyQt6.QtGui import QColor
        
        shadow = QGraphicsDropShadowEffect()
        shadow.setBlurRadius(10)
        shadow.setXOffset(0)
        shadow.setYOffset(2)
        shadow.setColor(QColor(0, 0, 0, 80))
        return shadow

    def setup_ui(self):
        # Row 1: Hình ảnh xe và biển số (nằm ngang)
        images_layout = QHBoxLayout()
        images_layout.setSpacing(8)
        
        # Hình ảnh xe (bên trái)
        self.vehicle_label = QLabel()
        self.vehicle_label.setFixedSize(160, 120)
        self.vehicle_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.vehicle_label.setStyleSheet("""
            background-color: #1e1e1e; 
            border: 1px solid #444444;
            border-radius: 4px;
        """)
        images_layout.addWidget(self.vehicle_label)
        
        # Hình ảnh biển số (bên phải)
        self.plate_label = QLabel()
        self.plate_label.setFixedSize(140, 120)
        self.plate_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.plate_label.setStyleSheet("""
            background-color: #1e1e1e; 
            border: 1px solid #444444;
            border-radius: 4px;
        """)
        images_layout.addWidget(self.plate_label)
        
        self.main_layout.addLayout(images_layout)

        # Row 2: Thông tin (bên dưới)
        info_layout = QVBoxLayout()
        info_layout.setSpacing(4)
        
        # Hiển thị biển số; nếu có warning thì đổi màu sang đỏ và thêm chú thích
        display_text = f"Biển số: {self.num_plate}"
        css = """
            color: #00ff88; 
            font-weight: bold; 
            font-size: 14px;
        """
        if isinstance(self.warning, str) and self.warning.strip():
            display_text = f"Biển số: {self.num_plate} ({self.warning})"
            css = """
                color: #ff4444; 
                font-weight: bold; 
                font-size: 14px;
            """

        self.num_plate_label = QLabel(display_text)
        self.num_plate_label.setStyleSheet(css)
        # Thêm tooltip để dễ đọc warning nếu bị cắt
        if isinstance(self.warning, str) and self.warning.strip():
            self.num_plate_label.setToolTip(self.warning)
        
        self.time_label = QLabel(f"Thời gian: {self.timestamp}")
        self.time_label.setStyleSheet("""
            color: #aaaaaa; 
            font-size: 12px;
        """)

        info_layout.addWidget(self.num_plate_label)
        info_layout.addWidget(self.time_label)

        self.main_layout.addLayout(info_layout)

        # Set images nếu có
        if self.obj_frame is not None:
            self.set_vehicle_image(self.obj_frame)
        
        if self.plate_frame is not None:
            self.set_plate_image(self.plate_frame)
    
    def set_vehicle_image(self, img: np.ndarray):
        """Set hình ảnh xe"""
        try:
            rgb = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
            h, w, ch = rgb.shape
            bytes_per_line = ch * w
            qimg = QImage(rgb.data, w, h, bytes_per_line, QImage.Format.Format_RGB888)
            pix = QPixmap.fromImage(qimg).scaled(
                self.vehicle_label.size(), 
                Qt.AspectRatioMode.KeepAspectRatio, 
                Qt.TransformationMode.SmoothTransformation
            )
            self.vehicle_label.setPixmap(pix)
        except Exception:
            pass

    def set_plate_image(self, img: np.ndarray):
        """Set hình ảnh biển số"""
        try:
            rgb = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
            h, w, ch = rgb.shape
            bytes_per_line = ch * w
            qimg = QImage(rgb.data, w, h, bytes_per_line, QImage.Format.Format_RGB888)
            pix = QPixmap.fromImage(qimg).scaled(
                self.plate_label.size(), 
                Qt.AspectRatioMode.KeepAspectRatio, 
                Qt.TransformationMode.SmoothTransformation
            )
            # Center the pixmap trong label
            self.plate_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            self.plate_label.setPixmap(pix)
        except Exception:
            pass
