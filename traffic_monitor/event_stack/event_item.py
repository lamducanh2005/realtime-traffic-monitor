from PyQt6.QtWidgets import QWidget, QLabel, QVBoxLayout, QHBoxLayout
from PyQt6.QtGui import QImage, QPixmap
from PyQt6.QtCore import Qt
import cv2
import numpy as np


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
