from PyQt6.QtWidgets import QWidget, QLabel, QVBoxLayout, QHBoxLayout, QTabWidget
from PyQt6.QtCore import Qt

from .event_stack.event_stack import EventStack
from .video_panel.video_panel import VideoPanel
from .statistic_panel.statistic_panel import StatisticPanel


class MonitorTab(QWidget):
    def __init__(self, camera_id: str):
        super().__init__()
        self.camera_id = camera_id

        self.main_layout = QVBoxLayout()
        self.main_layout.setContentsMargins(6, 6, 6, 6)
        self.main_layout.setSpacing(6)
        self.setLayout(self.main_layout)

        self.setup_ui()
    
    def setup_ui(self):

        # Video + event stack
        content = QHBoxLayout()
        content.setSpacing(5)

        # Video
        self.video_panel = VideoPanel(self.camera_id)
        content.addWidget(self.video_panel, stretch=3)

        # Event stack
        self.event_stack = EventStack(self.camera_id)
        self.event_stack.setMinimumWidth(240)
        content.addWidget(self.event_stack, stretch=1)

        self.main_layout.addLayout(content, stretch=8)

        # Stats bar
        self.stat_panel = StatisticPanel()
        self.main_layout.addWidget(self.stat_panel, stretch=0)

    def closeEvent(self, event):
        """Called automatically when widget is closed"""
        try:
            self.video_panel.close()
        except Exception:
            pass
        try:
            self.event_stack.close()
        except Exception:
            pass
        if event:
            event.accept()


class TrafficMonitorContainer(QWidget):
    """Container widget với header bar và tab widget."""
    
    def __init__(self):
        super().__init__()
        self.main_layout = QVBoxLayout()
        self.main_layout.setContentsMargins(0, 0, 0, 0)
        self.setLayout(self.main_layout)
        
        self.setup_ui()

    def setup_ui(self):
        
        # Header
        header = QLabel("🚨 Hệ thống giám sát giao thông")
        header.setStyleSheet("""
            color: #ffffff; 
            font-size: 18px;
            font-weight: bold;
            padding: 10px; 
            background-color: #393E46; 
            border-radius: 5px
        """)
        header.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.main_layout.addWidget(header)

        # Tab Widget
        self.tab_widget = QTabWidget()
        self.tab_widget.setTabPosition(QTabWidget.TabPosition.North)
        
        for i in range(1, 5):
            cam_id = f"cam{i}"
            tab = MonitorTab(cam_id)
            self.tab_widget.addTab(tab, f"Camera {i}")
        
        self.main_layout.addWidget(self.tab_widget)
    
    def close_all(self):
        """Đóng tất cả tabs"""
        for i in range(self.tab_widget.count()):
            w = self.tab_widget.widget(i)
            try:
                w.closeEvent(None)
            except Exception:
                pass
    
    def closeEvent(self, event):
        """Called when widget is closed"""
        self.close_all()
        if event:
            event.accept()