from PyQt6.QtWidgets import QWidget, QVBoxLayout, QLabel, QTabWidget
from PyQt6.QtCore import Qt
from .panel import CameraPanel

class EdgeDeviceContainer(QWidget):
    
    def __init__(self):
        super().__init__()

        self.setStyleSheet("""
            background-color: black
        """)

        self.main_layout = QVBoxLayout()
        self.main_layout.setContentsMargins(10, 10, 10, 10)
        self.main_layout.setSpacing(10)
        self.setLayout(self.main_layout)

        self.camera_panels = []
        self.setup_ui()

    
    def setup_ui(self):

        # Header Bar
        header_bar = self.__header_bar_ui()
        self.main_layout.addWidget(header_bar)

        # Camera Tab
        tab_widget = self.__tab_widget_ui()
        self.main_layout.addWidget(tab_widget)

    def __header_bar_ui(self):
        title = QLabel("🚨 Hệ thống Giả lập Camera - Edge Devices")
        title.setStyleSheet("""
            color: #ffffff; 
            border: none; 
            font-size: 22px; 
            font-weight: bold;
            padding: 10px;
            background-color: #393E46;
            border-radius: 5px;
        """)
        # use PyQt6 AlignmentFlag
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)

        return title
    
    def __tab_widget_ui(self):
        tab_widget = QTabWidget()
        tab_widget.setTabPosition(QTabWidget.TabPosition.North)
        tab_widget.setStyleSheet("""
            QTabWidget::pane {
                background: #111;
            }
            QTabBar::tab {
                background: #2b2f33;
                color: #ddd;
                padding: 8px 16px;
                border: 1px solid transparent;
                border-top-left-radius: 6px;
                border-top-right-radius: 6px;
                min-width: 110px;
                margin-right: 0px;
            }
            QTabBar::tab:hover {
                background: #3a4045;
            }
            QTabBar::tab:selected {
                background: #111;
                color: white;
            }
            QTabBar::tab:!selected {
                margin-top: 0px; /* tạo hiệu ứng tab nổi */
            }
            QTabBar::close-button {
                image: url(close.png); /* nếu có icon đóng */
            }
        """)
        
        for i in range(1, 4 + 1):
            camera_panel = CameraPanel(i)
            self.camera_panels.append(camera_panel)
            tab_widget.addTab(camera_panel, f"Camera {i}")

        return tab_widget

    def close_all(self):
        """Đóng tất cả camera"""
        for panel in self.camera_panels:
            if panel.thread and panel.thread.running:
                panel.stop_camera()