from PyQt6.QtWidgets import QMainWindow
from .main_container import TrafficMonitorContainer

class TrafficMonitorWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Traffic Monitor")
        self.setGeometry(100, 100, 1200, 900)

        self.setStyleSheet("""
            QMainWindow {
                background-color: #1e1e1e;
            }
        """)

        self.container = TrafficMonitorContainer()
        self.setCentralWidget(self.container)

    def closeEvent(self, event):
        self.container.close_all()
        event.accept()