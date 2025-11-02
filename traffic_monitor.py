from PyQt6.QtWidgets import QApplication, QMainWindow
import sys
from traffic_monitor import TrafficMonitorContainer

"""
    traffic_monitor.py
    Màn hình giám sát giao thông
"""

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

if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyle('Fusion')
    window = TrafficMonitorWindow()
    window.show()
    sys.exit(app.exec())