from PyQt6.QtWidgets import QApplication
import sys
from traffic_monitor import TrafficMonitorWindow

"""
    traffic_monitor.py
    Khởi động màn hình giám sát giao thông
"""

if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyle('Fusion')
    window = TrafficMonitorWindow()
    window.show()
    sys.exit(app.exec())