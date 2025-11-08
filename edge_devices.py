from PyQt6.QtWidgets import QApplication
import sys
from edge_devices import EdgeDeviceWindow

"""
    edge_device.py
    Chạy trình mô phỏng thiết bị biên
"""

if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyle('Fusion')
    window = EdgeDeviceWindow()
    window.show()
    sys.exit(app.exec())
