from PyQt5.QtWidgets import QApplication, QMainWindow
import sys
from edge_devices import EdgeDeviceContainer

"""
    edge_device.py
    Mô phỏng thiết bị biên
"""


class EdgeDeviceWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Edge Devices")
        self.setGeometry(100, 100, 1200, 1000)

        self.setStyleSheet("""
            QMainWindow {
                background-color: #1e1e1e;
            }
        """)

        self.central_widget = EdgeDeviceContainer()
        self.setCentralWidget(self.central_widget)

    def closeEvent(self, event):
        self.central_widget.close_all()
        event.accept()


if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyle('Fusion')
    window = EdgeDeviceWindow()
    window.show()
    sys.exit(app.exec_())
