from PyQt6.QtWidgets import QWidget, QLabel, QVBoxLayout

class StatisticPanel(QWidget):
    def __init__(self):
        super().__init__()

        self.setStyleSheet("background-color: #252526; border:1px solid #3e3e42; border-radius:6px; padding:8px")
        self.main_layout = QVBoxLayout()
        self.main_layout.setContentsMargins(10, 6, 10, 6)
        self.setLayout(self.main_layout)

        self.setup_ui()

    def setup_ui(self):
        title = QLabel("📊 Thống kê đường")
        title.setStyleSheet("color: #ffffff; font-weight:bold")
        self.main_layout.addWidget(title)

        self.info_label = QLabel("Tốc độ TB: -- km/h | Xe phát hiện: -- | Lưu lượng: -- xe/phút")
        self.info_label.setStyleSheet("color: #cccccc; font-size:12px")
        self.main_layout.addWidget(self.info_label)
