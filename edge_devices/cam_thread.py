import cv2
import base64
import json
import time
from datetime import datetime, timedelta
from PyQt6.QtCore import QThread, pyqtSignal
from kafka import KafkaProducer


BOOTSTRAP_SERVER = [f"10.11.193.172:{i}" for i in range(9092, 9092 + 6)]
STREAMING_TOPIC = "cam_raw"

class CameraThread(QThread):
    """Thread xử lý video cho mỗi camera"""
    frame_ready = pyqtSignal(object)
    
    def __init__(self, camera_id, video_path, model_path):
        super().__init__()
        self.camera_id = camera_id
        self.video_path = video_path
        self.video_name = None
        if video_path:
            raw_name = video_path.split('/')[-1] if '/' in video_path else video_path.split('\\')[-1]
            self.video_name = raw_name.rsplit('.', 1)[0]
        self.running = False
        self.cap = None

        # Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVER,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            compression_type='lz4',
            batch_size=25600,
            linger_ms=10,
        )

        # Cấu hình theo loại camera
        self.is_intersection = (camera_id % 2 == 0)

        # Buffer để delay hiển thị 5 giây
        self.display_buffer = []
        self.display_delay = 15.0  # seconds


    def run(self):
        """Chạy camera thread"""
        if not self.video_path:
            return

        self.cap = cv2.VideoCapture(self.video_path)
        if not self.cap.isOpened():
            print(f"Không thể mở video {self.video_path}")
            return

        fps = self.cap.get(cv2.CAP_PROP_FPS)
        frame_delay = 0.05

        self.running = True
        last_time = time.time()

        # Thời điểm bắt đầu video (frame đầu tiên)
        video_start_time = None

        # Số thứ tự frame gốc
        frame_no = 0

        # Lấy tên file video nếu chưa có
        if self.video_name is None and self.video_path:
            raw_name = self.video_path.split('/')[-1] if '/' in self.video_path else self.video_path.split('\\')[-1]
            self.video_name = raw_name.rsplit('.', 1)[0]

        while self.running and self.cap.isOpened():
            ret, frame = self.cap.read()

            if not ret:
                # Loop video: reset thời điểm bắt đầu
                self.cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
                video_start_time = None
                frame_no = 0
                continue

            # Timestamp hiện tại
            current_time = datetime.now()

            # Timestamp + 5 giây (để khi delay 5s thì hiển thị đúng thời gian)
            timestamp_send = (current_time + timedelta(seconds=self.display_delay)).strftime("%Y-%m-%d %H:%M:%S")

            # Viết timestamp (+5s) lên frame
            annotated_frame = frame.copy()
            cv2.putText(annotated_frame, f"Cam {self.camera_id} - {timestamp_send}", 
                       (10, 60), cv2.FONT_HERSHEY_SIMPLEX, 0.6, (0, 255, 0), 2)

            # Thời điểm bắt đầu video (frame đầu tiên)
            now_ts = time.time()
            if video_start_time is None:
                video_start_time = now_ts

            # Tính số giây từ frame đầu tiên
            elapsed_sec = round(now_ts - video_start_time, 1)

            # Thêm vào buffer để delay hiển thị 5 giây
            self.display_buffer.append({
                'frame': annotated_frame.copy(),
                'time': now_ts
            })

            # Hiển thị frame đã delay 5 giây
            current_timestamp = time.time()
            while self.display_buffer and (current_timestamp - self.display_buffer[0]['time']) >= self.display_delay:
                delayed_frame = self.display_buffer.pop(0)
                self.frame_ready.emit(delayed_frame['frame'])

            # Gửi frame tới Kafka với timestamp tăng 5 giây (NGAY LẬP TỨC), bổ sung trường 'time' và 'name'
            self._send_streaming(annotated_frame.copy(), timestamp_send, elapsed_sec, self.video_name, frame_no)

            # Control FPS
            elapsed = time.time() - last_time
            sleep_time = frame_delay - elapsed
            if sleep_time > 0:
                time.sleep(sleep_time)
            last_time = time.time()

            frame_no += 1

        self.cap.release()


    def _send_streaming(self, frame, timestamp, elapsed_sec, video_name, frame_no):
        """Gửi dữ liệu streaming tới STREAMING_TOPIC"""
        try:
            # Chuyển thành base64, giảm chất lượng ảnh
            encode_param = [int(cv2.IMWRITE_JPEG_QUALITY), 60]
            _, buffer = cv2.imencode('.jpg', frame, encode_param)
            frame_base64 = base64.b64encode(buffer).decode('utf-8')

            streaming_data = {
                "camera_id": f"{self.camera_id}",
                "timestamp": timestamp,
                "type": "intersection" if self.is_intersection else "street",
                # "frame": frame_base64,
                "time": elapsed_sec,
                "name": video_name,
                "no": frame_no
            }

            # gửi đi async
            self.producer.send(
                topic=STREAMING_TOPIC,
                value=streaming_data,
                key=f"cam{self.camera_id}".encode('utf-8')
            )
        except Exception as e:
            print("Streaming send error:", e)

    def stop(self):
        """Dừng camera thread"""
        self.running = False
        self.wait()
        
        if self.producer:
            self.producer.close()