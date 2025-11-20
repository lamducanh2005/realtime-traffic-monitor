import cv2
import base64
import json
import time
from datetime import datetime
from PyQt6.QtCore import QThread, pyqtSignal
from kafka import KafkaProducer


BOOTSTRAP_SERVER = [f"localhost:{i}" for i in range(9092, 9092 + 3)]
STREAMING_TOPIC = "cam_raw"

class CameraThread(QThread):
    """Thread xử lý video cho mỗi camera"""
    frame_ready = pyqtSignal(object)
    
    def __init__(self, camera_id, video_path, model_path):
        super().__init__()
        self.camera_id = camera_id
        self.video_path = video_path
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
        
    def run(self):
        """Chạy camera thread"""
        if not self.video_path:
            return
            
        self.cap = cv2.VideoCapture(self.video_path)
        if not self.cap.isOpened():
            print(f"Không thể mở video {self.video_path}")
            return
        
        fps = self.cap.get(cv2.CAP_PROP_FPS)
        frame_delay = 1.0 / fps
        
        self.running = True
        last_time = time.time()
        
        while self.running and self.cap.isOpened():
            ret, frame = self.cap.read()
            
            if not ret:
                # Loop video
                self.cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
                continue
            
            # timestamp
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # Resize frame về 1280x720
            frame = cv2.resize(frame, (1280, 720))

            # Viết ngày tháng giờ lên frame nhìn cho giống camera
            annotated_frame = frame.copy()
            cv2.putText(annotated_frame, f"Cam {self.camera_id} - {timestamp}", 
                       (10, 60), cv2.FONT_HERSHEY_SIMPLEX, 0.6, (0, 255, 0), 2)

            # Hiển thị lên màn hình (LUÔN hiển thị để UI mượt)
            self.frame_ready.emit(annotated_frame.copy())

            # Gửi frame tới Kafka
            self._send_streaming(annotated_frame.copy(), timestamp)
            
            # Control FPS
            elapsed = time.time() - last_time
            sleep_time = frame_delay - elapsed
            if sleep_time > 0:
                time.sleep(sleep_time)
            last_time = time.time()
        
        self.cap.release()
        
    def _send_streaming(self, frame, timestamp):
        """Gửi dữ liệu streaming tới STREAMING_TOPIC"""
        try:
            # Chuyển thành base64, giảm chất lượng ảnh
            encode_param = [int(cv2.IMWRITE_JPEG_QUALITY), 60]
            _, buffer = cv2.imencode('.jpg', frame, encode_param)
            frame_base64 = base64.b64encode(buffer).decode('utf-8')
            
            streaming_data = {
                "camera_id": f"cam{self.camera_id}",
                "timestamp": timestamp,
                "type": "intersection" if self.is_intersection else "street",
                "frame": frame_base64,
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