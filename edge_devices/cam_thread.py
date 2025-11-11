import cv2
import base64
import json
import time
from datetime import datetime
from PyQt6.QtCore import QThread, pyqtSignal
from kafka import KafkaProducer
from ultralytics import YOLO
import numpy as np
from concurrent.futures import ThreadPoolExecutor

BOOTSTRAP_SERVER = [f"localhost:{i}" for i in range(9092, 9092 + 3)]
STREAMING_TOPIC = "cam_streaming"
TRACKING_TOPIC = "cam_tracking"

class CameraThread(QThread):
    """Thread xử lý video cho mỗi camera"""
    frame_ready = pyqtSignal(object)
    
    def __init__(self, camera_id, video_path, model_path):
        super().__init__()
        self.camera_id = camera_id
        self.video_path = video_path
        self.model_path = model_path
        self.running = False
        self.cap = None
        self.model = None

        self._last_annotated_ts = 0.0
        self._annotated_display_duration = 1.0
        
        # Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVER,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            compression_type='lz4',
            batch_size=256000,
            linger_ms=100,
        )
        
        # Cấu hình theo loại camera
        self.is_intersection = (camera_id % 2 == 0)

        # Executor cho task tracking (chạy song song)
        self.executor = ThreadPoolExecutor(max_workers=1)
        
    def run(self):
        """Chạy camera thread"""
        if not self.video_path:
            return
            
        self.cap = cv2.VideoCapture(self.video_path)
        if not self.cap.isOpened():
            print(f"Không thể mở video {self.video_path}")
            return
            
        # Load YOLO model
        self.model = YOLO(self.model_path)
        
        fps = self.cap.get(cv2.CAP_PROP_FPS)
        frame_delay = 1 / fps if fps > 0 else 1/30
        
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

            # Hiển thị lên màn hình
            self.frame_ready.emit(annotated_frame.copy())

            # Gửi frame tới Kafka
            self._send_streaming(annotated_frame.copy(), timestamp)

            # Chạy riêng tác vụ tracking
            self.executor.submit(self._process_tracking, frame.copy(), timestamp)
            
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
            encode_param = [int(cv2.IMWRITE_JPEG_QUALITY), 30]
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

    def _process_tracking(self, original_frame, timestamp):
        """Thực hiện tracking nặng ở background và gửi cam_tracking; cũng emit annotated frame khi xong"""
        try:
            results = self.model.track(
                original_frame,
                classes=[2, 3, 5, 7],
                conf=0.5,
                iou=0.4,
                persist=True,
                verbose=False
            )

            # annotated_frame = results[0].plot()
            # cv2.putText(annotated_frame, f"Cam {self.camera_id} - {timestamp}", 
            #            (10, 60), cv2.FONT_HERSHEY_SIMPLEX, 0.6, (0, 255, 0), 2)
            # emit annotated frame (cập nhật UI khi tracking xong)
            # self.frame_ready.emit(annotated_frame)
            # ghi lại thời điểm annotated được emit để tránh quick_frame ghi đè ngay
            # try:
            #     self._last_annotated_ts = time.time()
            # except Exception:
            #     pass

            # build objects
            
            objects = []
            if results[0].boxes is not None and getattr(results[0].boxes, "id", None) is not None:
                boxes = results[0].boxes.xyxy.cpu().numpy()
                ids = results[0].boxes.id.cpu().numpy()
                classes = results[0].boxes.cls.cpu().numpy()
                for i in range(len(boxes)):
                    x1, y1, x2, y2 = boxes[i]
                    x, y, w, h = int(x1), int(y1), int(x2 - x1), int(y2 - y1)
                    obj = {
                        "id": str(ids[i]),
                        "box": [x, y, w, h],
                        "type": results[0].names[int(classes[i])]
                    }
                    objects.append(obj)

            # Ghi timestamp lên frame gốc trước khi encode
            tracking_frame = original_frame.copy()
            # cv2.putText(tracking_frame, f"Cam {self.camera_id} - {timestamp}", 
            #            (10, 60), cv2.FONT_HERSHEY_SIMPLEX, 0.6, (0, 255, 0), 2)
            
            # encode với chất lượng gốc
            encode_param = [int(cv2.IMWRITE_JPEG_QUALITY), 50]
            _, orig_buffer = cv2.imencode('.jpg', tracking_frame, encode_param)
            orig_frame_base64 = base64.b64encode(orig_buffer).decode('utf-8')

            tracking_data = {
                "camera_id": f"cam{self.camera_id}",
                "objects": objects,
                "timestamp": timestamp,
                "type": "intersection" if self.is_intersection else "street",
                "frame": orig_frame_base64,
            }

            self.producer.send(
                topic=TRACKING_TOPIC,
                value=tracking_data,
                key=f"cam{self.camera_id}".encode('utf-8')
            )

        except Exception as e:
            # print("Tracking task error:", e)
            pass

    def stop(self):
        """Dừng camera thread"""
        self.running = False
        self.wait()
        
        try:
            self.executor.shutdown(wait=False)
        except Exception:
            pass
        if self.producer:
            self.producer.close()