import cv2
import base64
import json
import time
from datetime import datetime
from PyQt5.QtCore import QThread, pyqtSignal
from kafka import KafkaProducer
from ultralytics import YOLO
import numpy as np

BOOTSTRAP_SERVER = "localhost:9092"
STREAMING_TOPIC = "cam_streaming"
TRACKING_TOPIC = "cam_tracking"

class CameraThread(QThread):
    """Thread xử lý video cho mỗi camera"""
    frame_ready = pyqtSignal(np.ndarray)
    
    def __init__(self, camera_id, video_path, model_path):
        super().__init__()
        self.camera_id = camera_id
        self.video_path = video_path
        self.model_path = model_path
        self.running = False
        self.cap = None
        self.model = None
        
        # Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVER,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            compression_type='lz4',
            batch_size=16384,
            linger_ms=10,
        )
        
        # Cấu hình theo loại camera
        # Camera lẻ: đường thường, Camera chẵn: ngã tư
        self.is_intersection = (camera_id % 2 == 0)
        
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
            
            # YOLO tracking
            results = self.model.track(
                frame, 
                classes=[2, 3, 5, 7], 
                conf=0.5,
                iou=0.4,
                persist=True, 
                verbose=False
            )
            
            # Vẽ frame với tracking
            annotated_frame = results[0].plot()
            
            # Thêm thông tin camera và timestamp
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            cv2.putText(annotated_frame, f"Cam {self.camera_id} - {timestamp}", 
                       (10, 60), cv2.FONT_HERSHEY_SIMPLEX, 0.6, (0, 255, 0), 2)
            
            # Emit frame để hiển thị
            self.frame_ready.emit(annotated_frame)
            
            # Gửi dữ liệu qua Kafka
            self._send_to_kafka(frame, annotated_frame, results[0], timestamp)
            
            # Control FPS
            elapsed = time.time() - last_time
            sleep_time = frame_delay - elapsed
            if sleep_time > 0:
                time.sleep(sleep_time)
            last_time = time.time()
        
        self.cap.release()
        
    def _send_to_kafka(self, original_frame, annotated_frame, result, timestamp):
        """Gửi dữ liệu qua Kafka"""
        
        # Luồng nhẹ - cam_streaming (frame đã vẽ + timestamp)
        _, buffer = cv2.imencode('.jpg', annotated_frame, [cv2.IMWRITE_JPEG_QUALITY, 70])
        frame_base64 = base64.b64encode(buffer).decode('utf-8')
        
        streaming_data = {
            "camera_id": f"cam{self.camera_id}",
            "frame": frame_base64,
            "timestamp": timestamp,
            "type": "intersection" if self.is_intersection else "street"
        }
        
        self.producer.send(
            topic=STREAMING_TOPIC,
            value=streaming_data,
            key=f"cam{self.camera_id}".encode('utf-8')
        )
        
        # Luồng nặng - cam_tracking (frame gốc + objects tracking)
        objects = []
        
        if result.boxes is not None and result.boxes.id is not None:
            boxes = result.boxes.xyxy.cpu().numpy()
            ids = result.boxes.id.cpu().numpy()
            classes = result.boxes.cls.cpu().numpy()
            
            for i in range(len(boxes)):
                obj = {
                    "id": int(ids[i]),
                    "box": boxes[i].tolist(),  # [x1, y1, x2, y2]
                    "type": result.names[int(classes[i])]
                }
                objects.append(obj)
        
        # Encode frame gốc
        _, orig_buffer = cv2.imencode('.jpg', original_frame, [cv2.IMWRITE_JPEG_QUALITY, 85])
        orig_frame_base64 = base64.b64encode(orig_buffer).decode('utf-8')
        
        tracking_data = {
            "camera_id": f"cam{self.camera_id}",
            "frame": orig_frame_base64,
            "objects": objects,
            "timestamp": timestamp,
            "type": "intersection" if self.is_intersection else "street"
        }
        
        self.producer.send(
            topic=TRACKING_TOPIC,
            value=tracking_data,
            key=f"cam{self.camera_id}".encode('utf-8')
        )
    
    def stop(self):
        """Dừng camera thread"""
        self.running = False
        self.wait()
        if self.producer:
            self.producer.close()
