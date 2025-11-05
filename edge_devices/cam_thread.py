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

BOOTSTRAP_SERVER = [f"localhost:{i}" for i in range(9092, 9092 + 12)]
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

            # 1) Phát trực tiếp (nhẹ): emit frame nhanh (tùy có thể resize)
            try:
                quick_frame = cv2.resize(frame, (640, int(frame.shape[0] * 640 / frame.shape[1])))
            except Exception:
                quick_frame = frame
            # Nếu vừa có annotated frame, ưu tiên hiển thị annotated trong khoảng thời gian ngắn
            # để tránh bị ghi đè bởi quick_frame liên tiếp.
            now = time.time()
            show_quick = (now - self._last_annotated_ts) >= self._annotated_display_duration

            if show_quick:
                # emit ngay để hiển thị trực tiếp
                self.frame_ready.emit(quick_frame.copy())

            # gửi luồng nhẹ (chỉ frame nhỏ, nhanh) luôn (không block)
            self._send_streaming(quick_frame.copy(), timestamp)

            # 2) Đẩy task tracking vào executor (chạy riêng, không chặn)
            # copy frame để tránh race
            # self.executor.submit(self._process_tracking, frame.copy(), timestamp)
            
            # Control FPS
            elapsed = time.time() - last_time
            sleep_time = frame_delay - elapsed
            if sleep_time > 0:
                time.sleep(sleep_time)
            last_time = time.time()
        
        self.cap.release()
        
    def _send_streaming(self, frame, timestamp):
        """Gửi dữ liệu nhẹ (frame) tới STREAMING_TOPIC — nhanh, nén nhỏ"""
        try:
            # Giảm resolution xuống 480px - cân bằng giữa chất lượng và kích thước (từ ~10KB xuống ~4-5KB)
            streaming_frame = cv2.resize(frame, (720, int(frame.shape[0] * 720 / frame.shape[1])))
            
            # Giảm JPEG quality xuống 35 để nén vừa phải, vẫn rõ ràng
            _, buffer = cv2.imencode('.jpg', streaming_frame, [cv2.IMWRITE_JPEG_QUALITY, 45])
            frame_base64 = base64.b64encode(buffer).decode('utf-8')
            streaming_data = {
                "camera_id": f"cam{self.camera_id}",
                "frame": frame_base64,
                "timestamp": timestamp,
                "type": "intersection" if self.is_intersection else "street"
            }
            # send async
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
            annotated_frame = results[0].plot()
            cv2.putText(annotated_frame, f"Cam {self.camera_id} - {timestamp}", 
                       (10, 60), cv2.FONT_HERSHEY_SIMPLEX, 0.6, (0, 255, 0), 2)
            # emit annotated frame (cập nhật UI khi tracking xong)
            self.frame_ready.emit(annotated_frame)
            # ghi lại thời điểm annotated được emit để tránh quick_frame ghi đè ngay
            try:
                self._last_annotated_ts = time.time()
            except Exception:
                pass

            # build objects
            objects = []
            if results[0].boxes is not None and getattr(results[0].boxes, "id", None) is not None:
                boxes = results[0].boxes.xyxy.cpu().numpy()
                ids = results[0].boxes.id.cpu().numpy()
                classes = results[0].boxes.cls.cpu().numpy()
                for i in range(len(boxes)):
                    obj = {
                        "id": int(ids[i]),
                        "box": boxes[i].tolist(),
                        "type": results[0].names[int(classes[i])]
                    }
                    objects.append(obj)

            # encode original frame for tracking topic
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
        except Exception as e:
            print("Tracking task error:", e)

    def stop(self):
        """Dừng camera thread"""
        self.running = False
        self.wait()
        # shutdown executor để chờ các task tracking hoàn tất (hoặc dùng wait=False nếu muốn kill)
        try:
            self.executor.shutdown(wait=True)
        except Exception:
            pass
        if self.producer:
            self.producer.close()