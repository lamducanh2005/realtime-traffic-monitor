import cv2
from ultralytics import YOLO
from pyflink.datastream.functions import FlatMapFunction, MapFunction
import json
import numpy as np
import base64
import torch
from typing import Iterator


class FrameProcessor(MapFunction):
    """Xử lý frame: decode, detect và vẽ bounding box (không tracking)"""
    
    def __init__(self):
        super().__init__()
        self.model = None
        self.device = None
        # Định nghĩa màu sắc cho từng class
        self.class_colors = {
            1: (255, 255, 0),   # bicycle - cyan
            2: (0, 165, 255),   # car - orange
            3: (0, 255, 0),     # motorcycle - green
            5: (0, 165, 255),   # bus - orange
            7: (0, 165, 255),   # truck - orange
        }
        # Tên class
        self.class_names = {
            0: 'person',
            1: 'bicycle',
            2: 'car',
            3: 'motorbike',
            5: 'bus',
            7: 'truck'
        }
    
    def open(self, runtime_context):
        """Khởi tạo model khi bắt đầu"""
        self.device = "cuda:0" if torch.cuda.is_available() else "cpu"
        onnx_path = "resources/models/yolo11n.onnx"
        self.model = YOLO(onnx_path)
    
    def map(self, value: str) -> str:
        """Xử lý frame đơn"""
        try:
            data = json.loads(value)
            
            # Decode frame từ base64
            frame_data = base64.b64decode(data["frame"])
            nparr = np.frombuffer(frame_data, np.uint8)
            frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
            
            if frame is None:
                return None
            
            # Chạy detection (không tracking)
            with torch.no_grad():
                results = self.model.predict(
                    frame,
                    classes=[1, 2, 3, 5, 7],  # bicycle, car, motorbike, bus, truck
                    imgsz=480,
                    conf=0.4,
                    iou=0.5,
                    verbose=False,
                    half=True,
                    device=self.device,
                )
            
            res = results[0]
            
            # Vẽ bounding box
            if len(res.boxes) > 0:
                boxes = res.boxes.xyxy.cpu().numpy()
                classes = res.boxes.cls.cpu().numpy().astype(int)
                
                for box, cls in zip(boxes, classes):
                    x1, y1, x2, y2 = map(int, box)
                    
                    # Lấy màu và tên class
                    color = self.class_colors.get(cls, (255, 255, 255))
                    class_name = self.class_names.get(cls, f'class_{cls}')
                    
                    # Vẽ bounding box
                    cv2.rectangle(frame, (x1, y1), (x2, y2), color, 2)
                    
                    # Vẽ tên class
                    cv2.putText(frame, class_name, (x1, y1 - 5),
                               cv2.FONT_HERSHEY_SIMPLEX, 0.6, color, 2)
            
            # Encode frame về base64
            _, buffer = cv2.imencode('.jpg', frame)
            frame_b64 = base64.b64encode(buffer).decode('utf-8')
            
            # Trả về output
            output = {
                "camera_id": data["camera_id"],
                "timestamp": data["timestamp"],
                "type": data["type"],
                "frame": frame_b64
            }
            
            return json.dumps(output)
        
        except Exception as e:
            print(f"Error processing frame: {e}")
            return None