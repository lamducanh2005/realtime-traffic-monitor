import cv2
from ultralytics import YOLO
from pyflink.datastream.functions import FlatMapFunction, MapFunction
import json
import numpy as np
import base64
import torch
from typing import Iterator


class FrameProcessor(MapFunction):
    """Xử lý frame: decode, detect và vẽ bounding box"""
    
    def __init__(self):
        super().__init__()
        self.model = None
        self.colors = {}
        self.device = None
    
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
            
            # Chạy tracking
            with torch.no_grad():
                results = self.model.track(
                    frame,
                    persist=True,
                    classes=[2, 3, 5, 7],
                    imgsz=480,
                    conf=0.4,
                    iou=0.5,
                    verbose=False,
                    half=True,
                    device=self.device,
                    tracker='bytetrack.yaml',
                )
            
            res = results[0]
            
            # Vẽ bounding box
            if hasattr(res.boxes, 'id') and res.boxes.id is not None:
                boxes = res.boxes.xyxy.cpu().numpy()
                track_ids = res.boxes.id.cpu().numpy().astype(int)
                
                for box, track_id in zip(boxes, track_ids):
                    x1, y1, x2, y2 = map(int, box)
                    
                    if track_id not in self.colors:
                        self.colors[track_id] = tuple(int(c) for c in np.random.randint(0, 255, 3))
                    color = self.colors[track_id]
                    
                    cv2.rectangle(frame, (x1, y1), (x2, y2), color, 2)
                    cv2.putText(frame, str(track_id), (x1, y1-5),
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