from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types

import cv2
from ultralytics import YOLO
import numpy as np
import base64
import json
import os

CAMERA_RAW_TOPIC = "camera_raw"
CAMERA_OUT_TOPIC = "camera_track"
BOOTSTRAP_SERVER = "localhost:9092"

model = YOLO("../models/yolo11n.pt")

### Khai báo môi trường Flink ###
env = StreamExecutionEnvironment.get_execution_environment()

# Cấu hình Flink
env.set_parallelism(1)  # Tương đương spark.master("local")

### Cấu hình Kafka Consumer ###
kafka_consumer = FlinkKafkaConsumer(
    topics=CAMERA_RAW_TOPIC,
    deserialization_schema=SimpleStringSchema(),
    properties={
        'bootstrap.servers': BOOTSTRAP_SERVER,
        'group.id': 'flink-video-processor',
        'auto.offset.reset': 'latest',
        'session.timeout.ms': '10000',
        'request.timeout.ms': '305000'
    }
)

### Đọc luồng dữ liệu từ Kafka ###
data_stream = env.add_source(kafka_consumer)


### Hàm xử lý detect objects ###
class ObjectTracker:
    def __init__(self):
        self.model = None
    
    def open(self, runtime_context):
        """Khởi tạo model khi task bắt đầu"""
        self.model = YOLO("../models/yolo11n.pt")
    
    def process_frame(self, json_str: str) -> str:
        """
        Nhận JSON string, decode frame, detect objects,
        trả về JSON string kết quả
        """
        try:
            # Parse JSON input
            data = json.loads(json_str)
            camera_id = data.get("camera_id")
            frame_base64 = data.get("frame")
            timestamp = data.get("timestamp")
            
            # Decode frame
            img_bytes = base64.b64decode(frame_base64)
            np_img = np.frombuffer(img_bytes, np.uint8)
            frame = cv2.imdecode(np_img, cv2.IMREAD_COLOR)
            
            # Track objects
            tracks = self.model.track(frame, persist=True)
            detections = tracks[0].boxes.xyxy.tolist() if tracks else []
            
            # Tạo kết quả
            result = {
                "camera_id": camera_id,
                "timestamp": timestamp,
                "raw_frame": frame_base64,
                "detections": detections
            }
            
            return json.dumps(result)
            
        except Exception as e:
            print(f"Error processing frame: {e}")
            return json.dumps({"error": str(e)})


### Map function để xử lý từng message ###
def track_objects(value: str) -> str:
    tracker = ObjectTracker()
    tracker.open(None)
    return tracker.process_frame(value)


### Xử lý dữ liệu ###
processed_stream = data_stream.map(
    track_objects,
    output_type=Types.STRING()
)

### Cấu hình Kafka Producer ###
kafka_producer = FlinkKafkaProducer(
    topic=CAMERA_OUT_TOPIC,
    serialization_schema=SimpleStringSchema(),
    producer_config={
        'bootstrap.servers': BOOTSTRAP_SERVER,
        'transaction.timeout.ms': '900000'
    }
)

### Ghi kết quả vào Kafka ###
processed_stream.add_sink(kafka_producer)

### Chạy job ###
env.execute("flink_video_processing")