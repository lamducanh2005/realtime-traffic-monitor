from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy, Types
from pyflink.datastream.functions import MapFunction
import json
import base64
import cv2
import numpy as np
from ultralytics import YOLO
import time
from pathlib import Path

# Configuration
KAFKA_BOOTSTRAP_SERVER = "localhost:9092"
CAMERA_RAW_TOPIC = "camera_raw"
CAMERA_TRACK_TOPIC = "camera_track"
MODEL_PATH = "../models/yolo11n.pt"

class YOLOProcessor(MapFunction):
    
    def __init__(self, model_path):
        self.model_path = model_path
        self.model = None
    
    def open(self, runtime_context):
        print(f"Loading YOLO model...")
        self.model = YOLO(self.model_path)
        print("YOLO model loaded!")
    
    def map(self, value):
        try:
            # Decode frame
            frame_base64 = value
            frame_bytes = base64.b64decode(frame_base64)
            frame = cv2.imdecode(np.frombuffer(frame_bytes, np.uint8), cv2.IMREAD_COLOR)
            
            if frame is None:
                return json.dumps({"raw_frame": "", "detections": [], "timestamp": time.time()})
            
            # YOLO tracking
            results = self.model.track(frame, persist=True, conf=0.3, classes=[2, 3, 5, 7], verbose=False)
            
            # Lấy detections (chỉ bbox)
            detections = []
            if results[0].boxes is not None:
                boxes = results[0].boxes.xyxy.cpu().numpy()
                detections = [box.tolist() for box in boxes]
            
            # Encode frame
            _, buffer = cv2.imencode('.jpg', frame)
            encoded_frame = base64.b64encode(buffer).decode('utf-8')
            
            # Output
            output = {
                "camera_id": "cam1",
                "raw_frame": encoded_frame,
                "detections": detections,
                "timestamp": time.time()
            }
            
            print(f"Detected: {len(detections)} vehicles")
            return json.dumps(output)
            
        except Exception as e:
            print(f"Error: {e}")
            return json.dumps({"raw_frame": "", "detections": [], "timestamp": time.time()})


def create_kafka_source(env):
    """Tạo Kafka source để đọc từ camera_raw topic"""
    
    source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVER) \
        .set_topics(CAMERA_RAW_TOPIC) \
        .set_group_id("flink-yolo-processor") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()
    
    return source


def create_kafka_sink():
    """Tạo Kafka sink để ghi vào camera_track topic"""
    
    sink = KafkaSink.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVER) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic(CAMERA_TRACK_TOPIC)
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ) \
        .build()
    
    return sink


def main():
    """Main function để chạy Flink streaming job"""
    
    # Tạo StreamExecutionEnvironment
    env = StreamExecutionEnvironment.get_execution_environment()


    fck_path = Path(r"C:\Users\Admin\Documents\Study\_INT3229 Big Data\final project\jars\flink-connector-kafka-4.0.1-2.0.jar")
    kc_path = Path(r"C:\Users\Admin\Documents\Study\_INT3229 Big Data\final project\jars\kafka-clients-3.4.0.jar")

    env.add_jars(fck_path.as_uri(), kc_path.as_uri())

    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)

    env.set_parallelism(1)  # Xử lý tuần tự để tracking hoạt động tốt
    
    # Tạo Kafka source
    kafka_source = create_kafka_source(env)
    
    # Tạo data stream từ Kafka
    stream = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "Kafka Source"
    )
    
    # Xử lý stream với YOLO
    processed_stream = stream.map(
        YOLOProcessor(MODEL_PATH),
        output_type=Types.STRING()
    )
    
    # Tạo Kafka sink
    kafka_sink = create_kafka_sink()
    
    # Ghi kết quả vào Kafka
    processed_stream.sink_to(kafka_sink)
    
    # In ra console để debug (optional)
    # processed_stream.print()
    
    # Execute job
    print("Starting Flink YOLO Processing Job...")
    print(f"Reading from topic: {CAMERA_RAW_TOPIC}")
    print(f"Writing to topic: {CAMERA_TRACK_TOPIC}")
    env.execute("YOLO Traffic Detection & Tracking")


if __name__ == "__main__":
    main()
