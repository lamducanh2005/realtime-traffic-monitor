from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy, Types
from pyflink.datastream.functions import MapFunction
from pyflink.common import WatermarkStrategy, Duration
from pyflink.common.watermark_strategy import TimestampAssigner
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
MODEL_PATH = "../models/yolov8n.pt"

class FrameTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        # Extract timestamp từ frame data
        try:
            data = json.loads(value)
            return int(data.get('timestamp', 0) * 1000)  # Convert to milliseconds
        except:
            return record_timestamp

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
            results = self.model(frame, conf=0.7, classes=[2, 3, 5, 7])
            
            
            detections = []
            # results[0].boxes: xyxy, cls, conf
            if len(results) > 0 and hasattr(results[0], "boxes") and results[0].boxes is not None:
                boxes = results[0].boxes.xyxy.cpu().numpy() if hasattr(results[0].boxes.xyxy, "cpu") else np.array(results[0].boxes.xyxy)
                classes = results[0].boxes.cls.cpu().numpy() if hasattr(results[0].boxes.cls, "cpu") else np.array(results[0].boxes.cls)
                confs = results[0].boxes.conf.cpu().numpy() if hasattr(results[0].boxes.conf, "cpu") else np.array(results[0].boxes.conf)
                
                # mapping class id -> label (COCO)
                label_map = {2: "car", 3: "motorcycle", 5: "bus", 7: "truck"}
                
                for (box, cls, conf) in zip(boxes, classes, confs):
                    x1, y1, x2, y2 = map(int, box)
                    cls_id = int(cls)
                    det = {
                        "x1": x1,
                        "y1": y1,
                        "x2": x2,
                        "y2": y2,
                        "class_id": cls_id,
                        "label": label_map.get(cls_id, str(cls_id)),
                        "confidence": float(conf)
                    }
                    detections.append(det)
                    
                    # Vẽ bounding box và label lên frame
                    cv2.rectangle(frame, (x1, y1), (x2, y2), (0, 255, 0), 2)
                    label_text = f"{det['label']}:{det['confidence']:.2f}"
                    cv2.putText(frame, label_text, (x1, max(15, y1-5)), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0,255,0), 2)
            
            # Encode frame
            _, buffer = cv2.imencode('.jpg', frame)
            encoded_frame = base64.b64encode(buffer).decode('utf-8')
            
            # Output
            output = {
                "camera_id": "cam1",
                "raw_frame": encoded_frame,
                # "detections": detections,
                "timestamp": time.time()
            }
            
            # print(f"Detected: {len(detections)} vehicles")
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
        .set_property("fetch.min.bytes", "1") \
        .set_property("fetch.max.wait.ms", "100") \
        .set_property("max.poll.records", "1") \
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
        .set_property("linger.ms", "0") \
        .set_property("batch.size", "1") \
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
    env.enable_checkpointing(50)
    env.set_buffer_timeout(1)
    
    # Tạo Kafka source
    kafka_source = create_kafka_source(env)

    watermark_strategy = WatermarkStrategy \
    .for_bounded_out_of_orderness(Duration.of_seconds(2)) \
    .with_timestamp_assigner(FrameTimestampAssigner())
    
    # Tạo data stream từ Kafka
    stream = env.from_source(
        kafka_source,
        watermark_strategy,
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
