from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy, Types
from pyflink.datastream.functions import MapFunction
from pyflink.common import Duration
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
        try:
            data = json.loads(value)
            return int(data.get('timestamp', 0) * 1000)
        except Exception:
            return record_timestamp

class YOLOProcessor(MapFunction):
    """MapFunction that performs inference on incoming JSON messages.
    Input: JSON string with fields: camera_id, frame (base64), timestamp (optional)
    Output: JSON string with fields: camera_id, raw_frame (base64 with bboxes drawn), detections, timestamp
    Note: This function will run once per parallel subtask (model loaded in open()).
    """
    def __init__(self, model_path):
        self.model_path = model_path
        self.model = None

    def open(self, runtime_context):
        print("[YOLOProcessor] Loading YOLO model...")
        self.model = YOLO(self.model_path)
        print("[YOLOProcessor] YOLO model loaded")

    def map(self, value):
        try:
            # value is the JSON string (we keep this function generic so it can be applied after keyBy)
            data = json.loads(value)
            camera_id = data.get('camera_id', 'unknown')
            frame_base64 = data.get('frame') or data.get('raw_frame')
            ts = data.get('timestamp', time.time())

            if not frame_base64:
                return json.dumps({"camera_id": camera_id, "raw_frame": "", "detections": [], "timestamp": ts})

            frame_bytes = base64.b64decode(frame_base64)
            frame = cv2.imdecode(np.frombuffer(frame_bytes, np.uint8), cv2.IMREAD_COLOR)

            if frame is None:
                return json.dumps({"camera_id": camera_id, "raw_frame": "", "detections": [], "timestamp": ts})

            # Run YOLO inference
            results = self.model(frame, conf=0.7, classes=[2, 3, 5, 7])

            detections = []
            if len(results) > 0 and hasattr(results[0], 'boxes') and results[0].boxes is not None:
                boxes = results[0].boxes.xyxy.cpu().numpy() if hasattr(results[0].boxes.xyxy, 'cpu') else np.array(results[0].boxes.xyxy)
                classes = results[0].boxes.cls.cpu().numpy() if hasattr(results[0].boxes.cls, 'cpu') else np.array(results[0].boxes.cls)
                confs = results[0].boxes.conf.cpu().numpy() if hasattr(results[0].boxes.conf, 'cpu') else np.array(results[0].boxes.conf)

                label_map = {2: 'car', 3: 'motorcycle', 5: 'bus', 7: 'truck'}
                for (box, cls, conf) in zip(boxes, classes, confs):
                    x1, y1, x2, y2 = map(int, box)
                    cls_id = int(cls)
                    det = {
                        'x1': x1, 'y1': y1, 'x2': x2, 'y2': y2,
                        'class_id': cls_id,
                        'label': label_map.get(cls_id, str(cls_id)),
                        'confidence': float(conf)
                    }
                    detections.append(det)

                    # Draw bbox and label on frame
                    cv2.rectangle(frame, (x1, y1), (x2, y2), (0, 255, 0), 2)
                    label_text = f"{det['label']}:{det['confidence']:.2f}"
                    cv2.putText(frame, label_text, (x1, max(15, y1-5)), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0,255,0), 2)

            # Encode annotated frame back to base64
            _, buffer = cv2.imencode('.jpg', frame)
            encoded_frame = base64.b64encode(buffer).decode('utf-8')

            output = {
                'camera_id': camera_id,
                'raw_frame': encoded_frame,
                'detections': detections,
                'timestamp': ts
            }
            return json.dumps(output)

        except Exception as e:
            print(f"[YOLOProcessor] Error processing message: {e}")
            return json.dumps({"camera_id": "unknown", "raw_frame": "", "detections": [], "timestamp": time.time()})


def create_kafka_source():
    source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVER) \
        .set_topics(CAMERA_RAW_TOPIC) \
        .set_group_id("flink-track-demo") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()
    return source


def create_kafka_sink():
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

def extract_camera_id(value):
    """Trả về camera_id nếu value là JSON; nếu không, trả 'unknown'."""
    try:
        data = json.loads(value)
        return data.get('camera_id', 'unknown')
    except Exception:
        return 'unknown'

def main():
    env = StreamExecutionEnvironment.get_execution_environment()

    # Add connectors jars if needed (update the paths if different)
    fck_path = Path(r"C:\Users\Admin\Documents\Study\_INT3229 Big Data\final project\jars\flink-connector-kafka-4.0.1-2.0.jar")
    kc_path = Path(r"C:\Users\Admin\Documents\Study\_INT3229 Big Data\final project\jars\kafka-clients-3.4.0.jar")
    env.add_jars(fck_path.as_uri(), kc_path.as_uri())

    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)

    # NOTE: Set the parallelism above 1 to utilize multiple slots — but ensure Kafka topic has enough partitions
    env.set_parallelism(2)
    env.enable_checkpointing(50)
    env.set_buffer_timeout(1)

    kafka_source = create_kafka_source()

    watermark_strategy = WatermarkStrategy \
        .for_bounded_out_of_orderness(Duration.of_seconds(2)) \
        .with_timestamp_assigner(FrameTimestampAssigner())

    stream = env.from_source(kafka_source, watermark_strategy, "Kafka Source")

    # Key by camera_id (extract from JSON). This ensures all messages for the same camera go to the same subtask.
    keyed = stream.key_by(extract_camera_id)

    # Run YOLO processing on each keyed partition
    processed = keyed.map(YOLOProcessor(MODEL_PATH), output_type=Types.STRING())

    kafka_sink = create_kafka_sink()
    processed.sink_to(kafka_sink)

    print("Starting Flink track_processor_demo1 job...")
    print(f"Reading from topic: {CAMERA_RAW_TOPIC}")
    print(f"Writing to topic: {CAMERA_TRACK_TOPIC}")

    env.execute("track_processor_demo1")


if __name__ == '__main__':
    main()
