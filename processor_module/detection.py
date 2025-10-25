from kafka import KafkaConsumer, KafkaProducer
import cv2
from ultralytics import YOLO
import numpy as np
import base64

CAMERA_RAW_TOPIC = "camera_raw"
CAMERA_OUT_TOPIC = "camera_track"

BOOTSTRAP_SERVER = "localhost:9092"

model = YOLO("../models/yolo11n.pt")

consumer = KafkaConsumer(
    CAMERA_RAW_TOPIC, 
    bootstrap_servers=BOOTSTRAP_SERVER
)

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVER,
    value_serializer=lambda x: x.encode('utf-8')
)


for message in consumer:

    # Nhận và giải mã ảnh
    # frame = np.frombuffer(message.value, dtype=np.uint8)
    frame_base64 = message.value.decode('utf-8')
    frame = np.frombuffer(base64.b64decode(frame_base64), dtype=np.uint8)
    frame = cv2.imdecode(frame, cv2.IMREAD_COLOR)

    # Thực hiện track vật thể
    results = model.track(frame, persist=True, classes=[2, 3, 5, 7], conf=0.3, iou=0.5)

    # Vẽ khung hình vào frame
    annotated_frame = results[0].plot()

    # Mã hóa hình ảnh trước khi gửi tới Kafka
    _, buffer = cv2.imencode('.jpg', frame)
    # frame_bytes = buffer.tobytes()
    frame_base64 = base64.b64encode(buffer).decode('utf-8')

    # Gửi tới kafka
    producer.send(CAMERA_OUT_TOPIC, value=frame_base64)








