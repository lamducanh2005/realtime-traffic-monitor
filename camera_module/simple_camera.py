import cv2
from kafka import KafkaProducer
import json
import base64
import time


CAMERA_OUT_TOPIC = "camera_raw"
VIDEO_PATH = "../data/street.mp4"
BOOTSTRAP_SERVER = "localhost:9092"
# FPS = 30

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVER,
    value_serializer=lambda x: x.encode('utf-8')
)

cap = cv2.VideoCapture(VIDEO_PATH)

if not cap.isOpened():
    raise SystemExit(f"ERROR: cannot open video: {VIDEO_PATH}")

fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
frame_delay = 1 / fps if fps > 0 else 1 / 30.0

last_time = time.time()
while cap.isOpened():

    # Lấy frame
    ret, frame = cap.read()

    if not ret:
        # end of video
        break

    # Mã hóa frame
    _, buffer = cv2.imencode('.jpg', frame)
    frame_base64 = base64.b64encode(buffer).decode('utf-8')

    data = json.dumps({
        "camera_id": "cam1",
        "raw_frame": frame_base64,
        "timestamp": time.time(),
        "detections": []
    })

    # Gửi đến kafka (send JSON string so consumer can parse)
    producer.send(
        topic=CAMERA_OUT_TOPIC,
        value=data
    )

    elapsed = time.time() - last_time
    sleep_time = frame_delay - elapsed

    if (sleep_time > 0):
        time.sleep(sleep_time)

    last_time = time.time()

cap.release()
producer.close()