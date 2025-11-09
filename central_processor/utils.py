import base64
import cv2
import numpy as np

class Base64:

    @staticmethod
    def encode_frame(frame):
        _, buf = cv2.imencode(".jpg", frame)
        frame_b64 = base64.b64encode(buf).decode("utf-8")
        return frame_b64
    
    @staticmethod
    def decode_frame(frame_b64):
        img_bytes = base64.b64decode(frame_b64)
        img_array = np.frombuffer(img_bytes, np.uint8)
        frame = cv2.imdecode(img_array, cv2.IMREAD_COLOR)
        return frame