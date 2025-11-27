import base64
import cv2
import numpy as np
from pymongo import MongoClient

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

MONGO_URI = "mongodb://root:123456@localhost:27017/"
client = MongoClient(MONGO_URI)

DB_NAME = "traffic-monitor"
app_db = client[DB_NAME]

class MongoVehicleService:
    def __init__(self):
        self.users = app_db["vehicles"]

    def check_vehicle(self, num_plate):
        "Truy tìm xe trong cơ sở dữ liệu"
        try:
            if not num_plate:
                return ""
            doc = self.users.find_one({"num_plate": num_plate}, {"warning": 1, "_id": 0})
            if not doc:
                return ""
            warning = doc.get("warning", "")
            if isinstance(warning, str) and warning.strip():
                return warning
            return ""
        except Exception:
            # Trong mọi lỗi, trả về chuỗi rỗng theo yêu cầu
            return ""

    def log_vehicle(self, num_plate, **kwargs):
        """
        Tìm xe có biển số là num_plate và ghi lại nhật ký chuyển vào ngày hôm đó
        """
        if not num_plate:
            return False
        event = {
            "time": kwargs["timestamp"],
            "speed": kwargs["speed"],
            "plate_frame": kwargs["plate_frame"],
            "obj_frame": kwargs["obj_frame"]
        }
        try:
            result = self.users.find_one({"num_plate": num_plate})
            if result:
                # Append event to events array
                self.users.update_one(
                    {"num_plate": num_plate},
                    {"$push": {"events": event}}
                )
            else:
                # Create new document
                doc = {
                    "num_plate": num_plate,
                    "events": [event]
                }
                self.users.insert_one(doc)
            return True
        except Exception as e:
            # Optionally log error
            return False

class MongoCamService:
    def __init__(self):
        self.cam = client["admin"]["data"]
        self.frame = client["admin"]["frame"]

    def get_event(self, name, time):
        try:
            camera = self.cam.find_one({"name": name}, {"timeline": 1, "_id": 0})
            if not camera or "timeline" not in camera:
                return []
            return [item for item in camera["timeline"] if item.get("time") == time]
        except Exception:
            return []
    
    def get_frame(self, name, no):
        camera = client["admin"][name]
        try:
            doc = camera.find_one({"no": no}, {"frame": 1, "_id": 0})
            if not doc or "frame" not in doc:
                return None
            return doc["frame"]
        except Exception:
            return None
            

    





