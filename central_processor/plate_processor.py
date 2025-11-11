import cv2
from ultralytics import YOLO
from pyflink.datastream.functions import FlatMapFunction, MapFunction, KeyedProcessFunction
import json
import numpy as np
import base64
from .utils import Base64

# plate_model = YOLO('resources/models/license_plate_detector.pt')

# ocr_model = YOLO('resources/models/Charcter-LP.pt')


def detect_and_crop_plate(vehicle_frame, plate_model):
    """
        Thực hiện tìm và crop phần biển số của phương tiện
        Args:
            vehicle_frame: Ma trận ảnh đầu vào đã cắt sẵn phương tiện
        Returns:
            Ma trận biểu diễn ảnh biển số của phương tiện
    """
    
    # Nhận diện vị trí biển số
    plate_detection = plate_model(vehicle_frame)[0].boxes.data.tolist()
    if len(plate_detection) != 1:
        return None
    else:
        plate_detection = plate_detection[0]
    
    # Khoanh vùng và cắt biển số
    x1, y1, x2, y2, score, class_id = plate_detection
    x1, y1, x2, y2 = map(int, [x1, y1, x2, y2])
    cropped_frame = vehicle_frame[y1:y2, x1:x2]

    return cropped_frame


def ocr_plate(plate_frame, ocr_model):
    """
    Thực hiện trích xuất thông tin biển số từ ảnh biển số phương tiện
    Args:
        plate_frame: Ma trận ảnh đầu vào đã cắt sẵn biển số
    Returns:
        str : Chuỗi biển số nhận diện được, nếu không thì trả về None
    """

    # Lấy danh sách nhãn từ model
    names = ocr_model.names
    max_id = max(names.keys())
    labels = [names.get(i, "") for i in range(max_id + 1)]

    # Thực hiện OCR
    results = ocr_model.predict(source=plate_frame, imgsz=480, conf=0.3, verbose=False)[0]

    # Nếu số lượng ký tự không phải từ 7-9 thì thôi
    num_boxes = len(getattr(results.boxes, "xyxy", []))
    if not (7 <= num_boxes <= 9):
        return None
    
    # Lấy thông tin các boxes
    xyxys = results.boxes.xyxy.cpu().numpy()
    clss = results.boxes.cls.cpu().numpy()

    pred_boxes = []
    for xyxy, c in zip(xyxys, clss):
        x1, y1, x2, y2 = map(int, xyxy)
        cls_id = int(c)
        pred_boxes.append({"cls": cls_id, "xyxy": (x1, y1, x2, y2)})
    
    # Sắp xếp theo hàng rồi theo tọa độ x (đọc từ trái sang phải, từ trên xuống)
    pred_boxes.sort(key=lambda b: (b['xyxy'][1] // 10, b['xyxy'][0]))

    # Ghép chuỗi biển số
    num_plate = ""
    for b in pred_boxes:
        cls_id = b['cls']
        label = labels[cls_id] if 0 <= cls_id < len(labels) else str(cls_id)
        num_plate += label

    return num_plate


class ExpandObject(FlatMapFunction):

    def flat_map(self, value):
        frame_data = json.loads(value)

        cam_id = frame_data["camera_id"]
        timestamp = frame_data["timestamp"]
        frame = Base64.decode_frame(frame_data["frame"])
        print("Num objects: ", len(frame_data["objects"]))

        for obj in frame_data["objects"]:
            x, y, w, h = obj["box"]
            obj_frame = Base64.encode_frame(frame[y:y+h, x:x+w])

            yield json.dumps({
                "camera_id": cam_id,
                "timestamp": timestamp,
                "obj_id": str(obj["id"]),
                "obj_frame": obj_frame
            })
    
class DetectPlate(FlatMapFunction):

    def open(self, runtime_context):
        self.plate_model = YOLO('resources/models/license_plate_detector.pt')
        self.ocr_model = YOLO('resources/models/Charcter-LP.pt')

    def flat_map(self, value):
        obj_data = json.loads(value)

        obj_frame = Base64.decode_frame(obj_data["obj_frame"])
        plate_frame = detect_and_crop_plate(obj_frame, self.plate_model)

        if plate_frame is None:
            return

        num_plate = ocr_plate(plate_frame, self.ocr_model)

        if num_plate is None:
            return
        
        obj_data["num_plate"] = num_plate
        obj_data["plate_frame"] = Base64.encode_frame(plate_frame)

        yield json.dumps(obj_data)
    
class VoteBestPlate(KeyedProcessFunction):

    def open(self, config):
        self.plate_history = self.get_runtime_context().get_state("plates")
    