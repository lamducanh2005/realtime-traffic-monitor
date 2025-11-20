import cv2
from ultralytics import YOLO
from pyflink.datastream.functions import FlatMapFunction, MapFunction, KeyedProcessFunction
import json
import numpy as np
import base64
from .utils import Base64
import torch
import time

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
    plate_detection = plate_model.predict(source=vehicle_frame, imgsz=480, conf=0.5, verbose=False, device='cuda:0')
    
    # Check if any plates detected
    if len(plate_detection) < 1 or len(plate_detection[0].boxes.data.tolist()) < 1:
        return None
    
    # Get first detection box
    plate_box = plate_detection[0].boxes.data.tolist()[0]
    
    # Khoanh vùng và cắt biển số
    x1, y1, x2, y2, score, class_id = plate_box
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

    # Danh sách classes từ Lab01
    classes = [
        '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A',
        'B', 'C', 'D', 'E', 'F', 'G', 'H', 'K', 'L', 'M',
        'N', 'P', 'S', 'T', 'U', 'V', 'X', 'Y', 'Z', '0'
    ]

    # Thực hiện OCR
    results = ocr_model.predict(source=plate_frame, imgsz=480, conf=0.3, verbose=False)
    r = results[0]

    # Nếu không có boxes
    if not (7 <= len(getattr(r.boxes, "xyxy", [])) <= 9):
        return None
    
    # Lấy thông tin các boxes
    xyxys = r.boxes.xyxy.cpu().numpy()
    clss = r.boxes.cls.cpu().numpy()
    confs = r.boxes.conf.cpu().numpy()

    pred_boxes = []
    for xyxy, c, cf in zip(xyxys, clss, confs):
        x1, y1, x2, y2 = map(int, xyxy)
        cls_id = int(c)
        conf = float(cf)
        pred_boxes.append({"cls": cls_id, "conf": conf, "xyxy": (x1, y1, x2, y2)})
    
    # Sắp xếp thông minh cho biển số nghiêng
    # Tính trung bình y để phát hiện số dòng
    if len(pred_boxes) > 0:
        y_centers = [(b['xyxy'][1] + b['xyxy'][3]) / 2 for b in pred_boxes]
        y_min, y_max = min(y_centers), max(y_centers)
        y_range = y_max - y_min
        
        # Nếu range nhỏ (< 30% chiều cao trung bình) → 1 dòng
        avg_height = sum([b['xyxy'][3] - b['xyxy'][1] for b in pred_boxes]) / len(pred_boxes)
        
        if y_range < avg_height * 0.5:  # Biển số 1 dòng
            # Chỉ sắp xếp theo x (trái → phải)
            pred_boxes.sort(key=lambda b: b['xyxy'][0])
        else:  # Biển số 2 dòng
            # Sắp xếp theo hàng (dựa vào y center) rồi theo x
            pred_boxes.sort(key=lambda b: ((b['xyxy'][1] + b['xyxy'][3]) / 2 // (avg_height * 0.7), b['xyxy'][0]))
    


    # Ghép chuỗi biển số
    num_plate = ""
    for b in pred_boxes:
        cls_id = b['cls']
        # Map qua bảng classes từ Lab01
        if 0 <= cls_id < len(classes):
            num_plate += classes[cls_id]
        else:
            num_plate += str(cls_id)

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


class DetectVehicle(FlatMapFunction):
    """
    Thực hiện tracking và tạo 2 loại output:
    1. Objects: Từng object riêng lẻ (type="object")
    2. Annotated frame: Frame đã vẽ bounding box (type="annotated_frame")
    """
    
    def __init__(self):
        self.model = None
        self.device = None
        self.colors = {}
        self.class_names = {
            2: 'car',
            3: 'motorcycle',
            5: 'bus',
            7: 'truck'
        }
    
    def open(self, runtime_context):
        self.model = YOLO("resources/models/yolo11n.onnx")
        self.device = "cuda:0" if torch.cuda.is_available() else "cpu"

    def flat_map(self, value):
        data = json.loads(value)
        cam_id = data.get("camera_id")
        timestamp = data.get("timestamp")
        frame = Base64.decode_frame(data['frame'])

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
        
        if results[0].boxes.id is None:
            yield json.dumps({
                "type": "annotated_frame",
                "camera_id": cam_id,
                "timestamp": timestamp,
                "frame": data['frame']
            })
            return

        boxes = results[0].boxes.xywh.cpu().numpy()
        boxes_xyxy = results[0].boxes.xyxy.cpu().numpy()
        track_ids = results[0].boxes.id.cpu().numpy().astype(int)
        classes = results[0].boxes.cls.cpu().numpy().astype(int)

        frame_h, frame_w = frame.shape[:2]
        annotated_frame = frame.copy()

        # Yield từng object
        for box, box_xyxy, track_id, obj_type in zip(boxes, boxes_xyxy, track_ids, classes):
            # Dùng box_xyxy để crop object (chính xác hơn)
            x1, y1, x2, y2 = map(int, box_xyxy)
            
            # Đảm bảo tọa độ trong giới hạn frame
            x1 = max(0, min(x1, frame_w))
            y1 = max(0, min(y1, frame_h))
            x2 = max(0, min(x2, frame_w))
            y2 = max(0, min(y2, frame_h))

            # Crop object frame
            obj_frame_b64 = Base64.encode_frame(frame[y1:y2, x1:x2])

            # Output 1: Object
            yield json.dumps({
                "type": "object",
                "camera_id": cam_id,
                "timestamp": timestamp,
                "obj_id": str(track_id),
                "partition": str(track_id % 2),
                "obj_type": str(obj_type),
                "obj_frame": obj_frame_b64
            })

            # Vẽ bounding box lên annotated frame (dùng lại x1, y1, x2, y2)
            
            # Lấy màu theo class thay vì track_id
            if obj_type not in self.colors:
                self.colors[obj_type] = tuple(int(c) for c in np.random.randint(0, 255, 3))
            color = self.colors[obj_type]
            
            # Lấy tên class
            class_name = self.class_names.get(obj_type, f"class_{obj_type}")
            
            cv2.rectangle(annotated_frame, (x1, y1), (x2, y2), color, 2)
            cv2.putText(annotated_frame, class_name, (x1, y1-5),
                       cv2.FONT_HERSHEY_SIMPLEX, 0.6, color, 2)

        # Output 2: Annotated frame
        annotated_frame_b64 = Base64.encode_frame(annotated_frame)
        yield json.dumps({
            "type": "annotated_frame",
            "camera_id": cam_id,
            "timestamp": timestamp,
            "frame": annotated_frame_b64
        })

    
class DetectPlate(FlatMapFunction):

    def open(self, runtime_context):
        self.plate_model = YOLO('resources/models/license_plate_detector.pt')
        self.ocr_model = YOLO('resources/models/ocr.pt')

    def flat_map(self, value):
        obj_data = json.loads(value)

        obj_frame = Base64.decode_frame(obj_data["obj_frame"])
        plate_frame = detect_and_crop_plate(obj_frame, self.plate_model)

        if plate_frame is None:
            return

        num_plate = ocr_plate(plate_frame, self.ocr_model)

        if num_plate is None:
            return
        
        # Kiểm tra biển số hợp lệ
        if not self.check_valid_plate(num_plate):
            return
        
        obj_data["num_plate"] = num_plate
        obj_data["plate_frame"] = Base64.encode_frame(plate_frame)

        yield json.dumps(obj_data)
    
    def check_valid_plate(self, plate: str):
        """
        Kiểm tra biển số hợp lệ theo format Việt Nam:
        - Độ dài: 7-9 ký tự
        - 2 ký tự đầu: SỐ
        - Ký tự thứ 3: CHỮ
        - 4 ký tự cuối: SỐ
        
        Format:
        - 7 ký tự: 29A1234
        - 8 ký tự: 29A12345
        - 9 ký tự: 29AB1234 hoặc 29A123456 (tùy vùng)
        """
        if not plate or not (7 <= len(plate) <= 9):
            return False
        
        # Kiểm tra 2 ký tự đầu là số
        if not (plate[0].isdigit() and plate[1].isdigit()):
            return False
        
        # Kiểm tra ký tự thứ 3 là chữ
        if not plate[2].isalpha():
            return False
        
        # Kiểm tra 4 ký tự cuối là số
        last_4 = plate[-4:]
        if not all(c.isdigit() for c in last_4):
            return False
        
        return True

class VoteBestPlate(KeyedProcessFunction):

    def open(self, config):
        self.plate_history = self.get_runtime_context().get_state("plates")
    