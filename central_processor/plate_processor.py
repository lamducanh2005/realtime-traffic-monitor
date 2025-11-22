import cv2
from ultralytics import YOLO
from pyflink.datastream.functions import FlatMapFunction, MapFunction, KeyedProcessFunction
import json
import numpy as np
import base64
from .utils import Base64
import torch
import time
from pyflink.common import Types

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


class DetectVehicle(FlatMapFunction):
    """
    Thực hiện tracking và chỉ yield ra objects
    Không vẽ bounding box, không yield annotated frame
    """
    
    def __init__(self):
        self.model = None
        self.device = None
    
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
            return

        boxes_xyxy = results[0].boxes.xyxy.cpu().numpy()
        track_ids = results[0].boxes.id.cpu().numpy().astype(int)
        classes = results[0].boxes.cls.cpu().numpy().astype(int)

        frame_h, frame_w = frame.shape[:2]

        # Chỉ yield objects
        for box_xyxy, track_id, obj_type in zip(boxes_xyxy, track_ids, classes):
            x1, y1, x2, y2 = map(int, box_xyxy)
            
            # Đảm bảo tọa độ trong giới hạn frame
            x1 = max(0, min(x1, frame_w))
            y1 = max(0, min(y1, frame_h))
            x2 = max(0, min(x2, frame_w))
            y2 = max(0, min(y2, frame_h))

            # Crop object frame
            start = time.time()
            obj_frame_b64 = Base64.encode_frame(frame[y1:y2, x1:x2])
            end = time.time()
            print(f'Encode object = {(end - start) * 1000:.2f}ms')

            yield json.dumps({
                "camera_id": cam_id,
                "timestamp": timestamp,
                "obj_id": str(track_id),
                "partition": str(track_id % 2),
                "obj_type": str(obj_type),
                "obj_frame": obj_frame_b64
            })

    
class DetectPlate(FlatMapFunction):

    def open(self, runtime_context):
        self.plate_model = YOLO('resources/models/license_plate_detector.pt')
        self.ocr_model = YOLO('resources/models/ocr.pt')

    def flat_map(self, value):
        obj_data = json.loads(value)
        start = time.time()
        obj_frame = Base64.decode_frame(obj_data["obj_frame"])
        end = time.time()
        print(f'Decode object = {(end - start) * 1000:.2f}ms')

        plate_frame = detect_and_crop_plate(obj_frame, self.plate_model)
        

        if plate_frame is None:
            return

        num_plate = ocr_plate(plate_frame, self.ocr_model)

        if num_plate is None:
            return
        

        if not self.check_valid_plate(num_plate):
            return
        
        obj_data["num_plate"] = self.format_plate(num_plate)
        obj_data["plate_frame"] = Base64.encode_frame(plate_frame)

        yield json.dumps(obj_data)
    
    def check_valid_plate(self, plate: str):
        return True
    
    def format_plate(self, plate: str):
        return plate

class CarDetectPlate(DetectPlate):
    def check_valid_plate(self, plate: str):
        """
        Format xe ô tô: 29A1234 hoặc 29A12345 (7-8 ký tự)
        - 2 số đầu: vùng (29, 30, 51...)
        - Ký tự 3: chữ cái (A-Z)
        - Ký tự 4 trở đi: tất cả là số
        """
        if not plate or not (7 <= len(plate) <= 8):
            return False
        
        # Kiểm tra 2 ký tự đầu là số
        if not (plate[0].isdigit() and plate[1].isdigit()):
            return False
        
        # Kiểm tra ký tự thứ 3 là chữ
        if not plate[2].isalpha():
            return False
        
        # Kiểm tra các ký tự từ vị trí 3 trở đi (index 3+) đều là số
        remaining = plate[3:]
        if not all(c.isdigit() for c in remaining):
            return False
        
        return True
    
    def format_plate(self, plate):
        """
        Format: 29A1234 -> 29A-1234
        Chèn dấu gạch ngang sau ký tự thứ 3
        """
        if len(plate) >= 3:
            return plate[:3] + '-' + plate[3:]
        return plate

class MotorDetectPlate(DetectPlate):
    def check_valid_plate(self, plate: str):
        """
        Format xe máy: 29AB1234 hoặc 29A11234 (8-9 ký tự)
        - 2 số đầu: vùng (29, 30, 51...)
        - Ký tự 3: chữ cái (A-Z)
        - Ký tự 4: chữ hoặc số (không kiểm tra)
        - Ký tự 5 trở đi: tất cả là số
        """
        if not plate or not (8 <= len(plate) <= 9):
            return False
        
        # Kiểm tra 2 ký tự đầu là số
        if not (plate[0].isdigit() and plate[1].isdigit()):
            return False
        
        # Kiểm tra ký tự thứ 3 là chữ
        if not plate[2].isalpha():
            return False
        
        
        # Kiểm tra các ký tự từ vị trí 5 trở đi (index 4+) đều là số
        remaining = plate[4:]
        if not all(c.isdigit() for c in remaining):
            return False
        
        
        return True
    
    def format_plate(self, plate):
        """
        Format: 29AB1234 -> 29AB-1234
        Chèn dấu gạch ngang sau ký tự thứ 4
        """
        if len(plate) >= 4:
            return plate[:4] + '-' + plate[4:]
        return plate
     
class Detect(FlatMapFunction):
    def __init__(self):
        self.model = None
        self.device = None
        self.plate_model = None
        self.ocr_model = None
    
    def open(self, runtime_context):
        self.model = YOLO("resources/models/yolo11n.onnx")
        self.plate_model = YOLO('resources/models/license_plate_detector.pt')
        self.ocr_model = YOLO('resources/models/ocr.pt')
        self.device = "cuda:0" if torch.cuda.is_available() else "cpu"

    def flat_map(self, value):
        data = json.loads(value)

        camera_id = data.get("camera_id")
        timestamp = data.get("timestamp")
        frame = Base64.decode_frame(data.get("frame"))

        # Tracking vật thể
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
            return

        boxes_xyxy = results[0].boxes.xyxy.cpu().numpy()
        track_ids = results[0].boxes.id.cpu().numpy().astype(int)
        classes = results[0].boxes.cls.cpu().numpy().astype(int)

        frame_h, frame_w = frame.shape[:2]

        # Duyệt qua từng object phát hiện được
        for box_xyxy, track_id, obj_type in zip(boxes_xyxy, track_ids, classes):
            x1, y1, x2, y2 = map(int, box_xyxy)
            
            # Đảm bảo tọa độ trong giới hạn frame
            x1 = max(0, min(x1, frame_w))
            y1 = max(0, min(y1, frame_h))
            x2 = max(0, min(x2, frame_w))
            y2 = max(0, min(y2, frame_h))

            obj_frame = frame[y1:y2, x1:x2]
            plate_frame = detect_and_crop_plate(obj_frame, self.plate_model)

            if plate_frame is None:
                continue
            
            num_plate = ocr_plate(plate_frame, self.ocr_model)

            if num_plate is None:
                continue
            
            if not self.check_valid_plate(num_plate, obj_type=obj_type):
                continue
            
            yield json.dumps({
                "camera_id": camera_id,
                "timestamp": timestamp,
                "obj_id": str(track_id),
                "num_plate": self.format_plate(num_plate, obj_type=obj_type),
                "obj_frame": Base64.encode_frame(obj_frame),
                "plate_frame": Base64.encode_frame(plate_frame)
            })

    def check_valid_plate(self, plate, obj_type=3):
        """
        Kiểm tra biển số hợp lệ theo loại xe
        obj_type = 3: xe máy (8-9 ký tự)
        obj_type khác: ô tô (7-8 ký tự)
        """
        # Kiểm tra xe máy (class 3)
        if obj_type == 3:
            if not plate or not (8 <= len(plate) <= 9):
                return False
            
            # Kiểm tra 2 ký tự đầu là số
            if not (plate[0].isdigit() and plate[1].isdigit()):
                return False
            
            # Kiểm tra ký tự thứ 3 là chữ
            if not plate[2].isalpha():
                return False
            
            # Kiểm tra các ký tự từ vị trí 5 trở đi (index 4+) đều là số
            remaining = plate[4:]
            if not all(c.isdigit() for c in remaining):
                return False
            
            return True
        
        # Kiểm tra ô tô (class 2, 5, 7)
        else:
            if not plate or not (7 <= len(plate) <= 8):
                return False
            
            # Kiểm tra 2 ký tự đầu là số
            if not (plate[0].isdigit() and plate[1].isdigit()):
                return False
            
            # Kiểm tra ký tự thứ 3 là chữ
            if not plate[2].isalpha():
                return False
            
            # Kiểm tra các ký tự từ vị trí 4 trở đi (index 3+) đều là số
            remaining = plate[3:]
            if not all(c.isdigit() for c in remaining):
                return False
            
            return True


    def format_plate(self, plate, obj_type=3):
        """
        Format biển số theo loại xe
        obj_type = 3: xe máy -> 29AB-1234
        obj_type khác: ô tô -> 29A-1234
        """
        if obj_type == 3:
            return plate[:4] + '-' + plate[4:]
        else:
            return plate[:3] + '-' + plate[3:]


class ReducePlate(KeyedProcessFunction):
    """
    Vote biển số tốt nhất cho mỗi track_id
    - Key: "camera_id_obj_id" (string)
    - Vote: Biển số xuất hiện nhiều nhất
    - Frame: Lấy frame đầu tiên của biển số đó
    """
    
    def open(self, runtime_context):
        from pyflink.datastream.state import ValueStateDescriptor
        
        # State: {num_plate: [(timestamp, obj_frame_b64, plate_frame_b64, obj_type), ...]}
        self.plate_candidates = runtime_context.get_state(
            ValueStateDescriptor("plate_candidates", Types.PICKLED_BYTE_ARRAY())
        )
        
        # State: Đã emit kết quả chưa
        self.emitted = runtime_context.get_state(
            ValueStateDescriptor("emitted", Types.BOOLEAN())
        )
        
        # State: Thời gian nhận message cuối
        self.last_update = runtime_context.get_state(
            ValueStateDescriptor("last_update", Types.PICKLED_BYTE_ARRAY())
        )
        
        # Timeout: Nếu không nhận message mới sau 2 giây → emit kết quả
        self.timeout_seconds = 2
    
    def process_element(self, value, ctx):
        from datetime import datetime
        
        data = json.loads(value)
        
        cam_id = data["camera_id"]
        track_id = data["obj_id"]
        num_plate = data["num_plate"]
        timestamp_str = data["timestamp"]
        obj_frame_b64 = data["obj_frame"]
        plate_frame_b64 = data["plate_frame"]
        # obj_type = data["obj_type"]
        
        # Parse timestamp
        current_time = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
        current_ts = current_time.timestamp()
        
        # Lấy state
        candidates = self.plate_candidates.value()
        if candidates is None:
            candidates = {}
        
        emitted = self.emitted.value() or False
        
        # Nếu đã emit rồi thì bỏ qua
        if emitted:
            return
        
        # Thêm candidate
        if num_plate not in candidates:
            candidates[num_plate] = []
        
        candidates[num_plate].append({
            "timestamp": timestamp_str,
            "timestamp_num": current_ts,
            "obj_frame": obj_frame_b64,
            "plate_frame": plate_frame_b64,
            # "obj_type": obj_type  # Lưu obj_type
        })
        
        # Update state
        self.plate_candidates.update(candidates)
        self.last_update.update(current_ts)
        
        # Đăng ký timer để check timeout
        timer_ts = int((current_ts + self.timeout_seconds) * 1000)  # milliseconds
        ctx.timer_service().register_processing_time_timer(timer_ts)
    
    def on_timer(self, timestamp, ctx):
        """Callback khi timer trigger (sau 2 giây không nhận message mới)"""
        from datetime import datetime
        
        candidates = self.plate_candidates.value()
        emitted = self.emitted.value() or False
        last_update = self.last_update.value()
        
        if emitted or candidates is None or len(candidates) == 0:
            return
        
        current_ts = datetime.now().timestamp()
        
        # Kiểm tra xem đã timeout chưa
        if current_ts - last_update < self.timeout_seconds:
            return  # Chưa timeout, chờ thêm
        
        # Vote: Tìm biển số xuất hiện nhiều nhất
        vote_counts = {plate: len(records) for plate, records in candidates.items()}
        best_plate = max(vote_counts, key=vote_counts.get)
        
        # Lấy record đầu tiên của biển số đó (sắp xếp theo timestamp)
        records = candidates[best_plate]
        records.sort(key=lambda r: r["timestamp_num"])
        first_record = records[0]
        
        # Parse key "camera_id_obj_id"
        key = ctx.get_current_key()
        cam_id, obj_id = key.split("_", 1)
        
        # Emit kết quả
        result = {
            "camera_id": cam_id,
            "obj_id": obj_id,
            "num_plate": best_plate,
            "timestamp": first_record["timestamp"],
            "obj_frame": first_record["obj_frame"],
            "plate_frame": first_record["plate_frame"],
            # "obj_type": first_record["obj_type"],
            "vote_count": vote_counts[best_plate],
            "total_detections": sum(vote_counts.values())
        }
        
        yield json.dumps(result)
        
        # Đánh dấu đã emit
        self.emitted.update(True)