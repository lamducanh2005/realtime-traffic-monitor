import cv2
from ultralytics import YOLO
from pyflink.datastream.functions import KeyedProcessFunction
import json
from .utils import Base64
import torch
import time
from pyflink.common import WatermarkStrategy, Types

class CountVehicleSimple(KeyedProcessFunction):
    """Đếm số object đi qua và tính tốc độ trung bình"""
    
    def open(self, runtime_context):
        from pyflink.datastream.state import ValueStateDescriptor
        
        # State: {num_plate: first_seen_timestamp} - theo dõi các xe đã đi qua
        self.seen_plates = runtime_context.get_state(
            ValueStateDescriptor("seen_plates", Types.PICKLED_BYTE_ARRAY())
        )
        
        # State: [(num_plate, timestamp, speed)] - danh sách xe trong khoảng thời gian
        self.vehicles_window = runtime_context.get_state(
            ValueStateDescriptor("vehicles_window", Types.PICKLED_BYTE_ARRAY())
        )

        # State: Lưu timestamp của lần emit cuối
        self.last_emit_time = runtime_context.get_state(
            ValueStateDescriptor("last_emit_time", Types.PICKLED_BYTE_ARRAY())
        )
    
    def process_element(self, value, ctx):
        data = json.loads(value)
        cam_id = data["camera_id"]
        num_plate = data["num_plate"]
        timestamp_str = data["timestamp"]
        
        # Lấy speed, nếu không parse được thì để 0
        try:
            speed = float(data.get("speed", 0))
        except (ValueError, TypeError):
            speed = 0.0
        
        # Parse timestamp
        from datetime import datetime
        current_time = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
        current_ts = current_time.timestamp()
        
        # Lấy state
        seen = self.seen_plates.value() or {}
        vehicles = self.vehicles_window.value() or []
        last_emit = self.last_emit_time.value() or 0
        
        # Chỉ đếm xe lần đầu xuất hiện (hoặc đã qua 60s kể từ lần cuối)
        should_count = False
        if num_plate and num_plate != "None" and num_plate.strip():
            last_seen = seen.get(num_plate, 0)
            if current_ts - last_seen > 60:  # Xe mới hoặc đã qua 60s
                should_count = True
                seen[num_plate] = current_ts
                vehicles.append((num_plate, current_ts, speed))
        
        # Cleanup: Xóa các xe cũ hơn 60s khỏi window
        cutoff = current_ts - 60
        vehicles = [(plate, ts, spd) for plate, ts, spd in vehicles if ts >= cutoff]
        
        # Cleanup seen plates cũ hơn 120s
        seen = {plate: ts for plate, ts in seen.items() if current_ts - ts <= 120}
        
        # Update state
        self.seen_plates.update(seen)
        self.vehicles_window.update(vehicles)
        
        # Output stats mỗi 2 giây
        if current_ts - last_emit >= 2.0:
            vehicle_count = len(vehicles)
            
            # Tính tốc độ trung bình (chỉ tính xe có speed > 0)
            speeds = [spd for _, _, spd in vehicles if spd > 0]
            avg_speed = sum(speeds) / len(speeds) if speeds else 0.0
            
            # Ước tính vehicles per minute (từ dữ liệu 60s)
            vehicles_per_minute = vehicle_count
            
            yield json.dumps({
                "type": "traffic_stats",
                "camera_id": cam_id,
                "timestamp": timestamp_str,
                "vehicle_count_60s": vehicle_count,
                "vehicles_per_minute": vehicles_per_minute,
                "avg_speed": round(avg_speed, 2),
                "vehicles_with_speed": len(speeds)
            })
            
            # Cập nhật thời gian emit
            self.last_emit_time.update(current_ts)