import cv2
from ultralytics import YOLO
from pyflink.datastream.functions import KeyedProcessFunction
import json
from .utils import Base64
import torch
import time
from pyflink.common import WatermarkStrategy, Types

class CountVehicleSimple(KeyedProcessFunction):
    """Đếm xe xuất hiện >= 10 frames"""
    
    def open(self, runtime_context):
        from pyflink.datastream.state import ValueStateDescriptor
        
        # State: {track_id: frame_count}
        self.frame_counts = runtime_context.get_state(
            ValueStateDescriptor("frame_counts", Types.PICKLED_BYTE_ARRAY())
        )
        
        # State: [(track_id, timestamp)] trong 30s
        self.valid_tracks = runtime_context.get_state(
            ValueStateDescriptor("valid_tracks", Types.PICKLED_BYTE_ARRAY())
        )

        # State: Lưu timestamp của lần emit cuối
        self.last_emit_time = runtime_context.get_state(
            ValueStateDescriptor("last_emit_time", Types.PICKLED_BYTE_ARRAY())
        )
    
    def process_element(self, value, ctx):
        data = json.loads(value)
        cam_id = data["camera_id"]
        track_id = data["obj_id"]
        timestamp_str = data["timestamp"]
        
        # Parse timestamp
        from datetime import datetime
        current_time = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
        current_ts = current_time.timestamp()
        
        # Lấy state
        counts = self.frame_counts.value() or {}
        valid = self.valid_tracks.value() or []
        last_emit = self.last_emit_time.value() or 0
        
        # Tăng frame count
        counts[track_id] = counts.get(track_id, 0) + 1
        
        # Nếu đủ 10 frames → đánh dấu valid
        if counts[track_id] == 10:
            valid.append((track_id, current_ts))
        
        # Cleanup tracks cũ hơn 30s
        cutoff = current_ts - 30
        valid = [(tid, ts) for tid, ts in valid if ts >= cutoff]
        
        # Update state
        self.frame_counts.update(counts)
        self.valid_tracks.update(valid)
        
        # Output stats
        if current_ts - last_emit >= 1.0:
            vehicles_in_30s = len(valid)
            vehicles_per_minute = vehicles_in_30s * 2
            
            yield json.dumps({
                "type": "traffic_stats",
                "camera_id": cam_id,
                "timestamp": timestamp_str,
                "vehicles_in_30s": vehicles_in_30s,
                "vehicles_per_minute": vehicles_per_minute
            })
            
            # Cập nhật thời gian emit
            self.last_emit_time.update(current_ts)