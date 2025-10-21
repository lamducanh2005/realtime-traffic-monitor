import tkinter as tk
from tkinter import filedialog, messagebox
import cv2
import PIL.Image, PIL.ImageTk
import threading
import time
from kafka import KafkaProducer
import json
import os

class Camera(tk.Frame):
    def __init__(self, master=None, camera_id=0, **kwargs):
        super().__init__(master, bg='#333333', relief='ridge', bd=1, **kwargs)
        self.camera_id = camera_id
        self.video_source = None
        self.vid = None
        self.is_playing = False
        self.kafka_producer = None
        self.kafka_topic = "cctv_data"

        self.delay = 33
        self.after_id = None

        self.setup_ui()
        
    def setup_ui(self):
        # Tạo canvas để hiển thị video
        self.canvas = tk.Canvas(self, width=300, height=200, bg='black')
        self.canvas.pack(fill='both', expand=True, padx=2, pady=2)
        
        # Panel điều khiển
        control_frame = tk.Frame(self, bg='#444444', height=30)
        control_frame.pack(fill='x', side=tk.BOTTOM)
        
        # Nút chọn file video
        self.select_btn = tk.Button(control_frame, text="Chọn video", command=self.select_video)
        self.select_btn.pack(side=tk.LEFT, padx=5, pady=2)
        
        # Nút play/pause
        self.play_btn = tk.Button(control_frame, text="▶", width=2, command=self.toggle_play)
        self.play_btn.pack(side=tk.LEFT, padx=5, pady=2)
        
        # Hiển thị tên camera
        self.name_label = tk.Label(control_frame, text=f"Camera {self.camera_id}", 
                                  bg='#444444', fg='white')
        self.name_label.pack(side=tk.RIGHT, padx=5, pady=2)
        
        # Label hiển thị trạng thái
        self.status = tk.Label(control_frame, text="Sẵn sàng", 
                              bg='#444444', fg='#00FF00', width=10)
        self.status.pack(side=tk.RIGHT, padx=5, pady=2)
    
    def select_video(self):
        """Chọn file video từ hệ thống"""
        file_path = filedialog.askopenfilename(
            title="Chọn file video",
            filetypes=[("Video Files", "*.mp4;*.avi;*.mov;*.mkv")]
        )
        
        if file_path:
            self.video_source = file_path
            self.name_label.config(text=os.path.basename(file_path))
            self.status.config(text="Đã chọn")
            
            # Kết nối với Kafka (nếu chưa kết nối)
            self.connect_to_kafka()
            
            # Dừng video hiện tại nếu có
            if self.is_playing:
                self.stop_video()
    
    def toggle_play(self):
        """Chuyển đổi giữa phát và tạm dừng"""
        if not self.video_source:
            messagebox.showwarning("Cảnh báo", "Vui lòng chọn file video trước")
            return
            
        if self.is_playing:
            self.stop_video()
            self.play_btn.config(text="▶")
            self.status.config(text="Tạm dừng", fg='yellow')
        else:
            self.play_video()
            self.play_btn.config(text="⏸")
            self.status.config(text="Đang phát", fg='#00FF00')
    
    def play_video(self):
        """Bắt đầu phát video"""
        self.vid = cv2.VideoCapture(self.video_source)
        if not self.vid.isOpened():
            messagebox.showerror("Lỗi", f"Không thể mở video {self.video_source}")
            return
            
        # Lấy FPS từ video để tính delay chính xác
        fps = self.vid.get(cv2.CAP_PROP_FPS)
        self.delay = int(800 / fps)
        
        self.is_playing = True
        self.update_frame()
    
    
    def stop_video(self):
        """Dừng phát video"""
        self.is_playing = False
        if self.vid and self.vid.isOpened():
            self.vid.release()

        
    def update_frame(self):
        """Cập nhật khung hình video và gửi dữ liệu đến Kafka"""
        if self.is_playing and self.vid and self.vid.isOpened():
            ret, frame = self.vid.read()
            
            if ret:
                # Chuyển đổi frame để hiển thị trên Tkinter
                frame_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                img = PIL.Image.fromarray(frame_rgb)
                
                # Điều chỉnh kích thước để vừa với canvas
                canvas_width = self.canvas.winfo_width()
                canvas_height = self.canvas.winfo_height()
                
                if canvas_width > 1 and canvas_height > 1:  # Đảm bảo canvas đã được render
                    # Tính toán tỷ lệ để giữ nguyên aspect ratio
                    img_width, img_height = img.size
                    ratio = min(canvas_width/img_width, canvas_height/img_height)
                    new_width = int(img_width * ratio)
                    new_height = int(img_height * ratio)
                    img = img.resize((new_width, new_height), PIL.Image.Resampling.LANCZOS)
                    
                    # Tạo ảnh mới với kích thước canvas và nền đen
                    background = PIL.Image.new('RGB', (canvas_width, canvas_height), (0, 0, 0))
                    # Tính toán vị trí để đặt ảnh đã resize ở giữa
                    x_offset = (canvas_width - new_width) // 2
                    y_offset = (canvas_height - new_height) // 2
                    # Ghép ảnh đã resize lên nền đen
                    background.paste(img, (x_offset, y_offset))
                    img = background
                    
                # Hiển thị lên canvas
                self.photo = PIL.ImageTk.PhotoImage(image=img)
                self.canvas.create_image(0, 0, image=self.photo, anchor=tk.NW)
                
                # Gửi dữ liệu đến Kafka trong một thread riêng
                if self.kafka_producer:
                    threading.Thread(
                        target=self.send_to_kafka,
                        args=(frame,),
                        daemon=True
                    ).start()
                
                # Lập lịch cập nhật frame tiếp theo bằng after
                self.after_id = self.after(self.delay, self.update_frame)
            else:
                # Video kết thúc, quay lại từ đầu
                self.vid.set(cv2.CAP_PROP_POS_FRAMES, 0)
                self.after_id = self.after(self.delay, self.update_frame)
    
    def connect_to_kafka(self):
        """Thiết lập kết nối đến Kafka broker"""
        try:
            # Khởi tạo Kafka producer (có thể thay đổi thông số kết nối)
            self.kafka_producer = KafkaProducer(
                bootstrap_servers=['localhost:9092'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                # Các cấu hình khác nếu cần
            )
        except Exception as e:
            print(f"Không thể kết nối Kafka: {e}")
            messagebox.showwarning("Cảnh báo", "Không thể kết nối đến Kafka broker")
    
    def send_to_kafka(self, frame):
        """Gửi dữ liệu khung hình đến Kafka"""
        if not self.kafka_producer:
            return
            
        try:
            # Trích xuất metadata hoặc thực hiện phân tích khung hình ở đây
            # Ví dụ: tính toán số lượng xe, người, etc.
            
            # Gửi metadata đến Kafka
            message = {
                "camera_id": self.camera_id,
                "timestamp": time.time(),
                "filename": os.path.basename(self.video_source),
                "frame_height": frame.shape[0],
                "frame_width": frame.shape[1],
                # Thêm các phân tích khác ở đây
            }
            
            self.kafka_producer.send(self.kafka_topic, message)
            
        except Exception as e:
            print(f"Lỗi khi gửi dữ liệu đến Kafka: {e}")