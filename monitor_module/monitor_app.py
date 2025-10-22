import tkinter as tk
from tkinter import ttk
from PIL import Image, ImageTk
import cv2
import numpy as np
import json
import base64
import threading
import queue
from kafka import KafkaConsumer

class HeaderBar(tk.Frame):
    def __init__(self, master, title: str = "Traffic Monitoring Dashboard", **kwargs):
        super().__init__(master, bg="#2d2d2d", height=60, **kwargs)
        self.grid_propagate(False)

        tk.Label(
            self,
            text=title,
            font=("Segoe UI", 18, "bold"),
            fg="#ffffff",
            bg="#2d2d2d"
        ).pack(side=tk.LEFT, padx=20)

        self.status_label = tk.Label(
            self,
            text="● Disconnected",
            font=("Segoe UI", 12),
            fg="#f44336",
            bg="#2d2d2d"
        )
        self.status_label.pack(side=tk.RIGHT, padx=20)

    def set_status(self, text: str, color: str):
        self.status_label.config(text=text, fg=color)

class CameraSidebar(tk.Frame):
    def __init__(self, master, cameras=None, on_select=None, **kwargs):
        super().__init__(master, bg="#1f1f1f", width=220, **kwargs)
        self.grid_propagate(False)
        self.on_select = on_select

        tk.Label(
            self,
            text="Cameras",
            font=("Segoe UI", 14, "bold"),
            fg="#ffffff",
            bg="#1f1f1f"
        ).pack(anchor="w", padx=16, pady=(16, 8))

        self.listbox = tk.Listbox(
            self,
            font=("Segoe UI", 11),
            bg="#2c2c2c",
            fg="#ffffff",
            selectbackground="#3f51b5",
            activestyle="none",
            highlightthickness=0
        )
        self.listbox.pack(fill="both", expand=True, padx=16, pady=(0, 16))
        self.listbox.bind("<<ListboxSelect>>", self._handle_select)

        for cam in cameras or []:
            self.listbox.insert(tk.END, cam)

    def _handle_select(self, event):
        if not self.on_select:
            return
        selection = event.widget.curselection()
        if selection:
            index = selection[0]
            self.on_select(event.widget.get(index))

class VideoPanel(tk.Frame):
    def __init__(self, master, **kwargs):
        super().__init__(master, bg="#202020", **kwargs)
        self.grid_rowconfigure(0, weight=1)
        self.grid_columnconfigure(0, weight=1)

        self.video_frame = tk.Frame(self, bg="#101010", bd=1, relief="solid")
        self.video_frame.grid(row=0, column=0, sticky="nsew")
        self.video_canvas = tk.Label(self.video_frame, bg="#000000")
        self.video_canvas.pack(fill="both", expand=True)

        self.route_box = tk.Frame(self, bg="#1f1f1f", height=140, bd=1, relief="solid")
        self.route_box.grid(row=1, column=0, sticky="ew", pady=(12, 0))
        self.route_box.grid_propagate(False)

        tk.Label(
            self.route_box,
            text="Route Information",
            font=("Segoe UI", 13, "bold"),
            fg="#ffffff",
            bg="#1f1f1f"
        ).pack(anchor="w", padx=16, pady=(12, 6))

        self.route_label = tk.Label(
            self.route_box,
            text="Select a camera to view route details.",
            font=("Segoe UI", 11),
            fg="#cccccc",
            bg="#1f1f1f",
            justify="left"
        )
        self.route_label.pack(anchor="w", padx=16)

        self.photo_image = None
        self.show_placeholder()

    def show_placeholder(self):
        placeholder = np.zeros((480, 720, 3), dtype=np.uint8)
        cv2.putText(placeholder, "Select a camera", (180, 240),
                    cv2.FONT_HERSHEY_SIMPLEX, 1.2, (255, 255, 255), 3)
        self.update_frame(placeholder)

    def update_frame(self, frame):
        rgb_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
        img = Image.fromarray(rgb_frame).resize((960, 540), Image.LANCZOS)
        self.photo_image = ImageTk.PhotoImage(image=img)
        self.video_canvas.configure(image=self.photo_image)

    def update_route_info(self, text: str):
        self.route_label.config(text=text)

class VehicleLog(tk.Frame):
    def __init__(self, master, **kwargs):
        super().__init__(master, bg="#1f1f1f", width=420, **kwargs)
        self.grid_propagate(False)
        self.rowconfigure(1, weight=1)

        tk.Label(
            self,
            text="Vehicle Log",
            font=("Segoe UI", 14, "bold"),
            fg="#ffffff",
            bg="#1f1f1f"
        ).grid(row=0, column=0, padx=16, pady=(16, 8), sticky="w")

        self.tree = ttk.Treeview(
            self,
            columns=("camera", "class", "id", "time"),
            show="headings",
            height=20
        )

        self.tree.heading("camera", text="Camera")
        self.tree.heading("class", text="Class")
        self.tree.heading("id", text="Track ID")
        self.tree.heading("time", text="Time")
        self.tree.column("camera", width=90, anchor="center")
        self.tree.column("class", width=90, anchor="center")
        self.tree.column("id", width=70, anchor="center")
        self.tree.column("time", width=110, anchor="center")
        self.tree.grid(row=1, column=0, padx=16, pady=(0, 16), sticky="nsew")

        style = ttk.Style(self)
        style.theme_use('clam')
        style.configure("Treeview", background="#2c2c2c", foreground="#ffffff", fieldbackground="#2c2c2c")
        style.configure("Treeview.Heading", background="#3a3a3a", foreground="#ffffff")
        style.map("Treeview", background=[("selected", "#3f51b5")])

    def append_entries(self, camera_id, vehicles, timestamp, error=None):
        if error:
            self.tree.insert("", tk.END, values=(camera_id, "Error", "-", error))
            return

        for vehicle in vehicles:
            self.tree.insert(
                "",
                tk.END,
                values=(
                    camera_id,
                    vehicle.get("class_name", "Vehicle"),
                    vehicle.get("track_id", "-"),
                    timestamp
                )
            )

class MonitorApp(tk.Tk):
    def __init__(self, brokers=None, topic="vehicle_tracking_data"):
        super().__init__()
        
        self.title("Traffic Monitor")
        self.geometry("1280x720")
        self.configure(bg="#D2D2D2")

        self.brokers = brokers or ["localhost:9092"]
        self.topic = topic

        self.frame_queue = queue.Queue()
        self.log_queue = queue.Queue()
        self.stop_event = threading.Event()

        self._build_layout()
        self._start_consumer_thread()
        self.after(200, self._poll_queues)

    def _build_layout(self):
        """ Định khung bố cục"""

        # Đây không phải định kiểu 1x1 #
        self.columnconfigure(1, weight=1)
        self.rowconfigure(1, weight=1)

        ### Tiêu đề ###
        self.header = HeaderBar(self)
        self.header.grid(row=0, column=0, columnspan=3, sticky="nsew")

        ### Sidebar ###
        cameras = ["Camera 1 - Highway", "Camera 2 - Downtown", "Camera 3 - Airport", "Camera 4 - Industrial"]
        self.sidebar = CameraSidebar(self, cameras=cameras, on_select=self._handle_camera_select)
        self.sidebar.grid(row=1, column=0, sticky="nsew")

        ### Panel ở giữa ###
        self.video_panel = VideoPanel(self)
        self.video_panel.grid(row=1, column=1, sticky="nsew", padx=(0, 12), pady=12)

        ### Nhật ký realtime ###
        self.log_panel = VehicleLog(self)
        self.log_panel.grid(row=1, column=2, sticky="nsew", padx=(12, 0), pady=12)

    def _handle_camera_select(self, camera_name):
        self.video_panel.update_route_info(
            f"Displaying live feed for {camera_name}.\nRoute details will appear here."
        )

    def _start_consumer_thread(self):
        """
            Gọi một luồng thực hiện consume dữ liệu đã tracking, từ kafka.
        """
        def consume():
            try:
                consumer = KafkaConsumer(
                    self.topic,
                    bootstrap_servers=self.brokers,
                    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                    auto_offset_reset="latest"
                )

                self.header.set_status("● Connected", "#4caf50")

                for message in consumer:
                    if self.stop_event.is_set():
                        break
                    payload = message.value
                    self.log_queue.put(payload)
                    image_b64 = payload.get("processed_image") or payload.get("base64_image")
                    if image_b64:
                        frame = self._decode_frame(image_b64)
                        if frame is not None:
                            self.frame_queue.put(frame)
            except Exception as exc:
                self.header.set_status("● Disconnected", "#f44336")
                self.log_queue.put({"camera_id": "N/A", "vehicles": [], "timestamp": "", "error": str(exc)})

        threading.Thread(target=consume, daemon=True).start()

    def _poll_queues(self):
        while not self.frame_queue.empty():
            frame = self.frame_queue.get_nowait()
            self.video_panel.update_frame(frame)

        while not self.log_queue.empty():
            payload = self.log_queue.get_nowait()
            self._append_log(payload)

        self.after(200, self._poll_queues)

    @staticmethod
    def _decode_frame(b64_string):
        try:
            data = base64.b64decode(b64_string)
            array = np.frombuffer(data, dtype=np.uint8)
            return cv2.imdecode(array, cv2.IMREAD_COLOR)
        except Exception:
            return None

    def _append_log(self, payload):
        camera_id = payload.get("camera_id", "Unknown")
        vehicles = payload.get("vehicles", [])
        timestamp = payload.get("timestamp", "")
        error = payload.get("error")
        self.log_panel.append_entries(camera_id, vehicles, timestamp, error)

    def on_close(self):
        self.stop_event.set()
        self.destroy()


app = MonitorApp()
app.mainloop()