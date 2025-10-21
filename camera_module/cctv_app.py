import tkinter as tk

from camera import Camera

class CCTVApp(tk.Tk):
    def __init__(self):
        super().__init__()

        self.title("Mô phỏng CCTV giao thông")
        self.geometry("800x600")

        self.setup_ui()

    def setup_ui(self):
        
        ### Header UI ###
        header_frame = tk.Frame(self, bg='darkblue', height=60)
        header_frame.pack(fill='x')

        title_label = tk.Label(
            header_frame,
            text="HỆ THỐNG CCTV GIAO THÔNG",
            font=('Arial', 18, 'bold'),
            bg='darkblue',
            fg='white'
        )
        title_label.pack(pady=15)

        ### Camera Grid UI ###
        grid_frame = tk.Frame(self, bg='#222222')
        grid_frame.pack(fill='both', expand=True, padx=10, pady=10)

        # Tạo lưới 2x2 camera
        self.cameras = []
        for row in range(2):
            grid_frame.rowconfigure(row, weight=1)
            for col in range(2):
                grid_frame.columnconfigure(col, weight=1)
                camera_id = row * 2 + col + 1
                camera = Camera(grid_frame, camera_id=camera_id)
                camera.grid(row=row, column=col, padx=5, pady=5, sticky="nsew")
                self.cameras.append(camera)
        
        # Footer với thông tin status
        footer_frame = tk.Frame(self, height=30, bg='#333333')
        footer_frame.pack(fill=tk.X, side=tk.BOTTOM)
        
        status_label = tk.Label(footer_frame, text="Trạng thái: Đã kết nối", 
                              bg='#333333', fg='#00FF00')
        status_label.pack(side=tk.LEFT, padx=10)


app = CCTVApp()
app.mainloop()


