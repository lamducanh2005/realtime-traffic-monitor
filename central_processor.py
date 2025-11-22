from central_processor.main_stream import MainStream
from central_processor.secondary_stream import SecondaryStream
import threading


def run_main_job():
    """
    Job 1: Detect objects đơn giản và gửi sang topic trung gian
    """
    print("Starting Main Job (Job 1) - Object Detection...")
    main_stream = MainStream()
    main_stream.set_up()
    main_stream.process_stream()
    main_stream.execute_job()


def run_secondary_job():
    """
    Job 2: Xử lý toàn diện - Detection + Tracking + Plate + Stats
    """
    print("Starting Secondary Job (Job 2) - Full Processing...")
    secondary_stream = SecondaryStream()
    secondary_stream.set_up()
    secondary_stream.process_stream()
    secondary_stream.execute_job()


if __name__ == "__main__":
    # Tạo 2 threads để chạy 2 jobs song song
    thread1 = threading.Thread(target=run_main_job, name="MainJob")
    thread2 = threading.Thread(target=run_secondary_job, name="SecondaryJob")

    # Khởi động cả 2 threads
    thread1.start()
    thread2.start()

    print("Both jobs are running in parallel...")

    # Đợi cả 2 threads hoàn thành
    thread1.join()
    thread2.join()

    print("Both jobs completed!")


# ========== CODE CŨ (COMMENTED) ==========
# from central_processor.main_stream import MainStream
#
# if __name__ == "__main__":
#     # Khởi tạo MainStream instance
#     main_stream = MainStream()
#
#     # Chuẩn bị và thiết đặt môi trường
#     main_stream.set_up()
#
#     # Xử lý
#     main_stream.process_stream()
#
#     # Thực thi
#     main_stream.execute_job()