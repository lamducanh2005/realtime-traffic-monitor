from central_processor import MainStream

if __name__ == "__main__":

    # Chuẩn bị và thiết đặt môi trường
    MainStream.set_up()

    # Xử lý
    MainStream.process_stream()

    # Thực thi
    MainStream.execute_job()