from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy, Types
from pathlib import Path
from .detect_processor import FrameProcessor
from .plate_processor import Detect, ReducePlate, DetectVehicle, CarDetectPlate, MotorDetectPlate
from .stats_processor import CountVehicleSimple
import os
import json


class MainStream:
    """
    Class quản lý streaming pipeline cho xử lý video từ camera
    """
    
    # Constants
    F2K_JAR = Path(r"C:\Users\Admin\Documents\Study\_INT3229 Big Data\final project\resources\jars\flink-connector-kafka-4.0.1-2.0.jar")
    KC_JAR = Path(r"C:\Users\Admin\Documents\Study\_INT3229 Big Data\final project\resources\jars\kafka-clients-3.4.0.jar")

    # Generate 6 bootstrap broker addresses compactly
    KAFKA_BOOTSTRAP_SERVER = ",".join(f"192.168.0.106:{p}" for p in range(9092, 9092 + 6))
    SOURCE_TOPIC = "cam_raw"
    EVENTS_TOPIC = "cam_event"
    STATS_TOPIC = "cam_statistic"
    TEST_TOPIC = "cam_test_topic"

    def __init__(self):
        """
        Khởi tạo các thành phần của streaming pipeline
        """
        self.env: StreamExecutionEnvironment = None
        self.source: KafkaSource = None
        self.events_sink: KafkaSink = None
        self.stats_sink: KafkaSink = None

    def set_up(self):
        """
        Thiết đặt môi trường:
            - Khởi động môi trường
            - Thêm file jar
            - Cài các tham số
            - Tạo kafka source và kafka sink
        """
        self.env = StreamExecutionEnvironment.get_execution_environment()
        
        # Thêm file jar
        self.env.add_jars(self.F2K_JAR.as_uri(), self.KC_JAR.as_uri())

        # Cài các tham số
        self.env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
        self.env.set_parallelism(1)

        # Tạo kafka source
        self.source = (
            KafkaSource.builder()
            .set_bootstrap_servers(self.KAFKA_BOOTSTRAP_SERVER)
            .set_topics(self.SOURCE_TOPIC)
            .set_group_id("plate")
            .set_starting_offsets(KafkaOffsetsInitializer.latest())
            .set_value_only_deserializer(SimpleStringSchema())
            .build()
        )

        # Tạo kafka sink
        self.events_sink = (
            KafkaSink.builder()
            .set_bootstrap_servers(self.KAFKA_BOOTSTRAP_SERVER)
            .set_record_serializer(
                KafkaRecordSerializationSchema.builder()
                .set_topic(self.EVENTS_TOPIC)
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
            )
            .build()
        )

        self.stats_sink = (
            KafkaSink.builder()
            .set_bootstrap_servers(self.KAFKA_BOOTSTRAP_SERVER)
            .set_record_serializer(
                KafkaRecordSerializationSchema.builder()
                .set_topic("cam_test_topic_2")
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
            )
            .build()
        )

        self.tracking_sink = (
            KafkaSink.builder()
            .set_bootstrap_servers(self.KAFKA_BOOTSTRAP_SERVER)
            .set_record_serializer(
                KafkaRecordSerializationSchema.builder()
                .set_topic("cam_tracking")
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
            )
            .build()
        )

    def process_stream(self):
        """
        Xử lý luồng dữ liệu từ Kafka source
        """
        stream = self.env.from_source(
            self.source,
            WatermarkStrategy.no_watermarks(),
            "frame_processor",
            Types.STRING()
        )

        # Sử dụng DetectVehicle để có objects (bỏ annotated frame)
        # main_stream = (
        #     stream
        #     .key_by(lambda x: json.loads(x).get("camera_id"), key_type=Types.STRING())
        #     .flat_map(Detect(), output_type=Types.STRING())
        #     .key_by(
        #         lambda x: f"{json.loads(x)['camera_id']}_{json.loads(x)['obj_id']}", 
        #         key_type=Types.STRING()
        #     )
        #     .process(ReducePlate(), output_type=Types.STRING())
        # )

        # counting_stream = (
        #     main_stream
        #     .key_by(lambda x: json.loads(x).get("camera_id"), key_type=Types.STRING())
        #     .process(CountVehicleSimple(), output_type=Types.STRING())
        # )

        # Gửi kết quả (bỏ annotated frames)
        # main_stream.sink_to(self.events_sink)
        # counting_stream.sink_to(self.stats_sink)

        combined_stream = (
            stream
            .key_by(lambda x: json.loads(x).get("camera_id"), key_type=Types.STRING())
            .flat_map(DetectVehicle(), output_type=Types.STRING())
        )

        annotated_frames_stream = combined_stream.filter(
            lambda x: json.loads(x).get("type") == "annotated_frame"
        )

        # Tách stream theo loại xe
        objects_stream = combined_stream.filter(lambda x: json.loads(x).get("type") == "object")
        
        # Stream cho xe ô tô (car, bus, truck)
        car_plate_stream = (
            objects_stream
            .filter(lambda x: json.loads(x).get("obj_type") in ["2", "5", "7"])  # car, bus, truck
            .flat_map(CarDetectPlate(), output_type=Types.STRING())
        )
        
        # Stream cho xe máy
        motor_plate_stream = (
            objects_stream
            .filter(lambda x: json.loads(x).get("obj_type") == "3")  # motorcycle
            .flat_map(MotorDetectPlate(), output_type=Types.STRING()) 
        )

        vehicle_plate_stream = (
            car_plate_stream.union(motor_plate_stream)
            .key_by(
                lambda x: f"{json.loads(x)['camera_id']}_{json.loads(x)['obj_id']}", 
                key_type=Types.STRING()
            )
            .process(ReducePlate(), output_type=Types.STRING())
        )
        


        counting_stream = (
            objects_stream
            .key_by(lambda x: json.loads(x).get("camera_id"), key_type=Types.STRING())
            .process(CountVehicleSimple(), output_type=Types.STRING())
        )

        vehicle_plate_stream.sink_to(self.events_sink)
        annotated_frames_stream.sink_to(self.tracking_sink)
        counting_stream.sink_to(self.stats_sink)



    def execute_job(self):
        """
        Thực thi Flink job
        """
        print("Executing job...")
        self.env.execute("main_stream")