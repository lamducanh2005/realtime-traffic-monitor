from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy, Types
from pathlib import Path
from .detect_processor import FrameProcessor
from .plate_processor import DetectVehicle, CarDetectPlate, MotorDetectPlate, ReducePlate
from .stats_processor import CountVehicleSimple
import os
import json


F2K_JAR = Path(r"C:\Users\Admin\Documents\Study\_INT3229 Big Data\final project\resources\jars\flink-connector-kafka-4.0.1-2.0.jar")
KC_JAR = Path(r"C:\Users\Admin\Documents\Study\_INT3229 Big Data\final project\resources\jars\kafka-clients-3.4.0.jar")

KAFKA_BOOTSTRAP_SERVER = "localhost:9092,localhost:9093,localhost:9094"
SOURCE_TOPIC = "cam_raw"
EVENTS_TOPIC = "cam_event"
STATS_TOPIC = "cam_statistic"
TEST_TOPIC = "cam_test_topic"


env: StreamExecutionEnvironment = None
source: KafkaSource = None
events_sink: KafkaSink = None
stats_sink: KafkaSink = None
tracking_sink: KafkaSink = None


def set_up():
    """
    Thiết đặt môi trường:
        - Khởi động môi trường
        - Thêm file jar
        - Cài các tham số
        - Tạo kafka source và kafka sink
    """
    global env, source, events_sink, stats_sink, tracking_sink

    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Thêm file jar
    env.add_jars(F2K_JAR.as_uri(), KC_JAR.as_uri())

    # Cài các tham số
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1)

    # config = env.get_config()
    # print(f"Parallelism: {env.get_parallelism()}")
    # print(f"Max Parallelism: {env.get_max_parallelism()}")
    # print(f"Runtime Mode: {env.get_runtime_mode()}")

    # Tạo kafka source
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVER)
        .set_topics(SOURCE_TOPIC)
        .set_group_id("plate")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # Tạo kafka sink
    events_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVER)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(EVENTS_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    stats_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVER)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("cam_test_topic_2")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    tracking_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVER)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("cam_tracking")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )


def process_stream():
    global env, source, events_sink, stats_sink, tracking_sink

    stream = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "frame_processor",
        Types.STRING()
    )

    # Sử dụng DetectVehicle để có cả objects và annotated frame
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

    # Gửi cả 2 stream vào events_sink
    vehicle_plate_stream.sink_to(events_sink)
    annotated_frames_stream.sink_to(tracking_sink)
    counting_stream.sink_to(stats_sink)


def execute_job():
    global env
    print("Executing job...")
    env.execute("main_stream")