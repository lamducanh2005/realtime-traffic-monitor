from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy, Types
from pathlib import Path
from .detect_processor import FrameProcessor
from .plate_processor import DetectVehicle, DetectPlate
import os
import json


F2K_JAR = Path(r"C:\Users\Admin\Documents\Study\_INT3229 Big Data\final project\resources\jars\flink-connector-kafka-4.0.1-2.0.jar")
KC_JAR = Path(r"C:\Users\Admin\Documents\Study\_INT3229 Big Data\final project\resources\jars\kafka-clients-3.4.0.jar")

KAFKA_BOOTSTRAP_SERVER = "localhost:9092,localhost:9093,localhost:9094"
KAFKA_RAW_TOPIC = "cam_raw"
KAFKA_STREAMING_TOPIC = "cam_streaming"
KAFKA_EVENTS_TOPIC = "cam_event"


env: StreamExecutionEnvironment = None
kafka_source: KafkaSource = None
kafka_sink: KafkaSink = None


def set_up():
    """
    Thiết đặt môi trường:
        - Khởi động môi trường
        - Thêm file jar
        - Cài các tham số
        - Tạo kafka source và kafka sink
    """
    global env, kafka_source, kafka_sink

    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Thêm file jar
    env.add_jars(F2K_JAR.as_uri(), KC_JAR.as_uri())

    # Cài các tham số
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1)

    # Tạo kafka source
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVER)
        .set_topics(KAFKA_RAW_TOPIC)
        .set_group_id("plate")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # Tạo kafka sink
    kafka_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVER)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(KAFKA_EVENTS_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )


def process_stream():
    global env, kafka_source, kafka_sink

    stream = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "frame_processor",
        Types.STRING()
    )

    (
        stream
        .key_by(lambda x: json.loads(x).get("camera_id"), key_type=Types.STRING())
        .flat_map(DetectVehicle(), output_type=Types.STRING())
        .flat_map(DetectPlate(), output_type=Types.STRING())
        .sink_to(kafka_sink)
    )


def execute_job():
    global env
    print("Executing job...")
    env.execute("main_stream")