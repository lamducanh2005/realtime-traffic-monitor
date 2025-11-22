from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy, Types
from pathlib import Path
from .detect_processor import FrameProcessor
import json


class SecondaryStream:
    """
    Job 2: Đơn giản - chỉ vẽ bounding box lên frames
    Nhận cam_raw, detect + vẽ khung, gửi đến cam_tracking
    """
    
    # Constants
    F2K_JAR = Path(r"C:\Users\Admin\Documents\Study\_INT3229 Big Data\final project\resources\jars\flink-connector-kafka-4.0.1-2.0.jar")
    KC_JAR = Path(r"C:\Users\Admin\Documents\Study\_INT3229 Big Data\final project\resources\jars\kafka-clients-3.4.0.jar")

    # Generate 6 bootstrap broker addresses compactly
    KAFKA_BOOTSTRAP_SERVER = ",".join(f"192.168.0.106:{p}" for p in range(9092, 9092 + 6))
    SOURCE_TOPIC = "cam_raw"  # Nhận từ camera
    TRACKING_TOPIC = "cam_tracking"

    def __init__(self):
        """
        Khởi tạo các thành phần của streaming pipeline thứ 2
        """
        self.env: StreamExecutionEnvironment = None
        self.source: KafkaSource = None
        self.tracking_sink: KafkaSink = None

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

        # Tạo kafka source - nhận từ cam_raw
        self.source = (
            KafkaSource.builder()
            .set_bootstrap_servers(self.KAFKA_BOOTSTRAP_SERVER)
            .set_topics(self.SOURCE_TOPIC)
            .set_group_id("frame_tracking")
            .set_starting_offsets(KafkaOffsetsInitializer.latest())
            .set_value_only_deserializer(SimpleStringSchema())
            .build()
        )

        # Tạo kafka sink cho tracking (annotated frames)
        self.tracking_sink = (
            KafkaSink.builder()
            .set_bootstrap_servers(self.KAFKA_BOOTSTRAP_SERVER)
            .set_record_serializer(
                KafkaRecordSerializationSchema.builder()
                .set_topic(self.TRACKING_TOPIC)
                .set_key_serialization_schema(SimpleStringSchema())
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
            )
            .build()
        )

    def process_stream(self):
        """
        Job 2: Chỉ vẽ bounding box lên frames
        """
        # Nhận frames từ Kafka (cam_raw)
        stream = self.env.from_source(
            self.source,
            WatermarkStrategy.no_watermarks(),
            "frame_tracking",
            Types.STRING()
        )

        # Vẽ bounding box lên frames
        annotated_stream = (
            stream
            .map(FrameProcessor(), output_type=Types.STRING())
        )

        # Gửi annotated frames đến cam_tracking
        annotated_stream.sink_to(self.tracking_sink)

    def execute_job(self):
        """
        Thực thi Flink job thứ 2
        """
        print("Executing secondary job...")
        self.env.execute("secondary_stream")
