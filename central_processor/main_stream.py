from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy, Types, Configuration
from pyflink.datastream.functions import MapFunction
from pyflink.common.watermark_strategy import TimestampAssigner
from pathlib import Path
from .plate_processor import ExpandObject, DetectPlate, VoteBestPlate
import os


F2K_JAR = Path(r"C:\Users\Admin\Documents\Study\_INT3229 Big Data\final project\resources\jars\flink-connector-kafka-4.0.1-2.0.jar")
KC_JAR = Path(r"C:\Users\Admin\Documents\Study\_INT3229 Big Data\final project\resources\jars\kafka-clients-3.4.0.jar")

KAFKA_BOOTSTRAP_SERVER = "localhost:9092,localhost:9093,localhost:9094"
KAFKA_TRACKING_TOPIC = "cam_tracking"
KAFKA_EVENTS_TOPIC = "cam_event"


env : StreamExecutionEnvironment = None
kafka_source : KafkaSource = None
kafka_sink : KafkaSink = None


def set_up():
    """
        Thực hiện thiết đặt môi trường:
            - Khởi động môi trường
            - Thêm file jar
            - Cài các tham số
            - Tạo kafka source và kafka sink
    """
    global env, kafka_source, kafka_sink

    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Thêm file jar cho 2 cái này
    env.add_jars(F2K_JAR.as_uri(), KC_JAR.as_uri())

    # Cài các tham số
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1)

    # Tạo kafka source và kafka sink
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVER)
        .set_topics(KAFKA_TRACKING_TOPIC)
        .set_group_id("plate")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

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
        "plate_recognize",
        Types.STRING()
    )

    (
        stream
        .flat_map(
            ExpandObject(), 
            output_type=Types.STRING()
        )
        # .key_by(lambda x: (json.loads(x)["camera_id"], json.loads(x)["obj_id"])) # Liệu có nhất thiết phải key by trong trường hợp này?
        .flat_map(
            DetectPlate(),
            output_type=Types.STRING()
        )
        # .filter(lambda x: x is not None)
        # .key_by(lambda x: (x["cam_id"], x["obj_id"]))
        # .process(VoteBestPlate())
        # .map(lambda x: json.dumps(x))
        .sink_to(kafka_sink)

    )

def execute_job():
    global env
    print("Executing job...")
    env.execute("main_stream")









    