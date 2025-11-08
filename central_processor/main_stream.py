from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy, Types
from pyflink.datastream.functions import MapFunction
from pyflink.common import WatermarkStrategy, Duration
from pyflink.common.watermark_strategy import TimestampAssigner
import json
import base64
import cv2
import numpy as np
from ultralytics import YOLO
import time
from pathlib import Path

F2K_JAR = Path(r"C:\Users\Admin\Documents\Study\_INT3229 Big Data\final project\jars\flink-connector-kafka-4.0.1-2.0.jar")
KC_JAR = Path(r"C:\Users\Admin\Documents\Study\_INT3229 Big Data\final project\jars\kafka-clients-3.4.0.jar")

KAFKA_BOOTSTRAP_SERVER = None


env : StreamExecutionEnvironment = None
kafka_source : KafkaSource = None
kafka_sink : KafkaSink = None


def set_up():
    """
        Thực hiện thiết đặt mô trường:
            - Khởi động môi trường
            - Thêm file jar
            - Cài các tham số
            - Tạo kafka source và kafka sink
    """
    global env, kafka_source, kafka_sink

    # Khởi động môi trường
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
        .build()
    )

    kafka_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVER)
        .build()
    )


def process_stream():
    pass

def execute_job():
    global env
    print("Executing job...")
    env.execute("main-stream")









    