from kafka import KafkaConsumer
import json
import sys
import time
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel
from datetime import datetime


CASSANDRA_HOST = 'database-cassandra'
CASSANDRA_PORT = 9042
KEYSPACE = 'kafkastorage'


def init_cassandra(retries=10, delay=5):
    for attempt in range(1, retries + 1):
        try:
            cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
            session = cluster.connect(KEYSPACE)

            insert_ps = session.prepare("""
                INSERT INTO sensor_data (
                    clientid, ts, topic, suhu, status,
                    kelembapan_udara, kelembapan_tanah,
                    durasi_siram, cahaya
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """)
            insert_ps.consistency_level = ConsistencyLevel.ONE

            print("Connected to Cassandra", flush=True)
            return cluster, session, insert_ps

        except Exception as e:
            print(f"Cassandra not ready (attempt {attempt}/{retries}): {e}", flush=True)
            time.sleep(delay)

    print("Fatal: Cassandra never became ready", flush=True)
    sys.exit(1)


def to_datetime(ms):
    try:
        return datetime.fromtimestamp(ms / 1000.0)
    except Exception:
        return datetime.utcnow()


def main():
    cluster, session, insert_ps = init_cassandra()

    consumer = KafkaConsumer(
        'mqtt-data',
        bootstrap_servers='broker:9092',
        group_id='iot-debug-v1',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8', errors='ignore'))
    )

    print("Kafka consumer started...", flush=True)

    try:
        for msg in consumer:
            data = msg.value
            print("RAW:", data, flush=True)

            clientid = data.get('clientid')
            topic = data.get('topic')

            if not clientid or not topic:
                print("Skipping invalid message", flush=True)
                continue

            ts = to_datetime(data.get('timestamp'))
            suhu = float(data['suhu']) if data.get('suhu') is not None else None
            status = data.get('status')
            kelembapan_udara = int(data['kelembapan_udara']) if data.get('kelembapan_udara') is not None else None
            kelembapan_tanah = int(data['kelembapan_tanah']) if data.get('kelembapan_tanah') is not None else None
            durasi_siram = int(data['durasi_siram']) if data.get('durasi_siram') is not None else None
            cahaya = float(data['cahaya']) if data.get('cahaya') is not None else None

            try:
                session.execute(
                    insert_ps,
                    (
                        clientid, ts, topic, suhu, status,
                        kelembapan_udara, kelembapan_tanah,
                        durasi_siram, cahaya
                    )
                )
                print("Inserted:", clientid, ts, flush=True)

            except Exception as e:
                print("Insert failed:", e, flush=True)

    except KeyboardInterrupt:
        print("Shutting down consumer...", flush=True)

    finally:
        consumer.close()
        session.shutdown()
        cluster.shutdown()


if __name__ == '__main__':
    main()
