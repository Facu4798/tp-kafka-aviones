#############################
#### instalar librerias #####
#############################
import os os.system("pip install kafka-python requests")


#################
#### IMPORTS ####
#################
import threading


# ─── CONFIGURACIÓN ───────────────────────────────────────────
KAFKA_BOOTSTRAP   = "localhost:9092"
KAFKA_TOPIC       = "opensky-argentina"
POLL_INTERVAL_SEC = 10


# Si tenés credenciales OpenSky, descomentar:
# OPENSKY_USER = "tu_usuario"
# OPENSKY_PASS = "tu_password"
# Y agregar auth=(OPENSKY_USER, OPENSKY_PASS) al requests.get


# ─── PRODUCTOR KAFKA ─────────────────────────────────────────
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
    retries=3,
)

from producer import kafka_produce

threading.Thread(target=kafka_produce, args=(producer,), daemon=True).start()
