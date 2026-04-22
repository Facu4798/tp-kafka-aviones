import os 
import time
os.system("clear")

######################
#### INSTALAR JDK ####
######################
os.system("sudo apt-get install -qq openjdk-17-jdk-headless -y > /dev/null")
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"


########################
#### INSTALAR KAFKA ####
########################
KAFKA_DIR = "./kafka"
if not os.path.exists(KAFKA_DIR):
    os.system("wget -q https://downloads.apache.org/kafka/3.9.2/kafka_2.13-3.9.2.tgz")
    os.system("tar -xzf kafka_2.13-3.9.2.tgz")
    os.system("mv kafka_2.13-3.9.2 kafka")
    os.system("rm kafka_2.13-3.9.2.tgz") 

#######################
#### INICIAR KAFKA ####
#######################

os.system("nohup ./kafka/bin/zookeeper-server-start.sh -daemon ./kafka/config/zookeeper.properties > /tmp/zookeeper.log 2>&1")
time.sleep(5)  # Esperar a que Zookeeper inicie

#### CREAR TOPIC ####
os.system((
    "./kafka/bin/kafka-topics.sh"
    "--create --topic opensky-argentina --bootstrap-server"
    "localhost:9092 --if-not-exists > /dev/null 2>&1"
))


#############################
#### instalar librerias #####
#############################

os.system("pip install kafka-python")


#################
#### IMPORTS ####
#################
import threading
from kafka import KafkaProducer
import json


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
