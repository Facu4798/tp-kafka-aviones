import os 
import time
os.system("clear")

######################
#### INSTALAR JDK ####
######################

command = '''
echo "📦 [1/6] Instalando Java 17..."
apt-get install -qq openjdk-17-jdk-headless -y 2>/dev/null || \
brew install openjdk@17 2>/dev/null || true
export JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
echo "   JAVA_HOME=$JAVA_HOME"
'''
os.system(command)


########################
#### INSTALAR KAFKA ####
########################

command = '''
KAFKA_DIR="./kafka"
if [ ! -d "$KAFKA_DIR" ]; then
    echo "📦 [2/6] Descargando Apache Kafka 3.9.2..."
    wget -q https://downloads.apache.org/kafka/3.9.2/kafka_2.13-3.9.2.tgz
    tar -xzf kafka_2.13-3.9.2.tgz
    mv kafka_2.13-3.9.2 kafka
    rm kafka_2.13-3.9.2.tgz
    echo "✅  Kafka descargado"
else
    echo "✅  Kafka ya existe"
fi
'''
os.system(command)

#######################
#### INICIAR KAFKA ####
#######################

# iniciar Zookeeper
os.system((
    "nohup /content/kafka/bin/zookeeper-server-start.sh """
    "/content/kafka/config/zookeeper.properties > /dev/null 2>&1 &"
))

time.sleep(5)  # Esperar a que Zookeeper inicie

# iniciar el broker Kafka
os.system((
    "nohup /content/kafka/bin/kafka-server-start.sh "
    "/content/kafka/config/server.properties > /dev/null 2>&1 &"
))



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
