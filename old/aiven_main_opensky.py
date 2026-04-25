import sys
sys.dont_write_bytecode = True
import os 
import time
import threading
import subprocess
import shutil
os.system("clear")

## remove checkpoint dir
if os.path.isdir("tmp/checkpoint"):
    shutil.rmtree("tmp/checkpoint")


######################
#### INSTALAR JDK ####
######################
print("📦 [1/6] Configurando Java 17...")
subprocess.run(
    "apt-get install -qq openjdk-17-jdk-headless -y 2>/dev/null || "
    "brew install openjdk@17 2>/dev/null || true",
    shell=True,
)

java_candidates = [
    "/usr/local/sdkman/candidates/java/21.0.10-ms",
    "/usr/lib/jvm/java-17-openjdk-amd64",
    "/usr/lib/jvm/java-17-openjdk",
    "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home",
    "/usr/local/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home",
    "/usr/local/sdkman/candidates/java/current",
]

for candidate in java_candidates:
    if os.path.isdir(candidate):
        os.environ["JAVA_HOME"] = candidate
        os.environ["PATH"] = f"{candidate}/bin:" + os.environ.get("PATH", "")
        break

java_bin = shutil.which("java")
if java_bin:
    print(f"   JAVA_HOME={os.environ.get('JAVA_HOME', '(no definido)')}")
    subprocess.run([java_bin, "-version"], check=False)
else:
    print("   No se encontro Java en PATH")

########################
#### START PRODUCER ####
########################


# ca.pem
ca_pem = """
-----BEGIN CERTIFICATE-----
MIIETTCCArWgAwIBAgIUajw8mZYj+mcbRr/twLsBVU9672UwDQYJKoZIhvcNAQEM
BQAwQDE+MDwGA1UEAww1ZjJlZTg0MzMtYzU4OS00OGIwLWFjNjAtM2QxOTJlMWNi
M2Q3IEdFTiAxIFByb2plY3QgQ0EwHhcNMjUwNDExMTY0NTM5WhcNMzUwNDA5MTY0
NTM5WjBAMT4wPAYDVQQDDDVmMmVlODQzMy1jNTg5LTQ4YjAtYWM2MC0zZDE5MmUx
Y2IzZDcgR0VOIDEgUHJvamVjdCBDQTCCAaIwDQYJKoZIhvcNAQEBBQADggGPADCC
AYoCggGBAMdr+LFPXCnvLaZWvRk0ADPssp8+Do9S9Lq1Z/HIAtNgG+CS9TW8fOAC
JqUyEUBcNzVValergT5KRRQGgtCaXXVzEN55voj6WgtRBfI4ig5LwI9d4y5+n/9c
zvNEkxVyN6A7sNu0k8dKdvPEyipbPD9uZ3i69y7BHxGv5OzY38jfK0jKkylf5XdI
IFLtUwoIiO2lsZ5BLFvhsbXlbJD1bhA9Ynw7jruiZMBCABzqg27aqxjRHqsZicuf
pNryg1JrOjBbeR5yBtZ8vY/aSe1ong545Pbt4KheTW5Le3im/1UXILw9doORN0f6
YDDdEyBkkH4bRd6tcDTT7v5dI9o0WuzdBLHETStvG9kYvydps7pegezukY22OSQ2
+VfU/CaKLy/H4MzyLAqAlvOnPBuZG0X6KQ9e1p7oHkO7DTQe0SlfvVY8i/qVTH6c
/WbhSoel9ODvyeqW7QaMKvZMn+EY7Sav3/3EdXP477h2MdwUqsU4RzRWnKqYd0bm
9cA6sO4WNwIDAQABoz8wPTAdBgNVHQ4EFgQUxi675P1HTrJCdv/eaB3x7e2YyYkw
DwYDVR0TBAgwBgEB/wIBADALBgNVHQ8EBAMCAQYwDQYJKoZIhvcNAQEMBQADggGB
AIdrPx3JD9+NwEDDgqloFT8IMTn8wfPOd3SrGz6mLaVIGpyT86hbWJFGSbAWMEHT
I3H5t9YNcuztpqZ2mZJjLVy311PrzVBU/ME94UYsNW2K63rcPeZtLIWmslBPuJiT
gt27tX8WTzUgqayQxa4zNPv+kC/ITva5waijBOOdA+GJc2cfQlE/vrD0yv9QMCIr
qmHRgPmUCsbqIeDfuqw9JNZzaTvRIb5LJv+4OQtvM4MDmW5MJCYNb2CWX8Obr0Zd
rYY3COy5h68LwaZFXVmLYEmfcWLR2l/24j9yRhcKyGqGUa0kfyQ4Dij3eVMUPOIC
5FzruyXbcNqRIke9BYsMEb37Iu0e9qMrZGVYes07yIZDIx/chooHBEIilPu5WhHE
BrZlc/TMkVzG+X4JKA154FNTrG2cfeCoo+KZ0A72lNU/aIQCPsxj0Lp0zAsCy7Nx
P5hjMG7GikvH9p8EYWSshslujjQvOeYc9ofZ2Meo74s+8OXqkMrmPvbZGzPzTO7R
yw==
-----END CERTIFICATE-----
"""
fh = open("ca.pem", "w")
fh.write(ca_pem.strip())
fh.close()

#service.certificate
service_cert = """
-----BEGIN CERTIFICATE-----
MIIEZDCCAsygAwIBAgIUXq5LIqvLfp+v6qwsxjo3eYXnO1QwDQYJKoZIhvcNAQEM
BQAwQDE+MDwGA1UEAww1ZjJlZTg0MzMtYzU4OS00OGIwLWFjNjAtM2QxOTJlMWNi
M2Q3IEdFTiAxIFByb2plY3QgQ0EwHhcNMjUxMjE0MDY1NjIyWhcNMjgwMzEzMDY1
NjIyWjA8MRQwEgYDVQQKDAtteS1wcm9kdWNlcjERMA8GA1UECwwIdTdtMHo2eDUx
ETAPBgNVBAMMCGF2bmFkbWluMIIBojANBgkqhkiG9w0BAQEFAAOCAY8AMIIBigKC
AYEAzIW3WKO0I/XUnrye7SOgT33UIwpt/DYrFyz4OMv9KNs41KFNiumtZ9DcAI9G
WHf94P/E0/+ldaT0WoZmD3YavtbdKUal1e8/SUPr4CgrkrSRwSqbZfoQHUBZfbEl
zjcePfNmHxOkt9lDxsfqLs5LxQDtjJSpgNL2/MJQmZuvzZkC9r34HU6SytTp0JjJ
LEw21dBjQIwUCl/OKhE581QqbydotWKNU90nnMno3X+BMi7xZs3silNC8il84k+E
yhinWSaHXtndOBkNpVxOT/t03ISdqm3g67UIROqNd+V2IRbbnZXT53TS2KgC4y2J
IKR/V2yhThlPSGMx5tt3EnNbG9UKg5Ncg3duXor5m/3i04vA23yWR4ky41fUXT+n
o1AerrEGnWSnNGP5Z0F28o7p0s3SDmemp4wooKlZsml/08VD02bklSorJoiUR7xw
ZQSwSEHVGkZzmQqdGaZiq3MjOtXiw/RuOm0YF117EbLQKTDmYSBB9/YgK4nfqbMM
R0LdAgMBAAGjWjBYMB0GA1UdDgQWBBQEOk3Hu+93ju46qhMgFTZycJ4jxjAJBgNV
HRMEAjAAMAsGA1UdDwQEAwIFoDAfBgNVHSMEGDAWgBTGLrvk/UdOskJ2/95oHfHt
7ZjJiTANBgkqhkiG9w0BAQwFAAOCAYEAMaq5BG/uhH22OVjPbQS/jSwEftPpK+lK
+7nnFwqJfg5aNmWct47DkUkNnioWgrYTZWiKro/Nj8Hwe4VgSxfY+5xejgwGeiXN
529SkAmDeggJChWfPp8hSIyD/3vBNb+qvHUD7XnpxpraiMV1J/mtoPiZkc3PCZrA
jI4eX1bW/g+snCz+bnJ8aVA3MIErQage86L+KH7rWcQyoIoIL+ojhilaYMJWSHRu
cael6oSoJYz3CQegzSnAE2iP8TrkTynGwhxawyaV3UEbdZk+JwL2W1Pix2iHeN5o
ZwWKAvr8lE7GjW3X1Lwz+qWeikU6l6hVHrM5zhs47ZhXbAuic4TTs9EuljU1yG5P
foSWdelkmqN8gcNCpEGmSgCiWOv11qGlig5qaP8BJpFgULgHftGypnjIv3mN1yQ9
k78I9WBeTzXOiUbftRUiPSodBZjlKB0a08+KaFNoxZ0iEAH3Vs/8i4/X0CI+mMnC
lQVEQp3cR86ozQsgQvdBy4s0Mc1WDpgz
-----END CERTIFICATE-----
"""
fh = open("service.cert", "w")
fh.write(service_cert.strip())
fh.close()


#service.key
service_key = """
-----BEGIN PRIVATE KEY-----
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
gyq/FXNLYdE9aP9z2E6eTrd0
-----END PRIVATE KEY-----
"""
fh = open("service.key", "w")
fh.write(service_key.strip())
fh.close()


############################
#### INSTALAR LIBRERIAS ####
############################
reqs = ["kafka-python", "pyspark", "Flask"]
for r in reqs:
    os.system(f"pip install -qq {r} 2>/dev/null || true")



##############################
#### Iniciar el productor ####
##############################



from kafka import KafkaProducer
from old.producer import kafka_produce
import json

producer = KafkaProducer(
    bootstrap_servers="my-producer-estimacionderiego1.i.aivencloud.com:24197",
    security_protocol="SSL",
    ssl_cafile="ca.pem",
    ssl_certfile="service.cert",
    ssl_keyfile="service.key",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

threading.Thread(
    target=kafka_produce,
    args=(producer,),
    daemon=True
).start()


####################################
#### SHARED STATE FOR DASHBOARD ####
####################################
global state_lock
global sse_queue
global shared_state
global sqt


from collections import defaultdict, deque
import queue
sse_queue = queue.Queue(maxsize=50)


state_lock = threading.Lock()

shared_state = {
    "aircraft":      {},                         # icao24 → último mensaje
    "trails":        defaultdict(lambda: deque(maxlen=8)),
    "alert_log":     deque(maxlen=15),
    "type_counts":   defaultdict(int),
    "total_history": deque(maxlen=100),
    "amba_history":  deque(maxlen=100),
    "last_update":   None,
    "batch_count":   0,
}








#########################
#### CONSUMER STREAM ####
#########################

from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
from pyspark.sql import types as st
import pyspark


#### convert credentials to java keystore ####

os.system((
    "openssl pkcs12 -export -in service.cert -inkey service.key "
    "-out service.p12 -name avnclient -CAfile ca.pem -caname root -password pass:charlie"
    " && keytool -importkeystore -srckeystore service.p12 -srcstoretype PKCS12 "
    "-srcstorepass charlie -destkeystore keystore.jks -deststoretype JKS "
    "-deststorepass charlie -noprompt"
    " && keytool -importcert -alias aiven-ca -file ca.pem "
    "-keystore truststore.jks -storepass charlie -noprompt"
))


kafka_scala_binary = "2.13" if pyspark.__version__.startswith("4.") else "2.12"
kafka_spark_package = (
    f"org.apache.spark:spark-sql-kafka-0-10_{kafka_scala_binary}:{pyspark.__version__}"
)

spark = SparkSession.builder\
    .appName("aviones")\
    .master("local[*]")\
    .config("spark.jars.packages", kafka_spark_package)\
    .getOrCreate()


sq = spark.readStream.format("kafka")\
    .option("kafka.bootstrap.servers", "my-producer-estimacionderiego1.i.aivencloud.com:24197")\
    .option("subscribe", "opensky-argentina")\
    .option("kafka.security.protocol", "SSL")\
    .option("kafka.ssl.truststore.location", "truststore.jks")\
    .option("kafka.ssl.truststore.password", "charlie")\
    .option("kafka.ssl.keystore.location", "keystore.jks")\
    .option("kafka.ssl.keystore.password", "charlie")\
    .option("kafka.ssl.key.password", "charlie")\
    .option("startingOffsets","latest")\
    .option("failOnDataLoss","false")\
    .load()


msg_schema = st.StructType([
    st.StructField("icao24",           st.StringType()),
    st.StructField("callsign",         st.StringType()),
    st.StructField("origin_country",   st.StringType()),
    st.StructField("latitude",         st.DoubleType()),
    st.StructField("longitude",        st.DoubleType()),
    st.StructField("baro_altitude_m",  st.DoubleType()),
    st.StructField("altitude_ft",      st.DoubleType()),
    st.StructField("on_ground",        st.BooleanType()),
    st.StructField("velocity_ms",      st.DoubleType()),
    st.StructField("velocity_kmh",     st.DoubleType()),
    st.StructField("heading",          st.DoubleType()),
    st.StructField("vertical_rate_ms", st.DoubleType()),
    st.StructField("aircraft_type",    st.StringType()),
    st.StructField("squawk",           st.StringType()),
    st.StructField("position_source",  st.LongType()),
    st.StructField("in_amba",          st.BooleanType()),
    st.StructField("anomaly",          st.StringType()),
    st.StructField("server_time",      st.LongType()),
    st.StructField("ingested_at",      st.StringType()),
])



sqt = sq.select(
    sf.col("value").cast("string").alias("value")
).select(
    sf.from_json("value", msg_schema).alias("value")
).select("value.*")\
    .filter("latitude IS NOT NULL AND longitude IS NOT NULL and icao24 IS NOT NULL")




def feb(df,batch_id):
    import datetime
    from datetime import datetime, timezone
    import json
    
    # ── Agregaciones Spark (se hacen en el DataFrame antes de collectear) ──
    # Conteo por tipo
    type_agg = df.groupBy("aircraft_type").count().collect()

    # Stats generales
    stats_row = df.agg(
        sf.count("*").alias("total"),
        sf.sum(sf.when(sf.col("in_amba"), 1).otherwise(0)).alias("amba_count"),
        sf.avg(sf.when(~sf.col("on_ground"), sf.col("altitude_ft"))).alias("avg_alt_ft"),
        sf.avg(sf.when(~sf.col("on_ground"), sf.col("velocity_kmh"))).alias("avg_vel_kmh"),
        sf.sum(sf.when(~sf.col("on_ground"), 1).otherwise(0)).alias("airborne"),
        sf.countDistinct("origin_country").alias("countries"),
    ).collect()
    
    # Top países
    top_countries = df.groupBy("origin_country") \
        .count().orderBy(sf.desc("count")).limit(5).collect()

    # Aeronaves con anomalía
    anomaly_rows = df.filter(
        sf.col("anomaly").isNotNull() & (sf.col("anomaly") != "")
    ).select("callsign","anomaly","altitude_ft","velocity_kmh").limit(10).collect()

    # Collect de todos los registros para el mapa
    all_rows = df.collect()




    # ── Actualizar estado compartido ──────────────────────────────
    with state_lock:
        now = datetime.now(timezone.utc)
        shared_state["last_update"] = now.isoformat()
        shared_state["batch_count"] = batch_id

        # Actualizar diccionario de aeronaves
        current_batch_icaos = set()
        for row in all_rows:
            icao = row["icao24"]
            current_batch_icaos.add(icao)
            shared_state["aircraft"][icao] = row.asDict()
            # Trail
            lat, lon = row["latitude"], row["longitude"]
            if lat and lon:
                shared_state["trails"][icao].append({"lat": lat, "lon": lon})

        # Limpiar aeronaves que no aparecieron en los últimos 2 batches
        # (no usamos timestamp para simplificar; con 10s poll esto es ~20s)
        stale = [k for k in list(shared_state["aircraft"].keys())
                 if k not in current_batch_icaos]
        for k in stale:
            del shared_state["aircraft"][k]
            if k in shared_state["trails"]:
                del shared_state["trails"][k]

        # Tipo counts desde Spark aggregation
        shared_state["type_counts"] = {r["aircraft_type"]: r["count"] for r in type_agg}

        # Historial
        stats = stats_row[0] if stats_row else None
        total  = int(stats["total"])  if stats and stats["total"]  else 0
        amba   = int(stats["amba_count"]) if stats and stats["amba_count"] else 0
        shared_state["total_history"].append(total)
        shared_state["amba_history"].append(amba)

        # Alertas
        for ar in anomaly_rows:
            cs  = ar["callsign"] or "N/A"
            an  = ar["anomaly"]
            alt = int(ar["altitude_ft"]) if ar["altitude_ft"] else "?"
            vel = int(ar["velocity_kmh"]) if ar["velocity_kmh"] else "?"
            ts  = now.strftime("%H:%M:%S")
            msg = f"{ts} | {cs} | {an} | {alt}ft | {vel}km/h"
            if not any(cs in a for a in shared_state["alert_log"]):
                shared_state["alert_log"].appendleft(msg)

        # Construir snapshot serializable para SSE
        snapshot = {
            "aircraft":      list(shared_state["aircraft"].values()),
            "trails":        {k: list(v) for k, v in shared_state["trails"].items()},
            "alert_log":     list(shared_state["alert_log"]),
            "type_counts":   dict(shared_state["type_counts"]),
            "total_history": list(shared_state["total_history"]),
            "amba_history":  list(shared_state["amba_history"]),
            "last_update":   shared_state["last_update"],
            "batch_id":      batch_id,
            "stats": {
                "total":    total,
                "amba":     amba,
                "airborne": int(stats["airborne"]) if stats and stats["airborne"] else 0,
                "avg_alt_ft":  float(stats["avg_alt_ft"])  if stats and stats["avg_alt_ft"]  else 0,
                "avg_vel_kmh": float(stats["avg_vel_kmh"]) if stats and stats["avg_vel_kmh"] else 0,
            },
            "top_countries": [{"country": r["origin_country"], "count": r["count"]}
                               for r in top_countries],
            "anomaly_aircraft": [ar.asDict() for ar in anomaly_rows],
        }

    if batch_id == 1:
        print(json.dumps(snapshot, indent=4))

    # Encolar para SSE (non-blocking: si la cola está llena, descartar el más viejo)
    try:
        sse_queue.put_nowait(snapshot)
    except queue.Full:
        try:
            sse_queue.get_nowait()
        except queue.Empty:
            pass
        sse_queue.put_nowait(snapshot)

    print(f"[{datetime.now()}] ✅ Batch {batch_id} - Actualización completada")


#####################
#### INICIAR API ####
#####################

from old.api import start_api

threading.Thread(target=start_api, args=(sse_queue, state_lock, shared_state, sqt), daemon=True).start()


#########################
#### ESCRIBIR STREAM ####
#########################


sqt.writeStream\
    .option("checkpointLocation", "tmp/checkpoint")\
    .foreachBatch(feb)\
    .trigger(processingTime="10 seconds")\
    .start().awaitTermination()

