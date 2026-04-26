import sys
sys.dont_write_bytecode = True
import os 
import time
import threading
import subprocess
import shutil
os.system("clear")


os.makedirs("tmp",exist_ok=True)
os.makedirs("credentials",exist_ok=True)

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
fh = open("credentials/ca.pem", "w")
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
fh = open("credentials/service.cert", "w")
fh.write(service_cert.strip())
fh.close()


#service.key
service_key = """
-----BEGIN PRIVATE KEY-----
MIIG/gIBADANBgkqhkiG9w0BAQEFAASCBugwggbkAgEAAoIBgQDMhbdYo7Qj9dSe
vJ7tI6BPfdQjCm38NisXLPg4y/0o2zjUoU2K6a1n0NwAj0ZYd/3g/8TT/6V1pPRa
hmYPdhq+1t0pRqXV7z9JQ+vgKCuStJHBKptl+hAdQFl9sSXONx4982YfE6S32UPG
x+ouzkvFAO2MlKmA0vb8wlCZm6/NmQL2vfgdTpLK1OnQmMksTDbV0GNAjBQKX84q
ETnzVCpvJ2i1Yo1T3Secyejdf4EyLvFmzeyKU0LyKXziT4TKGKdZJode2d04GQ2l
XE5P+3TchJ2qbeDrtQhE6o135XYhFtudldPndNLYqALjLYkgpH9XbKFOGU9IYzHm
23cSc1sb1QqDk1yDd25eivmb/eLTi8DbfJZHiTLjV9RdP6ejUB6usQadZKc0Y/ln
QXbyjunSzdIOZ6anjCigqVmyaX/TxUPTZuSVKismiJRHvHBlBLBIQdUaRnOZCp0Z
pmKrcyM61eLD9G46bRgXXXsRstApMOZhIEH39iArid+pswxHQt0CAwEAAQKCAYA1
iy5QdO802OeUtTHwSHrwRLN0hwb7WB0Y4KuNhw4ck6QBuscHvpyoyLvII7pZb/SV
4rAfhlbFgW19g6EVnChtXOgipb3Fz/BkGHWYxjt+GWrtRN/RJ1HvroifSPDqV3DB
7EaIjUZ64VUGphDvtw+MIG2Qd90WfmSuEJs0U9eHPm9RtLNPzOHOW0ZpV46XW86h
VICfhey7OJDLX54pz5eaaRlXH1l7orL7r7qYvohO4I5PiUrLfffRWT46K3JKzIAP
nWHw7REoGugYTb81DLnV/swiAXSsFc84JRxdW5JZc6dR9zldR3RHIqGzizBM7GNX
07wdY9o1hQohT34K7J2IclIMMkwUe6qWu3+7eqUPzvSKxM7DTecjHk814t7nndz/
msysnSMd8lkyyVtPJ3Uvcy5r98KJNI78TbM5icaake8m0QDoplitNlbKRyqvbPcP
aEpPZZD5iPun7+arNwEq2+ze/BgF4tJWCkPLoddOKr+V+VHrDLh05w18oCMYh+sC
gcEA5kD6OPhsbvwSMA6Xcnp0P82GUSFTLt67Fd/bM4loH0+HaLZFfrtlcVkrjHVi
e0/tfghnA+Fu9a1j796PZ847jn6k4iDCezZ8CW+2Kg65zFFdWfY6yoc2Oz+Cfs4n
nAW9h/V3k9AbK1G/VZZ0bux+Ia4pcru77A/+oCCxX3T/sBPUHyZsUH2UebTI+/99
hx/mxixQipt6nAe+5oL9vnpEZtx5Qdls4KWxbgnd9vqn8nFLIPjs4nHISVrXpcYX
+WMvAoHBAONkLJHiGIkDb2r7Gzd53z8WJfs9At8Bat5zZNwHQB4Nl+gsXrkLaF3T
yTqNCGSFHCFHGoPApt2loN6bGGBHR+V9kjK9POD6odwZZh7uzTM+WjMavf4LxpWG
8Y+hsHQUfA+hkgPvhZiQIc5eLMEUtvuI3ZipjzXCASbNZ1lJ8tnpBQpg7RoFFc4b
goiqvRlbgq6YKq1B2f1CDcZ2mtMW/UhxaFSx+L/Q0AZ1nNotpnMKsMcW4k8yqRJz
oV+66jFnswKBwQCfodTYwoKKv3/siXE+EN+fjGHexXfiOtEB7V0PcegaLQz378Zm
go1l9ChZDNjmHRfOFQ02h4hiE4ABwQ8zLKvlsq+lhojO24tLD4P/zI3LtW0+/ZLs
7qkOko1qVIrphKFOLURl2hW8BsTcFZ9Ld/JKb3CU9srmcoBZi9DtGTRbYqPhpMXb
V/UkG85rx0nD8L4SDo8YSYrLuiMDtTUuZZ4eoiP/Hzp41OcaHwUgQc9GKn2nsmyo
4bG2CaEQcTIV2EkCgcB48EZkMIQsGw+fLn6DOuZpfzYkMYbdZLuaJHV4MMMBOOma
Kj8W2+JeRM25vljAXatgZ5jKiiQ1rmmo+4QzcLXkcKzZH5zJa4O6tCeEPC4XGwqO
M6LpAwpTbLn9Ulmu41Y4Wg522WWMhtv0DlJvPr0zes+g6k2ooc5K/OQCsTnge/aa
VoLEXAAE5sy+M1Dvs6QFiBjqnSN7dy4sZ9hgfNGpPefnY+CQ5FY/mT4bLDxi40Sy
HpGu3ZrenXEuoRL9w9ECgcEA3GXi4q5d3MhxmWGZDm5+HAgXOUB9rqca+lZJaYrr
e5CDL099XY9sjKyocsx+GXdI/byya6qno+Uw+zG/d5B3PU6jsEjN3eOC/af6pvU4
lZdy7g/kqId+YSTWuZplfMgv3ySPyAShmxqpVPiJV4+dyniiZ9ufDSDxwqpKoT4R
UPWuTQnlq7HsqsAZVGK7Z2gpr8eyKmGCHe2lOlt50Kgl8Teyj+EUW02OD1U2HdIx
gyq/FXNLYdE9aP9z2E6eTrd0
-----END PRIVATE KEY-----
"""
fh = open("credentials/service.key", "w")
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
from producer import kafka_produce
import json

producer = KafkaProducer(
    bootstrap_servers="my-producer-estimacionderiego1.i.aivencloud.com:24197",
    security_protocol="SSL",
    ssl_cafile="credentials/ca.pem",
    ssl_certfile="credentials/service.cert",
    ssl_keyfile="credentials/service.key",
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
    "openssl pkcs12 -export -in credentials/service.cert -inkey credentials/service.key "
    "-out credentials/service.p12 -name avnclient -CAfile credentials/ca.pem -caname root -password pass:charlie"
    " && keytool -importkeystore -srckeystore credentials/service.p12 -srcstoretype PKCS12 "
    "-srcstorepass charlie -destkeystore credentials/keystore.jks -deststoretype JKS "
    "-deststorepass charlie -noprompt"
    " && keytool -importcert -alias aiven-ca -file credentials/ca.pem "
    "-keystore credentials/truststore.jks -storepass charlie -noprompt"
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
    .option("kafka.ssl.truststore.location", "credentials/truststore.jks")\
    .option("kafka.ssl.truststore.password", "charlie")\
    .option("kafka.ssl.keystore.location", "credentials/keystore.jks")\
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

    import requests
    requests.post("http://localhost:5000/post_message", json=snapshot)

    print(f"[{datetime.now()}] ✅ Batch {batch_id} - Actualización completada")


#####################
#### INICIAR API ####
#####################

from api2 import start_api

threading.Thread(target=start_api, args=(sse_queue, state_lock, shared_state, sqt), daemon=True).start()


#########################
#### ESCRIBIR STREAM ####
#########################


sqt.writeStream\
    .option("checkpointLocation", "tmp/checkpoint")\
    .foreachBatch(feb)\
    .trigger(processingTime="10 seconds")\
    .start()


# -----------------------------------------------
# segunda parte
# -----------------------------------------------

"""
╔══════════════════════════════════════════════════════════════════╗
║  CONSUMER ATC — Radar Aéreo Argentino                           ║
║  Dashboard estilo STARS/ERAM — Torre de Control / Aviónica      ║
║                                                                  ║
║  Paneles:                                                        ║
║   · Transponder Monitor    (Mode A/C/S/ADS-B)                   ║
║   · Conflict Alert         (separación vertical/horizontal)     ║
║   · Datalink / Source      (ADS-B vs MLAT vs ASTERIX)           ║
║   · Baro vs Geo Altitude   (QNH discrepancy)                    ║
║   · SPI / Emergency Panel  (squawks especiales)                 ║
║   · FIR Sector Load        (densidad por sectores)              ║
║   · Arrivals / Departures  (estimados SAEZ/SABE)                ║
╚══════════════════════════════════════════════════════════════════╝
"""

import json
import time
import math
from collections import defaultdict, deque
from datetime import datetime, timezone
from kafka import KafkaConsumer

# ─── CONFIGURACIÓN ──────────────────────────────────────────────
KAFKA_BOOTSTRAP   = "my-producer-estimacionderiego1.i.aivencloud.com:24197"
KAFKA_TOPIC       = "opensky-argentina"
DASHBOARD_FILE    = "dashboard_atc.html"
REFRESH_INTERVAL  = 5

AIRPORTS = {
    "SAEZ": {"name": "Ezeiza Ministro Pistarini",      "lat": -34.8222, "lon": -58.5358, "elev_ft": 67,   "rwy": "11/29"},
    "SABE": {"name": "Aeroparque Jorge Newbery",        "lat": -34.5592, "lon": -58.4156, "elev_ft": 13,   "rwy": "13/31"},
    "SACO": {"name": "Córdoba Ambrosio Taravella",      "lat": -31.3236, "lon": -64.2080, "elev_ft": 1604, "rwy": "18/36"},
    "SASJ": {"name": "San Salvador de Jujuy",           "lat": -24.3928, "lon": -65.0978, "elev_ft": 3019, "rwy": "09/27"},
    "SAWH": {"name": "Malvinas Argentinas Ushuaia",     "lat": -54.8433, "lon": -68.2958, "elev_ft": 102,  "rwy": "07/25"},
    "SAME": {"name": "Mendoza El Plumerillo",            "lat": -32.8317, "lon": -68.7929, "elev_ft": 2310, "rwy": "18/36"},
    "SAZB": {"name": "Bahía Blanca Cdte Espora",        "lat": -38.7250, "lon": -62.1693, "elev_ft": 246,  "rwy": "07/25"},
}

FIR_SECTORS = {
    "BAIRES_N": {"name": "BA NORTE",    "lat_min": -32, "lat_max": -21, "lon_min": -68, "lon_max": -53},
    "BAIRES_C": {"name": "BA CENTRO",   "lat_min": -38, "lat_max": -32, "lon_min": -68, "lon_max": -53},
    "BAIRES_S": {"name": "BA SUR",      "lat_min": -46, "lat_max": -38, "lon_min": -74, "lon_max": -53},
    "PATAG_N":  {"name": "PATAGONIA N", "lat_min": -46, "lat_max": -38, "lon_min": -74, "lon_max": -62},
    "PATAG_S":  {"name": "PATAGONIA S", "lat_min": -55, "lat_max": -46, "lon_min": -74, "lon_max": -53},
    "CUYO":     {"name": "CUYO",        "lat_min": -38, "lat_max": -28, "lon_min": -74, "lon_max": -68},
    "NOA":      {"name": "NOA",         "lat_min": -28, "lat_max": -21, "lon_min": -74, "lon_max": -62},
}

# ─── ESTADO EN MEMORIA ──────────────────────────────────────────
aircraft_state  = {}
aircraft_trails = defaultdict(lambda: deque(maxlen=10))
alert_log       = deque(maxlen=20)
squawk_log      = deque(maxlen=30)
arrival_list    = deque(maxlen=12)
departure_list  = deque(maxlen=12)


# ─── UTILIDADES ─────────────────────────────────────────────────
def haversine_nm(lat1, lon1, lat2, lon2):
    R = 3440.065
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat/2)**2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
         * math.sin(dlon/2)**2)
    return R * 2 * math.asin(math.sqrt(a))

def bearing_deg(lat1, lon1, lat2, lon2):
    dlon = math.radians(lon2 - lon1)
    x = math.sin(dlon) * math.cos(math.radians(lat2))
    y = (math.cos(math.radians(lat1)) * math.sin(math.radians(lat2))
         - math.sin(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.cos(dlon))
    return (math.degrees(math.atan2(x, y)) + 360) % 360

def get_sector(lat, lon):
    for sid, s in FIR_SECTORS.items():
        if s["lat_min"] <= lat <= s["lat_max"] and s["lon_min"] <= lon <= s["lon_max"]:
            return s["name"]
    return "OCEÁNICO"

def estimate_eta(aircraft, airport_icao):
    ap = AIRPORTS.get(airport_icao)
    if not ap:
        return None
    lat = aircraft.get("latitude")
    lon = aircraft.get("longitude")
    vel = aircraft.get("velocity_ms")
    if not all([lat, lon, vel]) or vel < 10:
        return None
    dist_nm  = haversine_nm(lat, lon, ap["lat"], ap["lon"])
    vel_kts  = vel * 1.94384
    if vel_kts < 5:
        return None
    return round((dist_nm / vel_kts) * 60, 0)

def detect_conflicts(aircraft_list):
    pairs = []
    ac_list = [(icao, a) for icao, a in aircraft_list.items()
               if not a.get("on_ground") and a.get("latitude") and a.get("altitude_ft")]
    for i in range(len(ac_list)):
        for j in range(i + 1, len(ac_list)):
            icao1, a1 = ac_list[i]
            icao2, a2 = ac_list[j]
            try:
                dist = haversine_nm(a1["latitude"], a1["longitude"],
                                    a2["latitude"], a2["longitude"])
                vert = abs((a1["altitude_ft"] or 0) - (a2["altitude_ft"] or 0))
                if dist < 5 and vert < 1000:
                    severity = "CRITICAL" if dist < 3 and vert < 500 else "WARNING"
                    pairs.append({
                        "cs1":      a1.get("callsign", icao1),
                        "cs2":      a2.get("callsign", icao2),
                        "dist_nm":  round(dist, 1),
                        "vert_ft":  vert,
                        "severity": severity,
                    })
            except Exception:
                pass
    return pairs[:8]

def classify_arrival_departure(aircraft, icao24):
    lat   = aircraft.get("latitude")
    lon   = aircraft.get("longitude")
    alt   = aircraft.get("altitude_ft", 0) or 0
    vrate = aircraft.get("vertical_rate_ms", 0) or 0

    results = []
    for ap_id in ("SAEZ", "SABE"):
        ap = AIRPORTS[ap_id]
        if not lat or not lon:
            continue
        dist_nm = haversine_nm(lat, lon, ap["lat"], ap["lon"])
        if dist_nm > 80:
            continue
        bearing  = bearing_deg(lat, lon, ap["lat"], ap["lon"])
        hdg      = aircraft.get("heading") or 0
        hdg_diff = abs(bearing - hdg)
        if hdg_diff > 180:
            hdg_diff = 360 - hdg_diff

        if vrate < -2 and hdg_diff < 45 and alt < 15000:
            eta = estimate_eta(aircraft, ap_id)
            results.append({
                "type": "ARR", "airport": ap_id,
                "callsign": aircraft.get("callsign", "N/A"),
                "dist_nm": round(dist_nm, 1), "alt_ft": alt,
                "eta_min": eta, "vrate": vrate, "icao24": icao24,
            })
        elif vrate > 3 and dist_nm < 30 and alt < 10000:
            results.append({
                "type": "DEP", "airport": ap_id,
                "callsign": aircraft.get("callsign", "N/A"),
                "dist_nm": round(dist_nm, 1), "alt_ft": alt,
                "eta_min": None, "vrate": vrate, "icao24": icao24,
            })
    return results


# ─── GENERADOR HTML ─────────────────────────────────────────────
def generate_atc_dashboard(aircraft_state):
    now_str  = datetime.now(timezone.utc).strftime("%H:%M:%S")
    date_str = datetime.now(timezone.utc).strftime("%d%b%Y").upper()
    total    = len(aircraft_state)
    print(f"total aircraft: {aircraft_state}")
    airborne = sum(1 for a in aircraft_state.values() if not a.get("on_ground"))

    # Source counts
    src_counts = defaultdict(int)
    for a in aircraft_state.values():
        src   = a.get("position_source", -1)
        label = {0: "ADS-B", 1: "ASTERIX", 2: "MLAT"}.get(src, "UNKNOWN")
        src_counts[label] += 1
    adsb_count    = src_counts.get("ADS-B", 0)
    mlat_count    = src_counts.get("MLAT", 0)
    asterix_count = src_counts.get("ASTERIX", 0)
    unk_count     = src_counts.get("UNKNOWN", 0)
    total_src     = total or 1

    mode_s_count = sum(1 for a in aircraft_state.values() if a.get("position_source") == 0)
    mode_c_count = sum(1 for a in aircraft_state.values() if a.get("position_source") in (1, 2))
    spi_count    = sum(1 for a in aircraft_state.values() if a.get("spi", False))

    # Squawk
    squawk_breakdown = defaultdict(int)
    special_squawks  = []
    for a in aircraft_state.values():
        sq = a.get("squawk", "") or ""
        if sq:
            squawk_breakdown[sq] += 1
        if sq in ("7700", "7600", "7500", "7000"):
            special_squawks.append({
                "callsign": a.get("callsign", "N/A"), "squawk": sq,
                "meaning":  {"7700": "EMERGENCY", "7600": "RADIO FAIL",
                              "7500": "HIJACK",    "7000": "VFR"}.get(sq, sq),
                "alt_ft":   a.get("altitude_ft"),
            })
    top_squawks = sorted(
        [(sq, cnt) for sq, cnt in squawk_breakdown.items()
         if sq not in ("7700","7600","7500","7000")],
        key=lambda x: -x[1]
    )[:10]

    # Baro/Geo discrepancy
    discrepancies = []
    for icao, a in aircraft_state.items():
        baro = a.get("baro_altitude_m")
        geo  = a.get("geo_altitude_m") or a.get("geo_altitude")
        if baro and geo and not a.get("on_ground"):
            diff_ft = (baro - geo) * 3.28084
            if abs(diff_ft) > 200:
                discrepancies.append({
                    "callsign": a.get("callsign", "N/A"),
                    "baro_ft":  round(baro * 3.28084),
                    "geo_ft":   round(geo  * 3.28084),
                    "diff_ft":  round(diff_ft),
                    "severity": "HIGH" if abs(diff_ft) > 800 else "MED",
                })
    discrepancies.sort(key=lambda x: -abs(x["diff_ft"]))
    discrepancies = discrepancies[:8]

    # Sector load
    sector_counts = defaultdict(int)
    for a in aircraft_state.values():
        lat, lon = a.get("latitude"), a.get("longitude")
        if lat and lon and not a.get("on_ground"):
            sector_counts[get_sector(lat, lon)] += 1

    # Conflicts
    conflicts = detect_conflicts(aircraft_state)

    # Stats
    alts = [a["altitude_ft"] for a in aircraft_state.values()
            if a.get("altitude_ft") and not a.get("on_ground")]
    vels = [a["velocity_kmh"] for a in aircraft_state.values()
            if a.get("velocity_kmh") and not a.get("on_ground")]
    avg_alt = int(sum(alts)/len(alts)) if alts else 0
    avg_vel = int(sum(vels)/len(vels)) if vels else 0
    max_alt = max(alts) if alts else 0

    # FL distribution
    fl_dist = defaultdict(int)
    for alt in alts:
        fl = (alt // 5000) * 5000
        fl_dist[fl] += 1

    # ── HTML BLOCKS ─────────────────────────────────────────────
    conflicts_html = ""
    for cf in conflicts:
        c = "#ff4b78" if cf["severity"] == "CRITICAL" else "#ffd166"
        conflicts_html += f"""
        <div class="conflict-row" style="border-left:3px solid {c};">
            <span class="cf-badge" style="color:{c};">{cf['severity'][:4]}</span>
            <span class="cf-cs">{cf['cs1'][:8]}</span>
            <span class="cf-sep">⟷</span>
            <span class="cf-cs">{cf['cs2'][:8]}</span>
            <span class="cf-data">{cf['dist_nm']}NM / {cf['vert_ft']}ft</span>
        </div>"""
    if not conflicts_html:
        conflicts_html = '<div class="no-alert">NO CONFLICTS DETECTED — SEP OK</div>'

    special_sq_html = ""
    sq_colors = {"7700":"#ff4b78","7600":"#ffd166","7500":"#a855f7","7000":"#00ff85"}
    for s in special_squawks[:6]:
        c = sq_colors.get(s["squawk"], "#c8d8e8")
        special_sq_html += f"""
        <div class="sq-special-row">
            <span class="sq-code" style="color:{c};">{s['squawk']}</span>
            <span class="sq-meaning" style="color:{c};">{s['meaning']}</span>
            <span class="sq-cs">{s['callsign'][:10]}</span>
            <span class="sq-alt">{s['alt_ft'] or '?'} ft</span>
        </div>"""
    if not special_sq_html:
        special_sq_html = '<div class="no-alert">SQUAWK 7000 VFR / NO EMERGENCY</div>'

    disc_html = ""
    for d in discrepancies[:6]:
        arrow = "▲" if d["diff_ft"] > 0 else "▼"
        c = "#ff4b78" if d["severity"] == "HIGH" else "#ffd166"
        disc_html += f"""
        <div class="disc-row">
            <span class="disc-cs">{d['callsign'][:9]}</span>
            <span class="disc-baro">{d['baro_ft']:,}</span>
            <span class="disc-sep">/</span>
            <span class="disc-geo">{d['geo_ft']:,}</span>
            <span class="disc-diff" style="color:{c};">{arrow}{abs(d['diff_ft'])}ft</span>
        </div>"""
    if not disc_html:
        disc_html = '<div class="no-alert">BARO/GEO WITHIN TOLERANCE</div>'

    sector_html = ""
    max_load = 25
    for sec_name, count in sorted(sector_counts.items(), key=lambda x: -x[1]):
        pct   = min(count / max_load * 100, 100)
        color = "#ff4b78" if count > 20 else "#ffd166" if count > 12 else "#00ff85"
        sector_html += f"""
        <div class="sector-row">
            <span class="sector-name">{sec_name[:12]}</span>
            <div class="sector-bar-wrap">
                <div class="sector-bar" style="width:{pct:.0f}%;background:{color};"></div>
            </div>
            <span class="sector-count" style="color:{color};">{count:02d}</span>
        </div>"""
    if not sector_html:
        sector_html = '<div class="no-alert">NO SECTOR DATA</div>'

    top_sq_html = ""
    for sq, cnt in top_squawks[:8]:
        top_sq_html += f"""
        <div class="sq-row">
            <span class="sq-num">{sq}</span>
            <div class="sq-bar-wrap"><div class="sq-bar" style="width:{min(cnt/5*100,100):.0f}%;"></div></div>
            <span class="sq-cnt">{cnt}</span>
        </div>"""

    # Source donut SVG
    def svg_arc(cx, cy, r, start_pct, end_pct, color):
        if end_pct <= start_pct:
            return ""
        sa = start_pct * 360 - 90
        ea = end_pct   * 360 - 90
        x1 = cx + r * math.cos(math.radians(sa))
        y1 = cy + r * math.sin(math.radians(sa))
        x2 = cx + r * math.cos(math.radians(ea))
        y2 = cy + r * math.sin(math.radians(ea))
        lg = 1 if (end_pct - start_pct) > 0.5 else 0
        return f'<path d="M {cx} {cy} L {x1:.1f} {y1:.1f} A {r} {r} 0 {lg} 1 {x2:.1f} {y2:.1f} Z" fill="{color}" opacity="0.85"/>'

    src_arcs  = ""
    src_data  = [("ADS-B", adsb_count, "#00ff85"), ("MLAT", mlat_count, "#5b9df5"),
                 ("ASTERIX", asterix_count, "#ffd166"), ("UNKN", unk_count, "#3d4d5e")]
    accum = 0
    for label, cnt, color in src_data:
        frac = cnt / total_src
        src_arcs += svg_arc(60, 60, 50, accum, accum + frac, color)
        accum += frac
    src_arcs += (f'<circle cx="60" cy="60" r="30" fill="#111822"/>'
                 f'<text x="60" y="57" text-anchor="middle" fill="#00d4ff" '
                 f'font-family="\'Space Mono\',monospace" font-size="14" font-weight="700">{total}</text>'
                 f'<text x="60" y="70" text-anchor="middle" fill="#3d4d5e" '
                 f'font-family="\'Space Mono\',monospace" font-size="8">ACFT</text>')

    src_legend = ""
    for label, cnt, color in src_data:
        pct = round(cnt / total_src * 100, 1)
        src_legend += (f'<div class="src-leg"><span class="src-dot" style="background:{color};'
                       f'box-shadow:0 0 5px {color};"></span>'
                       f'<span class="src-lbl">{label}</span>'
                       f'<span class="src-cnt" style="color:{color};">{cnt} ({pct}%)</span></div>')

    fl_html = ""
    max_fl = max(fl_dist.values()) if fl_dist else 1
    for fl in sorted(fl_dist.keys(), reverse=True):
        cnt = fl_dist[fl]
        pct = cnt / max_fl * 100
        fl_label = f"FL{fl//100:03d}+" if fl > 0 else "GND  "
        c = "#00ff85" if fl > 25000 else "#5b9df5" if fl > 10000 else "#ffd166"
        fl_html += f"""
        <div class="fl-row">
            <span class="fl-label">{fl_label}</span>
            <div class="fl-bar-wrap"><div class="fl-bar" style="width:{pct:.0f}%;background:{c};"></div></div>
            <span class="fl-cnt">{cnt}</span>
        </div>"""

    # Arrivals / Departures
    arr_html = ""
    for f in list(arrival_list)[:8]:
        eta_str = f"{int(f['eta_min'])}'" if f.get("eta_min") else "---"
        arr_html += f"""
        <div class="mov-row arr">
            <span class="mov-type arr-badge">ARR</span>
            <span class="mov-cs">{f['callsign'][:9]}</span>
            <span class="mov-ap">{f['airport']}</span>
            <span class="mov-dist">{f['dist_nm']}NM</span>
            <span class="mov-alt">{int(f['alt_ft']):,}ft</span>
            <span class="mov-eta" style="color:var(--cyan);">ETA {eta_str}</span>
        </div>"""
    if not arr_html:
        arr_html = '<div class="no-alert">NO ARRIVALS TRACKED</div>'

    dep_html = ""
    for f in list(departure_list)[:8]:
        fpm = int(f['vrate'] * 196.85)
        dep_html += f"""
        <div class="mov-row dep">
            <span class="mov-type dep-badge">DEP</span>
            <span class="mov-cs">{f['callsign'][:9]}</span>
            <span class="mov-ap">{f['airport']}</span>
            <span class="mov-dist">{f['dist_nm']}NM</span>
            <span class="mov-alt">{int(f['alt_ft']):,}ft</span>
            <span class="mov-eta" style="color:var(--gold);">+{fpm}fpm</span>
        </div>"""
    if not dep_html:
        dep_html = '<div class="no-alert">NO DEPARTURES TRACKED</div>'

    airports_html = ""
    for ap_id, ap in list(AIRPORTS.items())[:6]:
        airports_html += f"""
        <div class="ap-row">
            <span class="ap-id">{ap_id}</span>
            <div class="ap-info">
                <div class="ap-name">{ap['name'][:28]}</div>
                <div class="ap-detail">RWY {ap['rwy']} · ELEV {ap['elev_ft']}ft · {ap['lat']:.2f}/{ap['lon']:.2f}</div>
            </div>
        </div>"""

    conflict_color = "var(--red)" if conflicts else "var(--cyan)"

    # ══════════════════════════════════════════════════════════════
    # HTML FINAL
    # ══════════════════════════════════════════════════════════════
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta http-equiv="refresh" content="{REFRESH_INTERVAL}">
<title>ATC RADAR — FIR BUENOS AIRES</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Space+Mono:ital,wght@0,400;0,700;1,400&family=Barlow+Condensed:wght@300;400;600;700;900&display=swap" rel="stylesheet">
<style>
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
:root{{
    --bg:#080c10; --panel:#0d1117; --panel2:#111822;
    --border:rgba(0,212,255,0.12); --border2:rgba(255,255,255,0.06);
    --cyan:#00d4ff; --green:#00ff85; --red:#ff4b78;
    --gold:#ffd166; --purple:#a855f7; --blue:#5b9df5;
    --muted:#3d4d5e; --text:#c8d8e8; --text-dim:#6b7f91;
    --font-mono:'Space Mono',monospace;
    --font-ui:'Barlow Condensed',sans-serif;
    --radius:10px;
    --transition:0.4s ease;
    --amber:var(--gold);
    --white:#e8f4ff;
    --green2:#00cc88;
    --green3:rgba(0,212,255,0.2);
    --font:var(--font-mono);
    --font-hd:var(--font-ui);
}}
html,body{{background:var(--bg);color:var(--text);font-family:var(--font-ui);font-size:14px;line-height:1.4;min-height:100vh;overflow-x:hidden;}}
body::before{{content:'';position:fixed;top:0;left:0;right:0;bottom:0;background:repeating-linear-gradient(0deg,transparent,transparent 2px,rgba(0,0,0,0.025) 2px,rgba(0,0,0,0.025) 4px);pointer-events:none;z-index:9999;}}
body::after{{display:none;}}
.header{{background:linear-gradient(90deg,#0a0f15 0%,#0d1420 100%);border-bottom:1px solid var(--border);padding:0 22px;height:54px;display:flex;align-items:center;justify-content:space-between;position:sticky;top:0;z-index:200;}}
.hdr-left{{display:flex;align-items:center;gap:20px;}}
.hdr-title{{font-family:var(--font-ui);font-size:17px;font-weight:900;letter-spacing:3px;text-transform:uppercase;color:#fff;}}
.hdr-sub{{font-family:var(--font-mono);font-size:8px;color:var(--text-dim);letter-spacing:2px;margin-top:2px;}}
.hdr-center{{display:flex;gap:18px;align-items:center;}}
.hdr-stat{{text-align:center;}}
.hdr-stat-val{{font-family:var(--font-ui);font-size:20px;font-weight:900;color:var(--cyan);line-height:1;}}
.hdr-stat-lbl{{font-family:var(--font-mono);font-size:7px;color:var(--text-dim);letter-spacing:2px;margin-top:2px;}}
.hdr-right{{text-align:right;}}
.hdr-time{{font-family:var(--font-mono);font-size:18px;font-weight:700;color:var(--cyan);letter-spacing:3px;}}
.hdr-date{{font-family:var(--font-mono);font-size:9px;color:var(--text-dim);letter-spacing:2px;margin-top:1px;}}
.live-dot{{width:8px;height:8px;border-radius:50%;background:var(--green);box-shadow:0 0 8px var(--green);animation:pulse-dot 1.4s ease-in-out infinite;display:inline-block;margin-right:6px;}}
@keyframes pulse-dot{{0%,100%{{opacity:1;transform:scale(1);}}50%{{opacity:0.5;transform:scale(1.3);}}}}
@keyframes blink{{0%,100%{{opacity:1}}50%{{opacity:0.1}}}}
.hdr-div{{width:1px;height:32px;background:var(--border);}}
.shell{{display:grid;grid-template-columns:255px 1fr 255px;gap:1px;background:var(--border2);min-height:calc(100vh - 54px);}}
.col{{background:var(--bg);}}
.col-mid{{display:flex;flex-direction:column;gap:1px;background:var(--border2);}}
.panel{{background:var(--panel);padding:14px;border-bottom:1px solid var(--border2);}}
.panel:last-child{{border-bottom:none;}}
.panel-hdr{{font-family:var(--font-mono);font-size:9px;letter-spacing:3px;text-transform:uppercase;color:var(--cyan);opacity:0.7;margin-bottom:11px;display:flex;align-items:center;gap:8px;border-bottom:none;padding-bottom:0;}}
.panel-hdr::before{{content:'';display:inline-block;width:3px;height:12px;background:var(--cyan);border-radius:2px;opacity:1;}}
.panel-hdr.red{{color:var(--red);opacity:1;}}.panel-hdr.red::before{{background:var(--red);}}
.panel-hdr.amber{{color:var(--gold);opacity:1;}}.panel-hdr.amber::before{{background:var(--gold);}}
.panel-hdr.blue{{color:var(--blue);opacity:1;}}.panel-hdr.blue::before{{background:var(--blue);}}
.xpdr-grid{{display:grid;grid-template-columns:1fr 1fr;gap:7px;}}
.xpdr-card{{background:var(--panel2);border:1px solid var(--border2);border-radius:var(--radius);padding:9px 11px;position:relative;overflow:hidden;}}
.xpdr-card::before{{content:'';position:absolute;top:0;left:0;right:0;height:2px;}}
.xpdr-card.mode-s::before{{background:var(--cyan);}}.xpdr-card.mode-c::before{{background:var(--blue);}}
.xpdr-card.ads-b::before{{background:#00ffcc;}}.xpdr-card.mode-a::before{{background:var(--gold);}}
.xpdr-label{{font-family:var(--font-mono);font-size:8px;letter-spacing:2px;color:var(--text-dim);text-transform:uppercase;margin-bottom:4px;}}
.xpdr-val{{font-family:var(--font-ui);font-size:24px;font-weight:900;line-height:1;}}
.xpdr-card.mode-s .xpdr-val{{color:var(--cyan);}}.xpdr-card.mode-c .xpdr-val{{color:var(--blue);}}
.xpdr-card.ads-b .xpdr-val{{color:#00ffcc;}}.xpdr-card.mode-a .xpdr-val{{color:var(--gold);}}
.xpdr-sub{{font-family:var(--font-mono);font-size:8px;color:var(--text-dim);margin-top:3px;letter-spacing:1px;}}
.mode-line{{display:flex;justify-content:space-between;padding:3px 0;border-bottom:1px solid var(--border2);font-family:var(--font-mono);font-size:8px;}}
.mode-line:last-child{{border-bottom:none;}}.mode-name{{color:var(--text-dim);letter-spacing:1px;}}.mode-info{{color:var(--text);}}
.conflict-row{{display:grid;grid-template-columns:38px 70px 20px 70px 1fr;align-items:center;gap:5px;padding:5px 6px;margin-bottom:4px;background:rgba(255,75,120,0.05);border:1px solid rgba(255,75,120,0.1);border-radius:6px;font-family:var(--font-mono);font-size:10px;}}
.cf-badge{{font-size:8px;font-weight:700;letter-spacing:1px;}}.cf-cs{{color:var(--text);font-size:10px;letter-spacing:1px;}}
.cf-sep{{color:var(--text-dim);text-align:center;}}.cf-data{{font-size:9px;color:var(--gold);text-align:right;}}
.source-wrap{{display:flex;align-items:center;gap:14px;}}
.src-leg{{display:flex;align-items:center;gap:5px;margin-bottom:5px;font-family:var(--font-mono);font-size:9px;}}
.src-dot{{width:7px;height:7px;border-radius:50%;flex-shrink:0;}}.src-lbl{{color:var(--text-dim);width:48px;letter-spacing:1px;}}.src-cnt{{font-size:9px;}}
.sq-special-row{{display:grid;grid-template-columns:50px 78px 76px 1fr;align-items:center;gap:5px;padding:5px 5px;margin-bottom:4px;background:rgba(255,75,120,0.05);border:1px solid rgba(255,75,120,0.1);border-radius:6px;font-family:var(--font-mono);font-size:10px;}}
.sq-code{{font-family:var(--font-ui);font-size:16px;font-weight:900;letter-spacing:2px;}}
.sq-meaning{{font-size:7px;letter-spacing:2px;}}.sq-cs{{color:var(--text);}}.sq-alt{{color:var(--text-dim);font-size:9px;text-align:right;}}
.sq-row{{display:grid;grid-template-columns:50px 1fr 26px;align-items:center;gap:5px;margin-bottom:4px;}}
.sq-num{{font-family:var(--font-mono);color:var(--cyan);font-size:10px;letter-spacing:2px;}}
.sq-bar-wrap{{height:4px;background:rgba(255,255,255,0.05);border-radius:2px;}}.sq-bar{{height:100%;border-radius:2px;background:var(--cyan);opacity:0.4;}}
.sq-cnt{{font-family:var(--font-mono);color:var(--text-dim);font-size:9px;text-align:right;}}
.disc-row{{display:grid;grid-template-columns:68px 50px 10px 50px 54px;align-items:center;gap:4px;padding:4px 0;border-bottom:1px solid var(--border2);font-family:var(--font-mono);font-size:9px;}}
.disc-row:last-child{{border-bottom:none;}}.disc-cs{{color:var(--text);font-size:10px;}}
.disc-baro{{color:var(--blue);text-align:right;}}.disc-geo{{color:var(--gold);}}
.disc-sep{{color:var(--text-dim);text-align:center;}}.disc-diff{{font-size:10px;font-weight:700;text-align:right;}}
.sector-row{{display:grid;grid-template-columns:88px 1fr 26px;align-items:center;gap:6px;margin-bottom:6px;}}
.sector-name{{font-family:var(--font-mono);font-size:8px;color:var(--text-dim);letter-spacing:1px;text-transform:uppercase;}}
.sector-bar-wrap{{height:6px;background:rgba(255,255,255,0.05);border-radius:3px;overflow:hidden;}}
.sector-bar{{height:100%;border-radius:3px;transition:width 0.6s ease;}}.sector-count{{font-family:var(--font-ui);font-size:11px;font-weight:700;text-align:right;}}
.fl-row{{display:grid;grid-template-columns:50px 1fr 26px;align-items:center;gap:5px;margin-bottom:4px;}}
.fl-label{{font-family:var(--font-mono);font-size:8px;color:var(--text-dim);letter-spacing:1px;}}
.fl-bar-wrap{{height:5px;background:rgba(255,255,255,0.05);border-radius:3px;overflow:hidden;}}.fl-bar{{height:100%;border-radius:3px;}}
.fl-cnt{{font-family:var(--font-mono);font-size:9px;color:var(--cyan);text-align:right;}}
.mov-row{{display:grid;grid-template-columns:30px 70px 38px 42px 56px 1fr;align-items:center;gap:4px;padding:4px 5px;margin-bottom:3px;font-family:var(--font-mono);font-size:9px;border-radius:6px;}}
.mov-row.arr{{background:rgba(0,212,255,0.04);border-left:2px solid rgba(0,212,255,0.3);}}
.mov-row.dep{{background:rgba(255,209,102,0.04);border-left:2px solid rgba(255,209,102,0.3);}}
.arr-badge{{background:rgba(0,212,255,0.15);color:var(--cyan);font-size:7px;letter-spacing:1px;padding:1px 3px;border-radius:3px;text-align:center;}}
.dep-badge{{background:rgba(255,209,102,0.15);color:var(--gold);font-size:7px;letter-spacing:1px;padding:1px 3px;border-radius:3px;text-align:center;}}
.mov-cs{{color:var(--text);letter-spacing:1px;}}.mov-ap{{color:var(--blue);font-size:8px;}}.mov-dist{{color:var(--text-dim);}}.mov-alt{{color:var(--cyan);}}.mov-eta{{font-size:9px;font-weight:700;}}
.stats-grid{{display:grid;grid-template-columns:1fr 1fr;gap:6px;}}
.stat-item{{background:var(--panel2);border:1px solid var(--border2);padding:7px 8px;border-radius:var(--radius);}}
.stat-lbl{{font-family:var(--font-mono);font-size:7px;color:var(--text-dim);letter-spacing:2px;text-transform:uppercase;margin-bottom:3px;}}
.stat-val{{font-family:var(--font-ui);font-size:18px;font-weight:900;color:var(--cyan);}}.stat-sub{{font-family:var(--font-mono);font-size:8px;color:var(--text-dim);margin-top:1px;}}
.ap-row{{display:flex;gap:8px;padding:5px 0;border-bottom:1px solid var(--border2);align-items:flex-start;}}
.ap-row:last-child{{border-bottom:none;}}
.ap-id{{font-family:var(--font-ui);font-size:14px;font-weight:900;color:var(--cyan);min-width:38px;}}
.ap-info{{flex:1;}}.ap-name{{font-family:var(--font-mono);font-size:9px;color:var(--text);letter-spacing:0.5px;}}
.ap-detail{{font-family:var(--font-mono);font-size:8px;color:var(--text-dim);margin-top:1px;letter-spacing:0.5px;}}
.no-alert{{font-family:var(--font-mono);font-size:9px;color:var(--text-dim);letter-spacing:2px;padding:8px 0;text-align:center;text-transform:uppercase;}}
@keyframes sweep{{from{{transform:rotate(0deg)}}to{{transform:rotate(360deg)}}}}
.radar-sweep{{animation:sweep 4s linear infinite;transform-origin:center;}}
.footer{{background:var(--panel);border-top:1px solid var(--border2);padding:8px 22px;font-family:var(--font-mono);font-size:8px;color:var(--muted);letter-spacing:1px;display:flex;justify-content:space-between;}}
</style>
</head>
<body>

<header class="header">
    <div class="hdr-left">
        <div><span class="live-dot"></span><span style="font-size:9px;color:var(--green2);letter-spacing:2px;">LIVE</span></div>
        <div class="hdr-div"></div>
        <div>
            <div class="hdr-title">FIR BUENOS AIRES — ATC MONITOR</div>
            <div class="hdr-sub">OPENSKY NETWORK → KAFKA STREAM → AVIONICS DASHBOARD v2.0</div>
        </div>
    </div>
    <div class="hdr-center">
        <div class="hdr-stat"><div class="hdr-stat-val">{total}</div><div class="hdr-stat-lbl">TOTAL ACFT</div></div>
        <div class="hdr-div"></div>
        <div class="hdr-stat"><div class="hdr-stat-val" style="color:#00ffcc;">{adsb_count}</div><div class="hdr-stat-lbl">ADS-B</div></div>
        <div class="hdr-div"></div>
        <div class="hdr-stat"><div class="hdr-stat-val" style="color:var(--blue);">{mlat_count}</div><div class="hdr-stat-lbl">MLAT</div></div>
        <div class="hdr-div"></div>
        <div class="hdr-stat"><div class="hdr-stat-val" style="color:var(--gold);">{avg_alt:,}</div><div class="hdr-stat-lbl">AVG ALT FT</div></div>
        <div class="hdr-div"></div>
        <div class="hdr-stat"><div class="hdr-stat-val" style="color:{conflict_color};">{len(conflicts)}</div><div class="hdr-stat-lbl">CONFLICTS</div></div>
        <div class="hdr-div"></div>
        <div class="hdr-stat"><div class="hdr-stat-val" style="color:var(--cyan);">{len(list(arrival_list))}</div><div class="hdr-stat-lbl">ARRIVALS</div></div>
        <div class="hdr-div"></div>
        <div class="hdr-stat"><div class="hdr-stat-val" style="color:var(--gold);">{len(list(departure_list))}</div><div class="hdr-stat-lbl">DEPARTURES</div></div>
    </div>
    <div class="hdr-right">
        <div class="hdr-time">{now_str}Z</div>
        <div class="hdr-date">{date_str} · ZULU TIME</div>
    </div>
</header>

<div class="shell">

    <!-- COLUMNA IZQUIERDA -->
    <div class="col">
        <div class="panel">
            <div class="panel-hdr">TRANSPONDER MONITOR — MODE A/C/S/ADS-B</div>
            <div class="xpdr-grid">
                <div class="xpdr-card mode-s">
                    <div class="xpdr-label">MODE S / ADS-B</div>
                    <div class="xpdr-val">{mode_s_count}</div>
                    <div class="xpdr-sub">ICAO24 · 1090MHz ES</div>
                </div>
                <div class="xpdr-card mode-c">
                    <div class="xpdr-label">MLAT TRACKED</div>
                    <div class="xpdr-val">{mlat_count}</div>
                    <div class="xpdr-sub">MULTILATERATION</div>
                </div>
                <div class="xpdr-card ads-b">
                    <div class="xpdr-label">ADS-B EXT DO-260B</div>
                    <div class="xpdr-val">{adsb_count}</div>
                    <div class="xpdr-sub">GPS POS · NUC≥7</div>
                </div>
                <div class="xpdr-card mode-a">
                    <div class="xpdr-label">ASTERIX CAT048</div>
                    <div class="xpdr-val">{asterix_count}</div>
                    <div class="xpdr-sub">SSR SECUNDARIO</div>
                </div>
            </div>
            <div style="margin-top:9px;border-top:1px solid var(--border2);padding-top:7px;">
                <div class="mode-line"><span class="mode-name">FREQ TX</span><span class="mode-info">1090 MHz ES / 978 UAT</span></div>
                <div class="mode-line"><span class="mode-name">PROTOCOLO</span><span class="mode-info">DO-260B / RTCA SC-186</span></div>
                <div class="mode-line"><span class="mode-name">SPI ACTIVE</span><span class="mode-info" style="color:{'var(--gold)' if spi_count else 'var(--text-dim)'};">{spi_count} ACFT IDENT</span></div>
                <div class="mode-line"><span class="mode-name">ON GROUND</span><span class="mode-info">{total - airborne} / {total} ACFT</span></div>
            </div>
        </div>

        <div class="panel">
            <div class="panel-hdr red">SQUAWK — EMERGENCY / SPECIAL CODES</div>
            {special_sq_html}
        </div>

        <div class="panel">
            <div class="panel-hdr">SQUAWK DISTRIBUTION — MODE A CODES</div>
            {top_sq_html if top_sq_html else '<div class="no-alert">NO SQUAWK DATA</div>'}
        </div>

        <div class="panel">
            <div class="panel-hdr amber">ALTIMETRÍA BARO vs GEO — QNH DISCREPANCY CHECK</div>
            <div style="display:flex;justify-content:space-between;font-family:var(--font-mono);font-size:7px;color:var(--text-dim);margin-bottom:5px;letter-spacing:1px;">
                <span>CALLSIGN</span><span>BARO</span><span>/</span><span>GEO</span><span>Δ ERR</span>
            </div>
            {disc_html}
        </div>
    </div>

    <!-- COLUMNA CENTRO -->
    <div class="col-mid">

        <div class="panel">
            <div class="panel-hdr red">CONFLICT ALERT — TCAS SEPARATION MONITOR</div>
            <div style="font-family:var(--font-mono);font-size:8px;color:var(--text-dim);letter-spacing:1px;margin-bottom:6px;display:flex;gap:16px;">
                <span>CRITICAL: &lt;3NM / &lt;500ft</span>
                <span>WARNING: &lt;5NM / &lt;1000ft</span>
                <span style="color:var(--text-dim);">ICAO DOC 4444 PANS-ATM</span>
            </div>
            {conflicts_html}
        </div>

        <div class="panel">
            <div class="panel-hdr blue">ARRIVALS &amp; DEPARTURES — SAEZ · SABE TRAFFIC FEED</div>
            <div style="display:flex;justify-content:space-between;font-family:var(--font-mono);font-size:7px;color:var(--text-dim);letter-spacing:2px;margin-bottom:5px;text-transform:uppercase;border-bottom:1px solid var(--border2);padding-bottom:4px;">
                <span>TYPE</span><span>CALLSIGN</span><span>ARPT</span><span>DIST</span><span>ALT</span><span>ETA/CLB</span>
            </div>
            {arr_html}
            <div style="height:5px;border-top:1px dashed rgba(255,255,255,0.06);margin:6px 0 6px;"></div>
            {dep_html}
        </div>

        <div class="panel">
            <div class="panel-hdr">FLIGHT LEVEL DISTRIBUTION — RVSM FL290–FL410</div>
            <div style="font-family:var(--font-mono);font-size:7px;color:var(--text-dim);margin-bottom:6px;letter-spacing:1px;">ICAO ANNEX 2 · RVSM 1000ft VERTICAL SEP · STANDARD 2000ft BELOW FL290</div>
            {fl_html if fl_html else '<div class="no-alert">NO ALTITUDE DATA</div>'}
        </div>

        <div class="panel" style="flex:1;">
            <div class="panel-hdr">SURVEILLANCE SOURCE MIX — DATALINK ANALYSIS</div>
            <div class="source-wrap">
                <svg viewBox="0 0 120 120" width="110" height="110" style="flex-shrink:0;">
                    <circle cx="60" cy="60" r="57" fill="none" stroke="rgba(0,212,255,0.06)" stroke-width="1"/>
                    <circle cx="60" cy="60" r="40" fill="none" stroke="rgba(0,212,255,0.04)" stroke-width="1"/>
                    <circle cx="60" cy="60" r="20" fill="none" stroke="rgba(0,212,255,0.04)" stroke-width="1"/>
                    <g class="radar-sweep"><line x1="60" y1="60" x2="117" y2="60" stroke="rgba(0,212,255,0.15)" stroke-width="1.5"/></g>
                    {src_arcs}
                </svg>
                <div style="flex:1;">{src_legend}</div>
            </div>
            <div style="margin-top:8px;font-family:var(--font-mono);font-size:8px;color:var(--text-dim);letter-spacing:1px;border-top:1px solid var(--border2);padding-top:7px;line-height:1.9;">
                <div>ADS-B   · DO-260B · 1090MHz Extended Squitter · GPS position</div>
                <div>MLAT    · TDOA multilateration · ≥4 ground receivers</div>
                <div>ASTERIX · CAT048 radar plots · SSR Mode C/S returns</div>
            </div>
        </div>

    </div>

    <!-- COLUMNA DERECHA -->
    <div class="col">

        <div class="panel">
            <div class="panel-hdr">FIR SECTOR LOAD — BUENOS AIRES ACC</div>
            <div style="font-family:var(--font-mono);font-size:7px;color:var(--text-dim);margin-bottom:7px;letter-spacing:1px;">MAX CAPACITY: 25 ACFT/SECTOR · ICAO DOC 9426</div>
            {sector_html}
            <div style="margin-top:7px;font-family:var(--font-mono);font-size:8px;color:var(--text-dim);display:flex;gap:10px;border-top:1px solid var(--border2);padding-top:6px;flex-wrap:wrap;">
                <span style="color:var(--cyan);">■ NORMAL &lt;12</span>
                <span style="color:var(--gold);">■ HIGH 12–20</span>
                <span style="color:var(--red);">■ OVERLOAD &gt;20</span>
            </div>
        </div>

        <div class="panel">
            <div class="panel-hdr">SYSTEM STATUS — ATC DATA PIPELINE</div>
            <div class="stats-grid">
                <div class="stat-item">
                    <div class="stat-lbl">TRACKED</div>
                    <div class="stat-val">{total}</div>
                    <div class="stat-sub">AERONAVES</div>
                </div>
                <div class="stat-item">
                    <div class="stat-lbl">AIRBORNE</div>
                    <div class="stat-val" style="color:#00ffcc;">{airborne}</div>
                    <div class="stat-sub">EN VUELO</div>
                </div>
                <div class="stat-item">
                    <div class="stat-lbl">MAX FL</div>
                    <div class="stat-val" style="font-size:13px;color:var(--blue);">FL{int(max_alt/100):03d}</div>
                    <div class="stat-sub">{int(max_alt):,} FT</div>
                </div>
                <div class="stat-item">
                    <div class="stat-lbl">AVG GS</div>
                    <div class="stat-val" style="font-size:13px;color:var(--gold);">{avg_vel}</div>
                    <div class="stat-sub">KM/H</div>
                </div>
            </div>
            <div style="margin-top:9px;font-family:var(--font-mono);font-size:8px;color:var(--text-dim);letter-spacing:1px;line-height:2;border-top:1px solid var(--border2);padding-top:7px;">
                <div>KAFKA TOPIC  <span style="color:var(--cyan);">opensky-argentina</span></div>
                <div>POLL INT     <span style="color:var(--text);">10s anónimo</span></div>
                <div>REFRESH      <span style="color:var(--text);">{REFRESH_INTERVAL}s meta</span></div>
                <div>BARO DISC    <span style="color:{'var(--gold)' if discrepancies else 'var(--green)'};">{len(discrepancies)} alertas QNH</span></div>
                <div>SPI IDENT    <span style="color:{'var(--gold)' if spi_count else 'var(--green)'};">{spi_count} activos</span></div>
                <div>ARRIVALS     <span style="color:var(--cyan);">{len(list(arrival_list))} en approach</span></div>
                <div>DEPARTURES   <span style="color:var(--gold);">{len(list(departure_list))} en climb</span></div>
            </div>
        </div>

        <div class="panel" style="flex:1;">
            <div class="panel-hdr">AEROPUERTOS — FIR BUENOS AIRES</div>
            {airports_html}
        </div>

    </div>

</div>

<footer class="footer">
    <span>FIR BUENOS AIRES ATC MONITOR · ICAO: SAEF · ACC: CENTRO DE CONTROL EZEIZA · ANAC ARGENTINA</span>
    <span>DATOS: OPENSKY NETWORK CC BY 4.0 · NON-COMMERCIAL · opensky-network.org</span>
</footer>
</body>
</html>"""


# ══════════════════════════════════════════════════════════════
# CONSUMER LOOP
# ══════════════════════════════════════════════════════════════
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers="my-producer-estimacionderiego1.i.aivencloud.com:24197",
    security_protocol="SSL",
    ssl_cafile="credentials/ca.pem",
    ssl_certfile="credentials/service.cert",
    ssl_keyfile="credentials/service.key",
    auto_offset_reset="latest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    consumer_timeout_ms=1000,
)

print("╔══════════════════════════════════════════════════╗")
print("║  🗼  CONSUMER ATC — FIR BUENOS AIRES MONITOR    ║")
print("╚══════════════════════════════════════════════════╝")
print(f"📥 Topic:   {KAFKA_TOPIC}")
print(f"📁 Output:  {DASHBOARD_FILE}")
print(f"🔄 Refresh: cada {REFRESH_INTERVAL}s\n")
print("Abrí dashboard_atc.html en tu browser.\n")

while True:
    try:
        messages = consumer.poll(timeout_ms=REFRESH_INTERVAL * 1000, max_records=500)

        for tp, records in messages.items():
            for record in records:
                
                data = record.value
                print(data.get("icao24"))
                icao = data.get("icao24")
                if not icao:
                    continue

                aircraft_state[icao] = data

                lat, lon = data.get("latitude"), data.get("longitude")
                if lat and lon:
                    aircraft_trails[icao].append({"lat": lat, "lon": lon})

                # Arrivals / Departures
                movs = classify_arrival_departure(data, icao)
                for mov in movs:
                    cs = mov["callsign"]
                    if mov["type"] == "ARR":
                        found = False
                        for i, m in enumerate(list(arrival_list)):
                            if m["callsign"] == cs:
                                arrival_list[i] = mov
                                found = True
                                break
                        if not found:
                            arrival_list.appendleft(mov)
                    elif mov["type"] == "DEP":
                        if not any(m["callsign"] == cs for m in departure_list):
                            departure_list.appendleft(mov)

                # Squawk emergencia
                sq = data.get("squawk", "")
                if sq in ("7700", "7600", "7500"):
                    ts = datetime.now().strftime("%H:%M:%S")
                    entry = f"{ts} SQUAWK {sq} → {data.get('callsign','N/A')}"
                    if not any(data.get('callsign','') in e for e in squawk_log):
                        squawk_log.appendleft(entry)
                        alert_log.appendleft(entry)

        # # Limpiar viejos
        # cutoff = time.time() - 300
        # stale = [icao for icao, a in aircraft_state.items()
        #          if a.get("server_time", 0) < cutoff]
        # for icao in stale:
        #     del aircraft_state[icao]
        #     aircraft_trails.pop(icao, None)

        # # Limpiar arrivals aterrizados
        # fresh_arr = deque(
        #     [f for f in arrival_list if f.get("alt_ft", 9999) > 500],
        #     maxlen=12
        # )
        # arrival_list.clear()
        # arrival_list.extend(fresh_arr)

        # Generar dashboard
        html = generate_atc_dashboard(aircraft_state)
        with open(DASHBOARD_FILE, "w", encoding="utf-8") as f:
            f.write(html)

        ts_now = datetime.now().strftime("%H:%M:%S")
        cf_now = detect_conflicts(aircraft_state)
        print(f"✅ {ts_now}Z | ACFT:{len(aircraft_state):3d} | ARR:{len(list(arrival_list)):2d} | DEP:{len(list(departure_list)):2d} | CONFLICTS:{len(cf_now):2d} | ATC OK")

    except KeyboardInterrupt:
        print("\n⏹  Consumer ATC detenido.")
        consumer.close()
        break
    except Exception as e:
        print(f"❌ Error: {e}")
        time.sleep(2)
