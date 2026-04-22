def kafka_produce(producer)
        OPENSKY_URL       = (
        "https://opensky-network.org/api/states/all"
        "?lamin=-55.0&lomin=-74.0&lamax=-21.0&lomax=-53.0"
    )
    while True:
        try:
            response = requests.get(OPENSKY_URL, timeout=15)
            raw = response.json()

            states       = raw.get("states") or [] # empty message type check
            server_time  = raw.get("time", int(time.time()))
            ingested_at  = datetime.now(timezone.utc).isoformat()

            for sv in states:
                icao24    = sv[0]
                callsign  = (sv[1] or "").strip()
                origin    = sv[2] or "Unknown"
                lon       = sv[5]
                lat       = sv[6]
                baro_alt  = sv[7]
                on_ground = sv[8]
                velocity  = sv[9]
                heading   = sv[10]
                vrate     = sv[11]
                squawk    = sv[14] or ""
                pos_src   = sv[16]

                if lat is None or lon is None:
                    continue

                aircraft_type = classify_aircraft(callsign, squawk, baro_alt, velocity)
                anomaly       = detect_anomaly({
                    "baro_altitude": baro_alt, "velocity": velocity,
                    "vertical_rate": vrate, "on_ground": on_ground, "squawk": squawk,
                })

                # Zona AMBA (radio ~150km de Ezeiza)
                in_amba = False
                if lat and lon:
                    dlat = lat - (-34.8222)
                    dlon = lon - (-58.5358)
                    in_amba = ((dlat * 111) ** 2 + (dlon * 85) ** 2) ** 0.5 < 150

                msg = {
                    "icao24":           icao24,
                    "callsign":         callsign or "N/A",
                    "origin_country":   origin,
                    "latitude":         round(lat, 4),
                    "longitude":        round(lon, 4),
                    "baro_altitude_m":  round(baro_alt, 1) if baro_alt else None,
                    "altitude_ft":      round(baro_alt * 3.28084) if baro_alt else None,
                    "on_ground":        on_ground,
                    "velocity_ms":      round(velocity, 1) if velocity else None,
                    "velocity_kmh":     round(velocity * 3.6, 1) if velocity else None,
                    "heading":          round(heading, 1) if heading else None,
                    "vertical_rate_ms": round(vrate, 2) if vrate else None,
                    "aircraft_type":    aircraft_type,
                    "squawk":           squawk,
                    "position_source":  pos_src,
                    "in_amba":          in_amba,
                    "anomaly":          anomaly,
                    "server_time":      server_time,
                    "ingested_at":      ingested_at,
                }
                print(msg)
                producer.send(KAFKA_TOPIC, msg)

            producer.flush()

        except requests.exceptions.RequestException as e:
            print(f"⚠️  Error de red: {e}. Reintentando en {POLL_INTERVAL_SEC}s...")
        except Exception as e:
            print(f"❌ Error inesperado: {e}")

        time.sleep(POLL_INTERVAL_SEC)