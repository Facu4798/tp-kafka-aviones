# ─── CLASIFICADOR DE AERONAVES ────────────────────────────────
def classify_aircraft(callsign: str, squawk: str, altitude: float, velocity: float) -> str:
    cs = (callsign or "").strip().upper()

    if squawk == "7700": return "EMERGENCY"
    if squawk == "7600": return "RADIO_FAIL"
    if squawk == "7500": return "HIJACK"

    COMMERCIAL_PREFIXES = {
        "ARG", "AEP", "AZU", "GLO", "TAM", "LAT",
        "UAL", "AAL", "DAL", "IBE", "VLG",
        "KLM", "AFR", "DLH", "BAW",
        "LAN", "JJP", "FLY", "SKY", "JOT",
    }
    CARGO_PREFIXES    = {"UPS", "FDX", "DHL", "MAS", "ABX", "GTI", "CKS", "SHY"}
    MILITARY_PREFIXES = {"FAA", "ARM", "ARA"}

    for p in MILITARY_PREFIXES:
        if cs.startswith(p): return "MILITARY"
    for p in CARGO_PREFIXES:
        if cs.startswith(p): return "CARGO"
    for p in COMMERCIAL_PREFIXES:
        if cs.startswith(p): return "COMMERCIAL"

    if altitude and velocity:
        if altitude > 7000 and velocity > 150:  return "COMMERCIAL"
        if altitude < 300  and velocity < 50:   return "HELICOPTER"
        if altitude < 3000 and velocity < 120:  return "GENERAL_AVIATION"

    if cs and len(cs) >= 5 and cs[:3].isalpha() and cs[3:].isdigit():
        return "COMMERCIAL"

    return "UNKNOWN"


def detect_anomaly(aircraft: dict) -> str | None:
    anomalies = []
    alt    = aircraft.get("baro_altitude")
    vel    = aircraft.get("velocity")
    vrate  = aircraft.get("vertical_rate")
    squawk = aircraft.get("squawk", "")

    if squawk in ("7700", "7600", "7500"):
        anomalies.append(f"SQUAWK_{squawk}")
    if alt is not None and 0 < alt < 150 and not aircraft.get("on_ground"):
        anomalies.append("VERY_LOW_ALTITUDE")
    if vrate is not None and abs(vrate) > 20:
        anomalies.append("RAPID_CLIMB" if vrate > 0 else "RAPID_DESCENT")
    if vel is not None and vel > 350:
        anomalies.append("VERY_HIGH_SPEED")

    return ",".join(anomalies) if anomalies else None

