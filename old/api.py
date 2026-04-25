def start_api(sse_queue, state_lock, shared_state, sqt):

    import json, queue, logging
    from flask import Flask, Response, jsonify, send_from_directory, request
    app = Flask(__name__, static_folder=".")

    # ─── SSE Endpoint ────────────────────────────────────────────────────
    @app.route("/stream")
    def stream():
        """
        Server-Sent Events endpoint.
        El browser se conecta UNA vez y recibe eventos en push.
        No hay polling, no hay meta-refresh.
        """
        def event_generator():
            # Mandar heartbeat inmediato para confirmar conexión
            yield "event: connected\ndata: {\"status\": \"ok\"}\n\n"

            last_batch = -1
            while True:
                try:
                    # Esperar siguiente snapshot de Spark (max 30s)
                    snapshot = sse_queue.get(timeout=30)
                    bid = snapshot.get("batch_id", -1)

                    # No re-enviar el mismo batch
                    if bid == last_batch:
                        continue
                    last_batch = bid

                    # Formato SSE: "data: <json>\n\n"
                    payload = json.dumps(snapshot, default=str)
                    yield f"data: {payload}\n\n"

                except queue.Empty:
                    # Heartbeat cada 30s para mantener la conexión viva
                    yield "event: heartbeat\ndata: {}\n\n"
                except GeneratorExit:
                    break

        return Response(
            event_generator(),
            mimetype="text/event-stream",
            headers={
                "Cache-Control":    "no-cache",
                "X-Accel-Buffering": "no",   # Desactivar buffering en Nginx si hay proxy
                "Access-Control-Allow-Origin": "*",
            }
        )

    # ─── State snapshot REST (útil para debug) ───────────────────────────
    @app.route("/state")
    def state():
        with state_lock:
            return jsonify({
                "total_aircraft": len(shared_state["aircraft"]),
                "batch_count":    shared_state["batch_count"],
                "last_update":    shared_state["last_update"],
                "alert_count":    len(shared_state["alert_log"]),
                "type_counts":    dict(shared_state["type_counts"]),
            })

    # ─── Health check ────────────────────────────────────────────────────
    @app.route("/health")
    def health():
        return jsonify({
            "spark": "running" if spark_query.isActive else "stopped",
            "flask": "running",
            "queue_size": sse_queue.qsize(),
        })

    # ─── Servir el dashboard HTML estático ───────────────────────────────
    @app.route("/")
    def index():
        return send_from_directory(".", "dashboard.html")

    
    # ─── Iniciar servidor Flask ───────────────────────────────────────────
    app.run(host="localhost", port=5000, debug=False, use_reloader=False)