def start_api(sse_queue, state_lock, shared_state, sqt):

    import json, queue, logging
    from flask import Flask, Response, jsonify, send_from_directory, request
    app = Flask(__name__, static_folder=".")

    class State:
        def __init__(self):
            self.data = {}

    shared_state = State()
    # ─── SSE Endpoint ────────────────────────────────────────────────────
    @app.route("/post_message", methods=["POST"])
    def post_message(shared_state=shared_state):
        
        try:
            data = request.get_json()
        except Exception as e:
            return jsonify({"status": "error", "message": str(e)}), 500

        shared_state.data = data
        return jsonify({"status": "received"})
        
    @app.route("/state",methods=["GET"])
    def get_message():
        return jsonify(shared_state.data)

    # ─── Servir el dashboard HTML estático ───────────────────────────────
    @app.route("/")
    def index():
        return send_from_directory(".", "dashboard.html")

    @app.route("/dashboard2")
    def dashboard2():
        return send_from_directory(".", "dashboard_atc.html")
    # ─── Iniciar servidor Flask ───────────────────────────────────────────
    app.run(host="localhost", port=5000, debug=False, use_reloader=False)