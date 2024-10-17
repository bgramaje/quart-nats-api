from quart import Quart, render_template, websocket, request, jsonify
from nats.aio.client import Client as NATS
from dotenv import load_dotenv
import os
import uuid

load_dotenv()

app = Quart(__name__)
nc = NATS()


async def initialize_nats():
    """
    function to initialize nats connection
    """
    try:
        await nc.connect(servers=[os.getenv("NATS_HOST")])
        print(f"connected to nats with client_id {nc.client_id}")
    except Exception as e:
        print(f"[nats] connection error: {e}")


@app.before_serving
async def startup():
    """
    startup function to call all functions that must be executed before api started
    """
    await initialize_nats()


@app.after_serving
async def shutdown():
    """
    function to be called after killing api process. In this case closing the nats broker.
    """
    await nc.close()


@app.route("/", methods=["GET"])
async def index():
    """
    index route for basic information
    """
    response = {"status": 200, "message": "welcome to subsync api"}
    return jsonify(response), 200


@app.route("/health", methods=["GET"])
async def health_check():
    """
    health check function for calling the service via docker
    """
    return jsonify({"status": "200", "health": "OK"}), 200


@app.route("/job", methods=["POST"])
async def submit_job():
    """
    function to submit a job to nats
    """
    job_data = await request.get_json()

    if not job_data:
        return jsonify({"error": "No job data provided"}), 400

    job_uuid = str(uuid.uuid4())
    try:
        await nc.publish("job.notifications", job_uuid.encode())
        print(f"Published job UUID: {job_uuid}")
        return jsonify(
            {"status": 200, "job": job_uuid, "message": "Job submitted successfully!"}
        )
    except Exception as e:
        return jsonify({"error": f"Failed to publish job: {str(e)}"}), 500


if __name__ == "__main__":
    """
    main.py code
    """
    app.run(host="0.0.0.0", port=5000)
