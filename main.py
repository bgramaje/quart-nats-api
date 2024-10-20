from quart import Quart, render_template, websocket, request, jsonify
from nats.aio.client import Client as NATS
from dotenv import load_dotenv
from pymongo import MongoClient
import gridfs

import os
import uuid
import json

load_dotenv()

app = Quart(__name__)
nc = NATS()


# MongoDB connection
client = MongoClient('mongodb://localhost:27017/')  # Replace with your MongoDB URI
db = client['subsync']  # Replace with your database name
fs = gridfs.GridFS(db)


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
async def create_job():
    """
    function to create a job. Should be like your new project in your workspace
    """
    try:
        job_uuid = str(uuid.uuid4())
        return jsonify({ "status": 200, "job_id": job_uuid }), 200
    except Exception as e:
        return jsonify({"error": f"Failed to publish job: {str(e)}"}), 500

@app.route('/job/<job_id>/upload', methods=['POST'])
async def upload_video(job_id):
    try:
        if 'file' not in request.files:
            return jsonify({"error": "No file provided"}), 400

        file = request.files['file']
        
        # Validate file type (optional)
        if not file.filename.endswith(('.mp4', '.mov', '.avi')):
            return jsonify({"error": "File type not supported"}), 400

        # Store the video in GridFS
        video_id = fs.put(file.stream, filename=file.filename)

        return jsonify({"message": "Video uploaded successfully", "job_id": job_id, "video_id": str(video_id)}), 201
    except Exception as e:
        return {'error': str(e)}, 500
    

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
        nats_message = { "status": 200, "job_id": job_uuid }
        await nc.publish("job.notification", json.dumps(nats_message).encode())
        print(f"Published job UUID: {job_uuid}")
        return jsonify(nats_message), 200
    except Exception as e:
        return jsonify({"error": f"Failed to publish job: {str(e)}"}), 500


if __name__ == "__main__":
    """
    main.py code
    """
    app.run(host="0.0.0.0", port=5500)
