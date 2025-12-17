import os
import time
import json
import uuid
import requests
import boto3
import runpod

# ---------------- CONFIG ----------------

COMFY_URL = os.environ.get("COMFY_URL", "http://127.0.0.1:8188")
WORKFLOW_PATH = os.environ.get("WORKFLOW_PATH", "/comfyui/workflow.json")
COMFY_INPUT_DIR = os.environ.get("COMFY_INPUT_DIR", "/comfyui/input")
COMFY_OUTPUT_DIR = os.environ.get("COMFY_OUTPUT_DIR", "/comfyui/output")

R2_ENDPOINT = os.environ.get("R2_ENDPOINT")
R2_BUCKET = os.environ.get("R2_BUCKET")
R2_ACCESS_KEY = os.environ.get("R2_ACCESS_KEY")
R2_SECRET_KEY = os.environ.get("R2_SECRET_KEY")
R2_PUBLIC_BASE = os.environ.get("R2_PUBLIC_BASE")  # optional

# ---------------- HELPERS ----------------

def wait_for_comfyui(timeout=600):
    start = time.time()
    while time.time() - start < timeout:
        try:
            r = requests.get(f"{COMFY_URL}/system_stats", timeout=2)
            if r.status_code == 200:
                return
        except Exception:
            pass
        time.sleep(2)
    raise RuntimeError("ComfyUI did not start in time")

def load_workflow():
    with open(WORKFLOW_PATH, "r") as f:
        return json.load(f)

def upload_to_r2(local_path, key):
    s3 = boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
        region_name="auto",
    )
    s3.upload_file(local_path, R2_BUCKET, key)
    if R2_PUBLIC_BASE:
        return f"{R2_PUBLIC_BASE}/{key}"
    return f"{R2_ENDPOINT}/{R2_BUCKET}/{key}"

# ---------------- HANDLER ----------------

def handler(event):
    input_data = event.get("input", {})

    prompt = input_data.get("prompt", "")
    image_url = input_data.get("image_url")

    if not image_url:
        return {"error": "image_url is required"}

    # 1️⃣ Ждём ComfyUI
    wait_for_comfyui()

    # 2️⃣ Скачиваем изображение
    image_name = f"input_{uuid.uuid4().hex}.png"
    image_path = os.path.join(COMFY_INPUT_DIR, image_name)

    r = requests.get(image_url, timeout=60)
    r.raise_for_status()
    with open(image_path, "wb") as f:
        f.write(r.content)

    # 3️⃣ Загружаем workflow
    workflow = load_workflow()

    # 🔥 ВАЖНО: патчим КОНКРЕТНЫЕ НОДЫ
    workflow["148"]["inputs"]["image"] = image_name        # LoadImage
    workflow["134"]["inputs"]["text"] = prompt              # Positive prompt

    # 4️⃣ Отправляем в ComfyUI
    submit = requests.post(
        f"{COMFY_URL}/prompt",
        json={"prompt": workflow},
        timeout=10,
    )
    submit.raise_for_status()
    prompt_id = submit.json()["prompt_id"]

    # 5️⃣ Ждём результат
    while True:
        time.sleep(3)
        hist = requests.get(f"{COMFY_URL}/history/{prompt_id}").json()
        if prompt_id in hist:
            break

    outputs = hist[prompt_id]["outputs"]

    # 6️⃣ Ищем видео
    video_file = None
    for node in outputs.values():
        if "videos" in node:
            video_file = node["videos"][0]["filename"]
            break

    if not video_file:
        return {"error": "video not found in outputs"}

    local_video = os.path.join(COMFY_OUTPUT_DIR, video_file)
    r2_key = f"videos/{uuid.uuid4().hex}.mp4"

    video_url = upload_to_r2(local_video, r2_key)

    return {
        "status": "completed",
        "video_url": video_url,
    }

# ---------------- RUNPOD ----------------

runpod.serverless.start({"handler": handler})
