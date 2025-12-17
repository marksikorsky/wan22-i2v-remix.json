import os
import json
import time
import uuid
import shutil
import requests
import boto3
import runpod

# ================= CONFIG =================

COMFY_URL = os.environ.get("COMFY_URL", "http://127.0.0.1:8188")
WORKFLOW_PATH = os.environ.get("WORKFLOW_PATH", "/comfyui/workflow.json")
COMFY_INPUT_DIR = os.environ.get("COMFY_INPUT_DIR", "/comfyui/input")
COMFY_OUTPUT_DIR = os.environ.get("COMFY_OUTPUT_DIR", "/comfyui/output")

R2_ENDPOINT = os.environ.get("R2_ENDPOINT")
R2_BUCKET = os.environ.get("R2_BUCKET")
R2_ACCESS_KEY = os.environ.get("R2_ACCESS_KEY")
R2_SECRET_KEY = os.environ.get("R2_SECRET_KEY")
R2_PUBLIC_BASE = os.environ.get("R2_PUBLIC_BASE")  # optional

# ================= VALIDATION =================

def _require_env():
    missing = []
    for k, v in {
        "R2_ENDPOINT": R2_ENDPOINT,
        "R2_BUCKET": R2_BUCKET,
        "R2_ACCESS_KEY": R2_ACCESS_KEY,
        "R2_SECRET_KEY": R2_SECRET_KEY,
    }.items():
        if not v:
            missing.append(k)
    if missing:
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")

# ================= COMFY HELPERS =================

def wait_for_comfy(timeout=300):
    start = time.time()
    while True:
        try:
            r = requests.get(f"{COMFY_URL}/system_stats", timeout=5)
            if r.status_code == 200:
                return
        except Exception:
            pass

        if time.time() - start > timeout:
            raise TimeoutError("ComfyUI did not start in time")

        time.sleep(1)

def submit_prompt(workflow):
    r = requests.post(
        f"{COMFY_URL}/prompt",
        json={"prompt": workflow},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["prompt_id"]

def wait_for_result(prompt_id, timeout=3600):
    start = time.time()
    while True:
        r = requests.get(f"{COMFY_URL}/history/{prompt_id}")
        if r.status_code == 200:
            data = r.json()
            if prompt_id in data:
                return data[prompt_id]

        if time.time() - start > timeout:
            raise TimeoutError("Generation timeout")

        time.sleep(2)

# ================= R2 =================

def r2_client():
    return boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
        region_name="auto",
    )

def upload_to_r2(local_path, key):
    s3 = r2_client()
    s3.upload_file(
        local_path,
        R2_BUCKET,
        key,
        ExtraArgs={"ContentType": "video/mp4"},
    )
    if R2_PUBLIC_BASE:
        return f"{R2_PUBLIC_BASE.rstrip('/')}/{key}"
    return f"{R2_ENDPOINT.rstrip('/')}/{R2_BUCKET}/{key}"

# ================= MAIN HANDLER =================

def handler(event):
    _require_env()

    inp = event.get("input", {})
    prompt_text = inp.get("prompt")
    image_url = inp.get("image_url")

    if not prompt_text or not image_url:
        return {"error": "prompt and image_url are required"}

    # --- wait for comfy ---
    wait_for_comfy()

    # --- load workflow ---
    with open(WORKFLOW_PATH, "r") as f:
        workflow = json.load(f)

    # --- download image ---
    image_name = f"input_{uuid.uuid4().hex}.png"
    image_path = os.path.join(COMFY_INPUT_DIR, image_name)

    with requests.get(image_url, stream=True, timeout=30) as r:
        r.raise_for_status()
        with open(image_path, "wb") as f:
            shutil.copyfileobj(r.raw, f)

    # --- PATCH WORKFLOW ---
    # Node IDs based on твоём JSON:
    # 134 = positive prompt
    # 137 = negative prompt
    # 148 = LoadImage

    workflow["134"]["inputs"]["text"] = prompt_text
    workflow["148"]["inputs"]["image"] = image_name

    # --- run comfy ---
    prompt_id = submit_prompt(workflow)
    result = wait_for_result(prompt_id)

    # --- find output video ---
    video_file = None
    for node in result["outputs"].values():
        if "videos" in node:
            video_file = node["videos"][0]["filename"]

    if not video_file:
        return {"error": "No video produced"}

    local_video_path = os.path.join(COMFY_OUTPUT_DIR, video_file)

    # --- upload ---
    r2_key = f"videos/{uuid.uuid4().hex}.mp4"
    public_url = upload_to_r2(local_video_path, r2_key)

    return {
        "status": "success",
        "video_url": public_url,
        "prompt_id": prompt_id,
    }

# ================= RUNPOD =================

runpod.serverless.start({"handler": handler})
