import os
import time
import json
import uuid
import glob
import mimetypes
from pathlib import Path

import boto3
import requests
import runpod


# ================= CONFIG =================

COMFY_URL = os.environ.get("COMFY_URL", "http://127.0.0.1:8188")
WORKFLOW_PATH = os.environ.get("WORKFLOW_PATH", "/comfyui/workflow.json")
COMFY_OUTPUT_DIR = os.environ.get("COMFY_OUTPUT_DIR", "/comfyui/output")

# R2 (S3-compatible API)
R2_ENDPOINT = os.environ["R2_ENDPOINT"]
R2_BUCKET = os.environ["R2_BUCKET"]
R2_ACCESS_KEY = os.environ["R2_ACCESS_KEY"]
R2_SECRET_KEY = os.environ["R2_SECRET_KEY"]

# PUBLIC BASE (CUSTOM DOMAIN)
# example: https://cdn.yourdomain.com
R2_PUBLIC_BASE = os.environ["R2_PUBLIC_BASE"].rstrip("/")

COMFY_BOOT_TIMEOUT = 180
JOB_TIMEOUT = 60 * 60  # 1 hour


# ================= HELPERS =================

def wait_comfy():
    start = time.time()
    while time.time() - start < COMFY_BOOT_TIMEOUT:
        try:
            r = requests.get(f"{COMFY_URL}/system_stats", timeout=5)
            if r.status_code == 200:
                return
        except Exception:
            pass
        time.sleep(2)
    raise RuntimeError("ComfyUI did not start")


def load_workflow():
    with open(WORKFLOW_PATH, "r") as f:
        data = json.load(f)

    # unwrap if needed
    if "input" in data and "workflow" in data["input"]:
        return data["input"]["workflow"]

    return data


def patch_workflow(workflow, prompt=None, image_url=None):
    # prompt → node 134
    if prompt is not None:
        workflow["134"]["inputs"]["text"] = prompt

    # image → node 148 (LoadImage)
    if image_url:
        input_dir = Path("/comfyui/input")
        input_dir.mkdir(parents=True, exist_ok=True)

        ext = Path(image_url.split("?")[0]).suffix or ".png"
        fname = f"input_{uuid.uuid4().hex}{ext}"
        dst = input_dir / fname

        r = requests.get(image_url, timeout=60)
        r.raise_for_status()
        dst.write_bytes(r.content)

        workflow["148"]["inputs"]["image"] = fname


def submit_prompt(workflow):
    r = requests.post(
        f"{COMFY_URL}/prompt",
        json={"prompt": workflow},
        timeout=60,
    )
    if r.status_code != 200:
        raise RuntimeError(f"/prompt failed: {r.text}")
    return r.json()["prompt_id"]


def wait_history(prompt_id):
    start = time.time()
    while time.time() - start < JOB_TIMEOUT:
        r = requests.get(f"{COMFY_URL}/history/{prompt_id}", timeout=30)
        if r.status_code == 200:
            data = r.json()
            item = data.get(prompt_id) if isinstance(data, dict) else data
            if item and ("outputs" in item or item.get("status", {}).get("completed")):
                return item
        time.sleep(2)
    raise RuntimeError("Timeout waiting for history")


def extract_video_from_history(history):
    outputs = history.get("outputs", {})
    exts = {".mp4", ".webm", ".mov", ".mkv", ".gif"}

    def walk(v):
        if isinstance(v, dict):
            for x in v.values():
                yield from walk(x)
        elif isinstance(v, list):
            for x in v:
                yield from walk(x)
        elif isinstance(v, str):
            yield v

    for out in outputs.values():
        for v in walk(out):
            p = Path(v)
            if p.suffix.lower() in exts and p.exists():
                return str(p)

    return None


def find_latest_video():
    exts = ("*.mp4", "*.webm", "*.mov", "*.mkv", "*.gif")
    files = []
    for e in exts:
        files += glob.glob(f"{COMFY_OUTPUT_DIR}/**/{e}", recursive=True)
    if not files:
        return None
    files.sort(key=os.path.getmtime, reverse=True)
    return files[0]


def r2_client():
    return boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
        region_name="auto",
    )


def upload_to_r2(local_path):
    s3 = r2_client()
    ext = Path(local_path).suffix
    key = f"outputs/{uuid.uuid4().hex}{ext}"

    ctype, _ = mimetypes.guess_type(local_path)
    extra = {"ContentType": ctype} if ctype else {}

    s3.upload_file(local_path, R2_BUCKET, key, ExtraArgs=extra)

    return f"{R2_PUBLIC_BASE}/{key}"


# ================= HANDLER =================

def handler(job):
    wait_comfy()

    workflow = load_workflow()

    user_input = job.get("input", {})
    prompt = user_input.get("prompt")
    image_url = user_input.get("image_url")

    patch_workflow(workflow, prompt, image_url)

    prompt_id = submit_prompt(workflow)
    history = wait_history(prompt_id)

    video_path = extract_video_from_history(history)
    if not video_path:
        video_path = find_latest_video()

    if not video_path:
        raise RuntimeError("Video file not found")

    video_url = upload_to_r2(video_path)

    return {
        "prompt_id": prompt_id,
        "video_url": video_url,
    }


runpod.serverless.start({"handler": handler})
