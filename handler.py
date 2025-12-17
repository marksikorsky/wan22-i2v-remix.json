import os
import time
import json
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
R2_PUBLIC_BASE = os.environ.get("R2_PUBLIC_BASE")  # optional (recommended)


# ================= VALIDATION =================

def _require_env():
    missing = [k for k, v in {
        "R2_ENDPOINT": R2_ENDPOINT,
        "R2_BUCKET": R2_BUCKET,
        "R2_ACCESS_KEY": R2_ACCESS_KEY,
        "R2_SECRET_KEY": R2_SECRET_KEY,
    }.items() if not v]
    if missing:
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")


# ================= COMFY HELPERS =================

def _wait_for_comfy(timeout_s: int = 900) -> None:
    """
    Wait until ComfyUI HTTP server is reachable.
    For heavy nodes/models cold-start can be several minutes.
    """
    t0 = time.time()
    last_err = None
    while True:
        try:
            r = requests.get(f"{COMFY_URL}/system_stats", timeout=3)
            if r.status_code == 200:
                return
            last_err = f"status={r.status_code}"
        except Exception as e:
            last_err = str(e)

        if time.time() - t0 > timeout_s:
            raise TimeoutError(f"ComfyUI not ready after {timeout_s}s: {last_err}")

        time.sleep(2)


def _load_workflow_any() -> dict:
    with open(WORKFLOW_PATH, "r") as f:
        return json.load(f)


def _normalize_workflow(wf_any: dict) -> dict:
    """
    Accepts:
      1) {"input":{"workflow":{...}}}  (runpod wizard / example-request style)
      2) {"workflow":{...}}
      3) {...}                          (pure ComfyUI prompt graph)
    Returns: pure prompt graph dict.
    """
    if isinstance(wf_any, dict):
        if "input" in wf_any and isinstance(wf_any["input"], dict) and "workflow" in wf_any["input"]:
            inner = wf_any["input"]["workflow"]
            if isinstance(inner, dict):
                return inner
        if "workflow" in wf_any and isinstance(wf_any["workflow"], dict):
            return wf_any["workflow"]
    return wf_any


def _submit_to_comfy(prompt_graph: dict) -> str:
    """
    POST /prompt expects {"prompt": <graph>, "client_id": "..."}.
    We include client_id to avoid 400 on some builds.
    """
    payload = {
        "prompt": prompt_graph,
        "client_id": str(uuid.uuid4()),
    }

    r = requests.post(f"{COMFY_URL}/prompt", json=payload, timeout=60)

    # If 400/500, include body in error for debugging
    if not r.ok:
        body = r.text
        raise RuntimeError(f"ComfyUI /prompt failed: HTTP {r.status_code} body={body[:2000]}")

    data = r.json()
    if "prompt_id" not in data:
        raise RuntimeError(f"ComfyUI /prompt unexpected response: {data}")
    return data["prompt_id"]


def _wait_history(prompt_id: str, timeout_s: int = 7200) -> dict:
    t0 = time.time()
    while True:
        try:
            r = requests.get(f"{COMFY_URL}/history/{prompt_id}", timeout=30)
            if r.ok:
                hist = r.json()
                if isinstance(hist, dict) and prompt_id in hist:
                    return hist[prompt_id]
        except Exception:
            pass

        if time.time() - t0 > timeout_s:
            raise TimeoutError("Generation timeout waiting for history")

        time.sleep(2)


def _extract_first_video_filename(history_item: dict) -> str | None:
    outputs = history_item.get("outputs", {})
    if not isinstance(outputs, dict):
        return None

    # Prefer explicit "videos" list
    for node_out in outputs.values():
        if isinstance(node_out, dict) and "videos" in node_out and node_out["videos"]:
            v0 = node_out["videos"][0]
            if isinstance(v0, dict) and "filename" in v0:
                return v0["filename"]

    # Fallback: sometimes saved as "gifs" or "images" etc.
    return None


# ================= R2 HELPERS =================

def _r2_client():
    return boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
        region_name="auto",
    )


def _upload_to_r2(local_path: str, key: str) -> str:
    s3 = _r2_client()
    extra = {}

    # Try set content-type if mp4
    if local_path.lower().endswith(".mp4") or key.lower().endswith(".mp4"):
        extra["ContentType"] = "video/mp4"

    if extra:
        s3.upload_file(local_path, R2_BUCKET, key, ExtraArgs=extra)
    else:
        s3.upload_file(local_path, R2_BUCKET, key)

    if R2_PUBLIC_BASE:
        return f"{R2_PUBLIC_BASE.rstrip('/')}/{key}"

    # Non-pretty fallback (works if bucket is public / dev URL)
    return f"{R2_ENDPOINT.rstrip('/')}/{R2_BUCKET}/{key}"


# ================= MAIN HANDLER =================

def handler(event):
    _require_env()

    inp = event.get("input", {}) or {}
    prompt_text = inp.get("prompt", "")
    image_url = inp.get("image_url")

    if not image_url:
        return {"error": "image_url is required"}

    # 1) Wait for ComfyUI
    _wait_for_comfy(timeout_s=900)

    # 2) Load + normalize workflow graph
    wf_any = _load_workflow_any()
    wf = _normalize_workflow(wf_any)

    if not isinstance(wf, dict):
        return {"error": "workflow.json is not a dict prompt graph"}

    # 3) Download image into Comfy input dir
    image_name = f"input_{uuid.uuid4().hex}.png"
    image_path = os.path.join(COMFY_INPUT_DIR, image_name)

    try:
        with requests.get(image_url, stream=True, timeout=120) as r:
            r.raise_for_status()
            with open(image_path, "wb") as f:
                shutil.copyfileobj(r.raw, f)
    except Exception as e:
        return {"error": f"image_download_failed: {str(e)}"}

    # 4) Patch workflow: node 148 LoadImage, node 134 Positive prompt
    #    You said IDs are stable.
    try:
        if "148" not in wf:
            return {"error": "workflow_patch_failed: node 148 not found"}
        if "134" not in wf:
            return {"error": "workflow_patch_failed: node 134 not found"}

        if "inputs" not in wf["148"] or not isinstance(wf["148"]["inputs"], dict):
            return {"error": "workflow_patch_failed: node 148 has no inputs dict"}
        if "inputs" not in wf["134"] or not isinstance(wf["134"]["inputs"], dict):
            return {"error": "workflow_patch_failed: node 134 has no inputs dict"}

        # LoadImage expects 'image' filename in /comfyui/input
        wf["148"]["inputs"]["image"] = image_name

        # CLIPTextEncode expects 'text'
        wf["134"]["inputs"]["text"] = prompt_text
    except Exception as e:
        return {"error": f"workflow_patch_failed: {str(e)}"}

    # 5) Submit to ComfyUI
    try:
        prompt_id = _submit_to_comfy(wf)
    except Exception as e:
        return {"error": f"comfy_submit_failed: {str(e)}"}

    # 6) Wait for result
    try:
        hist_item = _wait_history(prompt_id, timeout_s=7200)
    except Exception as e:
        return {"error": f"comfy_history_failed: {str(e)}", "prompt_id": prompt_id}

    # 7) Extract video filename
    video_filename = _extract_first_video_filename(hist_item)
    if not video_filename:
        # Return a bit of history for debugging (trim)
        return {
            "error": "no_video_in_history_outputs",
            "prompt_id": prompt_id,
            "history_keys": list((hist_item.get("outputs") or {}).keys())[:50],
        }

    local_video_path = os.path.join(COMFY_OUTPUT_DIR, video_filename)
    if not os.path.exists(local_video_path):
        return {
            "error": "video_file_not_found_on_disk",
            "prompt_id": prompt_id,
            "expected_path": local_video_path,
        }

    # 8) Upload to R2
    r2_key = f"videos/{uuid.uuid4().hex}.mp4"
    try:
        public_url = _upload_to_r2(local_video_path, r2_key)
    except Exception as e:
        return {"error": f"r2_upload_failed: {str(e)}", "prompt_id": prompt_id}

    return {
        "status": "success",
        "prompt_id": prompt_id,
        "video_url": public_url,
    }


runpod.serverless.start({"handler": handler})
