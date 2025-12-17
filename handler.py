import os
import json
import time
import uuid
import requests

import boto3
import runpod

# ---- Config ----
COMFY_URL = os.environ.get("COMFY_URL", "http://127.0.0.1:8188")
WORKFLOW_PATH = os.environ.get("WORKFLOW_PATH", "/comfyui/workflow.json")
COMFY_INPUT_DIR = os.environ.get("COMFY_INPUT_DIR", "/comfyui/input")
COMFY_OUTPUT_DIR = os.environ.get("COMFY_OUTPUT_DIR", "/comfyui/output")

R2_ENDPOINT = os.environ.get("R2_ENDPOINT")   # S3 endpoint: https://<accountid>.r2.cloudflarestorage.com
R2_BUCKET = os.environ.get("R2_BUCKET")
R2_ACCESS_KEY = os.environ.get("R2_ACCESS_KEY")
R2_SECRET_KEY = os.environ.get("R2_SECRET_KEY")

# Optional: public URL base (r2.dev) for nicer links
# If not set, we fall back to R2_ENDPOINT-based URL.
R2_PUBLIC_BASE = os.environ.get("R2_PUBLIC_BASE")  # e.g. https://pub-xxxx.r2.dev


def _require_env():
    missing = [k for k, v in {
        "R2_ENDPOINT": R2_ENDPOINT,
        "R2_BUCKET": R2_BUCKET,
        "R2_ACCESS_KEY": R2_ACCESS_KEY,
        "R2_SECRET_KEY": R2_SECRET_KEY,
    }.items() if not v]
    if missing:
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")


def _r2_client():
    return boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
        region_name="auto",
    )


def _download_image(url: str, out_path: str) -> None:
    # simple, but robust enough
    r = requests.get(url, timeout=60, allow_redirects=True)
    r.raise_for_status()
    ct = (r.headers.get("content-type") or "").lower()
    if "image" not in ct:
        # Not fatal, but helpful to catch HTML/403 pages
        print(f"[warn] image_url content-type looks non-image: {ct}", flush=True)
    with open(out_path, "wb") as f:
        f.write(r.content)


def _submit_workflow(workflow: dict) -> str:
    r = requests.post(f"{COMFY_URL}/prompt", json={"prompt": workflow}, timeout=60)
    r.raise_for_status()
    data = r.json()
    return data["prompt_id"]


def _wait_history(prompt_id: str, timeout_s: int = 2400) -> dict:
    t0 = time.time()
    while True:
        r = requests.get(f"{COMFY_URL}/history/{prompt_id}", timeout=60)
        r.raise_for_status()
        h = r.json()
        if prompt_id in h:
            return h[prompt_id]
        if time.time() - t0 > timeout_s:
            raise TimeoutError("ComfyUI job timed out")
        time.sleep(1.0)


def _extract_saved_video_filenames(history: dict) -> list[str]:
    """
    Tries to find filenames from Comfy history outputs.
    SaveVideo usually reports something in outputs-><node_id>->videos.
    """
    files = []
    outputs = history.get("outputs", {}) or {}
    for _, out in outputs.items():
        if not isinstance(out, dict):
            continue
        for key in ("videos", "gifs"):
            arr = out.get(key)
            if isinstance(arr, list):
                for item in arr:
                    if isinstance(item, dict) and item.get("filename"):
                        files.append(item["filename"])
    return files


def _resolve_local_output_path(filename: str) -> str | None:
    """
    filename может быть:
    - "video/ComfyUI_00001.mp4"
    - "ComfyUI_00001.mp4"
    - иногда уже абсолютный (редко)
    """
    candidates = []

    if os.path.isabs(filename):
        candidates.append(filename)

    # /comfyui/output/<filename>
    candidates.append(os.path.join(COMFY_OUTPUT_DIR, filename))

    # если filename содержит подпапку video/..., ок
    # если нет — попробуем common subfolders
    candidates.append(os.path.join(COMFY_OUTPUT_DIR, "video", filename))
    candidates.append(os.path.join(COMFY_OUTPUT_DIR, "videos", filename))

    for p in candidates:
        if os.path.exists(p):
            return p
    return None


def _upload_mp4_to_r2(local_path: str) -> str:
    s3 = _r2_client()
    key = f"videos/{uuid.uuid4().hex}.mp4"
    s3.upload_file(local_path, R2_BUCKET, key, ExtraArgs={"ContentType": "video/mp4"})

    # Prefer public r2.dev base if provided
    if R2_PUBLIC_BASE:
        return f"{R2_PUBLIC_BASE.rstrip('/')}/{R2_BUCKET}/{key}"
    return f"{R2_ENDPOINT.rstrip('/')}/{R2_BUCKET}/{key}"


def handler(job):
    """
    Expects:
    {
      "input": {
        "prompt": "...",
        "image_url": "https://..."
      }
    }
    """
    try:
        _require_env()
    except Exception as e:
        return {"error": f"env_error: {str(e)}"}

    inp = job.get("input") or {}
    prompt = inp.get("prompt", "")
    image_url = inp.get("image_url")

    if not image_url:
        return {"error": "image_url is required"}

    print("[handler] job received", flush=True)
    print(f"[handler] image_url={image_url}", flush=True)

    # 1) download image into Comfy input
    os.makedirs(COMFY_INPUT_DIR, exist_ok=True)
    img_name = f"{uuid.uuid4().hex}.png"
    img_path = os.path.join(COMFY_INPUT_DIR, img_name)

    try:
        _download_image(image_url, img_path)
    except Exception as e:
        return {"error": f"image_download_failed: {str(e)}"}

    # 2) load workflow
    try:
        with open(WORKFLOW_PATH, "r", encoding="utf-8") as f:
            wf = json.load(f)
    except Exception as e:
        return {"error": f"workflow_load_failed: {str(e)}"}

    # 3) patch nodes (your known node ids)
    # prompt node 134
    try:
        wf["134"]["inputs"]["text"] = prompt
    except Exception:
        return {"error": "workflow_patch_failed: cannot set node 134 prompt"}

    # image node 148
    try:
        wf["148"]["inputs"]["image"] = img_name
    except Exception:
        return {"error": "workflow_patch_failed: cannot set node 148 image"}

    # fix fp16_fast crash (allow_fp16_accumulation)
    for nid in ("131", "132"):
        try:
            if nid in wf and "inputs" in wf[nid]:
                wf[nid]["inputs"]["precision"] = "fp16"
        except Exception:
            pass

    # 4) run comfy workflow
    try:
        pid = _submit_workflow(wf)
    except Exception as e:
        return {"error": f"comfy_submit_failed: {str(e)}"}

    print(f"[handler] comfy prompt_id={pid}", flush=True)

    try:
        hist = _wait_history(pid)
    except Exception as e:
        return {"prompt_id": pid, "error": f"comfy_wait_failed: {str(e)}"}

    # 5) locate output video
    vids = _extract_saved_video_filenames(hist)
    if not vids:
        return {"prompt_id": pid, "error": "no_video_in_history_outputs", "history": hist}

    # use the last one
    rel = vids[-1]
    local_path = _resolve_local_output_path(rel)
    if not local_path:
        ensure = {
            "prompt_id": pid,
            "error": "video_file_not_found_on_disk",
            "reported_filenames": vids,
            "output_dir": COMFY_OUTPUT_DIR,
        }
        return ensure

    print(f"[handler] local_video_path={local_path}", flush=True)

    # 6) upload to R2
    try:
        url = _upload_mp4_to_r2(local_path)
    except Exception as e:
        return {"prompt_id": pid, "error": f"r2_upload_failed: {str(e)}"}

    print(f"[handler] uploaded video_url={url}", flush=True)

    return {
        "prompt_id": pid,
        "video_url": url,
    }


print("[boot] handler.py loaded", flush=True)
print("[boot] starting runpod serverless loop", flush=True)

runpod.serverless.start({"handler": handler})
