import os
import json
import time
import uuid
from typing import Any, Dict, Optional, List

import requests
import boto3
import runpod


# -----------------------------
# Config
# -----------------------------
COMFY_URL = os.environ.get("COMFY_URL", "http://127.0.0.1:8188")
WORKFLOW_PATH = os.environ.get("WORKFLOW_PATH", "/comfyui/workflow.json")
COMFY_INPUT_DIR = os.environ.get("COMFY_INPUT_DIR", "/comfyui/input")
COMFY_OUTPUT_DIR = os.environ.get("COMFY_OUTPUT_DIR", "/comfyui/output")

R2_ENDPOINT = os.environ.get("R2_ENDPOINT")     # https://<accountid>.r2.cloudflarestorage.com
R2_BUCKET = os.environ.get("R2_BUCKET")
R2_ACCESS_KEY = os.environ.get("R2_ACCESS_KEY")
R2_SECRET_KEY = os.environ.get("R2_SECRET_KEY")

# Public base URL (optional, recommended). Example: https://pub-xxxx.r2.dev
# If set, we return: <R2_PUBLIC_BASE>/<bucket>/<key>
R2_PUBLIC_BASE = os.environ.get("R2_PUBLIC_BASE")


# -----------------------------
# Helpers
# -----------------------------
def _require_env() -> None:
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


def _download_image(image_url: str, out_path: str) -> None:
    r = requests.get(image_url, timeout=60, allow_redirects=True)
    r.raise_for_status()
    with open(out_path, "wb") as f:
        f.write(r.content)


def _load_workflow_file(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        wf = json.load(f)

    # Accept both:
    # 1) {"134": {...}, ...}
    # 2) {"input": {"workflow": {"134": {...}}}}
    if isinstance(wf, dict) and "input" in wf and isinstance(wf["input"], dict) and "workflow" in wf["input"]:
        wf = wf["input"]["workflow"]

    if not isinstance(wf, dict):
        raise RuntimeError("workflow json is not a dict (unexpected format)")

    return wf


def _find_node_id_by_predicate(wf: Dict[str, Any], predicate) -> Optional[str]:
    for nid, node in wf.items():
        if not isinstance(node, dict):
            continue
        if predicate(nid, node):
            return nid
    return None


def _set_prompt(wf: Dict[str, Any], prompt: str) -> str:
    # Try exact node id first (your expected)
    if "134" in wf and isinstance(wf["134"], dict):
        wf["134"].setdefault("inputs", {})["text"] = prompt
        return "134"

    # Fallback: find CLIPTextEncode with title containing "positive"
    def is_positive_clip(_nid, node):
        if node.get("class_type") != "CLIPTextEncode":
            return False
        title = ((node.get("_meta") or {}).get("title") or "").lower()
        return "positive" in title

    nid = _find_node_id_by_predicate(wf, is_positive_clip)
    if nid:
        wf[nid].setdefault("inputs", {})["text"] = prompt
        return nid

    # Last resort: any CLIPTextEncode (if titles differ)
    def any_clip(_nid, node):
        return node.get("class_type") == "CLIPTextEncode"

    nid = _find_node_id_by_predicate(wf, any_clip)
    if nid:
        wf[nid].setdefault("inputs", {})["text"] = prompt
        return nid

    raise RuntimeError("workflow_patch_failed: cannot find CLIPTextEncode node to set prompt")


def _set_image_filename(wf: Dict[str, Any], filename: str) -> str:
    # Try exact node id first (your expected)
    if "148" in wf and isinstance(wf["148"], dict):
        wf["148"].setdefault("inputs", {})["image"] = filename
        return "148"

    # Fallback: find LoadImage node
    def is_load_image(_nid, node):
        return node.get("class_type") == "LoadImage"

    nid = _find_node_id_by_predicate(wf, is_load_image)
    if nid:
        wf[nid].setdefault("inputs", {})["image"] = filename
        return nid

    raise RuntimeError("workflow_patch_failed: cannot find LoadImage node to set image filename")


def _submit_to_comfy(wf: Dict[str, Any]) -> str:
    r = requests.post(f"{COMFY_URL}/prompt", json={"prompt": wf}, timeout=60)
    r.raise_for_status()
    data = r.json()
    return data["prompt_id"]


def _wait_for_history(prompt_id: str, timeout_s: int = 3600) -> Dict[str, Any]:
    t0 = time.time()
    while True:
        r = requests.get(f"{COMFY_URL}/history/{prompt_id}", timeout=60)
        r.raise_for_status()
        h = r.json()
        if prompt_id in h:
            return h[prompt_id]
        if time.time() - t0 > timeout_s:
            raise TimeoutError("ComfyUI job timed out waiting for history")
        time.sleep(1.0)


def _extract_video_filenames(history: Dict[str, Any]) -> List[str]:
    files = []
    outputs = history.get("outputs") or {}
    if not isinstance(outputs, dict):
        return files

    for _nid, out in outputs.items():
        if not isinstance(out, dict):
            continue
        # common keys
        for key in ("videos", "gifs"):
            arr = out.get(key)
            if isinstance(arr, list):
                for item in arr:
                    if isinstance(item, dict) and item.get("filename"):
                        files.append(item["filename"])
    return files


def _resolve_output_path(filename: str) -> Optional[str]:
    candidates = []
    if os.path.isabs(filename):
        candidates.append(filename)

    candidates.append(os.path.join(COMFY_OUTPUT_DIR, filename))
    candidates.append(os.path.join(COMFY_OUTPUT_DIR, "video", filename))
    candidates.append(os.path.join(COMFY_OUTPUT_DIR, "videos", filename))

    for p in candidates:
        if os.path.exists(p):
            return p
    return None


def _upload_to_r2(local_path: str) -> str:
    s3 = _r2_client()
    key = f"videos/{uuid.uuid4().hex}.mp4"
    s3.upload_file(local_path, R2_BUCKET, key, ExtraArgs={"ContentType": "video/mp4"})

    if R2_PUBLIC_BASE:
        return f"{R2_PUBLIC_BASE.rstrip('/')}/{R2_BUCKET}/{key}"
    return f"{R2_ENDPOINT.rstrip('/')}/{R2_BUCKET}/{key}"


# -----------------------------
# RunPod handler
# -----------------------------
def handler(job):
    inp = job.get("input") or {}
    prompt = inp.get("prompt", "")
    image_url = inp.get("image_url")

    if not image_url:
        return {"error": "image_url is required"}

    try:
        _require_env()
    except Exception as e:
        return {"error": f"env_error: {str(e)}"}

    os.makedirs(COMFY_INPUT_DIR, exist_ok=True)

    # 1) download image into /comfyui/input
    img_name = f"{uuid.uuid4().hex}.png"
    img_path = os.path.join(COMFY_INPUT_DIR, img_name)
    try:
        _download_image(image_url, img_path)
    except Exception as e:
        return {"error": f"image_download_failed: {str(e)}"}

    # 2) load workflow (accept both wrapped/unwrapped)
    try:
        wf = _load_workflow_file(WORKFLOW_PATH)
    except Exception as e:
        return {"error": f"workflow_load_failed: {str(e)}", "workflow_path": WORKFLOW_PATH}

    # 3) patch prompt + image
    try:
        prompt_node = _set_prompt(wf, prompt)
        image_node = _set_image_filename(wf, img_name)
    except Exception as e:
        # add some debug context so you can see what file/keys were loaded
        keys_preview = list(wf.keys())[:15]
        return {
            "error": str(e),
            "workflow_path": WORKFLOW_PATH,
            "workflow_keys_preview": keys_preview,
        }

    # 4) run comfy
    try:
        pid = _submit_to_comfy(wf)
    except Exception as e:
        return {"error": f"comfy_submit_failed: {str(e)}", "prompt_node": prompt_node, "image_node": image_node}

    # 5) wait results
    try:
        hist = _wait_for_history(pid)
    except Exception as e:
        return {"error": f"comfy_wait_failed: {str(e)}", "prompt_id": pid}

    # 6) get saved video filename
    vids = _extract_video_filenames(hist)
    if not vids:
        return {"error": "no_video_in_history_outputs", "prompt_id": pid, "history": hist}

    rel = vids[-1]
    local_video = _resolve_output_path(rel)
    if not local_video:
        return {
            "error": "video_file_not_found_on_disk",
            "prompt_id": pid,
            "reported_filenames": vids,
            "output_dir": COMFY_OUTPUT_DIR,
        }

    # 7) upload to R2
    try:
        video_url = _upload_to_r2(local_video)
    except Exception as e:
        return {"error": f"r2_upload_failed: {str(e)}", "prompt_id": pid}

    return {
        "prompt_id": pid,
        "video_url": video_url,
        "debug": {
            "prompt_node": prompt_node,
            "image_node": image_node,
            "saved_filename": rel,
        }
    }


print("[boot] handler.py loaded", flush=True)
print("[boot] starting runpod serverless loop", flush=True)

runpod.serverless.start({"handler": handler})
