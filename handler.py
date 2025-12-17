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


# ---------------- Config ----------------
COMFY_URL = os.environ.get("COMFY_URL", "http://127.0.0.1:8188")
WORKFLOW_PATH = os.environ.get("WORKFLOW_PATH", "/comfyui/workflow.json")
COMFY_OUTPUT_DIR = os.environ.get("COMFY_OUTPUT_DIR", "/comfyui/output")

# Cloudflare R2 (S3-compatible)
R2_ENDPOINT = os.environ.get("R2_ENDPOINT")  # e.g. https://<accountid>.r2.cloudflarestorage.com
R2_BUCKET = os.environ.get("R2_BUCKET")
R2_ACCESS_KEY = os.environ.get("R2_ACCESS_KEY")
R2_SECRET_KEY = os.environ.get("R2_SECRET_KEY")

# Optional: nicer public base (r2.dev or custom domain)
R2_PUBLIC_BASE = os.environ.get("R2_PUBLIC_BASE")  # e.g. https://pub-xxxx.r2.dev or https://cdn.yourdomain.com

# Timeouts
COMFY_BOOT_TIMEOUT_SEC = int(os.environ.get("COMFY_BOOT_TIMEOUT_SEC", "180"))
JOB_TIMEOUT_SEC = int(os.environ.get("JOB_TIMEOUT_SEC", str(60 * 30)))  # 30 min default


# ---------------- Helpers ----------------
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


def _wait_comfy_ready():
    t0 = time.time()
    last_err = None
    while time.time() - t0 < COMFY_BOOT_TIMEOUT_SEC:
        try:
            r = requests.get(f"{COMFY_URL}/system_stats", timeout=5)
            if r.status_code == 200:
                return
            last_err = f"status={r.status_code} body={r.text[:200]}"
        except Exception as e:
            last_err = str(e)
        time.sleep(2)
    raise RuntimeError(f"ComfyUI not ready after {COMFY_BOOT_TIMEOUT_SEC}s. Last error: {last_err}")


def _load_workflow():
    with open(WORKFLOW_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def _deep_set(obj, path, value):
    cur = obj
    for p in path[:-1]:
        cur = cur[p]
    cur[path[-1]] = value


def _apply_runtime_inputs(workflow: dict, prompt: str | None, image_url: str | None):
    """
    Под твою схему:
    - prompt -> node 134.inputs.text (CLIPTextEncode positive)
    - image_url -> node 148.inputs.image (LoadImage) НЕ подойдет напрямую, там имя файла.
      Поэтому обычно делают: скачиваем картинку в /comfyui/input и в ноду пишем filename.
    """
    # prompt
    if prompt is not None:
        # node id "134" -> inputs.text
        if "134" in workflow and "inputs" in workflow["134"] and "text" in workflow["134"]["inputs"]:
            workflow["134"]["inputs"]["text"] = prompt
        else:
            raise RuntimeError("workflow_patch_failed: cannot set node 134 prompt (node/field not found)")

    # image
    if image_url:
        # скачиваем в /comfyui/input
        input_dir = Path("/comfyui/input")
        input_dir.mkdir(parents=True, exist_ok=True)

        ext = Path(image_url.split("?")[0]).suffix
        if not ext:
            ext = ".png"
        fname = f"input_{uuid.uuid4().hex}{ext}"
        dst = input_dir / fname

        resp = requests.get(image_url, timeout=60)
        resp.raise_for_status()
        dst.write_bytes(resp.content)

        # node 148 LoadImage expects file name relative to input
        if "148" in workflow and "inputs" in workflow["148"] and "image" in workflow["148"]["inputs"]:
            workflow["148"]["inputs"]["image"] = fname
        else:
            raise RuntimeError("workflow_patch_failed: cannot set node 148 image (node/field not found)")


def _submit_prompt(workflow: dict) -> str:
    payload = {"prompt": workflow}
    r = requests.post(f"{COMFY_URL}/prompt", json=payload, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"comfy_submit_failed: ComfyUI /prompt failed: HTTP {r.status_code} body={r.text}")
    data = r.json()
    # ComfyUI returns {"prompt_id": "...", ...}
    pid = data.get("prompt_id")
    if not pid:
        raise RuntimeError(f"comfy_submit_failed: no prompt_id in response: {data}")
    return pid


def _wait_history(prompt_id: str) -> dict:
    t0 = time.time()
    while True:
        if time.time() - t0 > JOB_TIMEOUT_SEC:
            raise RuntimeError(f"timeout_waiting_history: >{JOB_TIMEOUT_SEC}s prompt_id={prompt_id}")

        r = requests.get(f"{COMFY_URL}/history/{prompt_id}", timeout=30)
        if r.status_code == 200:
            hist = r.json()
            # Обычно hist[prompt_id] существует, но иногда возвращают сразу объект
            if prompt_id in hist:
                item = hist[prompt_id]
            else:
                item = hist

            # признак завершения: есть outputs или статус completed (у разных сборок по-разному)
            if isinstance(item, dict) and ("outputs" in item or "status" in item):
                # если outputs есть — готово
                if "outputs" in item and item["outputs"]:
                    return item
                # иногда outputs пустой, но это всё равно значит "done" — тогда fallback по файлу
                if item.get("status", {}).get("completed") is True:
                    return item

        time.sleep(2)


def _extract_video_from_history(history: dict) -> str | None:
    """
    Пытаемся найти сохранённый файл в history.outputs.
    Форматы у нод разные, поэтому ищем любые filename с видео-расширением.
    """
    outputs = history.get("outputs") or {}
    video_exts = {".mp4", ".webm", ".mov", ".mkv", ".gif"}

    def walk(v):
        if isinstance(v, dict):
            for vv in v.values():
                yield from walk(vv)
        elif isinstance(v, list):
            for vv in v:
                yield from walk(vv)
        else:
            yield v

    for node_id, out in outputs.items():
        for leaf in walk(out):
            if isinstance(leaf, str):
                p = Path(leaf)
                if p.suffix.lower() in video_exts:
                    # чаще ComfyUI хранит {"filename": "...", "subfolder": "..."}
                    return leaf

    # Частый вариант: outputs[node]["videos"] = [{"filename": "...", "subfolder": "..."}]
    for node_id, out in outputs.items():
        if isinstance(out, dict):
            vids = out.get("videos") or out.get("gifs")
            if isinstance(vids, list) and vids:
                v0 = vids[0]
                if isinstance(v0, dict) and v0.get("filename"):
                    sub = v0.get("subfolder", "")
                    # ComfyUI output path = /comfyui/output/{subfolder}/{filename}
                    return str(Path(COMFY_OUTPUT_DIR) / sub / v0["filename"])

    return None


def _find_latest_video_file() -> str | None:
    video_exts = ("*.mp4", "*.webm", "*.mov", "*.mkv", "*.gif")
    base = Path(COMFY_OUTPUT_DIR)

    candidates = []
    for pattern in video_exts:
        candidates += glob.glob(str(base / "**" / pattern), recursive=True)

    if not candidates:
        return None

    candidates.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return candidates[0]


def _r2_client():
    return boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
        region_name="auto",
    )


def _upload_to_r2(local_path: str) -> str:
    s3 = _r2_client()

    lp = Path(local_path)
    key = f"outputs/{uuid.uuid4().hex}{lp.suffix.lower()}"

    ctype, _ = mimetypes.guess_type(str(lp))
    extra = {}
    if ctype:
        extra["ContentType"] = ctype

    s3.upload_file(str(lp), R2_BUCKET, key, ExtraArgs=extra)

    if R2_PUBLIC_BASE:
        return f"{R2_PUBLIC_BASE.rstrip('/')}/{key}"
    # если публичной базы нет — всё равно вернём “s3-like” ссылку (может быть непубличной)
    return f"{R2_ENDPOINT.rstrip('/')}/{R2_BUCKET}/{key}"


# ---------------- RunPod Handler ----------------
def handler(job):
    _require_env()
    _wait_comfy_ready()

    job_input = (job.get("input") or {})
    prompt = job_input.get("prompt")
    image_url = job_input.get("image_url")

    workflow = _load_workflow()

    # IMPORTANT: workflow.json у тебя должен быть API-prompt формата ComfyUI:
    # {"115": {...}, "116": {...}, ...} без обёртки {"input": {"workflow": ...}}
    # Если он у тебя вдруг с обёрткой — распакуем:
    if "input" in workflow and isinstance(workflow["input"], dict) and "workflow" in workflow["input"]:
        workflow = workflow["input"]["workflow"]

    _apply_runtime_inputs(workflow, prompt=prompt, image_url=image_url)

    prompt_id = _submit_prompt(workflow)
    history = _wait_history(prompt_id)

    # 1) try history outputs
    video_path = _extract_video_from_history(history)

    # 2) fallback: disk scan
    if not video_path or not Path(video_path).exists():
        video_path = _find_latest_video_file()

    if not video_path or not Path(video_path).exists():
        raise RuntimeError(
            "no_video_in_history_outputs: video file not found after prompt execution. "
            f"prompt_id={prompt_id}"
        )

    video_url = _upload_to_r2(video_path)

    return {
        "prompt_id": prompt_id,
        "video_path": video_path,
        "video_url": video_url,
    }


runpod.serverless.start({"handler": handler})
