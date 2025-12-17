import os
import json
import time
import uuid
import requests
import runpod
import boto3

COMFY_URL = os.environ.get("COMFY_URL", "http://127.0.0.1:8188")
WORKFLOW_PATH = os.environ.get("WORKFLOW_PATH", "/comfyui/workflow.json")
COMFY_INPUT_DIR = os.environ.get("COMFY_INPUT_DIR", "/comfyui/input")
COMFY_OUTPUT_DIR = os.environ.get("COMFY_OUTPUT_DIR", "/comfyui/output")

R2_ENDPOINT = os.environ["R2_ENDPOINT"]
R2_BUCKET = os.environ["R2_BUCKET"]
R2_ACCESS_KEY = os.environ["R2_ACCESS_KEY"]
R2_SECRET_KEY = os.environ["R2_SECRET_KEY"]

def r2_client():
    return boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
        region_name="auto",
    )

def upload_to_r2(local_path: str, key: str) -> str:
    s3 = r2_client()
    s3.upload_file(local_path, R2_BUCKET, key, ExtraArgs={"ContentType": "video/mp4"})
    # Public bucket URL (MVP)
    return f"{R2_ENDPOINT}/{R2_BUCKET}/{key}"

def download_image(url: str, out_path: str) -> None:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    with open(out_path, "wb") as f:
        f.write(r.content)

def submit_workflow(workflow: dict) -> str:
    r = requests.post(f"{COMFY_URL}/prompt", json={"prompt": workflow}, timeout=60)
    r.raise_for_status()
    return r.json()["prompt_id"]

def wait_history(prompt_id: str, timeout_s: int = 2400) -> dict:
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

def extract_saved_video_filenames(history: dict) -> list[str]:
    files = []
    outputs = history.get("outputs", {})
    for _, out in outputs.items():
        for key in ("videos", "gifs"):
            if key in out and isinstance(out[key], list):
                for item in out[key]:
                    fn = item.get("filename")
                    if fn:
                        files.append(fn)
    return files

def handler(job):
    inp = job.get("input", {})
    prompt = inp.get("prompt", "")
    image_url = inp.get("image_url")
    if not image_url:
        return {"error": "image_url is required"}

    # 1) подготовить входную картинку
    os.makedirs(COMFY_INPUT_DIR, exist_ok=True)
    img_name = f"{uuid.uuid4().hex}.png"
    img_path = os.path.join(COMFY_INPUT_DIR, img_name)
    download_image(image_url, img_path)

    # 2) загрузить workflow
    with open(WORKFLOW_PATH, "r", encoding="utf-8") as f:
        wf = json.load(f)

    # 3) подставить prompt и картинку (твои node_id)
    # prompt node: 134  (CLIPTextEncode positive)
    wf["134"]["inputs"]["text"] = prompt
    # image node: 148   (LoadImage)
    wf["148"]["inputs"]["image"] = img_name

    # 4) убрать fp16_fast чтобы не падать на allow_fp16_accumulation
    for nid in ("131", "132"):
        if nid in wf and "inputs" in wf[nid]:
            wf[nid]["inputs"]["precision"] = "fp16"

    # 5) запустить
    pid = submit_workflow(wf)
    hist = wait_history(pid)

    # 6) найти сохранённый видео-файл (SaveVideo)
    vids = extract_saved_video_filenames(hist)
    if not vids:
        # на всякий случай вернём history, чтобы видеть где сломалось
        return {"prompt_id": pid, "error": "no video files in outputs", "history": hist}

    # Comfy обычно отдаёт относительный путь типа "video/ComfyUI_00001.mp4"
    rel = vids[-1]
    local_video_path = os.path.join(COMFY_OUTPUT_DIR, rel)
    if not os.path.exists(local_video_path):
        # иногда filename уже включает подпапку, но output dir другой — вернём диагностику
        return {
            "prompt_id": pid,
            "error": "video file not found on disk",
            "saved": vids,
            "expected_path": local_video_path,
        }

    # 7) загрузить в R2
    key = f"videos/{uuid.uuid4().hex}.mp4"
    url = upload_to_r2(local_video_path, key)

    return {
        "prompt_id": pid,
        "video_url": url,
    }

runpod.serverless.start({"handler": handler})
