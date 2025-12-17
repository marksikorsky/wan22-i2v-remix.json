#!/bin/bash
set -e

# Стартуем ComfyUI только локально внутри контейнера
python3 /comfyui/main.py --listen 127.0.0.1 --port 8188 &
sleep 5

# Стартуем RunPod serverless handler
python3 /handler.py
