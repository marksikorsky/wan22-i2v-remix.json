# clean base image containing only comfyui, comfy-cli and comfyui-manager
FROM runpod/worker-comfyui:5.5.0-base

# install custom nodes into comfyui
RUN comfy node install --exit-on-fail ComfyUI-WanVideoWrapper@1.4.3
RUN comfy node install --exit-on-fail comfyui-wanvideowrapper@1.4.3
RUN comfy node install --exit-on-fail comfyui-kjnodes@1.2.1
RUN comfy node install --exit-on-fail comfyui-frame-interpolation@1.0.7
RUN comfy node install --exit-on-fail comfyui-custom-scripts@1.2.5
RUN comfy node install --exit-on-fail comfyui-easy-use@1.3.4
RUN pip install --no-cache-dir boto3
RUN pip install --no-cache-dir runpod
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    g++ \
    make \
    gfortran \
    python3-dev \
    pkg-config \
    cmake \
    ninja-build \
    autoconf \
    automake \
    libtool \
    patchelf \
    ffmpeg \
    imagemagick \
    git \
    curl \
    wget \
    unzip \
    zip \
    ca-certificates \
    openssl \
    jq \
    aria2 \
    rsync \
    nano \
    vim \
    lsb-release \
    pciutils \
    usbutils \
    iproute2 \
    iputils-ping \
    net-tools \
    dnsutils \
    procps \
    htop \
    tmux \
    tree \
    locales \
    tzdata \
    libgl1 \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender1 \
    libx11-6 \
    libx11-xcb1 \
    libxkbcommon0 \
    libxi6 \
    libxrandr2 \
    libxcursor1 \
    libxdamage1 \
    libxcomposite1 \
    libxfixes3 \
    libxinerama1 \
    libxss1 \
    libxshmfence1 \
    libwayland-client0 \
    libwayland-egl1 \
    libwayland-server0 \
    libegl1 \
    libgles2 \
    mesa-utils \
    libstdc++6 \
    libgcc-s1 \
    libgomp1 \
    libnuma1 \
    libc6 \
    libssl-dev \
    zlib1g-dev \
    libffi-dev \
    libbz2-dev \
    liblzma-dev \
    libreadline-dev \
    libsqlite3-dev \
    libncursesw5-dev \
    libgdbm-dev \
    libnss3-dev \
    libsndfile1 \
    libsndfile1-dev \
    libopenblas-dev \
    liblapack-dev \
    libatlas-base-dev \
    libjpeg-dev \
    libpng-dev \
    libtiff-dev \
    libwebp-dev \
    libfreetype6-dev \
    libharfbuzz-dev \
    libfribidi-dev \
    libfontconfig1-dev \
    libxml2-dev \
    libxslt1-dev \
    libhdf5-dev \
    libprotobuf-dev \
    protobuf-compiler \
    libopencv-dev \
    && rm -rf /var/lib/apt/lists/*

# download models into comfyui
RUN comfy model download --url https://huggingface.co/Comfy-Org/Wan_2.1_ComfyUI_repackaged/resolve/main/split_files/vae/wan_2.1_vae.safetensors --relative-path models/vae --filename wan_2.1_vae.safetensors
RUN comfy model download --url https://huggingface.co/wavespeed/misc/blob/main/rife/rife47.pth --relative-path models/checkpoints --filename rife47.pth
RUN comfy model download --url https://huggingface.co/NSFW-API/NSFW-Wan-UMT5-XXL/resolve/main/nsfw_wan_umt5-xxl_fp8_scaled.safetensors --relative-path models/text_encoders --filename nsfw_wan_umt5-xxl_fp8_scaled.safetensors
RUN comfy model download --url https://huggingface.co/FX-FeiHou/wan2.2-Remix/resolve/main/NSFW/Wan2.2_Remix_NSFW_i2v_14b_low_lighting_v2.0.safetensors  --relative-path models/diffusion_models --filename Wan2.2_Remix_NSFW_i2v_14b_low_lighting_v2.0.safetensors
RUN comfy model download --url https://huggingface.co/FX-FeiHou/wan2.2-Remix/resolve/main/NSFW/Wan2.2_Remix_NSFW_i2v_14b_high_lighting_v2.0.safetensors  --relative-path models/diffusion_models --filename Wan2.2_Remix_NSFW_i2v_14b_high_lighting_v2.0.safetensors
RUN comfy model download --url https://huggingface.co/Kijai/WanVideo_comfy/resolve/main/LoRAs/Wan22-Lightning/old/Wan2.2-Lightning_I2V-A14B-4steps-lora_HIGH_fp16.safetensors --relative-path models/loras --filename Wan2.2-Lightning_I2V-A14B-4steps-lora_HIGH_fp16.safetensors
RUN comfy model download --url https://huggingface.co/Kijai/WanVideo_comfy/resolve/main/LoRAs/Wan22-Lightning/old/Wan2.2-Lightning_I2V-A14B-4steps-lora_LOW_fp16.safetensors --relative-path models/loras --filename Wan2.2-Lightning_I2V-A14B-4steps-lora_LOW_fp16.safetensors

COPY workflow.json /comfyui/workflow.json
COPY handler.py /handler.py
COPY start.sh /start.sh
RUN chmod +x /start.sh

CMD ["/start.sh"]
# copy all input data (like images or videos) into comfyui (uncomment and adjust if needed)
# COPY input/ /comfyui/input/
