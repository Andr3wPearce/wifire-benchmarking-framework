# Image for the core blocks on the WSTC Dagster: LANDFIRE, roads override, station wind,
# moisture, ignition, reproject, reference perimeter, metrics. They only need rasterio on
# top of the server's own image (dagster 1.13.26, dagster-k8s 0.29.26, numpy, requests).
# Block code is NOT baked in: each workflow file carries its blocks' code.
# Build + push: scripts/build_wstc_image.sh
FROM docker.io/dagster/dagster-k8s:1.13.26
# rasterio's wheel links the system expat
RUN apt-get update && apt-get install -y --no-install-recommends libexpat1 \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    && python -c "import dagster, rasterio; print('dagster', dagster.__version__, 'rasterio', rasterio.__version__)"
