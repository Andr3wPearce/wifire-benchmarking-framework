# The blocks' environment as one image, for steps on the WSTC Dagster (NRP).
# A step pod runs `dagster api execute_step`, so the image carries the server's Dagster
# (dagster 1.13.26, dagster-k8s / dagster-postgres 0.29.26) plus what the server image has
# (pandas, boto3). Python 3.12, not the official Dagster image's 3.10: pyretechnics needs >= 3.11.
# Block code is NOT baked in: each workflow file carries its blocks' code, so changing a
# block never needs a rebuild.   Build + push: scripts/build_wstc_image.sh
FROM python:3.12-slim-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt /tmp/requirements.txt
RUN pip wheel --no-cache-dir -w /wheels -r /tmp/requirements.txt

FROM python:3.12-slim-bookworm
# rasterio's wheel links the system expat
RUN apt-get update && apt-get install -y --no-install-recommends libexpat1 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /wheels /wheels
RUN pip install --no-cache-dir --no-index --find-links /wheels /wheels/*.whl && rm -rf /wheels \
    && python -c "import dagster, dagster_k8s, numpy, rasterio, pyretechnics.eulerian_level_set; \
print('dagster', dagster.__version__, 'numpy', numpy.__version__, 'rasterio', rasterio.__version__)"
