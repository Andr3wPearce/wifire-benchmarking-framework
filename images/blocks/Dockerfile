# The blocks' environment as one image, for steps on the WSTC Dagster (NRP).
# Base = the official Dagster image at the server's version (dagster 1.13.26,
# dagster-k8s 0.29.26), so a step can run in it. Block code is NOT baked in: each
# workflow file carries its blocks' code, so changing a block never needs a rebuild.
# Build + push: scripts/build_wstc_image.sh
ARG DAGSTER_VERSION=1.13.26
FROM docker.io/dagster/dagster-k8s:${DAGSTER_VERSION} AS build
RUN apt-get update && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt /tmp/requirements.txt
RUN pip wheel --no-cache-dir -w /wheels -r /tmp/requirements.txt

FROM docker.io/dagster/dagster-k8s:${DAGSTER_VERSION}
# rasterio's wheel links the system expat; slim images may lack it
RUN apt-get update && apt-get install -y --no-install-recommends libexpat1 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /wheels /wheels
RUN pip install --no-cache-dir --no-index --find-links /wheels /wheels/*.whl && rm -rf /wheels \
    && python -c "import dagster, numpy, rasterio, pyretechnics.eulerian_level_set; \
print('dagster', dagster.__version__, 'numpy', numpy.__version__, 'rasterio', rasterio.__version__)"
