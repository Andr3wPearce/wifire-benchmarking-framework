# Image for the ELMFIRE block (pyretechnics) on the WSTC Dagster (NRP).
#
# Two Pythons, on purpose:
#   * Dagster runs in the official image's Python 3.10, exactly as in the server's own step
#     pods (built FROM the server's Dagster version, like bench-core), so a step always starts.
#   * The model runs in its own Python 3.12 at /opt/blocks (pyretechnics needs >= 3.11). The
#     step runs blocks as a subprocess with BENCH_PYTHON, so the two never meet.
# Block code is NOT baked in: each workflow file carries its blocks' code, so changing a
# block never needs a rebuild.   Build + push: scripts/build_wstc_image.sh

# wheels for the model's environment, compiled once (pyretechnics needs a C compiler)
FROM python:3.12-slim-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt /tmp/requirements.txt
RUN pip wheel --no-cache-dir -w /wheels -r /tmp/requirements.txt

FROM docker.io/dagster/dagster-k8s:1.13.26
# rasterio's wheel links the system expat
RUN apt-get update && apt-get install -y --no-install-recommends libexpat1 \
    && rm -rf /var/lib/apt/lists/*
# the model's Python: CPython 3.12 (python-build-standalone, via uv), in /opt so any user can run it
ENV UV_PYTHON_INSTALL_DIR=/opt/uv-python
RUN pip install --no-cache-dir uv \
    && uv venv --python 3.12 --python-preference only-managed /opt/blocks
COPY --from=build /wheels /wheels
RUN uv pip install --python /opt/blocks/bin/python --no-index --find-links /wheels /wheels/*.whl
RUN rm -rf /wheels && chmod -R a+rX /opt/uv-python /opt/blocks \
    && /opt/blocks/bin/python -c "import numpy, rasterio, matplotlib, pyretechnics.eulerian_level_set; \
print('blocks: numpy', numpy.__version__, 'rasterio', rasterio.__version__)" \
    && python -c "import dagster; print('dagster', dagster.__version__)"
ENV BENCH_PYTHON=/opt/blocks/bin/python
