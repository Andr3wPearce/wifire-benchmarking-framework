# Image for the ELMFIRE block (pyretechnics) on the WSTC Dagster (NRP).
# A step pod runs `dagster api execute_step`, which loads the server's instance config
# (run storage, compute logs, ...), so the image needs exactly the Dagster packages the
# server's image has. We can't build FROM it (it is Python 3.10; pyretechnics needs >= 3.11),
# so the `server` stage reads that list from the official image and the final stage
# installs the same versions on Python 3.12.
# Block code is NOT baked in: each workflow file carries its blocks' code, so changing a
# block never needs a rebuild.   Build + push: scripts/build_wstc_image.sh
FROM docker.io/dagster/dagster-k8s:1.13.26 AS server
RUN pip freeze | grep -iE '^dagster[a-z0-9_-]*==' > /server-dagster.txt && cat /server-dagster.txt

FROM python:3.12-slim-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*
# (requirements.txt is byte-identical to the earlier single-image recipe, so the compiled
#  pyretechnics layer comes from cache; its first comment line predates the split)
COPY requirements.txt /tmp/requirements.txt
RUN pip wheel --no-cache-dir -w /wheels -r /tmp/requirements.txt

FROM python:3.12-slim-bookworm
# rasterio's wheel links the system expat
RUN apt-get update && apt-get install -y --no-install-recommends libexpat1 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /wheels /wheels
RUN pip install --no-cache-dir --no-index --find-links /wheels /wheels/*.whl && rm -rf /wheels \
    && python -c "import dagster, dagster_k8s, numpy, rasterio, pyretechnics.eulerian_level_set"
# every Dagster package the server's image has, at the same versions (NumPy stays at 1.26)
COPY --from=server /server-dagster.txt /tmp/server-dagster.txt
RUN pip install --no-cache-dir -r /tmp/server-dagster.txt "numpy~=1.26.2" \
    && python -c "import dagster, numpy, pyretechnics.eulerian_level_set; \
print('dagster', dagster.__version__, 'numpy', numpy.__version__)" && pip freeze | grep -iE '^dagster'

