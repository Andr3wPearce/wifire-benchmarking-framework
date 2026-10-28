# ELMFIRE (pyretechnics) model block.
# Build into minikube:   minikube image build -t bench/elmfire:0.1 images/elmfire
FROM python:3.12-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends build-essential && rm -rf /var/lib/apt/lists/*
COPY requirements.txt /tmp/
# pyretechnics ships as source only; it compiles here (Cython), so the runtime image stays slim
RUN pip install --no-cache-dir --prefix=/install -r /tmp/requirements.txt

FROM python:3.12-slim
# rasterio's wheel links against the system expat, which the slim image leaves out
RUN apt-get update && apt-get install -y --no-install-recommends libexpat1 && rm -rf /var/lib/apt/lists/*
COPY --from=build /install /usr/local
COPY elmfire_block.py /app/elmfire_block.py
ENV BENCH_ARTIFACT_ROOT=/artifacts PYTHONUNBUFFERED=1
RUN python -c "import pyretechnics.eulerian_level_set, rasterio, dagster_pipes; print('ok')"
ENTRYPOINT ["python", "/app/elmfire_block.py"]
