FROM python:3.11-slim

# Keep Python output unbuffered and avoid .pyc clutter in the image.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DICOM_INGEST_DATA_DIR=/data

WORKDIR /srv

# Install dependencies first to leverage Docker layer caching.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Application code, local source service and sample generator.
COPY app ./app
COPY source_app ./source_app
COPY scripts ./scripts

# Build the synthetic samples used by the upload demo and the immutable study
# served by the local center-imaging source service.
RUN python3 scripts/generate_samples.py /srv/samples/generated \
    && python3 scripts/generate_samples.py --source-data /source-data/ \
    && mkdir -p /data/objects

# Run as a non-root user; the mounted volume is chowned at startup entrypoint.
RUN useradd --create-home --uid 10001 appuser \
    && chown -R appuser:appuser /srv /data /source-data
COPY docker/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh
USER appuser

EXPOSE 8000

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
