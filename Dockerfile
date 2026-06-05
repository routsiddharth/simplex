# syntax=docker/dockerfile:1
# Multi-stage build for the Simplex stage 1 ingest.
# Python 3.13 to match what we developed/tested against (3.13.5).

# ---- builder: install the package + deps into a venv -----------------------
FROM python:3.13-slim AS builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /build
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Only the files needed to build the wheel (maximizes layer caching).
COPY pyproject.toml ./
COPY src ./src

RUN pip install --no-cache-dir . \
    # Fail the build now if schema.sql didn't get packaged (db.py reads it at runtime).
    && test -f /opt/venv/lib/python3.13/site-packages/simplex_ingest/schema.sql

# ---- runtime: slim image, non-root app process -----------------------------
FROM python:3.13-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/opt/venv/bin:$PATH" \
    SIMPLEX_DATA_DIR=/data

# gosu: drop from root (needed to chown the volume) to the app user, while still
#       forwarding SIGTERM to the Python process for clean shutdown.
# ca-certificates: system trust store (certifi is also bundled in the venv and is
#       what the websockets TLS handshake uses — see entrypoint/app).
RUN apt-get update \
    && apt-get install -y --no-install-recommends gosu ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 appuser

COPY --from=builder /opt/venv /opt/venv

WORKDIR /app
# The tracked series set is discovered at runtime and persisted in DuckDB on the
# mounted volume — nothing series-related is baked into the image.
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# Health server port (constants.HEALTH_PORT). Railway must be told to target this
# via a PORT=8080 service variable (see README-DEPLOY.md).
EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["python", "-m", "simplex_ingest"]
