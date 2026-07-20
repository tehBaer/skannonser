FROM python:3.12-slim

# supercronic: container-friendly cron runner.
# On an ARM server use build-arg SUPERCRONIC_URL=...supercronic-linux-arm64
ARG SUPERCRONIC_URL=https://github.com/aptible/supercronic/releases/download/v0.2.33/supercronic-linux-amd64
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl sqlite3 \
    && curl -fsSL -o /usr/local/bin/supercronic "$SUPERCRONIC_URL" \
    && chmod +x /usr/local/bin/supercronic \
    && apt-get purge -y curl && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml ./
COPY skannonser ./skannonser
RUN pip install --no-cache-dir .

COPY docker/crontab /app/docker/crontab
# tini (compose init: true) is PID 1 and reaps; supercronic must not self-reap
CMD ["supercronic", "-no-reap", "/app/docker/crontab"]
