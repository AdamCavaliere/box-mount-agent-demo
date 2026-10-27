# One agent sandbox: the box-mount sync daemon (user boxsvc) and that role's agent worker
# (user agent), started and restarted by entrypoint.sh.
FROM --platform=linux/arm64 ubuntu:24.04

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates tini procps tree acl git \
 && rm -rf /var/lib/apt/lists/*
COPY --from=ghcr.io/astral-sh/uv:0.9.13 /uv /usr/local/bin/uv

# Daemon and agent are separate users, per Box's guidance: only boxsvc can read the Box
# credentials; the agent reaches the mount directory through the shared boxmount group.
RUN groupadd boxmount \
 && useradd -m -g boxmount boxsvc \
 && useradd -m -G boxmount agent \
 && mkdir -p /mnt/box \
 && chown boxsvc:boxmount /mnt/box && chmod 2770 /mnt/box

# Python and the locked dependencies; the demo's own code is bind-mounted at /app/demo.
WORKDIR /app
ENV UV_PYTHON_INSTALL_DIR=/opt/python UV_PROJECT_ENVIRONMENT=/app/.venv UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 PYTHONUNBUFFERED=1
COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --frozen --no-dev && chmod -R a+rX /opt/python /app

COPY --chmod=755 private/box-mount-0.6.0/box-mount /usr/local/bin/box-mount
COPY --chmod=755 docker/entrypoint.sh /usr/local/bin/entrypoint.sh

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/entrypoint.sh"]
