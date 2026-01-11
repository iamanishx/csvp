FROM python:3.11-slim

WORKDIR /app

# Install uv for fast package management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Copy project definition
COPY pyproject.toml .

# Install dependencies using uv into the system python environment
# This is safe in a container and faster/smaller than virtualenvs
RUN uv pip install --system .

COPY src ./src

CMD ["python", "src/main.py"]