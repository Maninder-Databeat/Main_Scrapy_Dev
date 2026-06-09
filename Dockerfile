FROM python:3.13-slim

WORKDIR /app

RUN pip install uv

COPY pyproject.toml uv.lock ./
RUN uv sync

COPY . .

ENV PYTHONUNBUFFERED=1

# Use UV to run the script (automatically activates .venv)
CMD ["uv", "run", "main.py"]
