FROM python:3.11-slim

WORKDIR /app

# Устанавливаем зависимости для сборки
RUN apt-get update && apt-get install -y curl && \
    pip install --upgrade pip && \
    pip install uv && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Копируем проект
COPY . .

# Устанавливаем зависимости через uv
RUN uv sync --frozen --no-dev

# Устанавливаем пакет приложения
RUN uv pip install --no-deps .

# Открываем порт
EXPOSE 3024

# Запуск FastAPI-приложения через uv
CMD ["uv", "run", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "3024"]