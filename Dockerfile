FROM python:3.11-slim

WORKDIR /app

RUN pip install --upgrade pip
RUN pip install uv

COPY . .

RUN pip install .

EXPOSE 3024

# Запуск FastAPI-приложения посредством uvicorn (через uv можно тоже, если хотите)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "3024", "--reload"]
