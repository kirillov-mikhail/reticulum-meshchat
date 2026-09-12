FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENV RETICULUM_MESHCHAT_SEND_DIRECTORIES=/app/send_dir
CMD ["python", "-u", "cli.py"]