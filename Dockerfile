FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ /app/app/

EXPOSE 8080

# ENTRY env 切换 FastAPI app entry, 让同一 image 跑两个 service:
#   - kol-automation service: ENTRY=app.main:app (默认)
#   - dtc-weekly service: ENTRY=app.weekly_main:app
ENV ENTRY=app.main:app
CMD ["sh", "-c", "uvicorn ${ENTRY} --host 0.0.0.0 --port 8080"]
