FROM python:3.11-slim-bookworm

RUN apt-get update \
    && apt-get install --no-install-recommends -y openjdk-17-jre-headless curl \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY ui_dashboard ./ui_dashboard

EXPOSE 8501

CMD ["streamlit", "run", "ui_dashboard/app.py", "--server.address", "0.0.0.0", "--server.port", "8501"]
