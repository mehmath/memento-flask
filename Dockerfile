FROM python:3.9-slim

ENV SECRET_KEY='my_secret_key'
ENV MONGO_URI='mongodb://172.20.0.2//movie_watchlist'

WORKDIR /

COPY requirements.txt .

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       libpq-dev \
       gcc \
       python3-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install -r requirements.txt

COPY / .

EXPOSE 5003
CMD ["python", "app.py"]
