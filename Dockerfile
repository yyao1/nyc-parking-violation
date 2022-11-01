FROM python:3.7

COPY requirements.txt .

RUN pip install -r requirements.txt

WORKDIR /app

COPY src/main.py /app

COPY . .
