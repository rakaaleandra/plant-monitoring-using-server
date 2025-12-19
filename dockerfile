FROM python:3.11-slim

WORKDIR /app
COPY consumer.py .

RUN pip install kafka-python
RUN pip install cassandra-driver

CMD ["python", "consumer.py"]

# FROM node:18-slim
# WORKDIR /app
# COPY consumer.js .
# RUN npm install kafkajs
# CMD ["node", "consumer.js"]