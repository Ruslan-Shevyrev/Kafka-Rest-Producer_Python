import uvicorn
import json
import os
from fastapi import BackgroundTasks, FastAPI, Body
from kafka import KafkaProducer

# KAFKA_BOOTSTRAP_SERVER = 'oraapex-draft:9092'
KAFKA_BOOTSTRAP_SERVER = os.environ["KAFKA_BOOTSTRAP_SERVER"]

app = FastAPI()

producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVER)


def put_message(body_json):

    producer.send(topic=body_json.get('topic'),
                  key=bytes(body_json.get('key'), 'utf-8'),
                  value=bytes(body_json.get('value'), 'utf-8'))
    producer.flush()


@app.get("/")
async def root():
    return {"service available"}


@app.post("/put_message")
async def message(background_tasks: BackgroundTasks, data=Body()):
    json_message = json.loads(data.decode('utf-8'))
    background_tasks.add_task(put_message, json_message)
    return {"success"}

uvicorn.run(app, host="127.0.0.1", port=8000)
