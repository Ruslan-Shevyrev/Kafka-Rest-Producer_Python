import uvicorn
import json
import os
from fastapi import BackgroundTasks, FastAPI, Body
from fastapi.responses import JSONResponse
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVER = os.environ["KAFKA_BOOTSTRAP_SERVER"]

app = FastAPI()

producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVER)


@app.get("/")
async def root():
    return {"service available"}


@app.post("/put_message")
async def message(data=Body()):
    try:
        json_message = json.loads(data.decode('utf-8'))
        producer.send(topic=json_message.get('topic'),
                      key=bytes(json_message.get('key'), 'utf-8'),
                      value=bytes(json_message.get('value'), 'utf-8'))
        producer.flush()
        return {"success"}
    except Exception as e:
        return JSONResponse(status_code=501,
                            content={"error": str(e)})

uvicorn.run(app, host="127.0.0.1", port=8000)
