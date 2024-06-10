import uvicorn
import json
import os
from fastapi import FastAPI, Body, Security
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from kafka import KafkaProducer

try:
    KAFKA_BOOTSTRAP_SERVER = os.environ["KAFKA_BOOTSTRAP_SERVER"]
except KeyError:
    KAFKA_BOOTSTRAP_SERVER = ''

try:
    UVICORN_HOST = os.environ["UVICORN_HOST"]
except KeyError:
    UVICORN_HOST = '127.0.0.1'

try:
    TOKEN = os.environ["TOKEN"]
except KeyError:
    TOKEN = 'test_token'

security = HTTPBearer()
app = FastAPI()

producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVER)


@app.get("/")
async def root():
    return {"service available"}


@app.post("/put_message")
async def message(data=Body(), credentials: HTTPAuthorizationCredentials = Security(security)):
    if credentials.credentials != TOKEN:
        return JSONResponse(status_code=401, content="Not authenticated")
    try:
        json_message = json.loads(data.decode('utf-8'))
        topic = json_message.get('topic')
        key = json_message.get('key')

        if key is not None:
            if type(key) == int:
                key = str(key)
            key = bytes(key, 'utf-8')

        value = json_message.get('value')

        if type(value) == int:
            value = str(value)
        value = bytes(value, 'utf-8')

        producer.send(topic=topic,
                      key=key,
                      value=value)
        producer.flush()
        return {"success"}
    except Exception as e:
        return JSONResponse(status_code=501,
                            content={"error": str(e)})


uvicorn.run(app, host=UVICORN_HOST, port=8000)
