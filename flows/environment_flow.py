import os

from prefect import task, Flow
import prefect
from config import settings

@task
def read_env():
    logger = prefect.context.get("logger")
    logger.info(f"logger")

with Flow("Stupid flow") as flow:
    read_result = read_env()
