import json
import os
import logging

from datetime import datetime, timedelta
from typing import List

from package import requests
from package import pymysql
from package.pyjwt import jwt
from package.pytz import timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# set rds
host = os.environ.get("HOST")
user = os.environ.get("USER")
password = os.environ.get("PASSWORD")
database = os.environ.get("DATABASE")
port = int(os.environ.get("PORT"))

# set enum
AWS_ACCESS_KEY = os.environ.get("ACCESS_KEY")
AWS_SECRET_ACCESS_KEY = os.environ.get("SECRET_ACCESS_KEY")
AWS_REGION_NAME = os.environ.get("AWS_REGION_NAME")

# SQS
SQS_BASE = os.environ.get("SQS_BASE")
SQS_NAME = os.environ.get("SQS_NAME")

# SLACK
SLACK_TOKEN = os.environ.get("SLACK_TOKEN")
CHANNEL = os.environ.get("CHANNEL")

# Jarvis Host
JARVIS_HOST = os.environ.get("JARVIS_HOST")
JWT_SECRET_KEY = os.environ.get("JWT_SECRET_KEY")

conn = None


def send_slack_message(message, title):
    text = title + " -> " + message
    requests.post(
        "https://slack.com/api/chat.postMessage",
        headers={"Authorization": "Bearer " + SLACK_TOKEN},
        data={"channel": CHANNEL, "text": text},
    )


def openConnection():
    global conn
    try:
        logger.info("Opening Connection")
        if conn is None:
            conn = pymysql.connect(
                host=host,
                db=database,
                user=user,
                password=password,
                port=port,
                connect_timeout=5,
            )
        else:
            conn.ping(reconnect=True)

    except Exception as e:
        logger.exception("Unexpected error: Could not connect to RDS instance. %s", e)
        raise e


def push_user_data_to_lake_schema(msg_list: List):
    try:
        openConnection()
        with conn.cursor() as cur:
            for msg in msg_list:
                try:
                    data = json.loads(msg)
                    data = data["msg"]
                    cur.execute(
                        """
                            INSERT INTO TANOS_USER_INFO_TB (user_id,user_profile_id,code,value,created_at,updated_at)
                            VALUES (%s,%s,%s,%s,%s,%s)
                            ON DUPLICATE KEY UPDATE value=%s, updated_at=%s
                        """,
                        (
                            data["user_id"],
                            data["user_profile_id"],
                            data["code"],
                            data["value"],
                            datetime.now(),
                            datetime.now(),
                            data["value"],
                            datetime.now(),
                        ),
                    )
                    conn.commit()
                except Exception as e:
                    logger.exception("Error while upsert data lake schema %s", e)
                    # 처리되지 못한 message list에서 삭제처리 -> message delete 처리 차단
                    msg_list.remove(msg)

        logger.info("Upsert user_data to lake end")
    except Exception as e:
        logger.exception("Error while opening connection or processing. %s", e)
    finally:
        if conn or conn.is_connected():
            conn.close()
            logger.info("Closing Connection")


def call_jarvis_surveys_analysis_api(survey_step: int, user_id: int) -> int:
    host_url = JARVIS_HOST
    data = dict(
        survey_step=survey_step,
        user_id=user_id,
    )
    encoded_jwt = jwt.encode(
        {
            "identity": user_id,
            "exp": datetime.now(timezone("Asia/Seoul")) + timedelta(seconds=30),
        },
        JWT_SECRET_KEY,
        algorithm="HS256",
    )
    response = requests.post(
        url=host_url + "/api/jarvis/v1/predicts/surveys",
        headers={
            "Content-Type": "application/json",
            "Cache-Control": "no-cache",
            "Authorization": "Bearer " + encoded_jwt,
        },
        data=json.dumps(data),
    )

    return response


def receive_sqs(event):
    total = 0
    msg_list = []
    try:
        for message in event["Records"]:
            if message["body"]:
                msg_list.append(message["body"])
                total += 1
                logger.info("[receive_sqs] Target Message %s", message["body"])
    except Exception as e:
        logger.info("SQS Fail : {}".format(e))
        send_slack_message(
            "Exception: {}".format(str(e)), "[FAIL] SQS_USER_DATA_SYNC_TO_LAKE"
        )

    # 처리 로직 -> data push to data lake ####
    push_user_data_to_lake_schema(msg_list)

    for msg in msg_list:
        user_id = json.loads(msg)["msg"]["user_id"]
        survey_step = json.loads(msg)["msg"]["survey_step"]

        # 1단계 설문 완료 이후 부터 api call
        if survey_step >= 2:
            response = call_jarvis_surveys_analysis_api(
                user_id=user_id, survey_step=survey_step
            )

            if response.status_code != 200:
                send_slack_message(
                    "jarvis call error by user_id={}".format(user_id),
                    str(response.json()),
                )
    #########################################

    dict_ = {
        "result": True,
        "total": total,
    }
    return dict_


def lambda_handler(event, context):
    result: dict = receive_sqs(event)
    return result
