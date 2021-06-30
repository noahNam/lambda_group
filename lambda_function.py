import json
import os
import boto3
import logging

from datetime import datetime
from typing import List
import chardet
from package import requests
from package import pymysql

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

conn = None


def send_slack_message(message, title):
    text = title + " -> " + message
    requests.post("https://slack.com/api/chat.postMessage",
                  headers={"Authorization": "Bearer " + SLACK_TOKEN},
                  data={"channel": CHANNEL, "text": text}
                  )


def openConnection():
    global conn
    try:
        logger.info("Opening Connection")
        if conn is None:
            conn = pymysql.connect(
                host=host, db=database, user=user, password=password, port=port, connect_timeout=5)
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
                    data = json.loads(msg['Body'])
                    data = data['msg']
                    cur.execute(
                        """
                            INSERT INTO TANOS_USER_INFO_TB (user_id,user_prfile_id,code,value,created_at,updated_at)
                            VALUES (%s,%s,%s,%s,%s,%s)
                            ON DUPLICATE KEY UPDATE value=%s, updated_at=%s
                        """,
                        (data['user_id'], data['user_profile_id'], data['code'], data['value'], datetime.now(),
                         datetime.now(), data['value'], datetime.now()))
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


def receive_sqs():
    _sqs = boto3.client(
        "sqs",
        region_name=AWS_REGION_NAME,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

    total = 0
    msg_list = []
    try:
        for message in receive_after_delete(_sqs):
            if message['Body']:
                msg_list.append(message)
                total += 1
                logger.info("[receive_sqs] Target Message %s", message['Body'])
    except Exception as e:
        logger.info("SQS Fail : {}".format(e))
        send_slack_message('Exception: {}'.format(str(e)), "[FAIL] SQS_USER_DATA_SYNC_TO_LAKE")

    #### 처리 로직 -> data push to data lake ####
    push_user_data_to_lake_schema(msg_list)
    ##########################################

    if msg_list:
        entries = [
            {'Id': msg['MessageId'], 'ReceiptHandle': msg['ReceiptHandle']}
            for msg in msg_list
        ]
        try:
            resp = _sqs.delete_message_batch(
                QueueUrl=SQS_BASE + "/" + SQS_NAME, Entries=entries
            )
        except Exception as e:
            logger.exception("Error while delete messages or processing. %s", e)

        if len(resp['Successful']) != len(entries):
            send_slack_message('Exception: {}'.format(str(e)), f"Failed to delete messages: entries={entries!r} resp={resp!r}")
            raise RuntimeError(
                f"Failed to delete messages: entries={entries!r} resp={resp!r}"
            )

    dict_ = {
        'result': True, 'total': total,
    }
    return dict_


def receive_after_delete(sqs_client) -> bool:
    _sqs = sqs_client
    __target_q = None

    while True:
        if not __target_q:
            __target_q = (
                    SQS_BASE
                    + "/"
                    + SQS_NAME
            )
            logger.debug(
                "[receive_after_delete] Target Queue {0}".format(__target_q)
            )

        # SQS에 큐가 비워질때까지 메세지 조회
        resp = _sqs.receive_message(
            QueueUrl=__target_q,
            AttributeNames=['All'],
            MaxNumberOfMessages=10
        )

        try:
            """
            제너레이터는 함수 끝까지 도달하면 StopIteration 예외가 발생. 
            마찬가지로 return도 함수를 끝내므로 return을 사용해서 함수 중간에 빠져나오면 StopIteration 예외가 발생.
            특히 제너레이터 안에서 return에 반환값을 지정하면 StopIteration 예외의 에러 메시지로 들어감
            """
            yield from resp['Messages']
        except KeyError:
            # not has next
            return False


def lambda_handler(event, context):
    result: dict = receive_sqs()

    return result
