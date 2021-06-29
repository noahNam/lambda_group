import json
import os
from typing import List
from datetime import datetime

import pymysql
import boto3
import logging

from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# log 출력 형식
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# log 출력
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

host = "localhost"
user = "admin"
password = "!wjstngks117"
database = "apartalk_data_lake"
port = "3306"

# set enum
AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY") or "AKIATBBH6H6PNXVM54ND"
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY") or "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"
AWS_REGION_NAME = os.environ.get("AWS_REGION_NAME") or "ap-northeast-2"

# SQS
SQS_BASE = os.environ.get("SQS_BASE") or "https://sqs.ap-northeast-2.amazonaws.com/208389685150"
SQS_NAME = os.environ.get("SQS_NAME") or "USER_DATA_SYNC_TO_LAKE_QUEUE"

conn = None


def openConnection():
    """
    conn.closed
    1 -> STATUS_BEGIN
    0 -> STATUS_READY
    """
    global conn
    try:
        logger.info("Opening Connection")
        if conn is None:
            conn = pymysql.connect(
                host=host, dbname=database, user=user, password=password, port=port, connect_timeout=5)
            logger.info("conn.status is %s", conn.status)
        elif conn.status == 1:
            conn = pymysql.connect(
                host=host, dbname=database, user=user, password=password, port=port, connect_timeout=5)
            logger.info("conn.status is %s", conn.status)

    except Exception as e:
        logger.exception("Unexpected error: Could not connect to RDS instance. %s", e)
        raise e


# def update_notification_schema(query_result: List):
#     logger.info("Update notification schema start")
#     update_data = list()
#     for data in query_result:
#         update_data.append((data[0], data[1], data[5], datetime.now()))
#     try:
#         openConnection()
#         with conn.cursor() as cur:
#             execute_values(cur,
#                            """
#                            update notifications
#                            set endpoint=data.endpoint,status=data.status, updated_at=data.updated_at
#                            from (VALUES %s) as data (id, endpoint, status, updated_at)
#                            where notifications.id=data.id
#                            """, update_data)
#             conn.commit()
#         logger.info("Update notification schema end")
#     except Exception as e:
#         logger.exception("Error while opening connection or processing. %s", e)
#     finally:
#         logger.info("Closing Connection")
#         if conn is not None and conn.status == STATUS_BEGIN:
#             conn.close()


def get_push_target_user_data():
    item_count = 0
    query_result = list()
    try:
        openConnection()
        with conn.cursor() as cur:
            cur.execute(
                f"select id, endpoint, data, user_id, token, status from notifications where category='{CATEGORY}' and status = '{STATUS}' ")
            cur.execute('insert into Employee3 (EmpID, Name) values(1, "Joe")')
            for row in cur:
                rslt = list()
                for data in row:
                    rslt.append(data)

                query_result.append(rslt)
                item_count += 1
    except Exception as e:
        logger.exception("Error while opening connection or processing. %s", e)
    finally:
        logger.info("Closing Connection")
        if conn is not None and conn.status == 1:
            conn.close()

    return dict(item_count=item_count, query_result=query_result)


def receive_sqs():
    total = 0
    try:
        for message in receive_after_delete():
            test = message
            if message['Body']:
                #### 처리 로직 -> data push to data lake ####

                ##########################################
                print("[receive_after_delete] Target Message {0}", message['Body'])
                total += 1
    except Exception as e:
        print("SQS Fail : {}".format(e))
        # send_slack_message('Exception: {}'.format(str(e)), "[FAIL]" + SqsTypeEnum.USER_DATA_SYNC_TO_LAKE.value)

    dict_ = {
        'result': True, 'total': total,
    }
    return dict_


def receive_after_delete() -> bool:
    _sqs = boto3.client(
        "sqs",
        region_name=AWS_REGION_NAME,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
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

        entries = [
            {'Id': msg['MessageId'], 'ReceiptHandle': msg['ReceiptHandle']}
            for msg in resp['Messages']
        ]

        resp = _sqs.delete_message_batch(
            QueueUrl=__target_q, Entries=entries
        )

        if len(resp['Successful']) != len(entries):
            raise RuntimeError(
                f"Failed to delete messages: entries={entries!r} resp={resp!r}"
            )


def run():
    result = receive_sqs()
    return result


# result: dict = get_push_target_user_data()

# delete endpoint
# delete_application_endpoint(result['query_result'])
# return

# if len(result['query_result']) > 0:
#     send_sns_notification(result['query_result'])
#     update_notification_schema(result['query_result'])

# return "Selected %d items from RDS table" % result['item_count']


if __name__ == "__main__":
    run()
