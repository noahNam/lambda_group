import os
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# set rds
host = os.environ.get("HOST")
user = os.environ.get("USER")
password = os.environ.get("PASSWORD")
database = os.environ.get("DATABASE")
port = os.environ.get("PORT")

# set enum
AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
AWS_REGION_NAME = os.environ.get("AWS_REGION_NAME")

# SQS
SQS_BASE = os.environ.get("SQS_BASE")
SQS_NAME = os.environ.get("SQS_NAME")

conn = None


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


def lambda_handler(event, context):
    result: dict = receive_sqs()

    return result
