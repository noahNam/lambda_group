import datetime

import psycopg2
import os
import logging

import requests
from psycopg2.extensions import STATUS_BEGIN

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# set rds
host = os.environ.get("HOST")
user = os.environ.get("USER")
password = os.environ.get("PASSWORD")
database = os.environ.get("DATABASE")
port = os.environ.get("PORT")

# status
WAIT = 0
SUCCESS = 1
FAILURE = 2

# SLACK
SLACK_TOKEN = os.environ.get("SLACK_TOKEN")
CHANNEL = os.environ.get("CHANNEL")

conn = None


def send_slack_message(message, title):
    text = title + "\n" + message
    requests.post(
        "https://slack.com/api/chat.postMessage",
        headers={"Authorization": "Bearer " + SLACK_TOKEN},
        data={"channel": CHANNEL, "text": text},
    )


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
            conn = psycopg2.connect(
                host=host,
                dbname=database,
                user=user,
                password=password,
                port=port,
                connect_timeout=5,
            )
            logger.info("conn.status is %s", conn.status)
        elif conn.status == STATUS_BEGIN:
            conn = psycopg2.connect(
                host=host,
                dbname=database,
                user=user,
                password=password,
                port=port,
                connect_timeout=5,
            )
            logger.info("conn.status is %s", conn.status)

    except Exception as e:
        logger.info("Unexpected error: Could not connect to RDS instance. %s", e)
        raise e


def get_dau():
    try:
        openConnection()
        with conn.cursor() as cur:
            cur.execute(
                f"select p.cond1 as "'하루전접속자수'", "
                f"p.cond2 as "'이틀전접속자'", "
                f"p.cond3 as "'하루전새로가입한유저수'", "
                f"p.cond4 as "'이틀전새로가입한유저수'" "
                f"from ( "
                f"select count(1) as cond1, 0 as cond2, 0 as cond3, 0 as cond4 "
                f"from users "
                f"where to_char(current_connection_time, 'YYYY-MM-DD') = to_char(now() - '1 day'::interval, 'YYYY-MM-DD') "
                f"union all "
                f"select count(1) as cond1, 0 as cond2, 0 as cond3, 0 as cond4 "
                f"from users "
                f"where to_char(current_connection_time, 'YYYY-MM-DD') = to_char(now() - '2 day'::interval, 'YYYY-MM-DD') "
                f"union all "
                f"select count(1) as cond1, 0 as cond2, 0 as cond3, 0 as cond4 "
                f"from users "
                f"where to_char(created_at, 'YYYY-MM-DD') = to_char(now() - '1 day'::interval, 'YYYY-MM-DD') "
                f"union all "
                f"select count(1) as cond1, 0 as cond2, 0 as cond3, 0 as cond4 "
                f"from users "
                f"where to_char(created_at, 'YYYY-MM-DD') = to_char(now() - '2 day'::interval, 'YYYY-MM-DD') "
                f" ) as p"
            )

            current_user_one_day = 0
            current_user_two_day = 0
            new_user_one_day = 0
            new_user_two_day = 0
            current_user_percentage = 0
            new_user_percentage = 0

            for idx, row in enumerate(cur):
                if idx == 0:
                    current_user_one_day = row[0]
                elif idx == 1:
                    current_user_two_day = row[0]
                elif idx == 2:
                    new_user_one_day = row[0]
                else:
                    new_user_two_day = row[0]

            print("하루전접속자수 ------> ", current_user_one_day)
            print("이틀전접속자 ------> ", current_user_two_day)
            print("하루전새로가입한유저수 ------> ", new_user_one_day)
            print("이틀전새로가입한유저수 ------> ", new_user_two_day)

            # 전날대비 접속자 비율 (어제 접속자 수 - 이틀전 접속자 수) / 이틀전 접속자 수
            if current_user_two_day != 0:
                current_user_percentage = (current_user_one_day - current_user_two_day) / current_user_two_day * 100
            else:
                current_user_percentage = current_user_one_day * 100

            # 전날대비 가입자 비율 (어제 접속자 수 - 이틀전 접속자 수) / 이틀전 접속자 수
            if new_user_two_day != 0:
                new_user_percentage = (new_user_one_day - new_user_two_day) / new_user_two_day * 100
            else:
                new_user_percentage = new_user_one_day * 100

    except Exception as e:
        logger.info("Error while opening connection or processing. %s", e)

    return dict(
        current_user_one_day=current_user_one_day,
        current_user_two_day=current_user_two_day,
        new_user_one_day=new_user_one_day,
        new_user_two_day=new_user_two_day,
        current_user_percentage=current_user_percentage,
        new_user_percentage=new_user_percentage
    )


def lambda_handler(event, context):
    dau_dict: dict = get_dau()

    now = datetime.datetime.now()
    title = f'🚀 사용자 일일 지표 [{now.strftime("%Y-%m-%d")}] '
    message = f' DAU -> {dau_dict.get("current_user_one_day")}명 \n새로 가입한 유저 -> {dau_dict.get("new_user_one_day")}명 \n전날대비 접속자 비율 -> {dau_dict.get("current_user_percentage")}% \n전날대비 가입자 비율 -> {dau_dict.get("new_user_percentage")}%'

    send_slack_message(message=message, title=title)

    return "Selected %d items from RDS table"
