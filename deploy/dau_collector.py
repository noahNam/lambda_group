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
                f"select sum(p.total_user)    as total_user,"
                f"       sum(p.new_user)      as new_user,"
                f"       sum(p.out_user)      as out_user,"
                f"       sum(p.post_count)    as post_count,"
                f"       sum(p.comment_count) as comment_count"
                f" from (select count(1) as total_user,"
                f"             0        as new_user,"
                f"             0        as out_user,"
                f"             0        as post_count,"
                f"             0        as comment_count"
                f"      from users"
                f"      where is_available = TRUE"
                f"      union all"
                f"      select 0        as total_user,"
                f"             count(1) as new_user,"
                f"             0        as out_user,"
                f"             0        as post_count,"
                f"             0        as comment_count"
                f"      from users"
                f"      where join_date = to_char(now(), 'YYYYMMDD')"
                f"      union all"
                f"      select 0        as total_user,"
                f"             0        as new_user,"
                f"             count(1) as out_user,"
                f"             0        as post_count,"
                f"             0        as comment_count"
                f"      from users"
                f"      where is_out = True"
                f"        and to_char(now(), 'YYYYMMDD') = to_char(updated_at, 'YYYYMMDD')"
                f"      union all"
                f"      select 0        as total_user,"
                f"             0        as new_user,"
                f"             0        as out_user,"
                f"             count(1) as post_count,"
                f"             0        as comment_count"
                f"      from posts"
                f"      where is_deleted = false"
                f"          and to_char(created_at, 'YYYYMMDD') = to_char(now(), 'YYYYMMDD')"
                f"      union all"
                f"      select 0        as total_user,"
                f"             0        as new_user,"
                f"             0        as out_user,"
                f"             0        as post_count,"
                f"             count(1) as comment_count"
                f"      from comments"
                f"      where is_deleted = false"
                f"          and to_char(created_at, 'YYYYMMDD') = to_char(now(), 'YYYYMMDD')) as p"
            )

            total_user = 0
            new_user = 0
            out_user = 0
            post_count = 0
            comment_count = 0

            for idx, row in enumerate(cur):
                if idx == 0:
                    total_user = row[0]
                elif idx == 1:
                    new_user = row[0]
                elif idx == 2:
                    out_user = row[0]
                elif idx == 3:
                    post_count = row[0]
                else:
                    comment_count = row[0]

            print("총 활성화 유저 ------> ", total_user)
            print("신규 가입 유저 ------> ", new_user)
            print("탈퇴 유저 ------> ", out_user)
            print("새 글수 ------> ", post_count)
            print("새 댓글수 ------> ", comment_count)

    except Exception as e:
        logger.info("Error while opening connection or processing. %s", e)

    return dict(
        total_user=total_user,
        new_user=new_user,
        out_user=out_user,
        post_count=post_count,
        comment_count=comment_count,
    )


def lambda_handler(event, context):
    dau_dict: dict = get_dau()

    title = f'🚀 사용자 일일 지표'
    message = f' 총 활성화 유저 -> {dau_dict.get("total_user")}명 \n신규 유저 -> {dau_dict.get("new_user")}명 \n탈퇴 유저 -> {dau_dict.get("out_user")}명 \n새 글수 -> {dau_dict.get("post_count")}개 \n새 댓글수 -> {dau_dict.get("comment_count")}개'

    send_slack_message(message=message, title=title)

    return "Selected %d items from RDS table"
