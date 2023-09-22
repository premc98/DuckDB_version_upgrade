import boto3
import os
import subprocess
import json
import shutil
import botocore
import struct
import pika
import ssl
import functools
import threading
from aws_secretsmanager_caching import SecretCache, SecretCacheConfig
import logging
from datetime import datetime
import uuid
import psycopg2


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Directories for downloaded and processed files
DOWNLOAD_DIR = "batch_download"
UPLOAD_DIR = "batch_upload"
EXPORT_DIR = 'exportfolder'

s3 = boto3.client('s3')

duckdb_5 = "duckdb0.5.1"
duckdb_6 = "duckdb0.6.1"
duckdb_7 = "duckdb0.7.1"
duckdb_8 = "duckdb0.8.1"
duckdb_unsupported = "unsupported"

# Function that checks duckdb version of the file
def duckdb_version(filename):
    pattern = struct.Struct('<8x4sQ')
    with open(filename, 'rb') as fh:
        version = pattern.unpack(fh.read(pattern.size))
        return {
            38: duckdb_5,
            39: duckdb_6,
            43: duckdb_7,
            51: duckdb_8,
        }.get(version[1], duckdb_unsupported)

def generate_unique_id():
    # Generate a random UUID and convert it to a 64-bit integer
    id = uuid.uuid4().int >> 64

    # To ensure it fits into int8, we can take its absolute value
    # and make sure it's less than the maximum value for int8
    id = abs(id) % 9223372036854775807
    return id


def insert_postgres_lb_log(event, pg_result, pg_message, pg_log_type, subject ):

    tracker_id = event['tracker-id']
    lb_log_id = generate_unique_id()
    context = [{"value": str(tracker_id), "lb_table": "goal", "lb_column": "goal_id"}]
    log_level = "1"
    intent_key = event['intent_key']
    intent = event['intent']
    position_id = event['position_id']
    created_by = event['user_id']
    updated_by = event['user_id']
    user_id = event['user_id']
    user_key = event['user_key']
    result = pg_result
    message = pg_message
    log_type = pg_log_type
    subject = subject

    # Create a client and cache for AWS Secrets Manager
    client = botocore.session.get_session().create_client("secretsmanager", region_name='us-east-1')
    cache_config = SecretCacheConfig()
    cache = SecretCache(config=cache_config, client=client)

    # Load DB secrets from AWS Secrets Manager
    db_secret = json.loads(cache.get_secret_string(os.environ.get("DB_SECRET", "perform_database_dev")))

    # Connect to the Postgres database
    connection = psycopg2.connect(
        dbname=db_secret['dbname'],
        user=db_secret['username'],
        password=db_secret['password'],
        host=db_secret['write_host'],
        port="5432"
    )

    insert_sql = """
        INSERT INTO lb_log
            (lb_log_id, log_level, intent_key, context, position_id, created_by, updated_by,
             user_id, user_key, result, message, created_at, updated_at, log_type, intent, subject)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now(), %s, %s, %s)
    """

    cursor = connection.cursor()
    # Execute the INSERT statement

    cursor.execute(insert_sql, (lb_log_id, log_level, intent_key, json.dumps(context), position_id, created_by, updated_by,
                             user_id, user_key, result, message, log_type, intent, subject))

    # Commit the transaction
    connection.commit()
    cursor.close()
    connection.close()


    return {
        "lb_log_id": lb_log_id,
        "log_level": log_level,
        "intent_key": intent_key,
        "context": context,
        "position_id": position_id,
        "created_by": created_by,
        "updated_by": updated_by,
        "user_id": user_id,
        "user_key": user_key,
        "result": result,
        "message": message,
        "log_type": log_type,
        "subject": subject,
        "intent": intent

    }