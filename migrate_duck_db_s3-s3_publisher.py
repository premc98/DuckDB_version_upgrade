import pika
import psycopg2
import json
import ssl
import os
import boto3
import botocore
from aws_secretsmanager_caching import SecretCache, SecretCacheConfig
import time
import uuid

s3 = boto3.client('s3')

subdirectories = ["1", "100", "105", "116", "121", "122", "123", "129",
                  "145", "160", "165", "170", "180", "20", "25", "30", "35",
                  "40", "41", "42", "44", "46", "50", "51", "54", "55", "56",
                  "58", "60", "70", "75", "80", "85", "90", "91", "95", "999",
                  "1121", "1020", "1002", "1001", "1000", "1003", "1090"]

ltree_mapping = {"Fl": "1", "Na": "30", "Mn": "35", "Az": "40", "Ar": "41",
                 "Ia": "42", "Ks": "44", "La": "46", "Hi": "50", "Mo": "51",
                 "Oh": "54", "Ok": "55", "Tx": "56", "Tn": "58", "Sc": "60",
                 "Ky": "70", "In": "75", "Nm": "80", "Co": "85", "Wa": "95",
                 "Il": "100", "Md": "105", "De": "116", "Ak": "1121", "Id": "129",
                 "Or": "145", "Sd": "160", "Nd": "165", "Ne": "170", "Dc": "180", "Ca": "1020", "East": "1001", "West": "1003", "Central": "1002", "Ny": "1090", "National": "1000" }

# get detination to construct destination key
def get_site_for_tracker_from_path_org(subdirectories, ltree_mapping, tracker_id):
     # Create a client and cache for AWS Secrets Manager
    client = botocore.session.get_session().create_client("secretsmanager", region_name='us-east-1')
    cache_config = SecretCacheConfig()
    cache = SecretCache(config=cache_config, client=client)

    # Load DB secrets from AWS Secrets Manager
    db_secret = json.loads(cache.get_secret_string(os.environ.get("DB_SECRET", "perform_database_stage")))

    # Connect to the Postgres database
    conn = psycopg2.connect(
        dbname=db_secret['dbname'],
        user=db_secret['username'],
        password=db_secret['password'],
        host=db_secret['write_host'],
        port="5432"
    )
    cur = conn.cursor()
    query = f"SELECT goal_id, subpath(path_org, 0, 5) AS path_org FROM goal where goal_id = '{tracker_id}' and parent_goal_id is null and goal_category = 'Tracker';"
    cur.execute(query)
    goals = cur.fetchall()

    for goal in goals:
        goal_id, path_org = goal

        if goal_id is not None and path_org is not None:
            ltree_values = path_org.split('.')
            last_value = ltree_values[-1]

            if last_value in subdirectories:
                destination = last_value
            elif last_value in ltree_mapping:
                destination = ltree_mapping[last_value]
            else:
                print(f"Unmapped ltree last value: {last_value} for path_org: {path_org}, {goal_id}")
                destination = 'NA'
                continue

    return destination
