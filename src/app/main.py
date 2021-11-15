import asyncio
import json
import os
from datetime import time, timedelta
from typing import List, Set
import time

import aioboto3
from boto3.dynamodb.conditions import Key
from botocore.errorfactory import ClientError

session = aioboto3.Session()


def handler(event, context):
    """
    :param event: SQS message
    :param context: Lambda Context
    :return: AsyncIO loop
    """
    print(event)
    parser_func = parse_sns_record if "Sns" in event["Records"][0] else parse_sqs_record
    pusher = Pusher(
        event["Records"],
        parser_func
    )
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(pusher.notify_all_records())


class Pusher:
    """
    Asynchronous Batch Notifications Pusher
    """

    def __init__(self, records: List, parse_record_func):
        """
        :param records: SQS Records (Notifications Tasks)
        """
        self.endpoint_url = os.environ["APIGW_ENDPOINT"]
        self.connections_table_name = os.environ["CONNECTIONS_TABLE_NAME"]
        self.records = records
        self.start_time = time.time()
        self.stale_connections = []
        self.total_notified_connections = 0
        self.deleted_stale_connections = 0
        self.parse_record_func = parse_record_func

    @staticmethod
    async def retrieve_all_users_connections(table, exclude_users_ids: List[str]):
        """
        Coroutine to retrieve single user connections
        :param table: connections table
        :param exclude_users_ids: list users to exclude
        :return: List of connections
        """
        params = get_exclusion_filter(exclude_users_ids)
        result = await table.scan(**params)
        connections = result.get("Items")
        while result.get("LastEvaluatedKey"):
            result = await table.scan(ExclusiveStartKey=result["LastEvaluatedKey"], **params)
            connections.extend(result["Items"])
        return connections

    @staticmethod
    async def retrieve_user_connections(table, user_id: str):
        """
        Coroutine to retrieve single user connections
        :param table: connections table
        :param user_id: the user id (Hash Key)
        :return: List of connections
        """
        result = await table.query(
            KeyConditionExpression=Key("user_id").eq(user_id)
        )
        return result.get("Items", [])

    async def notify_connection(self, apigw, user_id: str, connection_id: str, data: str):
        """
        Coroutine to notify single user connection
        :param apigw: APIGatewayManagementAPI client
        :param user_id: the user id (Hash Key)
        :param connection_id: API Gateway connection id
        :param data: binary data
        """
        try:
            await apigw.post_to_connection(
                Data=data,
                ConnectionId=connection_id
            )
            self.total_notified_connections += 1
        except ClientError as error:
            if error.response['Error']['Code'] == 'GoneException':
                self.stale_connections.append(
                    {'user_id': user_id, 'connection_id': connection_id}
                )

            else:
                print(error)

    async def notify_user(self, table, apigw, user_id: str, data):
        """
        Coroutine to notify all connections of a single user
        :param table: connections table
        :param apigw: APIGatewayManagementAPI client
        :param user_id: user_id
        :param data: binary data
        :return: binary data to send to single user
        """
        connections = await self.retrieve_user_connections(table, user_id)
        # If user has active connections (Online), then notify.
        if len(connections):
            notifications = [
                self.notify_connection(
                    apigw, user_id, connection["connection_id"], data
                )
                for connection in connections
            ]

            await asyncio.wait(notifications)

    async def notify_selected_users(self, table, apigw, users_ids: Set[str], data):
        """
        Coroutine to notify all connections of selected users
        :param table: connections table
        :param apigw: APIGatewayManagementAPI client
        :param users_ids: List of users' ids
        :param data: binary data to send to all users
        """
        notifications = [
            self.notify_user(table, apigw, user_id, data) for user_id in users_ids
        ]

        await asyncio.wait(notifications)

    async def notify_all_users(self, table, apigw, exclude_users_ids: List[str], data):
        """
        Coroutine to notify all connections of all users
        :param table: connections table
        :param apigw: APIGatewayManagementAPI client
        :param exclude_users_ids: APIGatewayManagementAPI client
        :param data: binary data to send to all users
        """
        connections = await self.retrieve_all_users_connections(table, exclude_users_ids)
        if len(connections):
            notifications = [
                self.notify_connection(
                    apigw, connection["user_id"], connection["connection_id"], data
                )
                for connection in connections
            ]

            await asyncio.wait(notifications)

    async def delete_all_stale_connections(self, table):
        """
        Coroutine to delete all stale connections
        :param table: connections table
        """
        async with table.batch_writer() as batch:
            for stale_connection in self.stale_connections:
                await batch.delete_item(Key=stale_connection)

    async def notify_all_records(self):
        """
        Coroutine to notify all connections of all or selected users in all SQS batch records
        """
        async with session.resource("dynamodb") as ddb:
            table = await ddb.Table(self.connections_table_name)
            async with session.client("apigatewaymanagementapi", endpoint_url=self.endpoint_url) as apigw:
                notifications = []
                for record in self.records:
                    users, exclude_users, data = self.parse_record_func(record)
                    if users:
                        notifications.append(self.notify_selected_users(table, apigw, users, data))
                    else:
                        notifications.append(self.notify_all_users(table, apigw, exclude_users, data))
                await asyncio.wait(notifications)
            await self.delete_all_stale_connections(table)
        await self.print_stats()

    async def print_stats(self):
        elapsed = (time.time() - self.start_time)
        total_elapsed_human = str(timedelta(seconds=elapsed))
        print(f"[STATS] Processed {len(self.records)} SQS records")
        print(f"[STATS] Notified {self.total_notified_connections} connections")
        print(f"[STATS] Finished in {total_elapsed_human}")
        print(f"[STATS] Deleted {len(self.stale_connections)} stale connections")


####################
# Helpers Functions
####################
def get_unique_users(users: List[str]):
    return set(users or [])


def parse_sqs_record(record):
    body = json.loads(record["body"])
    users = get_unique_users(body.get("users", []))
    exclude_users = get_unique_users(body.get("exclude_users", []))
    data = json.dumps(body["data"])
    return users, exclude_users, data


def parse_sns_record(record):
    message = json.loads(record["Sns"]["Message"])
    users = get_unique_users(message.get("users", []))
    exclude_users = get_unique_users(message.get("exclude_users", []))
    data = json.dumps(message["data"])
    return users, exclude_users, data


def get_exclusion_filter(exclude_users_ids):
    if exclude_users_ids:
        excluded_users = ', '.join([f":id{idx}" for idx, _ in enumerate(exclude_users_ids)])
        return dict(
            ExpressionAttributeNames={
                "#user_id": "user_id"
            },
            FilterExpression=f"NOT(#user_id in ({excluded_users}))",
            ExpressionAttributeValues={f":id{idx}": user_id for idx, user_id in enumerate(exclude_users_ids)}
        )
    else:
        return {}
