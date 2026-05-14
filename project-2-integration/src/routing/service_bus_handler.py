"""
Azure Service Bus Handler
--------------------------
Sends messages to an Azure Service Bus queue for guaranteed delivery.
Used by the Procore poller for batch events that need reliable queuing.
In local dev mode, logs the message instead.
"""

import json
import logging
import os

from azure.servicebus import ServiceBusClient, ServiceBusMessage
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

CONNECTION_STRING = os.getenv("AZURE_SERVICEBUS_CONNECTION_STRING", "")
QUEUE_NAME        = os.getenv("AZURE_SERVICEBUS_QUEUE_NAME", "integration-events")


def send_to_queue(payload: dict) -> bool:
    """
    Send a single payload dict to the Service Bus queue.
    Falls back to local logging if connection string is not configured.
    """
    if not CONNECTION_STRING:
        logger.info(
            f"[DEV MODE] Service Bus not configured — logging message locally\n"
            f"Queue: {QUEUE_NAME}\n"
            f"Payload: {json.dumps(payload, indent=2)}"
        )
        return True

    try:
        with ServiceBusClient.from_connection_string(CONNECTION_STRING) as client:
            with client.get_queue_sender(QUEUE_NAME) as sender:
                message = ServiceBusMessage(json.dumps(payload))
                sender.send_messages(message)
                logger.info(f"Message sent to Service Bus queue: {QUEUE_NAME}")
                return True

    except Exception as e:
        logger.error(f"Service Bus send failed: {e}")
        return False


def receive_from_queue(max_messages: int = 10) -> list[dict]:
    """
    Pull up to max_messages from the Service Bus queue.
    Used by the bronze_writer consumer to drain the queue into Delta Lake.
    """
    if not CONNECTION_STRING:
        logger.info("[DEV MODE] Service Bus not configured — returning empty list")
        return []

    messages = []
    try:
        with ServiceBusClient.from_connection_string(CONNECTION_STRING) as client:
            with client.get_queue_receiver(QUEUE_NAME, max_wait_time=5) as receiver:
                for msg in receiver.receive_messages(max_message_count=max_messages):
                    messages.append(json.loads(str(msg)))
                    receiver.complete_message(msg)
        logger.info(f"Received {len(messages)} messages from Service Bus")
    except Exception as e:
        logger.error(f"Service Bus receive failed: {e}")

    return messages
