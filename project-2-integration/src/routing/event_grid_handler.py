"""
Azure Event Grid Publisher
--------------------------
Publishes normalized integration events to an Azure Event Grid topic.
In local dev mode (no endpoint configured), logs the event instead
so the rest of the pipeline can be tested without Azure credentials.
"""

import json
import logging
import os
import uuid
from datetime import datetime, timezone

import httpx
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

EVENTGRID_ENDPOINT = os.getenv("AZURE_EVENTGRID_ENDPOINT", "")
EVENTGRID_KEY      = os.getenv("AZURE_EVENTGRID_KEY", "")


async def publish_event(event_type: str, subject: str, data: dict) -> bool:
    """
    Publish a single event to Azure Event Grid.
    Falls back to local logging if endpoint is not configured.

    Args:
        event_type: e.g. "integration.freshdesk.ticket"
        subject:    e.g. "tickets/12345"
        data:       normalized event payload dict

    Returns:
        True if published (or logged in dev mode), False on error
    """
    event = [
        {
            "id":          str(uuid.uuid4()),
            "eventType":   event_type,
            "subject":     subject,
            "eventTime":   datetime.now(timezone.utc).isoformat(),
            "dataVersion": "1.0",
            "data":        data,
        }
    ]

    if not EVENTGRID_ENDPOINT or not EVENTGRID_KEY:
        logger.info(
            f"[DEV MODE] Event Grid not configured — logging event locally\n"
            f"Type: {event_type} | Subject: {subject}\n"
            f"Data: {json.dumps(data, indent=2)}"
        )
        return True

    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                EVENTGRID_ENDPOINT,
                headers={
                    "aeg-sas-key":  EVENTGRID_KEY,
                    "Content-Type": "application/json",
                },
                json=event,
                timeout=10,
            )
            response.raise_for_status()
            logger.info(f"Event published to Event Grid: {event_type} / {subject}")
            return True

    except httpx.HTTPError as e:
        logger.error(f"Event Grid publish failed: {e}")
        return False
