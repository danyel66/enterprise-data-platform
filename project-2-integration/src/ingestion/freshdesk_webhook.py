"""
Freshdesk Webhook Receiver
--------------------------
Receives incoming webhook POST requests from Freshdesk when tickets
are created or updated. Validates the payload, normalizes the event
structure, and hands it off to the Event Grid handler for routing.
"""

import hashlib
import hmac
import json
import logging
import os
from datetime import datetime, timezone

from dotenv import load_dotenv
from fastapi import APIRouter, Header, HTTPException, Request

from src.routing.event_grid_handler import publish_event

load_dotenv()

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/webhooks", tags=["Freshdesk"])

WEBHOOK_SECRET = os.getenv("FRESHDESK_WEBHOOK_SECRET", "")


def verify_signature(payload: bytes, signature: str) -> bool:
    """
    Validate the HMAC-SHA256 signature Freshdesk sends with every webhook.
    If the secret is not configured, skip verification (dev mode).
    """
    if not WEBHOOK_SECRET:
        logger.warning("FRESHDESK_WEBHOOK_SECRET not set — skipping signature check")
        return True

    expected = hmac.new(
        WEBHOOK_SECRET.encode(), payload, hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, signature or "")


def normalize_ticket(raw: dict) -> dict:
    """
    Flatten Freshdesk's nested ticket payload into a consistent
    Bronze-layer record format shared across all integration sources.
    """
    ticket = raw.get("freshdesk_webhook", raw)
    return {
        "source":           "freshdesk",
        "event_type":       "ticket_created" if not ticket.get("ticket_id") else "ticket_updated",
        "ingested_at":      datetime.now(timezone.utc).isoformat(),
        "ticket_id":        str(ticket.get("ticket_id", "")),
        "subject":          ticket.get("ticket_subject", ""),
        "status":           ticket.get("ticket_status", ""),
        "priority":         ticket.get("ticket_priority", ""),
        "requester_email":  ticket.get("requester", {}).get("email", ""),
        "agent_email":      ticket.get("agent", {}).get("email", ""),
        "group_name":       ticket.get("group", {}).get("name", ""),
        "created_at":       ticket.get("ticket_created_at", ""),
        "updated_at":       ticket.get("ticket_updated_at", ""),
        "raw_payload":      json.dumps(raw),
    }


@router.post("/freshdesk")
async def freshdesk_webhook(
    request: Request,
    x_freshdesk_signature: str = Header(default=""),
):
    """
    Endpoint Freshdesk calls when a ticket event fires.
    1. Verify signature
    2. Normalize payload
    3. Publish to Event Grid for routing to Bronze layer
    """
    body = await request.body()

    if not verify_signature(body, x_freshdesk_signature):
        logger.error("Freshdesk webhook signature mismatch")
        raise HTTPException(status_code=401, detail="Invalid signature")

    try:
        raw = json.loads(body)
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {e}")

    normalized = normalize_ticket(raw)
    logger.info(f"Freshdesk ticket received: {normalized['ticket_id']}")

    await publish_event(
        event_type="integration.freshdesk.ticket",
        subject=f"tickets/{normalized['ticket_id']}",
        data=normalized,
    )

    return {"status": "accepted", "ticket_id": normalized["ticket_id"]}
