"""
Procore API Poller
------------------
Polls Procore's REST API on a schedule for new project activity
(RFIs, submittals, observations). Normalizes each record and sends
it to Azure Service Bus for reliable queuing into the Bronze layer.
"""

import logging
import os
from datetime import datetime, timedelta, timezone

import requests
from dotenv import load_dotenv

from src.routing.service_bus_handler import send_to_queue

load_dotenv()

logger        = logging.getLogger(__name__)
CLIENT_ID     = os.getenv("PROCORE_CLIENT_ID", "")
CLIENT_SECRET = os.getenv("PROCORE_CLIENT_SECRET", "")
COMPANY_ID    = os.getenv("PROCORE_COMPANY_ID", "")
BASE_URL      = "https://sandbox.procore.com"


class ProcoreClient:

    def __init__(self):
        self.token        = None
        self.token_expiry = None
        self.session      = requests.Session()
        self.mock_mode    = not CLIENT_ID or not CLIENT_SECRET

    def get_rfis(self, project_id: str, updated_after: datetime) -> list[dict]:
        if self.mock_mode:
            return self._mock_rfis(project_id)

        headers = {"Authorization": f"Bearer {self._get_token()}"}
        params  = {
            "project_id":    project_id,
            "updated_after": updated_after.isoformat(),
            "per_page":      100,
        }
        r = self.session.get(
            f"{BASE_URL}/rest/v1.0/rfis",
            headers=headers,
            params=params,
        )
        r.raise_for_status()
        return r.json()

    def _get_token(self) -> str:
        if self.token and datetime.now(timezone.utc) < self.token_expiry:
            return self.token
        response = self.session.post(
            f"{BASE_URL}/oauth/token",
            data={
                "grant_type":    "client_credentials",
                "client_id":     CLIENT_ID,
                "client_secret": CLIENT_SECRET,
            },
        )
        response.raise_for_status()
        data             = response.json()
        self.token       = data["access_token"]
        self.token_expiry = datetime.now(timezone.utc) + timedelta(
            seconds=data.get("expires_in", 3600) - 60
        )
        return self.token

    def _mock_rfis(self, project_id: str) -> list[dict]:
        logger.info(f"[MOCK] Generating mock RFIs for project {project_id}")
        return [
            {
                "id":         f"RFI-{i:04d}",
                "project_id": project_id,
                "subject":    f"Mock RFI subject {i}",
                "status":     "open" if i % 2 == 0 else "closed",
                "created_at": datetime.now(timezone.utc).isoformat(),
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "created_by": {"name": "Mock User", "email": "mock@example.com"},
            }
            for i in range(1, 6)
        ]


def normalize_rfi(raw: dict, company_id: str) -> dict:
    return {
        "source":      "procore",
        "event_type":  "rfi_updated",
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "company_id":  company_id,
        "project_id":  str(raw.get("project_id", "")),
        "rfi_id":      str(raw.get("id", "")),
        "subject":     raw.get("subject", ""),
        "status":      raw.get("status", ""),
        "created_by":  raw.get("created_by", {}).get("email", ""),
        "created_at":  raw.get("created_at", ""),
        "updated_at":  raw.get("updated_at", ""),
    }


def poll_and_queue(project_ids: list[str], lookback_hours: int = 1) -> int:
    client        = ProcoreClient()
    updated_after = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
    total_queued  = 0

    for project_id in project_ids:
        try:
            rfis = client.get_rfis(project_id, updated_after)
            for rfi in rfis:
                record = normalize_rfi(rfi, COMPANY_ID)
                send_to_queue(record)
                total_queued += 1
            logger.info(f"Project {project_id}: queued {len(rfis)} RFIs")
        except Exception as e:
            logger.error(f"Failed to poll project {project_id}: {e}")

    logger.info(f"Procore poll complete — {total_queued} records queued")
    return total_queued


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    count = poll_and_queue(project_ids=["MOCK-001", "MOCK-002"])
    print(f"Queued: {count} records")
