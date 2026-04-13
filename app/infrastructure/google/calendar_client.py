from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from app.core.config import settings


SCOPES = ["https://www.googleapis.com/auth/calendar"]


class GoogleCalendarClientError(Exception):
    pass


@dataclass
class CalendarSlot:
    start: datetime
    end: datetime


@dataclass
class CreatedCalendarEvent:
    event_id: str
    html_link: str
    status: str


class GoogleCalendarClient:
    def __init__(self) -> None:
        self.enabled = settings.google_calendar_enabled
        self.calendar_id = settings.google_calendar_id
        self.service_account_file = settings.google_service_account_file
        self.timezone = settings.google_calendar_timezone
        self._service = None

    def is_configured(self) -> bool:
        return bool(
            self.enabled
            and self.calendar_id
            and self.service_account_file
            and Path(self.service_account_file).exists()
        )

    def _get_service(self):
        if self._service is not None:
            return self._service

        if not self.is_configured():
            raise GoogleCalendarClientError("Google Calendar is not fully configured.")

        credentials = service_account.Credentials.from_service_account_file(
            self.service_account_file,
            scopes=SCOPES,
        )

        self._service = build(
            "calendar",
            "v3",
            credentials=credentials,
            cache_discovery=False,
        )
        return self._service

    @staticmethod
    def _to_rfc3339(dt: datetime) -> str:
        if dt.tzinfo is None:
            raise GoogleCalendarClientError("Datetime must be timezone-aware.")
        return dt.isoformat()

    def healthcheck(self) -> dict[str, Any]:
        if not self.is_configured():
            return {
                "enabled": self.enabled,
                "configured": False,
                "connected": False,
                "reason": "Missing Google Calendar env or credentials file.",
            }

        try:
            service = self._get_service()
            calendar = service.calendars().get(calendarId=self.calendar_id).execute()

            return {
                "enabled": True,
                "configured": True,
                "connected": True,
                "calendar_id": self.calendar_id,
                "calendar_summary": calendar.get("summary"),
                "calendar_timezone": calendar.get("timeZone"),
            }
        except Exception as exc:
            return {
                "enabled": True,
                "configured": True,
                "connected": False,
                "reason": str(exc),
            }

    def get_busy_periods(self, time_min: datetime, time_max: datetime) -> list[CalendarSlot]:
        try:
            service = self._get_service()

            body = {
                "timeMin": self._to_rfc3339(time_min),
                "timeMax": self._to_rfc3339(time_max),
                "timeZone": self.timezone,
                "items": [{"id": self.calendar_id}],
            }

            response = service.freebusy().query(body=body).execute()
            calendar_data = response.get("calendars", {}).get(self.calendar_id, {})
            busy_items = calendar_data.get("busy", [])

            result: list[CalendarSlot] = []
            for item in busy_items:
                result.append(
                    CalendarSlot(
                        start=datetime.fromisoformat(item["start"]),
                        end=datetime.fromisoformat(item["end"]),
                    )
                )
            return result

        except HttpError as exc:
            raise GoogleCalendarClientError(
                f"Google Calendar freebusy failed: {exc}"
            ) from exc
        except Exception as exc:
            raise GoogleCalendarClientError(
                f"Unexpected Google Calendar error: {exc}"
            ) from exc

    def is_time_available(self, start_dt: datetime, end_dt: datetime) -> bool:
        if end_dt <= start_dt:
            raise GoogleCalendarClientError("end_dt must be after start_dt")

        busy_periods = self.get_busy_periods(start_dt, end_dt)

        for busy in busy_periods:
            overlaps = start_dt < busy.end and end_dt > busy.start
            if overlaps:
                return False

        return True

    def create_event(
        self,
        start_dt: datetime,
        end_dt: datetime,
        summary: str,
        description: str = "",
    ) -> CreatedCalendarEvent:
        if end_dt <= start_dt:
            raise GoogleCalendarClientError("end_dt must be after start_dt")

        try:
            service = self._get_service()

            event_body = {
                "summary": summary,
                "description": description,
                "start": {
                    "dateTime": self._to_rfc3339(start_dt),
                    "timeZone": self.timezone,
                },
                "end": {
                    "dateTime": self._to_rfc3339(end_dt),
                    "timeZone": self.timezone,
                },
            }

            created = (
                service.events()
                .insert(calendarId=self.calendar_id, body=event_body)
                .execute()
            )

            return CreatedCalendarEvent(
                event_id=created["id"],
                html_link=created.get("htmlLink", ""),
                status=created.get("status", "confirmed"),
            )

        except HttpError as exc:
            raise GoogleCalendarClientError(
                f"Google Calendar create event failed: {exc}"
            ) from exc
        except Exception as exc:
            raise GoogleCalendarClientError(
                f"Unexpected Google Calendar create event error: {exc}"
            ) from exc
            