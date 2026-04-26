from __future__ import annotations

import json
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from .http import urlopen_with_certifi


DEFAULT_BASE_URL = "https://api.bambulab.com/v1"

DEFAULT_HEADERS = {
    "User-Agent": "bambu_network_agent/01.09.05.01",
    "X-BBL-Client-Name": "OrcaSlicer",
    "X-BBL-Client-Type": "slicer",
    "X-BBL-Client-Version": "01.09.05.51",
    "X-BBL-Language": "en-US",
    "X-BBL-OS-Type": "linux",
    "X-BBL-OS-Version": "6.2.0",
    "X-BBL-Agent-Version": "01.09.05.01",
    "X-BBL-Executable-info": "{}",
    "X-BBL-Agent-OS-Type": "linux",
    "Accept": "application/json",
    "Content-Type": "application/json",
}


@dataclass(frozen=True)
class CloudLoginResult:
    status: str
    access_token: str = ""
    message: str = ""


@dataclass(frozen=True)
class CloudFilamentUsage:
    filament_id: str
    weight_g: float
    name: str = ""
    material: str = ""
    color_hex: str = ""


@dataclass(frozen=True)
class CloudTaskDetail:
    task_id: str
    job_id: str
    title: str
    cover_url: str
    total_weight_g: float
    filaments: List[CloudFilamentUsage]
    raw: Dict[str, Any]


class BambuCloudError(RuntimeError):
    pass


class BambuCloudClient:
    def __init__(
        self,
        access_token: str = "",
        base_url: str = DEFAULT_BASE_URL,
        timeout_seconds: float = 20.0,
    ) -> None:
        self.access_token = access_token
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds

    def login(self, email: str, password: str = "", verification_code: str = "") -> CloudLoginResult:
        if not email:
            return CloudLoginResult("bad_credentials", message="Missing Bambu Cloud email")
        if not password and not verification_code:
            return CloudLoginResult("bad_credentials", message="Missing Bambu Cloud password or verification code")

        payload = {"account": email, "password": password}
        if verification_code:
            payload = {"account": email, "code": verification_code}

        data = self._request_json("POST", "/user-service/user/login", payload=payload, auth=False)
        token = str(data.get("accessToken") or "")
        if token:
            self.access_token = token
            return CloudLoginResult("success", access_token=token, message="Login successful")

        if data.get("loginType") == "verifyCode":
            self.send_verification_code(email)
            return CloudLoginResult(
                "needs_verification_code",
                message="Bambu Cloud requested email verification; a code was sent to your email",
            )

        return CloudLoginResult("unknown_error", message="Login response did not include an access token")

    def send_verification_code(self, email: str) -> bool:
        payload = {"email": email, "type": "codeLogin"}
        self._request_json("POST", "/user-service/user/sendemail/code", payload=payload, auth=False)
        return True

    def test_token(self) -> Dict[str, Any]:
        return self._request_json("GET", "/iot-service/api/user/bind", auth=True)

    def get_job_id(self, task_id: str) -> Optional[str]:
        if not task_id or str(task_id) == "0":
            return None
        data = self._request_json("GET", "/iot-service/api/user/task/%s" % task_id, auth=True)
        value = data.get("job_id")
        return str(value) if value else None

    def get_task_detail_for_task_id(self, task_id: str) -> Optional[CloudTaskDetail]:
        job_id = self.get_job_id(task_id)
        if not job_id:
            return None
        return self.get_task_detail_by_job_id(task_id=task_id, job_id=job_id)

    def get_task_detail_by_job_id(self, task_id: str, job_id: str) -> Optional[CloudTaskDetail]:
        data = self._request_json("GET", "/user-service/my/tasks", auth=True)
        hits = data.get("hits")
        if not isinstance(hits, list):
            return None
        for hit in hits:
            if not isinstance(hit, dict):
                continue
            if str(hit.get("id") or "") == str(job_id):
                return parse_task_detail(task_id=task_id, job_id=job_id, payload=hit)
        return None

    def _request_json(
        self,
        method: str,
        path: str,
        payload: Optional[Dict[str, Any]] = None,
        auth: bool = True,
    ) -> Dict[str, Any]:
        headers = dict(DEFAULT_HEADERS)
        if auth:
            if not self.access_token:
                raise BambuCloudError("Missing Bambu Cloud access token")
            headers["Authorization"] = "Bearer %s" % self.access_token

        body = None
        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
        request = urllib.request.Request(
            self.base_url + path,
            data=body,
            headers=headers,
            method=method,
        )
        try:
            with urlopen_with_certifi(request, timeout=self.timeout_seconds) as response:
                text = response.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as exc:
            error_text = exc.read().decode("utf-8", errors="replace")
            raise BambuCloudError("Bambu Cloud HTTP %s: %s" % (exc.code, error_text)) from exc
        except urllib.error.URLError as exc:
            raise BambuCloudError("Bambu Cloud request failed: %s" % exc) from exc

        if not text:
            return {}
        try:
            data = json.loads(text)
        except json.JSONDecodeError as exc:
            raise BambuCloudError("Bambu Cloud returned invalid JSON") from exc
        if not isinstance(data, dict):
            raise BambuCloudError("Bambu Cloud returned unexpected JSON")
        return data


def parse_task_detail(task_id: str, job_id: str, payload: Dict[str, Any]) -> CloudTaskDetail:
    total_weight = number_or_zero(
        first_present(payload, "weight", "totalWeight", "total_weight", "printWeight", "print_weight")
    )
    mappings = first_present(payload, "amsDetailMapping", "ams_detail_mapping", "filaments") or []
    filaments: List[CloudFilamentUsage] = []
    if isinstance(mappings, list):
        for item in mappings:
            if not isinstance(item, dict):
                continue
            weight = number_or_zero(first_present(item, "weight", "usedWeight", "used_weight", "printWeight"))
            if weight <= 0:
                continue
            filaments.append(
                CloudFilamentUsage(
                    filament_id=str(first_present(item, "filamentId", "filament_id", "id") or ""),
                    weight_g=weight,
                    name=str(
                        first_present(
                            item,
                            "filamentName",
                            "filament_name",
                            "name",
                            "displayName",
                            "display_name",
                        )
                        or ""
                    ),
                    material=str(
                        first_present(
                            item,
                            "filamentType",
                            "filament_type",
                            "type",
                            "material",
                            "materialType",
                            "material_type",
                        )
                        or ""
                    ),
                    color_hex=normalize_color(
                        str(
                            first_present(
                                item,
                                "color",
                                "colorHex",
                                "color_hex",
                                "filamentColor",
                                "filament_color",
                                "filamentColour",
                                "filament_colour",
                            )
                            or ""
                        )
                    ),
                )
            )
    if total_weight <= 0:
        total_weight = sum(filament.weight_g for filament in filaments)
    return CloudTaskDetail(
        task_id=str(task_id),
        job_id=str(job_id),
        title=str(first_present(payload, "title", "name", "subject") or ""),
        cover_url=str(first_present(payload, "cover", "coverUrl", "cover_url") or ""),
        total_weight_g=total_weight,
        filaments=filaments,
        raw=payload,
    )


def first_present(data: Dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in data:
            return data[key]
    return None


def number_or_zero(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def normalize_color(value: str) -> str:
    text = (value or "").strip().upper()
    if text.startswith("#"):
        text = text[1:]
    if len(text) == 8 and text.endswith("FF"):
        text = text[:6]
    return text
