from __future__ import annotations

import ssl
import urllib.request
from typing import Optional

import certifi


def urlopen_with_certifi(
    request: urllib.request.Request,
    timeout: Optional[float] = None,
):
    context = ssl.create_default_context(cafile=certifi.where())
    return urllib.request.urlopen(request, timeout=timeout, context=context)
