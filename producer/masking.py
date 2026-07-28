"""로그·페이로드에 시크릿이 남지 않게 하는 마스킹 유틸.

외부 의존성이 없다. 테스트에서 단독으로 import 하기 위해서다.

공공데이터포털 API는 헤더 인증을 지원하지 않아 서비스 키를 쿼리스트링으로
보낸다. 그래서 requests의 연결 예외 문자열에는 serviceKey를 포함한 전체 URL이
들어간다. 그 문자열이 last_error에 담기면 로그뿐 아니라 data_warnings를 통해
Kafka 페이로드와 알림 이메일 본문까지 흘러간다. 로그 마스킹만으로는 부족하고
값이 만들어지는 지점에서 지워야 하는 이유다.
"""

import logging
import re

MASK = "***"

# (패턴, 치환) — 캡처 그룹 1은 키 이름 부분이라 남기고 값만 가린다.
_RULES = (
    # serviceKey=... / ServiceKey=... (URL 쿼리스트링, & 또는 공백까지)
    (re.compile(r"(serviceKey=)[^&\s\"'<>]+", re.IGNORECASE), r"\1" + MASK),
    # "access_token": "...", refresh_token=..., 'client_secret': '...'
    (re.compile(
        r"((?:access_token|refresh_token|client_secret|rest_api_key|api_key|password)"
        r"[\"']?\s*[:=]\s*[\"']?)[^\s,&}\"']+",
        re.IGNORECASE,
    ), r"\1" + MASK),
    # Authorization: Bearer xxx
    (re.compile(r"(Bearer\s+)[\w.\-]+", re.IGNORECASE), r"\1" + MASK),
)


def mask_secrets(value):
    """문자열에 섞인 시크릿 값을 가린다. 문자열이 아니면 그대로 돌려준다."""
    if not isinstance(value, str):
        return value
    for pattern, replacement in _RULES:
        value = pattern.sub(replacement, value)
    return value


class SecretMaskingFilter(logging.Filter):
    """로그 레코드의 메시지와 인자에서 시크릿을 가리는 필터.

    값 생성 지점 마스킹의 안전망이다. 앞으로 누군가 URL이나 토큰을 그대로
    logger에 넘겨도 출력 단계에서 한 번 더 걸린다.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.msg, str):
            record.msg = mask_secrets(record.msg)
        if record.args:
            if isinstance(record.args, dict):
                record.args = {k: mask_secrets(v) for k, v in record.args.items()}
            else:
                record.args = tuple(mask_secrets(a) for a in record.args)
        return True


def install_secret_filter(logger: logging.Logger = None) -> None:
    """루트 로거(또는 지정한 로거)에 마스킹 필터를 한 번만 붙인다."""
    target = logger or logging.getLogger()
    if any(isinstance(f, SecretMaskingFilter) for f in target.filters):
        return
    target.addFilter(SecretMaskingFilter())
    # 루트 로거의 필터는 핸들러를 거쳐 온 레코드에 적용되지 않으므로
    # 이미 붙어 있는 핸들러에도 같이 건다.
    for handler in target.handlers:
        if not any(isinstance(f, SecretMaskingFilter) for f in handler.filters):
            handler.addFilter(SecretMaskingFilter())
