"""시크릿 마스킹 검증 (#38 회귀 방지).

이전 동작: 공공데이터포털 API가 헤더 인증을 지원하지 않아 serviceKey를
쿼리스트링으로 보내는데, requests 예외 문자열에 전체 URL이 들어간다.
그 문자열이 last_error에 담겨 로그뿐 아니라 data_warnings를 통해
Kafka 페이로드와 알림 이메일 본문까지 흘러갔다.
"""
import logging

from masking import MASK, mask_secrets, SecretMaskingFilter, install_secret_filter


# ---------- serviceKey ----------

def test_service_key_in_url_is_masked():
    text = ("HTTPSConnectionPool: Max retries exceeded with url: "
            "/B552584/ArpltnInforInqireSvc?serviceKey=abc123DEF%2Bxyz&returnType=json")
    masked = mask_secrets(text)
    assert "abc123DEF" not in masked
    assert f"serviceKey={MASK}" in masked
    # 뒤따르는 파라미터는 살아 있어야 원인 파악이 된다
    assert "returnType=json" in masked


def test_service_key_case_insensitive():
    assert "MYKEY" not in mask_secrets("?ServiceKey=MYKEY&x=1")


def test_service_key_at_end_of_string():
    assert mask_secrets("url?serviceKey=tail").endswith(MASK)


# ---------- 토큰류 ----------

def test_tokens_are_masked():
    body = '{"access_token": "aaa.bbb.ccc", "refresh_token": "rrr-111"}'
    masked = mask_secrets(body)
    assert "aaa.bbb.ccc" not in masked
    assert "rrr-111" not in masked


def test_bearer_header_is_masked():
    assert "sekret" not in mask_secrets("Authorization: Bearer sekret")


def test_client_secret_and_password_masked():
    assert "s3cr3t" not in mask_secrets("client_secret=s3cr3t&grant_type=refresh_token")
    assert "hunter2" not in mask_secrets("SMTP_PASSWORD='hunter2'")


# ---------- 무해한 값은 건드리지 않는다 ----------

def test_normal_text_untouched():
    text = "에어코리아 API 응답 오류: 500 Internal Server Error"
    assert mask_secrets(text) == text


def test_non_string_passthrough():
    assert mask_secrets(None) is None
    assert mask_secrets(42) == 42
    assert mask_secrets({"a": 1}) == {"a": 1}


# ---------- 로깅 필터 ----------

def test_logging_filter_masks_message():
    record = logging.LogRecord(
        "t", logging.ERROR, "f", 1,
        "호출 실패: https://x/api?serviceKey=LEAKED&a=1", None, None,
    )
    SecretMaskingFilter().filter(record)
    assert "LEAKED" not in record.getMessage()


def test_logging_filter_masks_args():
    record = logging.LogRecord(
        "t", logging.ERROR, "f", 1, "실패: %s", ("serviceKey=LEAKED",), None,
    )
    SecretMaskingFilter().filter(record)
    assert "LEAKED" not in record.getMessage()


def test_install_is_idempotent():
    logger = logging.getLogger("test.masking.install")
    install_secret_filter(logger)
    install_secret_filter(logger)
    assert sum(isinstance(f, SecretMaskingFilter) for f in logger.filters) == 1
