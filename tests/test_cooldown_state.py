"""쿨다운 상태 문서와 최대 무발송 간격 검증 (#42 회귀 방지).

이전 동작: 직전 시그니처를 이력 인덱스 검색(size=1, sort desc)으로 읽었다.
- 직전 문서의 refresh(기본 1초)보다 조회가 빠르면 방금 쓴 문서를 못 봐
  백로그 연속 처리 시 같은 등급인데도 중복 발송됐다.
- 등급이 오래 유지되면 발송이 계속 생략돼, 조용한 게 정상인지 파이프라인이
  죽은 건지 구분할 수 없었다.
"""
import json
from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("requests")
pytest.importorskip("dotenv")

from rules import should_send  # noqa: E402
from alert import OpenSearchAlertSender  # noqa: E402

KST = timezone(timedelta(hours=9))
NOW = datetime(2026, 7, 30, 12, 0, tzinfo=KST)
H12 = 12 * 3600


# ---------- should_send: 최대 무발송 간격 ----------

def test_signature_change_always_sends():
    assert should_send("pm10=좋음", "pm10=나쁨") is True


def test_same_signature_within_silence_window_skips():
    sent = (NOW - timedelta(hours=3)).isoformat()
    assert should_send("s", "s", last_sent_at=sent,
                       max_silence_seconds=H12, now=NOW) is False


def test_same_signature_past_silence_window_resends():
    sent = (NOW - timedelta(hours=13)).isoformat()
    assert should_send("s", "s", last_sent_at=sent,
                       max_silence_seconds=H12, now=NOW) is True


def test_exactly_at_boundary_resends():
    sent = (NOW - timedelta(hours=12)).isoformat()
    assert should_send("s", "s", last_sent_at=sent,
                       max_silence_seconds=H12, now=NOW) is True


def test_silence_disabled_never_heartbeats():
    # MAX_SILENCE_HOURS=0 → 기존 동작 그대로
    sent = (NOW - timedelta(days=30)).isoformat()
    assert should_send("s", "s", last_sent_at=sent,
                       max_silence_seconds=0, now=NOW) is False


def test_missing_or_bad_last_sent_does_not_spam():
    # 상태가 불완전하면 간격 판정을 건너뛴다 — 발송을 늘리는 쪽으로 기울면 스팸
    assert should_send("s", "s", last_sent_at=None,
                       max_silence_seconds=H12, now=NOW) is False
    assert should_send("s", "s", last_sent_at="not-a-date",
                       max_silence_seconds=H12, now=NOW) is False
    naive = (NOW - timedelta(hours=13)).replace(tzinfo=None).isoformat()
    assert should_send("s", "s", last_sent_at=naive,
                       max_silence_seconds=H12, now=NOW) is False


def test_two_arg_call_keeps_old_contract():
    # 기존 호출부(2인자)의 동작 불변
    assert should_send(None, "s") is True
    assert should_send("s", "s") is False


# ---------- 상태 문서 기록/조회 ----------

class FakeClient:
    """index/update/get을 dict로 흉내내는 최소 클라이언트."""

    def __init__(self):
        self.docs = {}    # (index, id) -> body
        self.states = {}  # id -> state doc

    def index(self, index, body, id=None):
        self.docs[(index, id or f"auto{len(self.docs)}")] = body
        return {"_id": id or "auto"}

    def update(self, index, id, body):
        doc = self.states.setdefault(id, {})
        doc.update(body["doc"])
        return {"_id": id}

    def get(self, index, id):
        if id not in self.states:
            raise KeyError404(id)
        return {"_source": self.states[id]}

    def search(self, index, body):
        return {"hits": {"hits": []}}


class KeyError404(Exception):
    status_code = 404


@pytest.fixture
def sender(tmp_path, monkeypatch):
    monkeypatch.setenv("SIGNATURE_STATE_PATH", str(tmp_path / "sig.json"))
    return OpenSearchAlertSender(FakeClient())


DOC = {
    "timestamp": "2026-07-30T12:00:00+09:00", "region": "서울",
    "indices": {}, "levels": {}, "recommendations": {}, "action_groups": {},
    "grade_signature": "pm10=나쁨", "event_id": "서울:2026073012",
}


def test_delivered_send_records_last_sent(sender):
    sender.send(dict(DOC, delivered_channels=["kakao"]))
    state = sender.cooldown_state("서울")
    assert state["grade_signature"] == "pm10=나쁨"
    assert state["last_external_send_at"]  # 발송했으니 기록


def test_suppressed_send_keeps_last_sent(sender):
    """쿨다운 생략 회차가 last_sent를 갱신하면 무발송 간격이 영영 차지 않는다."""
    sender.send(dict(DOC, delivered_channels=["kakao"]))
    first_sent = sender.cooldown_state("서울")["last_external_send_at"]

    sender.send(dict(DOC, delivered_channels=[]))  # 생략 회차 (이력만 기록)
    state = sender.cooldown_state("서울")
    assert state["last_external_send_at"] == first_sent  # 유지
    assert state["grade_signature"] == "pm10=나쁨"


def test_state_doc_is_read_by_get_not_search(sender):
    """검색이 아니라 문서 GET — refresh 지연 레이스가 구조적으로 없다."""
    sender.send(dict(DOC, delivered_channels=["kakao"]))
    called = {"search": 0}
    original = sender.client.search
    sender.client.search = lambda **kw: called.__setitem__("search", 1) or original(**kw)
    sender.cooldown_state("서울")
    assert called["search"] == 0


def test_missing_state_doc_falls_back_to_history(sender):
    # 상태 문서가 없으면(이행 직후) 이력 검색으로 폴백한다
    assert sender.cooldown_state("서울") == {}


def test_disconnected_uses_local_cache_with_last_sent(sender):
    sender.send(dict(DOC, delivered_channels=["kakao"]))
    sender.attach(None)
    state = sender.cooldown_state("서울")
    assert state["grade_signature"] == "pm10=나쁨"
    assert state.get("last_external_send_at")  # 캐시에도 발송 시각이 남는다


def test_old_string_cache_format_still_readable(sender, tmp_path):
    # 이전 배포의 캐시({region: "시그니처"})와 호환
    sender.state_path.write_text(json.dumps({"서울": "pm10=좋음"}), encoding="utf-8")
    sender.attach(None)
    assert sender.cooldown_state("서울") == {"grade_signature": "pm10=좋음"}
