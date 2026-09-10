"""weather_writer 최소 권한 계정이 실수로 넓어지지 않게 잡는 가드 (#118).

YAML 파서 의존성을 추가하지 않으려고 텍스트 검사로 한다. **한계**: OpenSearch
action group(`crud` 등)이 어떤 하위 액션으로 펼쳐지는지는 여기서 못 본다 —
`'*'` 문자열이 없다는 검사는 그래서 완전한 보증이 아니다. 실제 권한 경계는
격리 Compose 검증(403 확인)에서 본다. 여기는 명백한 확장(전역 패턴·admin
와일드카드)만 잡는 회귀 방지선이다.
"""
from pathlib import Path

CFG = Path(__file__).resolve().parent.parent / "config" / "opensearch-security"


def _block(text: str, key: str) -> str:
    """`key:` 로 시작하는 최상위 블록만 잘라낸다(다음 최상위 키 전까지)."""
    lines = text.splitlines()
    out, collecting = [], False
    for line in lines:
        if line.startswith(f"{key}:"):
            collecting = True
            continue
        if collecting and line and not line[0].isspace() and not line.startswith("#"):
            break
        if collecting:
            out.append(line)
    return "\n".join(out)


def test_weather_manager_role_is_scoped():
    roles = (CFG / "roles.yml").read_text(encoding="utf-8")
    block = _block(roles, "weather_manager")
    assert block, "weather_manager 역할이 roles.yml에 없다"
    # 인덱스 권한은 weather-* 로만. 전역 패턴 금지.
    assert "'weather-*'" in block
    assert "'*'" not in block and '"*"' not in block
    # 클러스터 관리 와일드카드 금지
    for forbidden in ("cluster:admin/*", "cluster_manage_pipelines", "restapi:admin",
                      "cluster:admin/opendistro/security", "indices:admin/delete"):
        assert forbidden not in block, f"weather_manager에 위험 권한: {forbidden}"


def test_weather_writer_user_maps_only_to_weather_manager():
    users = (CFG / "internal_users.yml").read_text(encoding="utf-8")
    block = _block(users, "weather_writer")
    assert block, "weather_writer 계정이 internal_users.yml에 없다"
    assert "weather_manager" in block
    for forbidden in ("all_access", "admin", "security_rest_api", "readall"):
        assert forbidden not in block, f"weather_writer에 과한 역할: {forbidden}"


def test_unused_demo_users_removed():
    # 알려진 비밀번호를 가진 데모 계정이 weather-network 안에서 접근 가능하면 안 된다
    users = (CFG / "internal_users.yml").read_text(encoding="utf-8")
    for gone in ("anomalyadmin:", "kibanaro:", "logstash:", "readall:", "snapshotrestore:"):
        assert gone not in users, f"안 쓰는 데모 계정이 남아 있다: {gone}"


def test_admin_kept_as_break_glass_with_pinned_demo_hash():
    # admin은 break-glass로 남긴다. 데모 해시는 이미지 2.8.0에 고정 — 바뀌면 알아채야 한다
    users = (CFG / "internal_users.yml").read_text(encoding="utf-8")
    assert "$2a$12$VcCDgh2NDk07JGN0rjGbM.Ad41qVR/YFJcgHp0UGns5JDymv..TOG" in users


def test_internal_users_are_only_the_three_we_keep():
    import re
    users = (CFG / "internal_users.yml").read_text(encoding="utf-8")
    # 최상위 매핑 키(들여쓰기 없이 `이름:` 으로 끝나는 줄)만
    defined = set(re.findall(r"^([a-z_][a-z0-9_]*):\s*$", users, re.MULTILINE)) - {"_meta"}
    assert defined == {"admin", "kibanaserver", "weather_writer"}, defined


def test_all_ten_security_files_present():
    # 부분 마운트하면 OpenSearch 부팅이 실패한다 — 전체가 있어야 한다
    expected = {
        "action_groups.yml", "allowlist.yml", "audit.yml", "config.yml",
        "internal_users.yml", "nodes_dn.yml", "roles.yml", "roles_mapping.yml",
        "tenants.yml", "whitelist.yml",
    }
    present = {p.name for p in CFG.iterdir() if p.suffix == ".yml"}
    assert expected <= present, f"누락: {expected - present}"
