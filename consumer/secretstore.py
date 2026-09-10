"""Docker/K8s secret 파일을 우선 읽고, 없으면 환경변수로 폴백한다.

`FOO_FILE=/run/secrets/foo` 가 있으면 그 파일 내용(양끝 공백 제거)을, 없으면
`FOO` 환경변수를 반환한다. postgres·mysql 등 공식 이미지가 쓰는 `_FILE` 관례와
같다. 외부 의존성 없음 — 테스트에서 단독 import 하기 위해서다.

ponytail: producer/secretstore.py 와 쌍둥이(두 런타임이 코드를 공유하지 못한다 —
consumer 이미지는 consumer/ 만 COPY). 세 번째 소비자가 생기면 common/ 으로 추출.
"""
import os


def read_secret(name: str, default: str | None = None) -> str | None:
    """`{name}_FILE` 이 가리키는 파일, 없으면 `{name}` env, 없으면 default."""
    path = os.environ.get(f"{name}_FILE")
    if path:
        try:
            with open(path, encoding="utf-8") as fh:
                return fh.read().strip()
        except OSError as exc:
            raise RuntimeError(f"{name}_FILE 읽기 실패: {path}") from exc
    return os.environ.get(name, default)
