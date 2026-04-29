"""
로컬에서 카카오 OAuth refresh token을 1회 발급받는 스크립트.

사용 전 Kakao Developers 콘솔의 Redirect URI에 아래 값을 등록하세요.
http://localhost:8088/kakao/callback
"""

import json
import os
import threading
import urllib.parse
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer

import requests
from dotenv import load_dotenv


REDIRECT_URI = os.getenv("KAKAO_REDIRECT_URI", "http://localhost:8088/kakao/callback")
SCOPE = "talk_message"


class KakaoCallbackHandler(BaseHTTPRequestHandler):
    server_version = "KakaoOAuthCallback/1.0"

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        query = urllib.parse.parse_qs(parsed.query)

        if parsed.path != "/kakao/callback":
            self.send_response(404)
            self.end_headers()
            self.wfile.write("Not found".encode("utf-8"))
            return

        error = query.get("error", [None])[0]
        if error:
            description = query.get("error_description", [""])[0]
            self.server.result = {"error": error, "error_description": description}
            self._respond("카카오 인증이 취소되었거나 실패했습니다. 터미널을 확인하세요.")
            return

        code = query.get("code", [None])[0]
        if not code:
            self.server.result = {"error": "missing_code"}
            self._respond("인가 코드가 없습니다. 터미널을 확인하세요.")
            return

        self.server.result = {"code": code}
        self._respond("인가 코드 수신 완료. 이 창은 닫아도 됩니다.")

    def log_message(self, format, *args):
        return

    def _respond(self, body: str):
        html = f"""
        <!doctype html>
        <html lang="ko">
          <head><meta charset="utf-8"><title>Kakao OAuth</title></head>
          <body style="font-family: sans-serif;">
            <h2>{body}</h2>
          </body>
        </html>
        """
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(html.encode("utf-8"))
        threading.Thread(target=self.server.shutdown, daemon=True).start()


def exchange_code_for_token(code: str, rest_api_key: str, client_secret: str = ""):
    data = {
        "grant_type": "authorization_code",
        "client_id": rest_api_key,
        "redirect_uri": REDIRECT_URI,
        "code": code,
    }
    if client_secret:
        data["client_secret"] = client_secret

    response = requests.post(
        "https://kauth.kakao.com/oauth/token",
        data=data,
        timeout=15,
    )
    response.raise_for_status()
    return response.json()


def main():
    load_dotenv()
    rest_api_key = os.getenv("KAKAO_REST_API_KEY", "").strip()
    client_secret = os.getenv("KAKAO_CLIENT_SECRET", "").strip()

    if not rest_api_key:
        raise SystemExit(".env에 KAKAO_REST_API_KEY를 먼저 입력하세요.")

    auth_params = {
        "response_type": "code",
        "client_id": rest_api_key,
        "redirect_uri": REDIRECT_URI,
        "scope": SCOPE,
    }
    auth_url = "https://kauth.kakao.com/oauth/authorize?" + urllib.parse.urlencode(auth_params)

    print("브라우저에서 카카오 로그인/동의를 진행하세요.")
    print(f"Redirect URI: {REDIRECT_URI}")
    print(f"Auth URL: {auth_url}")

    server = HTTPServer(("localhost", 8088), KakaoCallbackHandler)
    server.result = {}
    webbrowser.open(auth_url)
    server.serve_forever()

    if server.result.get("error"):
        raise SystemExit(json.dumps(server.result, ensure_ascii=False, indent=2))

    token_data = exchange_code_for_token(
        server.result["code"],
        rest_api_key,
        client_secret,
    )

    print("\n발급 성공. .env에 아래 값을 넣으세요.")
    print(f"KAKAO_REFRESH_TOKEN={token_data.get('refresh_token', '')}")
    print("\n전체 응답:")
    print(json.dumps(token_data, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
