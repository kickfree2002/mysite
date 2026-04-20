"""
kabuステーション API 認証モジュール

kabuステーションのローカルREST APIからトークンを取得・管理します。
HyperSBI2と異なり、Webスクレイピング不要のシンプルなトークン認証です。

APIドキュメント:
    POST http://localhost:18080/kabusapi/token
    Body: {"APIPassword": "<パスワード>"}
    Response: {"Token": "<トークン文字列>"}
"""

import os
import time
from dataclasses import dataclass
from typing import Optional

import requests
from dotenv import load_dotenv
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

load_dotenv()


@dataclass
class TokenInfo:
    """APIトークン情報"""
    token: str = ""
    issued_at: float = 0.0
    # kabuステーションのトークンは当日中有効だが、念のため1時間でリフレッシュ
    ttl_seconds: int = 3600

    def is_expired(self) -> bool:
        """トークンが期限切れかどうか"""
        if not self.token:
            return True
        return (time.time() - self.issued_at) >= self.ttl_seconds

    def remaining_seconds(self) -> float:
        """残り有効時間(秒)"""
        return max(0.0, self.ttl_seconds - (time.time() - self.issued_at))

    @property
    def headers(self) -> dict:
        """APIリクエスト用ヘッダー"""
        return {"X-API-KEY": self.token, "Content-Type": "application/json"}


class KabuAuthError(Exception):
    """認証エラー"""
    pass


class KabuAuthClient:
    """
    kabuステーション APIトークン管理クライアント

    使用例:
        client = KabuAuthClient()
        token_info = client.get_token()
        headers = token_info.headers
        # → {"X-API-KEY": "xxxx", "Content-Type": "application/json"}

    コンテキストマネージャとしても使用可能:
        with KabuAuthClient() as auth:
            headers = auth.token_info.headers
    """

    TOKEN_ENDPOINT = "/kabusapi/token"

    def __init__(
        self,
        password: Optional[str] = None,
        base_url: Optional[str] = None,
        token_ttl: int = 3600,
    ):
        """
        Args:
            password: APIパスワード (省略時は環境変数 KABU_API_PASSWORD を使用)
            base_url: APIベースURL (省略時は KABU_BASE_URL または http://localhost:18080)
            token_ttl: トークンの有効期間(秒)
        """
        self.password = password or os.getenv("KABU_API_PASSWORD", "")
        self.base_url = (base_url or os.getenv("KABU_BASE_URL", "http://localhost:18080")).rstrip("/")
        self.token_ttl = token_ttl

        if not self.password:
            raise KabuAuthError(
                "APIパスワードが設定されていません。\n"
                ".env に KABU_API_PASSWORD を設定してください。"
            )

        self._token_info = TokenInfo(ttl_seconds=token_ttl)
        self._session = requests.Session()
        self._session.headers.update({"Content-Type": "application/json"})

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=5),
        retry=retry_if_exception_type(requests.exceptions.ConnectionError),
        reraise=True,
    )
    def fetch_token(self) -> TokenInfo:
        """
        kabuステーションからトークンを新規取得します。

        Returns:
            TokenInfo: 取得したトークン情報

        Raises:
            KabuAuthError: 認証失敗時
            requests.exceptions.ConnectionError: kabuステーション未起動時
        """
        url = f"{self.base_url}{self.TOKEN_ENDPOINT}"
        logger.debug(f"トークン取得中... ({url})")

        try:
            resp = self._session.post(
                url,
                json={"APIPassword": self.password},
                timeout=5,
            )
        except requests.exceptions.ConnectionError:
            raise requests.exceptions.ConnectionError(
                f"kabuステーションに接続できません ({self.base_url})\n"
                "kabuステーションが起動しているか確認してください。"
            )

        if resp.status_code == 200:
            token = resp.json().get("Token", "")
            if not token:
                raise KabuAuthError("レスポンスにTokenが含まれていません")
            self._token_info = TokenInfo(
                token=token,
                issued_at=time.time(),
                ttl_seconds=self.token_ttl,
            )
            logger.success(f"トークン取得完了 (有効期間: {self.token_ttl}秒)")
            return self._token_info

        # エラーレスポンスの解析
        try:
            err = resp.json()
            code = err.get("Code", resp.status_code)
            message = err.get("Message", "不明なエラー")
        except Exception:
            code, message = resp.status_code, resp.text

        raise KabuAuthError(f"トークン取得失敗 [Code={code}]: {message}")

    def get_token(self, force_refresh: bool = False) -> TokenInfo:
        """
        有効なトークンを返します。期限切れの場合は自動再取得します。

        Args:
            force_refresh: Trueの場合、期限に関わらず再取得

        Returns:
            TokenInfo: 有効なトークン情報
        """
        if force_refresh or self._token_info.is_expired():
            self.fetch_token()
        return self._token_info

    @property
    def token_info(self) -> TokenInfo:
        """現在のトークン情報 (期限切れの場合は自動更新)"""
        return self.get_token()

    def get_headers(self, force_refresh: bool = False) -> dict:
        """APIリクエスト用ヘッダーを返します"""
        return self.get_token(force_refresh).headers

    def __enter__(self):
        self.fetch_token()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # kabuステーション APIにはログアウトエンドポイントなし
        logger.debug("KabuAuthClient セッション終了")


if __name__ == "__main__":
    import sys
    logger.remove()
    logger.add(sys.stdout, format="{time:HH:mm:ss} | {level} | {message}", level="DEBUG")

    logger.info("kabuステーション 認証モジュール テスト")
    try:
        client = KabuAuthClient()
        info = client.fetch_token()
        logger.info(f"Token    : {info.token[:8]}...")
        logger.info(f"残り有効時間: {info.remaining_seconds():.0f}秒")
        logger.info(f"Headers  : {list(info.headers.keys())}")
    except KabuAuthError as e:
        logger.error(f"認証エラー: {e}")
        sys.exit(1)
    except requests.exceptions.ConnectionError as e:
        logger.error(str(e))
        sys.exit(1)
