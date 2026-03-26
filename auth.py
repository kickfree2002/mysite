"""
SBI証券 HyperSBI2 認証モジュール

HyperSBI2はWebベースのセッション認証を使用します。
このモジュールはログイン・セッション管理・トークンリフレッシュを担当します。
"""

import os
import time
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import urlencode

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

load_dotenv()


@dataclass
class SessionInfo:
    """セッション情報を保持するデータクラス"""
    session_id: str = ""
    login_time: float = 0.0
    last_refresh: float = 0.0
    is_authenticated: bool = False
    username: str = ""
    # HyperSBI2のセッションCookieを保持
    cookies: dict = field(default_factory=dict)

    def is_expired(self, timeout_seconds: int = 1800) -> bool:
        """セッションが期限切れかどうかを確認"""
        if not self.is_authenticated:
            return True
        return (time.time() - self.last_refresh) > timeout_seconds

    def age_seconds(self) -> float:
        """ログインからの経過秒数"""
        return time.time() - self.login_time if self.login_time else 0.0


class AuthenticationError(Exception):
    """認証エラー"""
    pass


class SessionExpiredError(Exception):
    """セッション期限切れエラー"""
    pass


class SBIAuthClient:
    """
    SBI証券 HyperSBI2 認証クライアント

    使用例:
        client = SBIAuthClient()
        client.login()
        session = client.get_session()
        # ... API呼び出し ...
        client.logout()
    """

    LOGIN_URL = "https://site3.sbisec.co.jp/ETGate/"
    LOGIN_POST_URL = "https://site3.sbisec.co.jp/ETGate/"
    KEEP_ALIVE_URL = "https://site3.sbisec.co.jp/ETGate/?_ControlID=WPLEThmR001Control&_PageID=WPLEThmR001Ath20&_ActionID=DefaultAID&_SeqNo=1"

    def __init__(
        self,
        username: Optional[str] = None,
        password: Optional[str] = None,
        session_timeout: int = None,
    ):
        self.username = username or os.getenv("SBI_USERNAME", "")
        self.password = password or os.getenv("SBI_PASSWORD", "")
        self.session_timeout = session_timeout or int(os.getenv("SESSION_TIMEOUT", "1800"))

        if not self.username or not self.password:
            raise ValueError("SBI_USERNAME と SBI_PASSWORD を設定してください")

        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ja,en-US;q=0.7,en;q=0.3",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
        })

        self._session_info = SessionInfo(username=self.username)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(requests.exceptions.RequestException),
        reraise=True,
    )
    def login(self) -> SessionInfo:
        """
        SBI証券にログインしてセッションを確立します。

        Returns:
            SessionInfo: 確立されたセッション情報

        Raises:
            AuthenticationError: 認証失敗時
        """
        logger.info(f"SBI証券にログイン中... (ユーザー: {self.username})")

        try:
            # Step 1: ログインページを取得してCSRFトークン等を取得
            resp = self._session.get(self.LOGIN_URL, timeout=15)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "lxml")

            # ログインフォームのhiddenフィールドを収集
            login_data = self._extract_form_data(soup)

            # 認証情報をセット
            login_data.update({
                "user_id": self.username,
                "user_password": self.password,
                "ACT_login": "ログイン",
            })

            # Step 2: ログインPOST
            resp = self._session.post(
                self.LOGIN_POST_URL,
                data=login_data,
                timeout=15,
                allow_redirects=True,
            )
            resp.raise_for_status()

            # Step 3: ログイン成功確認
            if not self._verify_login(resp):
                raise AuthenticationError(
                    "ログインに失敗しました。ユーザーID・パスワードを確認してください。"
                )

            # セッション情報を保存
            now = time.time()
            self._session_info = SessionInfo(
                session_id=self._extract_session_id(),
                login_time=now,
                last_refresh=now,
                is_authenticated=True,
                username=self.username,
                cookies=dict(self._session.cookies),
            )

            logger.success(f"ログイン成功 (セッションID: {self._session_info.session_id[:8]}...)")
            return self._session_info

        except AuthenticationError:
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"ネットワークエラー: {e}")
            raise

    def logout(self) -> None:
        """ログアウトしてセッションを終了します"""
        if not self._session_info.is_authenticated:
            return
        try:
            logger.info("ログアウト中...")
            self._session.get(
                "https://site3.sbisec.co.jp/ETGate/?_ControlID=WPLEThmR001Control"
                "&_PageID=WPLEThmR001Ath20&_ActionID=logout",
                timeout=10,
            )
        except Exception as e:
            logger.warning(f"ログアウトリクエストでエラー: {e}")
        finally:
            self._session_info.is_authenticated = False
            self._session.cookies.clear()
            logger.info("セッションをクリアしました")

    def refresh_session(self) -> bool:
        """
        セッションをリフレッシュします（タイムアウト防止）

        Returns:
            bool: リフレッシュ成功かどうか
        """
        if not self._session_info.is_authenticated:
            logger.warning("未ログイン状態でのリフレッシュは無効です")
            return False
        try:
            resp = self._session.get(self.KEEP_ALIVE_URL, timeout=10)
            if resp.status_code == 200:
                self._session_info.last_refresh = time.time()
                logger.debug("セッションリフレッシュ完了")
                return True
            else:
                logger.warning(f"セッションリフレッシュ失敗: HTTP {resp.status_code}")
                return False
        except Exception as e:
            logger.warning(f"セッションリフレッシュエラー: {e}")
            return False

    def ensure_authenticated(self) -> None:
        """
        認証済み状態を保証します。
        セッション切れの場合は自動再ログインします。

        Raises:
            AuthenticationError: 再ログインにも失敗した場合
        """
        if self._session_info.is_expired(self.session_timeout):
            logger.info("セッション期限切れ。再ログインします...")
            self.login()
        elif (time.time() - self._session_info.last_refresh) > (self.session_timeout * 0.7):
            # 期限の70%を超えたらリフレッシュ
            self.refresh_session()

    def get_session(self) -> requests.Session:
        """認証済みrequests.Sessionオブジェクトを返します"""
        self.ensure_authenticated()
        return self._session

    def get_session_info(self) -> SessionInfo:
        """現在のセッション情報を返します"""
        return self._session_info

    def _extract_form_data(self, soup: BeautifulSoup) -> dict:
        """ログインフォームのhiddenフィールドを抽出"""
        data = {}
        form = soup.find("form", {"name": "form1"}) or soup.find("form")
        if form:
            for inp in form.find_all("input", {"type": "hidden"}):
                name = inp.get("name")
                value = inp.get("value", "")
                if name:
                    data[name] = value
        return data

    def _verify_login(self, response: requests.Response) -> bool:
        """レスポンスからログイン成功を確認"""
        # ログイン失敗パターンの検出
        failure_patterns = [
            "ログインできませんでした",
            "認証エラー",
            "パスワードが正しくありません",
            "ユーザーIDが見つかりません",
            "login_error",
        ]
        body = response.text
        for pattern in failure_patterns:
            if pattern in body:
                logger.debug(f"ログイン失敗パターン検出: '{pattern}'")
                return False

        # 成功パターンの確認
        success_patterns = [
            "ログアウト",
            "お客様情報",
            "口座情報",
            "マイページ",
        ]
        for pattern in success_patterns:
            if pattern in body:
                return True

        # どちらも検出できない場合はリダイレクト先で判断
        return "sbisec.co.jp" in response.url and "login" not in response.url.lower()

    def _extract_session_id(self) -> str:
        """セッションCookieからセッションIDを抽出"""
        for name in ["JSESSIONID", "SID", "SESSION", "session_id"]:
            value = self._session.cookies.get(name)
            if value:
                return value
        # Cookieがない場合はタイムスタンプベースのIDを生成
        return f"local_{int(time.time())}"

    def __enter__(self):
        self.login()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.logout()


def get_authenticated_session() -> tuple[SBIAuthClient, requests.Session]:
    """
    簡便な認証済みセッション取得関数

    Returns:
        (SBIAuthClient, requests.Session) のタプル

    使用例:
        client, session = get_authenticated_session()
        resp = session.get("https://...")
        client.logout()
    """
    client = SBIAuthClient()
    client.login()
    return client, client.get_session()


if __name__ == "__main__":
    import sys
    logger.remove()
    logger.add(sys.stdout, format="{time:HH:mm:ss} | {level} | {message}", level="DEBUG")

    logger.info("認証モジュール テスト")
    try:
        with SBIAuthClient() as client:
            info = client.get_session_info()
            logger.info(f"セッションID: {info.session_id}")
            logger.info(f"認証状態: {info.is_authenticated}")
            logger.info(f"ログイン経過時間: {info.age_seconds():.1f}秒")
    except AuthenticationError as e:
        logger.error(f"認証エラー: {e}")
        sys.exit(1)
