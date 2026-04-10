"""
kabuステーション API 接続確認スクリプト

kabuステーションを起動した状態で実行してください。

実行方法:
    python check_connection.py
"""

import sys
import os
from dotenv import load_dotenv
import requests
from loguru import logger

load_dotenv()

logger.remove()
logger.add(sys.stdout, format="{time:HH:mm:ss} | {level} | {message}", level="INFO")

BASE_URL = os.getenv("KABU_BASE_URL", "http://localhost:18080")
TIMEOUT = 5


def check_env_vars() -> bool:
    """必須環境変数の確認"""
    logger.info("環境変数チェック中...")
    required = ["KABU_API_PASSWORD"]
    missing = [v for v in required if not os.getenv(v)]
    if missing:
        logger.error(f"未設定の環境変数: {', '.join(missing)}")
        logger.error(".env ファイルを確認してください (.env.example を参照)")
        return False
    logger.success("環境変数: OK")
    return True


def check_kabu_station_running() -> bool:
    """kabuステーションが起動しているか確認"""
    logger.info(f"kabuステーション起動確認中... ({BASE_URL})")
    try:
        resp = requests.get(f"{BASE_URL}/kabusapi/token", timeout=TIMEOUT)
        # 405 Method Not Allowed = GETは不可だがサーバーは動いている
        if resp.status_code in (200, 400, 405):
            logger.success(f"kabuステーション: 起動中 (HTTP {resp.status_code})")
            return True
        logger.warning(f"kabuステーション: 予期しないレスポンス (HTTP {resp.status_code})")
        return False
    except requests.exceptions.ConnectionError:
        logger.error(
            f"kabuステーションに接続できません ({BASE_URL})\n"
            "  → kabuステーションを起動してください\n"
            "  → APIポートが 18080 であることを確認してください"
        )
        return False
    except requests.exceptions.Timeout:
        logger.error(f"接続タイムアウト ({TIMEOUT}秒)")
        return False


def check_token_endpoint() -> bool:
    """トークン取得エンドポイントの確認"""
    logger.info("トークン取得テスト中...")
    password = os.getenv("KABU_API_PASSWORD", "")
    if not password:
        logger.warning("KABU_API_PASSWORD 未設定のためスキップ")
        return True
    try:
        resp = requests.post(
            f"{BASE_URL}/kabusapi/token",
            json={"APIPassword": password},
            headers={"Content-Type": "application/json"},
            timeout=TIMEOUT,
        )
        if resp.status_code == 200:
            token = resp.json().get("Token", "")
            logger.success(f"トークン取得: OK (Token: {token[:8]}...)")
            return True
        body = resp.json() if resp.content else {}
        logger.error(f"トークン取得失敗: HTTP {resp.status_code} - {body}")
        return False
    except Exception as e:
        logger.error(f"トークン取得エラー: {e}")
        return False


def check_board_endpoint() -> bool:
    """板情報エンドポイントの疎通確認 (トヨタ自動車で確認)"""
    logger.info("板情報エンドポイント確認中...")
    password = os.getenv("KABU_API_PASSWORD", "")
    if not password:
        logger.warning("KABU_API_PASSWORD 未設定のためスキップ")
        return True
    try:
        # まずトークン取得
        resp = requests.post(
            f"{BASE_URL}/kabusapi/token",
            json={"APIPassword": password},
            headers={"Content-Type": "application/json"},
            timeout=TIMEOUT,
        )
        if resp.status_code != 200:
            logger.warning("トークン取得できないため板情報確認をスキップ")
            return True
        token = resp.json()["Token"]

        # 板情報取得 (7203@1 = トヨタ自動車・東証プライム)
        resp = requests.get(
            f"{BASE_URL}/kabusapi/board/7203@1",
            headers={"X-API-KEY": token},
            timeout=TIMEOUT,
        )
        if resp.status_code == 200:
            data = resp.json()
            price = data.get("CurrentPrice", "N/A")
            logger.success(f"板情報取得: OK (7203 トヨタ自動車 現在値: {price}円)")
            return True
        logger.error(f"板情報取得失敗: HTTP {resp.status_code} - {resp.json()}")
        return False
    except Exception as e:
        logger.error(f"板情報確認エラー: {e}")
        return False


def check_dependencies() -> bool:
    """必須ライブラリの確認"""
    logger.info("依存ライブラリチェック中...")
    packages = ["requests", "websocket", "dotenv", "pandas", "loguru", "tenacity"]
    missing = []
    for pkg in packages:
        try:
            __import__(pkg)
        except ImportError:
            missing.append(pkg)
    if missing:
        logger.error(f"未インストール: {', '.join(missing)}")
        logger.error("pip install -r requirements.txt を実行してください")
        return False
    logger.success("依存ライブラリ: OK")
    return True


def check_python_version() -> bool:
    """Pythonバージョンの確認"""
    logger.info("Pythonバージョンチェック中...")
    v = sys.version_info
    if v < (3, 8):
        logger.error(f"Python 3.8以上が必要です (現在: {v.major}.{v.minor})")
        return False
    logger.success(f"Python {v.major}.{v.minor}.{v.micro}: OK")
    return True


def main():
    logger.info("=" * 50)
    logger.info("kabuステーション API 接続確認ツール")
    logger.info("=" * 50)

    checks = [
        ("Pythonバージョン",         check_python_version),
        ("依存ライブラリ",           check_dependencies),
        ("環境変数",                 check_env_vars),
        ("kabuステーション起動確認", check_kabu_station_running),
        ("トークン取得",             check_token_endpoint),
        ("板情報取得",               check_board_endpoint),
    ]

    results = {}
    for name, fn in checks:
        results[name] = fn()
        print()

    logger.info("=" * 50)
    logger.info("チェック結果サマリー")
    logger.info("=" * 50)
    all_passed = True
    for name, ok in results.items():
        if ok:
            logger.success(f"  [OK] {name}")
        else:
            logger.error(f"  [NG] {name}")
            all_passed = False

    print()
    if all_passed:
        logger.success("全チェック通過。システムを起動できます。")
        sys.exit(0)
    else:
        logger.error("一部チェックに失敗しました。上記のエラーを確認してください。")
        sys.exit(1)


if __name__ == "__main__":
    main()
