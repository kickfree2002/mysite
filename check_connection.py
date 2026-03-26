"""
SBI証券 HyperSBI2 API 接続確認スクリプト

実行方法:
    python check_connection.py
"""

import sys
import os
from dotenv import load_dotenv
import requests
from loguru import logger

load_dotenv()

# ログ設定
logger.remove()
logger.add(sys.stdout, format="{time:HH:mm:ss} | {level} | {message}", level="INFO")

BASE_URL = os.getenv("HYPERSBI2_BASE_URL", "https://site3.sbisec.co.jp/ETGate/")
API_BASE_URL = os.getenv("HYPERSBI2_API_BASE_URL", "https://kabuka.sbisec.co.jp/api/")

TIMEOUT = 10


def check_env_vars() -> bool:
    """必須環境変数の確認"""
    logger.info("環境変数チェック中...")
    required = ["SBI_USERNAME", "SBI_PASSWORD"]
    missing = [v for v in required if not os.getenv(v)]
    if missing:
        logger.error(f"未設定の環境変数: {', '.join(missing)}")
        logger.error(".env ファイルを確認してください (.env.example を参照)")
        return False
    logger.success("環境変数: OK")
    return True


def check_network_reachability() -> bool:
    """ネットワーク到達性チェック"""
    logger.info("ネットワーク接続チェック中...")
    targets = [
        ("SBI証券 メインサイト", "https://www.sbisec.co.jp"),
        ("SBI証券 ETGate", BASE_URL),
        ("SBI証券 株価API", API_BASE_URL),
    ]
    all_ok = True
    for name, url in targets:
        try:
            resp = requests.get(url, timeout=TIMEOUT, allow_redirects=True)
            # 認証エラー(401/403)でもエンドポイント自体は到達できている
            if resp.status_code < 500:
                logger.success(f"{name}: HTTP {resp.status_code} - 到達可能")
            else:
                logger.warning(f"{name}: HTTP {resp.status_code} - サーバーエラー")
                all_ok = False
        except requests.exceptions.ConnectionError:
            logger.error(f"{name}: 接続失敗 (DNS解決またはTCP接続エラー)")
            all_ok = False
        except requests.exceptions.Timeout:
            logger.error(f"{name}: タイムアウト ({TIMEOUT}秒)")
            all_ok = False
        except requests.exceptions.RequestException as e:
            logger.error(f"{name}: エラー - {e}")
            all_ok = False
    return all_ok


def check_ssl_certificate() -> bool:
    """SSL証明書の確認"""
    logger.info("SSL証明書チェック中...")
    try:
        import ssl
        import socket
        hostname = "www.sbisec.co.jp"
        ctx = ssl.create_default_context()
        with ctx.wrap_socket(socket.socket(), server_hostname=hostname) as s:
            s.settimeout(TIMEOUT)
            s.connect((hostname, 443))
            cert = s.getpeercert()
            subject = dict(x[0] for x in cert.get("subject", []))
            logger.success(f"SSL証明書: OK (発行先: {subject.get('commonName', '不明')})")
        return True
    except ssl.SSLCertVerificationError as e:
        logger.error(f"SSL証明書エラー: {e}")
        return False
    except Exception as e:
        logger.warning(f"SSL確認スキップ: {e}")
        return True  # ネットワーク環境によってはスキップ


def check_dependencies() -> bool:
    """必須ライブラリの確認"""
    logger.info("依存ライブラリチェック中...")
    required_packages = [
        "requests", "httpx", "bs4", "dotenv",
        "pandas", "schedule", "loguru", "tenacity",
    ]
    missing = []
    for pkg in required_packages:
        try:
            __import__(pkg)
        except ImportError:
            missing.append(pkg)
    if missing:
        logger.error(f"未インストールのライブラリ: {', '.join(missing)}")
        logger.error("pip install -r requirements.txt を実行してください")
        return False
    logger.success("依存ライブラリ: OK")
    return True


def check_python_version() -> bool:
    """Pythonバージョンの確認"""
    logger.info("Pythonバージョンチェック中...")
    version = sys.version_info
    if version < (3, 8):
        logger.error(f"Python 3.8以上が必要です (現在: {version.major}.{version.minor})")
        return False
    logger.success(f"Python {version.major}.{version.minor}.{version.micro}: OK")
    return True


def main():
    logger.info("=" * 50)
    logger.info("SBI HyperSBI2 API 接続確認ツール")
    logger.info("=" * 50)

    checks = [
        ("Pythonバージョン", check_python_version),
        ("依存ライブラリ", check_dependencies),
        ("環境変数", check_env_vars),
        ("ネットワーク到達性", check_network_reachability),
        ("SSL証明書", check_ssl_certificate),
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
        status = "OK" if ok else "NG"
        if ok:
            logger.success(f"  [{status}] {name}")
        else:
            logger.error(f"  [{status}] {name}")
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
