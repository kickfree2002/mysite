"""
kabuステーション API 価格監視モジュール

2つの監視モードをサポートします:

  websocket (推奨):
    kabuステーションのWebSocket Push通知を受信します。
    銘柄を事前登録 (PUT /kabusapi/registers) し、
    価格変動があった時だけイベントを受信する低負荷モードです。
    ws://localhost:18080/kabusapi/websocket

  polling:
    REST API (GET /kabusapi/board) を定期的に呼び出します。
    WebSocketが使えない環境でのフォールバックとして使用します。

実行方法:
    python monitor.py
    python monitor.py --mode websocket --symbols 7203@1,9984@1
    python monitor.py --mode polling  --symbols 7203@1 --interval 3
"""

import argparse
import json
import os
import requests
import signal
import sys
import threading
import time
from collections import defaultdict
from datetime import datetime
from typing import Optional

from dotenv import load_dotenv
from loguru import logger

from auth import KabuAuthClient, KabuAuthError
from stock import StockClient, StockQuote, StockDataError

load_dotenv()

log_file = os.getenv("LOG_FILE", "logs/trading.log")
log_level = os.getenv("LOG_LEVEL", "INFO")
logger.remove()
logger.add(sys.stdout, format="{time:HH:mm:ss} | {level:<8} | {message}", level=log_level)
logger.add(
    log_file,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    level="DEBUG",
    rotation="1 day",
    retention="30 days",
)


class PriceAlert:
    """価格変動アラート"""
    def __init__(self, symbol: str, name: str, price: float, change_pct: float, direction: str):
        self.symbol = symbol
        self.name = name
        self.price = price
        self.change_pct = change_pct
        self.direction = direction
        self.timestamp = datetime.now()

    def __str__(self) -> str:
        arrow = "↑" if self.direction == "UP" else "↓"
        sign = "+" if self.change_pct >= 0 else ""
        return (
            f"[アラート] {self.symbol} {self.name} {arrow} "
            f"{self.price:,.1f}円 ({sign}{self.change_pct:.2f}%)"
        )


class PriceMonitor:
    """
    kabuステーション API 株価監視クラス

    使用例:
        auth = KabuAuthClient()
        monitor = PriceMonitor(auth, symbols=["7203@1", "9984@1"])
        monitor.start()  # Ctrl+C で停止
    """

    WS_ENDPOINT = "ws://localhost:18080/kabusapi/websocket"
    REGISTER_ENDPOINT = "/kabusapi/registers"

    def __init__(
        self,
        auth: KabuAuthClient,
        symbols: list[str],
        mode: str = "websocket",
        interval: int = None,
        alert_threshold_pct: float = None,
    ):
        """
        Args:
            auth               : KabuAuthClient インスタンス
            symbols            : ["7203@1", "9984@1"] 形式の銘柄リスト
            mode               : "websocket" または "polling"
            interval           : pollingモード時の取得間隔(秒)
            alert_threshold_pct: 前日比アラート閾値(%)
        """
        self._auth = auth
        self.symbols = self._parse_symbols(symbols)
        self.mode = mode
        self.interval = interval or int(os.getenv("MONITOR_INTERVAL_SECONDS", "3"))
        self.alert_threshold_pct = alert_threshold_pct or float(
            os.getenv("PRICE_ALERT_THRESHOLD", "2.0")
        )

        self._stock_client = StockClient(auth)
        self._running = False
        self._alerts: list[PriceAlert] = []
        self._errors = defaultdict(int)
        self._stats = {
            "start_time": None,
            "received": 0,
            "alerts_fired": 0,
        }

    # ------------------------------------------------------------------
    # 公開インターフェース
    # ------------------------------------------------------------------

    def start(self) -> None:
        """監視を開始します。Ctrl+C で停止。"""
        self._running = True
        self._stats["start_time"] = datetime.now()

        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

        self._print_banner()

        try:
            if self.mode == "websocket":
                self._start_websocket()
            else:
                self._start_polling()
        except KeyboardInterrupt:
            pass
        finally:
            self._shutdown()

    # ------------------------------------------------------------------
    # WebSocket モード
    # ------------------------------------------------------------------

    def _start_websocket(self) -> None:
        """WebSocketモードで監視を開始。失敗時はpollingに自動フォールバック"""
        try:
            import websocket as ws_lib
        except ImportError:
            logger.warning(
                "websocket-client がインストールされていません。pollingモードに切り替えます。\n"
                "  pip install websocket-client"
            )
            self._start_polling()
            return

        # 銘柄登録
        if not self._register_symbols():
            logger.warning(
                "銘柄登録失敗 (模擬環境ではWebSocket Push非対応の場合があります)。\n"
                "pollingモードに自動切り替えします。"
            )
            self._start_polling()
            return

        ws_url = f"ws://{self._auth.base_url.replace('http://', '')}/kabusapi/websocket"
        logger.info(f"WebSocket接続中... ({ws_url})")

        def on_message(ws, message):
            self._on_ws_message(message)

        def on_error(ws, error):
            logger.error(f"WebSocketエラー: {error}")

        def on_close(ws, close_status_code, close_msg):
            logger.warning(f"WebSocket切断: code={close_status_code}")
            if self._running:
                logger.info("5秒後に再接続します...")
                time.sleep(5)
                ws.run_forever()

        def on_open(ws):
            logger.success("WebSocket接続確立。リアルタイム受信中...")

        ws_app = ws_lib.WebSocketApp(
            ws_url,
            header={"X-API-KEY": self._auth.token_info.token},
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )

        wst = threading.Thread(target=ws_app.run_forever, daemon=True)
        wst.start()

        # メインスレッドは待機
        try:
            while self._running:
                time.sleep(1)
        except KeyboardInterrupt:
            pass
        finally:
            ws_app.close()

    def _on_ws_message(self, message: str) -> None:
        """WebSocketメッセージ受信時の処理"""
        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            logger.warning(f"JSONデコード失敗: {message[:100]}")
            return

        self._stats["received"] += 1
        quote = StockQuote.from_api(data)
        self._display_quote(quote)

        alert = self._check_alert(quote)
        if alert:
            self._fire_alert(alert)

    def _register_symbols(self) -> bool:
        """WebSocket通知を受け取る銘柄を登録する"""
        url = f"{self._auth.base_url}{self.REGISTER_ENDPOINT}"
        payload = {
            "Symbols": [
                {"Symbol": sym, "Exchange": exch}
                for sym, exch in self.symbols
            ]
        }
        logger.debug(f"銘柄登録リクエスト: {payload}")
        try:
            resp = requests.put(
                url,
                headers=self._auth.get_headers(),
                json=payload,
                timeout=5,
            )
            if resp.status_code == 200:
                regist_list = resp.json().get("RegistList", [])
                logger.success(f"銘柄登録完了: {len(regist_list)}銘柄")
                return True
            logger.warning(f"銘柄登録失敗: HTTP {resp.status_code} - {resp.text}")
            return False
        except Exception as e:
            logger.warning(f"銘柄登録エラー: {e}")
            return False

    # ------------------------------------------------------------------
    # Polling モード
    # ------------------------------------------------------------------

    def _start_polling(self) -> None:
        """Pollingモードで監視を開始"""
        logger.info(f"Pollingモード起動 (間隔: {self.interval}秒)")
        iteration = 0
        while self._running:
            iteration += 1
            logger.info(f"--- {datetime.now().strftime('%H:%M:%S')} 取得 (#{iteration}) ---")

            for sym, exch in self.symbols:
                self._stats["received"] += 1
                try:
                    quote = self._stock_client.get_quote(sym, exch, use_cache=False)
                    self._errors[f"{sym}@{exch}"] = 0
                    self._display_quote(quote)
                    alert = self._check_alert(quote)
                    if alert:
                        self._fire_alert(alert)
                except (StockDataError, Exception) as e:
                    key = f"{sym}@{exch}"
                    self._errors[key] += 1
                    logger.warning(f"[{key}] 取得失敗 (連続{self._errors[key]}回): {e}")

            self._interruptible_sleep(self.interval)

    # ------------------------------------------------------------------
    # 共通処理
    # ------------------------------------------------------------------

    def _display_quote(self, quote: StockQuote) -> None:
        """株価を整形して表示"""
        if quote.current_price is None:
            logger.warning(f"  [{quote.symbol}] {quote.name}: データ取得不可")
            return

        change_str = ""
        if quote.change is not None and quote.change_pct is not None:
            sign = "+" if quote.change >= 0 else ""
            arrow = "▲" if quote.change >= 0 else "▼"
            change_str = f" {arrow}{sign}{quote.change:.1f}円 ({sign}{quote.change_pct:.2f}%)"

        spread_str = ""
        if quote.ask and quote.bid:
            spread_str = f"  気配: 売{quote.ask:,.1f} / 買{quote.bid:,.1f}"

        vol_str = f"  出来高: {quote.volume:,.0f}" if quote.volume else ""

        logger.info(
            f"  [{quote.symbol}] {quote.name:12s} "
            f"{quote.current_price:>10,.1f}円{change_str}{spread_str}{vol_str}"
        )

    def _check_alert(self, quote: StockQuote) -> Optional[PriceAlert]:
        """前日比が閾値を超えていればアラートを返す"""
        if quote.current_price is None or quote.change_pct is None:
            return None
        if abs(quote.change_pct) >= self.alert_threshold_pct:
            direction = "UP" if quote.change_pct >= 0 else "DOWN"
            return PriceAlert(
                symbol=quote.symbol,
                name=quote.name,
                price=quote.current_price,
                change_pct=quote.change_pct,
                direction=direction,
            )
        return None

    def _fire_alert(self, alert: PriceAlert) -> None:
        self._alerts.append(alert)
        self._stats["alerts_fired"] += 1
        logger.warning(str(alert))

    def _interruptible_sleep(self, seconds: int) -> None:
        for _ in range(seconds):
            if not self._running:
                break
            time.sleep(1)

    def _handle_shutdown(self, signum, frame) -> None:
        logger.info("停止シグナル受信...")
        self._running = False

    def _shutdown(self) -> None:
        elapsed = (
            (datetime.now() - self._stats["start_time"]).total_seconds()
            if self._stats["start_time"] else 0
        )
        logger.info("=" * 60)
        logger.info("監視終了 - 統計情報")
        logger.info(f"  稼働時間   : {elapsed:.0f}秒 ({elapsed / 60:.1f}分)")
        logger.info(f"  受信件数   : {self._stats['received']}")
        logger.info(f"  アラート   : {self._stats['alerts_fired']} 回")
        if self._alerts:
            logger.info("  直近アラート:")
            for a in self._alerts[-5:]:
                logger.info(f"    {a.timestamp.strftime('%H:%M:%S')} {a}")
        logger.info("=" * 60)

    def _print_banner(self) -> None:
        sym_list = ", ".join(f"{s}@{e}" for s, e in self.symbols)
        logger.info("=" * 60)
        logger.info("kabuステーション 価格監視システム 起動")
        logger.info(f"モード     : {self.mode}")
        logger.info(f"監視銘柄   : {sym_list}")
        logger.info(f"アラート閾値: ±{self.alert_threshold_pct}%")
        logger.info("停止: Ctrl+C")
        logger.info("=" * 60)

    @staticmethod
    def _parse_symbols(symbol_list: list[str]) -> list[tuple[str, int]]:
        """["7203@1", "9984@1"] → [("7203", 1), ("9984", 1)]"""
        result = []
        for spec in symbol_list:
            spec = spec.strip()
            if "@" in spec:
                sym, exch = spec.split("@", 1)
                result.append((sym.strip(), int(exch.strip())))
            else:
                result.append((spec, 1))  # デフォルト: 東証プライム
        return result


# ------------------------------------------------------------------
# CLI エントリポイント
# ------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="kabuステーション API 株価監視",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--symbols", "-s",
        default=os.getenv("WATCH_SYMBOLS", "7203@1,9984@1,6758@1"),
        help="監視銘柄 (カンマ区切り、例: 7203@1,9984@1)",
    )
    parser.add_argument(
        "--mode", "-m",
        choices=["websocket", "polling"],
        default=os.getenv("MONITOR_MODE", "websocket"),
        help="監視モード",
    )
    parser.add_argument(
        "--interval", "-i",
        type=int,
        default=int(os.getenv("MONITOR_INTERVAL_SECONDS", "3")),
        help="pollingモード時の取得間隔(秒)",
    )
    parser.add_argument(
        "--alert-threshold",
        type=float,
        default=float(os.getenv("PRICE_ALERT_THRESHOLD", "2.0")),
        help="アラート閾値(%)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    symbols = [s for s in args.symbols.split(",") if s.strip()]

    if not symbols:
        logger.error("監視銘柄を指定してください (--symbols または .env の WATCH_SYMBOLS)")
        sys.exit(1)

    try:
        auth = KabuAuthClient()
        auth.fetch_token()
    except KabuAuthError as e:
        logger.error(f"認証エラー: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"kabuステーション起動確認: {e}")
        sys.exit(1)

    monitor = PriceMonitor(
        auth=auth,
        symbols=symbols,
        mode=args.mode,
        interval=args.interval,
        alert_threshold_pct=args.alert_threshold,
    )
    monitor.start()


if __name__ == "__main__":
    main()
