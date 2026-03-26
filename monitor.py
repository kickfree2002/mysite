"""
SBI証券 HyperSBI2 価格監視ループ

設定した銘柄の株価を定期的に取得し、価格変動を監視・通知します。
Ctrl+C で安全に停止できます。

実行方法:
    python monitor.py
    python monitor.py --symbols 7203,9984,6758 --interval 10
"""

import argparse
import os
import signal
import sys
import time
from collections import defaultdict
from datetime import datetime
from typing import Optional

from dotenv import load_dotenv
from loguru import logger

from stock import StockClient, StockQuote, StockDataError

load_dotenv()

# ログ設定
log_file = os.getenv("LOG_FILE", "logs/trading.log")
log_level = os.getenv("LOG_LEVEL", "INFO")
logger.remove()
logger.add(sys.stdout, format="{time:HH:mm:ss} | {level:<8} | {message}", level=log_level)
logger.add(log_file, format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
           level="DEBUG", rotation="1 day", retention="30 days")


class PriceAlert:
    """価格変動アラート"""
    def __init__(self, symbol: str, price: float, change_pct: float, direction: str):
        self.symbol = symbol
        self.price = price
        self.change_pct = change_pct
        self.direction = direction  # "UP" or "DOWN"
        self.timestamp = datetime.now()

    def __str__(self):
        arrow = "↑" if self.direction == "UP" else "↓"
        return (
            f"[アラート] {self.symbol} {arrow} "
            f"{self.price:,.0f}円 "
            f"({'+' if self.change_pct >= 0 else ''}{self.change_pct:.2f}%)"
        )


class PriceMonitor:
    """
    株価監視クラス

    指定した銘柄の株価を定期的に取得し、
    価格変動が閾値を超えた場合にアラートを発します。

    使用例:
        monitor = PriceMonitor(symbols=["7203", "9984"])
        monitor.start()  # Ctrl+C で停止
    """

    def __init__(
        self,
        symbols: list[str],
        interval_open: int = None,
        interval_closed: int = None,
        alert_threshold_pct: float = None,
        session=None,
    ):
        """
        Args:
            symbols: 監視対象銘柄コードのリスト
            interval_open: 市場開放時の取得間隔(秒)
            interval_closed: 市場閉鎖時の取得間隔(秒)
            alert_threshold_pct: 価格変動アラート閾値(%)
            session: 認証済みrequests.Session (省略可)
        """
        self.symbols = [str(s).strip().zfill(4) for s in symbols if s.strip()]
        self.interval_open = interval_open or int(os.getenv("MONITOR_INTERVAL_SECONDS", "5"))
        self.interval_closed = interval_closed or int(os.getenv("MONITOR_INTERVAL_CLOSED", "60"))
        self.alert_threshold_pct = alert_threshold_pct or float(
            os.getenv("PRICE_ALERT_THRESHOLD", "2.0")
        )

        self._client = StockClient(session=session)
        self._running = False
        self._iteration = 0
        self._errors = defaultdict(int)  # {symbol: 連続エラー数}

        # 直前の株価を保持 (変動検知用)
        self._prev_prices: dict[str, Optional[float]] = {s: None for s in self.symbols}
        # アラート履歴
        self._alerts: list[PriceAlert] = []
        # 統計情報
        self._stats = {
            "start_time": None,
            "total_fetches": 0,
            "successful_fetches": 0,
            "failed_fetches": 0,
            "alerts_fired": 0,
        }

    def start(self) -> None:
        """監視ループを開始します。Ctrl+C で停止。"""
        self._running = True
        self._stats["start_time"] = datetime.now()

        # シグナルハンドラ設定 (Ctrl+C の安全な処理)
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

        logger.info("=" * 60)
        logger.info("SBI HyperSBI2 価格監視システム 起動")
        logger.info(f"監視銘柄: {', '.join(self.symbols)}")
        logger.info(f"取得間隔: 市場開放時 {self.interval_open}秒 / 市場閉鎖時 {self.interval_closed}秒")
        logger.info(f"アラート閾値: ±{self.alert_threshold_pct}%")
        logger.info("停止するには Ctrl+C を押してください")
        logger.info("=" * 60)

        try:
            while self._running:
                self._iteration += 1
                self._run_once()
                interval = self._get_current_interval()
                logger.debug(f"次回取得まで {interval}秒待機...")
                self._interruptible_sleep(interval)
        except KeyboardInterrupt:
            pass
        finally:
            self._shutdown()

    def _run_once(self) -> list[StockQuote]:
        """1回分の株価取得・アラートチェックを実行"""
        now = datetime.now()
        logger.info(f"--- {now.strftime('%H:%M:%S')} 株価取得 (#{self._iteration}) ---")

        quotes = []
        for symbol in self.symbols:
            self._stats["total_fetches"] += 1
            try:
                quote = self._client.get_quote(symbol, use_cache=False)
                quotes.append(quote)
                self._stats["successful_fetches"] += 1
                self._errors[symbol] = 0  # エラーカウントリセット

                # 表示
                self._display_quote(quote)

                # アラートチェック
                alert = self._check_alert(quote)
                if alert:
                    self._fire_alert(alert)

                # 前回価格を更新
                self._prev_prices[symbol] = quote.current_price

            except StockDataError as e:
                self._stats["failed_fetches"] += 1
                self._errors[symbol] += 1
                consecutive = self._errors[symbol]
                logger.warning(f"[{symbol}] 取得失敗 (連続{consecutive}回): {e}")

                if consecutive >= 5:
                    logger.error(f"[{symbol}] 連続{consecutive}回エラー。ネットワークまたは認証を確認してください。")

        # 市場状態の表示
        if quotes:
            is_open = quotes[0].is_market_open
            market_status = "開場中" if is_open else "閉場中"
            logger.info(f"市場状態: {market_status}")

        return quotes

    def _display_quote(self, quote: StockQuote) -> None:
        """株価を見やすく表示"""
        if not quote.current_price:
            logger.warning(f"  [{quote.symbol}] {quote.name or '(名称不明)'}: データ取得不可")
            return

        change_str = ""
        if quote.change is not None and quote.change_pct is not None:
            sign = "+" if quote.change >= 0 else ""
            arrow = "▲" if quote.change >= 0 else "▼"
            change_str = f" {arrow} {sign}{quote.change:.0f}円 ({sign}{quote.change_pct:.2f}%)"

        volume_str = f"  出来高: {quote.volume:,}" if quote.volume else ""
        logger.info(
            f"  [{quote.symbol}] {quote.name or '':12s} "
            f"{quote.current_price:>10,.0f}円{change_str}{volume_str}"
        )

    def _check_alert(self, quote: StockQuote) -> Optional[PriceAlert]:
        """価格変動が閾値を超えていればアラートを返す"""
        if not quote.current_price or not quote.change_pct:
            return None

        abs_change = abs(quote.change_pct)
        if abs_change >= self.alert_threshold_pct:
            direction = "UP" if quote.change_pct >= 0 else "DOWN"
            return PriceAlert(
                symbol=quote.symbol,
                price=quote.current_price,
                change_pct=quote.change_pct,
                direction=direction,
            )
        return None

    def _fire_alert(self, alert: PriceAlert) -> None:
        """アラートを発火"""
        self._alerts.append(alert)
        self._stats["alerts_fired"] += 1
        logger.warning(str(alert))

    def _get_current_interval(self) -> int:
        """現在の市場状態に応じた取得間隔を返す"""
        now = datetime.now()
        if now.weekday() >= 5:
            return self.interval_closed
        hour, minute = now.hour, now.minute
        if (9, 0) <= (hour, minute) <= (11, 30) or (12, 30) <= (hour, minute) <= (15, 30):
            return self.interval_open
        return self.interval_closed

    def _interruptible_sleep(self, seconds: int) -> None:
        """Ctrl+C で中断可能なスリープ"""
        for _ in range(seconds):
            if not self._running:
                break
            time.sleep(1)

    def _handle_shutdown(self, signum, frame) -> None:
        """シグナルを受けて安全にシャットダウン"""
        logger.info("停止シグナル受信。監視を終了します...")
        self._running = False

    def _shutdown(self) -> None:
        """シャットダウン処理: 統計情報を表示"""
        elapsed = (
            (datetime.now() - self._stats["start_time"]).total_seconds()
            if self._stats["start_time"] else 0
        )
        logger.info("=" * 60)
        logger.info("監視終了 - 統計情報")
        logger.info(f"  稼働時間: {elapsed:.0f}秒 ({elapsed/60:.1f}分)")
        logger.info(f"  取得回数: {self._stats['total_fetches']} 回")
        logger.info(f"  成功: {self._stats['successful_fetches']} / 失敗: {self._stats['failed_fetches']}")
        logger.info(f"  アラート発火: {self._stats['alerts_fired']} 回")
        if self._alerts:
            logger.info("  直近アラート:")
            for alert in self._alerts[-5:]:
                logger.info(f"    {alert.timestamp.strftime('%H:%M:%S')} {alert}")
        logger.info("=" * 60)


def parse_args() -> argparse.Namespace:
    """コマンドライン引数のパース"""
    parser = argparse.ArgumentParser(
        description="SBI HyperSBI2 株価監視システム",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--symbols", "-s",
        type=str,
        default=os.getenv("WATCH_SYMBOLS", "7203,9984,6758"),
        help="監視銘柄コード (カンマ区切り)",
    )
    parser.add_argument(
        "--interval", "-i",
        type=int,
        default=int(os.getenv("MONITOR_INTERVAL_SECONDS", "5")),
        help="市場開放時の取得間隔(秒)",
    )
    parser.add_argument(
        "--interval-closed",
        type=int,
        default=int(os.getenv("MONITOR_INTERVAL_CLOSED", "60")),
        help="市場閉鎖時の取得間隔(秒)",
    )
    parser.add_argument(
        "--alert-threshold",
        type=float,
        default=float(os.getenv("PRICE_ALERT_THRESHOLD", "2.0")),
        help="価格変動アラート閾値(%)",
    )
    parser.add_argument(
        "--no-auth",
        action="store_true",
        help="認証なしで実行 (公開データのみ取得)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]

    if not symbols:
        logger.error("監視銘柄が指定されていません。--symbols または .env の WATCH_SYMBOLS を設定してください。")
        sys.exit(1)

    session = None
    if not args.no_auth:
        try:
            from auth import SBIAuthClient, AuthenticationError
            logger.info("SBI証券に認証中...")
            auth_client = SBIAuthClient()
            auth_client.login()
            session = auth_client.get_session()
            logger.success("認証完了")
        except ImportError:
            logger.warning("auth.py が見つかりません。未認証モードで起動します。")
        except Exception as e:
            logger.warning(f"認証失敗: {e}。未認証モードで起動します。")

    monitor = PriceMonitor(
        symbols=symbols,
        interval_open=args.interval,
        interval_closed=args.interval_closed,
        alert_threshold_pct=args.alert_threshold,
        session=session,
    )
    monitor.start()


if __name__ == "__main__":
    main()
