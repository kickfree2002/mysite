"""
kabuステーション API 株価取得モジュール

kabuステーションのローカルREST API (/kabusapi/board) から
板情報・株価データを取得します。スクレイピング不要です。

主要フィールド (kabu STATION API /board レスポンス):
    Symbol          : 銘柄コード
    SymbolName      : 銘柄名
    Exchange        : 取引所コード
    ExchangeName    : 取引所名
    CurrentPrice    : 現在値
    CurrentPriceTime: 現在値時刻
    OpeningPrice    : 始値
    HighPrice       : 高値
    LowPrice        : 安値
    PreviousClose   : 前日終値
    ChangePreviousClose      : 前日比
    ChangePreviousClosePer   : 前日比(%)
    TradingVolume   : 出来高
    TradingValue    : 売買代金
"""

import os
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import requests
from dotenv import load_dotenv
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from auth import KabuAuthClient, KabuAuthError

load_dotenv()


@dataclass
class StockQuote:
    """株価データ (kabu STATION API /board レスポンスに対応)"""
    symbol: str                             # 銘柄コード (例: "7203")
    exchange: int = 1                       # 取引所コード (1=東証プライム)
    name: str = ""                          # 銘柄名
    exchange_name: str = ""                 # 取引所名
    current_price: Optional[float] = None  # 現在値
    current_price_time: Optional[str] = None  # 現在値時刻
    open_price: Optional[float] = None     # 始値
    high_price: Optional[float] = None     # 高値
    low_price: Optional[float] = None      # 安値
    prev_close: Optional[float] = None     # 前日終値
    change: Optional[float] = None         # 前日比(円)
    change_pct: Optional[float] = None     # 前日比(%)
    volume: Optional[float] = None         # 出来高
    turnover: Optional[float] = None       # 売買代金
    # 気配値 (最良買・売気配)
    ask: Optional[float] = None            # 売気配値
    ask_qty: Optional[float] = None        # 売気配数量
    bid: Optional[float] = None            # 買気配値
    bid_qty: Optional[float] = None        # 買気配数量
    timestamp: datetime = field(default_factory=datetime.now)

    @classmethod
    def from_api(cls, data: dict) -> "StockQuote":
        """kabu STATION API レスポンスdictからStockQuoteを生成"""
        return cls(
            symbol=str(data.get("Symbol", "")),
            exchange=data.get("Exchange", 1),
            name=data.get("SymbolName", ""),
            exchange_name=data.get("ExchangeName", ""),
            current_price=data.get("CurrentPrice"),
            current_price_time=data.get("CurrentPriceTime"),
            open_price=data.get("OpeningPrice"),
            high_price=data.get("HighPrice"),
            low_price=data.get("LowPrice"),
            prev_close=data.get("PreviousClose"),
            change=data.get("ChangePreviousClose"),
            change_pct=data.get("ChangePreviousClosePer"),
            volume=data.get("TradingVolume"),
            turnover=data.get("TradingValue"),
            ask=data.get("Ask"),
            ask_qty=data.get("AskQty"),
            bid=data.get("Bid"),
            bid_qty=data.get("BidQty"),
            timestamp=datetime.now(),
        )

    def __str__(self) -> str:
        price_str = f"{self.current_price:,.1f}円" if self.current_price is not None else "取得不可"
        change_str = ""
        if self.change is not None and self.change_pct is not None:
            sign = "+" if self.change >= 0 else ""
            arrow = "▲" if self.change >= 0 else "▼"
            change_str = f" {arrow}{sign}{self.change:.1f}円 ({sign}{self.change_pct:.2f}%)"
        return f"[{self.symbol}] {self.name}: {price_str}{change_str}"

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "exchange": self.exchange,
            "name": self.name,
            "exchange_name": self.exchange_name,
            "current_price": self.current_price,
            "current_price_time": self.current_price_time,
            "open_price": self.open_price,
            "high_price": self.high_price,
            "low_price": self.low_price,
            "prev_close": self.prev_close,
            "change": self.change,
            "change_pct": self.change_pct,
            "volume": self.volume,
            "turnover": self.turnover,
            "ask": self.ask,
            "bid": self.bid,
            "timestamp": self.timestamp.isoformat(),
        }


class StockDataError(Exception):
    """株価データ取得エラー"""
    pass


class StockClient:
    """
    kabuステーション API 株価取得クライアント

    使用例:
        auth = KabuAuthClient()
        client = StockClient(auth)

        # 単一銘柄取得
        quote = client.get_quote("7203")   # 東証プライム(デフォルト)
        quote = client.get_quote("7203", exchange=1)

        # 複数銘柄取得
        quotes = client.get_quotes(["7203@1", "9984@1", "6758@1"])
    """

    BOARD_ENDPOINT = "/kabusapi/board/{symbol}@{exchange}"
    SYMBOL_ENDPOINT = "/kabusapi/symbol/{symbol}@{exchange}"

    def __init__(self, auth: KabuAuthClient):
        self._auth = auth
        self._base_url = auth.base_url
        self._session = requests.Session()
        self._cache: dict[str, tuple[StockQuote, float]] = {}
        self._cache_ttl = 2  # キャッシュ有効期間(秒)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=5),
        retry=retry_if_exception_type(requests.exceptions.ConnectionError),
        reraise=True,
    )
    def get_quote(self, symbol: str, exchange: int = 1, use_cache: bool = True) -> StockQuote:
        """
        板情報から株価を取得します。

        Args:
            symbol  : 銘柄コード (例: "7203") または "7203@1" 形式
            exchange: 取引所コード (1=東証P, 3=東証S, 5=東証G, 2=名証, 6=名証S)
                      symbol に "@1" 等が含まれる場合はそちらが優先されます
            use_cache: Trueの場合、TTL内ならキャッシュを返す

        Returns:
            StockQuote

        Raises:
            StockDataError: データ取得失敗時
        """
        # "7203@1" 形式の解析
        if "@" in str(symbol):
            parts = str(symbol).split("@", 1)
            symbol, exchange = parts[0].strip(), int(parts[1].strip())

        cache_key = f"{symbol}@{exchange}"

        if use_cache and cache_key in self._cache:
            cached_quote, cached_time = self._cache[cache_key]
            if time.time() - cached_time < self._cache_ttl:
                logger.debug(f"[{cache_key}] キャッシュから取得")
                return cached_quote

        url = f"{self._base_url}{self.BOARD_ENDPOINT.format(symbol=symbol, exchange=exchange)}"
        headers = self._auth.get_headers()

        try:
            resp = self._session.get(url, headers=headers, timeout=5)
        except requests.exceptions.ConnectionError as e:
            raise requests.exceptions.ConnectionError(
                f"kabuステーションに接続できません: {e}"
            )

        if resp.status_code == 200:
            quote = StockQuote.from_api(resp.json())
            self._cache[cache_key] = (quote, time.time())
            logger.debug(f"[{cache_key}] 取得完了: {quote.current_price}")
            return quote

        if resp.status_code == 401:
            # トークン期限切れの可能性 → 強制リフレッシュして1回リトライ
            logger.warning("トークン期限切れの可能性。再取得します...")
            self._auth.get_token(force_refresh=True)
            headers = self._auth.get_headers()
            resp = self._session.get(url, headers=headers, timeout=5)
            if resp.status_code == 200:
                quote = StockQuote.from_api(resp.json())
                self._cache[cache_key] = (quote, time.time())
                return quote

        try:
            err = resp.json()
            message = err.get("Message", resp.text)
        except Exception:
            message = resp.text

        raise StockDataError(
            f"[{cache_key}] 板情報取得失敗 HTTP {resp.status_code}: {message}"
        )

    def get_quotes(self, symbol_specs: list[str]) -> dict[str, StockQuote]:
        """
        複数銘柄の株価を一括取得します。

        Args:
            symbol_specs: ["7203@1", "9984@1", ...] 形式のリスト

        Returns:
            {"7203@1": StockQuote, ...}
        """
        results = {}
        for spec in symbol_specs:
            spec = spec.strip()
            try:
                quote = self.get_quote(spec)
                key = f"{quote.symbol}@{quote.exchange}"
                results[key] = quote
            except (StockDataError, Exception) as e:
                logger.warning(f"取得失敗 [{spec}]: {e}")
                # 失敗した場合は空のStockQuoteをセット
                sym, exch = (spec.split("@") + ["1"])[:2]
                results[spec] = StockQuote(symbol=sym, exchange=int(exch))
        return results

    def clear_cache(self) -> None:
        self._cache.clear()


def get_stock_price(symbol: str, exchange: int = 1) -> Optional[float]:
    """
    銘柄コードから現在株価を取得する簡易関数

    Args:
        symbol  : 銘柄コード
        exchange: 取引所コード

    Returns:
        現在値 (取得失敗時はNone)
    """
    try:
        auth = KabuAuthClient()
        client = StockClient(auth)
        quote = client.get_quote(symbol, exchange)
        return quote.current_price
    except Exception as e:
        logger.error(f"株価取得失敗 [{symbol}@{exchange}]: {e}")
        return None


if __name__ == "__main__":
    import sys
    logger.remove()
    logger.add(sys.stdout, format="{time:HH:mm:ss} | {level} | {message}", level="DEBUG")

    specs = os.getenv("WATCH_SYMBOLS", "7203@1,9984@1,6758@1").split(",")
    logger.info(f"株価取得テスト: {specs}")

    try:
        auth = KabuAuthClient()
        auth.fetch_token()
        client = StockClient(auth)
        quotes = client.get_quotes(specs)

        print()
        print("=" * 60)
        for key, quote in quotes.items():
            print(quote)
        print("=" * 60)
    except KabuAuthError as e:
        logger.error(f"認証エラー: {e}")
        sys.exit(1)
