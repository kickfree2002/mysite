"""
SBI証券 HyperSBI2 株価取得モジュール

現在値・始値・高値・安値・出来高などの株価情報を取得します。
"""

import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

load_dotenv()


@dataclass
class StockQuote:
    """株価データを保持するデータクラス"""
    symbol: str                      # 銘柄コード (例: "7203")
    name: str = ""                   # 銘柄名 (例: "トヨタ自動車")
    current_price: Optional[float] = None   # 現在値
    open_price: Optional[float] = None      # 始値
    high_price: Optional[float] = None      # 高値
    low_price: Optional[float] = None       # 安値
    prev_close: Optional[float] = None      # 前日終値
    change: Optional[float] = None          # 前日比(円)
    change_pct: Optional[float] = None      # 前日比(%)
    volume: Optional[int] = None            # 出来高
    turnover: Optional[float] = None        # 売買代金(円)
    market: str = "東証プライム"            # 市場区分
    timestamp: datetime = field(default_factory=datetime.now)  # 取得時刻
    is_market_open: bool = False            # 市場開放中かどうか

    def __str__(self) -> str:
        price_str = f"{self.current_price:,.0f}円" if self.current_price else "取得不可"
        change_str = ""
        if self.change is not None and self.change_pct is not None:
            sign = "+" if self.change >= 0 else ""
            change_str = f" ({sign}{self.change:.0f}円 / {sign}{self.change_pct:.2f}%)"
        return f"[{self.symbol}] {self.name}: {price_str}{change_str}"

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "name": self.name,
            "current_price": self.current_price,
            "open_price": self.open_price,
            "high_price": self.high_price,
            "low_price": self.low_price,
            "prev_close": self.prev_close,
            "change": self.change,
            "change_pct": self.change_pct,
            "volume": self.volume,
            "turnover": self.turnover,
            "market": self.market,
            "timestamp": self.timestamp.isoformat(),
            "is_market_open": self.is_market_open,
        }


class StockDataError(Exception):
    """株価データ取得エラー"""
    pass


class StockClient:
    """
    SBI証券 株価取得クライアント

    SBI証券の株価ページからスクレイピングで株価を取得します。
    認証済みセッションまたは未認証セッションで動作します。

    使用例:
        from auth import SBIAuthClient
        with SBIAuthClient() as auth:
            client = StockClient(session=auth.get_session())
            quote = client.get_quote("7203")
            print(quote)
    """

    # SBI証券 株価情報URL (未認証でもアクセス可能)
    QUOTE_URL = "https://finance.yahoo.co.jp/quote/{symbol}.T"
    # SBI証券ログイン後の株価詳細URL
    SBI_QUOTE_URL = (
        "https://site3.sbisec.co.jp/ETGate/"
        "?_ControlID=WPLETsiR001Control"
        "&_PageID=WPLETsiR001Isi10"
        "&_ActionID=stockInfoTop"
        "&stock_sec_code={symbol}"
        "&ref_from=1"
    )

    def __init__(self, session: Optional[requests.Session] = None):
        """
        Args:
            session: 認証済みrequests.Session。Noneの場合は新規セッションを作成。
        """
        if session:
            self._session = session
        else:
            self._session = requests.Session()
            self._session.headers.update({
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                "Accept-Language": "ja,en-US;q=0.7,en;q=0.3",
            })

        self._cache: dict[str, tuple[StockQuote, float]] = {}  # {symbol: (quote, timestamp)}
        self._cache_ttl = 3  # キャッシュ有効期間(秒)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=5),
        retry=retry_if_exception_type(requests.exceptions.RequestException),
        reraise=True,
    )
    def get_quote(self, symbol: str, use_cache: bool = True) -> StockQuote:
        """
        指定銘柄の株価を取得します。

        Args:
            symbol: 銘柄コード (例: "7203", "9984")
            use_cache: Trueの場合、TTL内であればキャッシュを返す

        Returns:
            StockQuote: 株価データ

        Raises:
            StockDataError: データ取得失敗時
        """
        symbol = str(symbol).zfill(4)

        # キャッシュチェック
        if use_cache and symbol in self._cache:
            cached_quote, cached_time = self._cache[symbol]
            if time.time() - cached_time < self._cache_ttl:
                logger.debug(f"[{symbol}] キャッシュから取得")
                return cached_quote

        logger.debug(f"[{symbol}] 株価取得中...")
        url = self.QUOTE_URL.format(symbol=symbol)

        try:
            resp = self._session.get(url, timeout=10)
            resp.raise_for_status()
            quote = self._parse_yahoo_finance(symbol, resp.text)
            # キャッシュに保存
            self._cache[symbol] = (quote, time.time())
            return quote
        except requests.exceptions.RequestException as e:
            raise StockDataError(f"[{symbol}] HTTP取得エラー: {e}") from e

    def get_quotes(self, symbols: list[str]) -> dict[str, StockQuote]:
        """
        複数銘柄の株価を一括取得します。

        Args:
            symbols: 銘柄コードのリスト

        Returns:
            {銘柄コード: StockQuote} の辞書
        """
        results = {}
        for symbol in symbols:
            try:
                results[symbol] = self.get_quote(symbol)
                logger.debug(f"取得完了: {results[symbol]}")
            except StockDataError as e:
                logger.warning(f"取得失敗: {e}")
                results[symbol] = StockQuote(symbol=symbol)  # 空データで埋める
        return results

    def _parse_yahoo_finance(self, symbol: str, html: str) -> StockQuote:
        """Yahoo!ファイナンスのHTMLから株価情報をパース"""
        soup = BeautifulSoup(html, "lxml")
        quote = StockQuote(symbol=symbol, timestamp=datetime.now())

        # 銘柄名取得
        name_tag = (
            soup.find("h1", class_=re.compile(r"(stockName|name|title)"))
            or soup.find("title")
        )
        if name_tag:
            raw_name = name_tag.get_text(strip=True)
            # タイトルから銘柄名を抽出 ("トヨタ自動車(7203) 株価 - Yahoo!ファイナンス")
            raw_name = raw_name.split("(")[0].split("【")[0].strip()
            quote.name = raw_name[:20]  # 最大20文字

        # 現在値のパース (複数のセレクタを試みる)
        price_selectors = [
            {"data-field": "regularMarketPrice"},
            {"class": re.compile(r"(stoksPrice|price|currentPrice)")},
        ]
        for selector in price_selectors:
            tag = soup.find(attrs=selector)
            if tag:
                quote.current_price = self._parse_price(tag.get_text(strip=True))
                if quote.current_price:
                    break

        # 前日比・前日比率
        change_tag = soup.find(attrs={"data-field": "regularMarketChange"})
        if change_tag:
            quote.change = self._parse_price(change_tag.get_text(strip=True))

        change_pct_tag = soup.find(attrs={"data-field": "regularMarketChangePercent"})
        if change_pct_tag:
            text = change_pct_tag.get_text(strip=True).replace("%", "").replace("(", "").replace(")", "")
            quote.change_pct = self._parse_float(text)

        # OHLV情報
        field_map = {
            "regularMarketOpen": "open_price",
            "regularMarketDayHigh": "high_price",
            "regularMarketDayLow": "low_price",
            "regularMarketPreviousClose": "prev_close",
            "regularMarketVolume": "volume",
        }
        for data_field, attr in field_map.items():
            tag = soup.find(attrs={"data-field": data_field})
            if tag:
                value = self._parse_price(tag.get_text(strip=True))
                if attr == "volume" and value:
                    setattr(quote, attr, int(value))
                else:
                    setattr(quote, attr, value)

        # 前日比の計算 (直接取得できない場合)
        if quote.current_price and quote.prev_close and quote.change is None:
            quote.change = quote.current_price - quote.prev_close
            quote.change_pct = (quote.change / quote.prev_close) * 100

        # 市場開放状態の推定
        quote.is_market_open = self._is_market_open()

        return quote

    def _parse_price(self, text: str) -> Optional[float]:
        """価格文字列をfloatにパース (例: "2,345.6" → 2345.6)"""
        if not text:
            return None
        cleaned = re.sub(r"[,円\s+]", "", text)
        cleaned = cleaned.lstrip("+")
        return self._parse_float(cleaned)

    def _parse_float(self, text: str) -> Optional[float]:
        """文字列をfloatにパース (エラー時はNone)"""
        try:
            return float(text)
        except (ValueError, TypeError):
            return None

    def _is_market_open(self) -> bool:
        """現在時刻が東証の取引時間内かどうかを判定"""
        now = datetime.now()
        # 土日は閉市
        if now.weekday() >= 5:
            return False
        hour, minute = now.hour, now.minute
        # 前場: 9:00-11:30
        if (9, 0) <= (hour, minute) <= (11, 30):
            return True
        # 後場: 12:30-15:30 (2024年11月より15:30まで延長)
        if (12, 30) <= (hour, minute) <= (15, 30):
            return True
        return False

    def clear_cache(self) -> None:
        """キャッシュをクリア"""
        self._cache.clear()
        logger.debug("株価キャッシュをクリアしました")


def get_stock_price(symbol: str) -> Optional[float]:
    """
    銘柄コードから現在株価を取得する簡易関数

    Args:
        symbol: 銘柄コード

    Returns:
        現在値 (取得失敗時はNone)
    """
    try:
        client = StockClient()
        quote = client.get_quote(symbol)
        return quote.current_price
    except Exception as e:
        logger.error(f"株価取得失敗 [{symbol}]: {e}")
        return None


if __name__ == "__main__":
    import sys
    logger.remove()
    logger.add(sys.stdout, format="{time:HH:mm:ss} | {level} | {message}", level="DEBUG")

    symbols = os.getenv("WATCH_SYMBOLS", "7203,9984,6758").split(",")
    logger.info(f"株価取得テスト: {symbols}")

    client = StockClient()
    quotes = client.get_quotes(symbols)

    print()
    print("=" * 60)
    for symbol, quote in quotes.items():
        if quote.current_price:
            print(quote)
        else:
            print(f"[{symbol}] データ取得失敗")
    print("=" * 60)
