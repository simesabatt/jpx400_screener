"""
Yahoo Finance を用いた市場センチメントスコアリング
"""

import json
import sys
import os
from contextlib import contextmanager
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd

try:
    import yfinance as yf
except ImportError:  # pragma: no cover - 実行環境に依存
    yf = None


@contextmanager
def suppress_stderr():
    """標準エラー出力を一時的に抑制"""
    with open(os.devnull, "w") as devnull:
        old_stderr = sys.stderr
        sys.stderr = devnull
        try:
            yield
        finally:
            sys.stderr = old_stderr


class YahooFinanceSentimentScorer:
    """Yahoo Financeを使用した市場センチメントスコアリングクラス"""

    def __init__(self, weights: Optional[Dict[str, float]] = None, weights_version: int = 1):
        default_weights = {
            "nikkei225": 1.0,
            "nikkei225_futures": 0.0,  # 無効化: Yahoo Financeでは日経平均先物データは提供されていないため
            # 米主要指数
            "dji": 1.0,
            "nasdaq": 1.0,
            "russell2000": 0.8,
            # 金利・通貨
            "us10y": 0.8,
            "dxy": 0.8,
            "sp500": 1.2,
            "sox": 1.0,
            "usdjpy": 0.8,
            "advance_decline": 1.0,
            "vix": 1.0,
            "volume": 0.6,
            "sector": 0.8,
            "rsi": 0.6,
            "moving_average": 0.8,
            "bollinger_bands": 0.5,
        }
        self.weights: Dict[str, float] = weights or default_weights
        self.weights_version: int = weights_version

    # -----------------------------
    # 公開API
    # -----------------------------
    def calculate_score(
        self,
        jpx_symbols: Optional[List[str]] = None,
        sector_map: Optional[Dict[str, List[str]]] = None,
        topix_etf_symbol: str = "1306.T",
    ) -> Dict:
        """総合スコアを計算"""
        self._ensure_yfinance()

        indicators: Dict[str, Dict] = {}

        # マクロ経済
        indicators["nikkei225"] = self.fetch_nikkei225()
        
        # 日経平均先物（無効化: Yahoo Financeではデータが提供されていないため）
        # 重みが0の場合は取得処理をスキップ
        if self.weights.get("nikkei225_futures", 0.0) > 0:
            nikkei_futures = self.fetch_nikkei225_futures()
            if nikkei_futures.get("value") is not None:
                indicators["nikkei225_futures"] = nikkei_futures
            else:
                print("[INFO] 日経平均先物のデータが取得できませんでした。他の指標でスコアを計算します。")
                indicators["nikkei225_futures"] = self._empty_indicator("nikkei225_futures")
        else:
            # 重みが0の場合は空の指標を設定（スコア計算に影響しない）
            indicators["nikkei225_futures"] = self._empty_indicator("nikkei225_futures")
        
        # 米主要株価指数
        indicators["dji"] = self.fetch_dji()
        indicators["nasdaq"] = self.fetch_nasdaq()
        indicators["russell2000"] = self.fetch_russell2000()

        # 米長期金利・ドル指数
        indicators["us10y"] = self.fetch_us10y()
        indicators["dxy"] = self.fetch_dxy()

        indicators["sp500"] = self.fetch_sp500()
        indicators["sox"] = self.fetch_sox()
        indicators["usdjpy"] = self.fetch_usdjpy()

        # 市場全体
        indicators["vix"] = self.fetch_vix()
        indicators["advance_decline"] = (
            self.calculate_advance_decline(jpx_symbols) if jpx_symbols else self._empty_indicator("advance_decline")
        )
        indicators["volume"] = self.fetch_topix_etf_volume(topix_etf_symbol)

        # テクニカル（日経平均）
        technical = self.calculate_nikkei_technical()
        indicators.update(technical)

        # セクター
        indicators["sector"] = (
            self.calculate_sector_strength(sector_map) if sector_map else self._empty_indicator("sector")
        )

        # スコア合計（データが取得できた指標のみをカウント）
        total_score = 0.0
        total_weight = 0.0  # 正規化用の重み合計
        
        for key, data in indicators.items():
            weight = self.weights.get(key, 1.0)
            score = data.get("score", 0.0)
            
            # データが取得できた指標のみをカウント（valueがNoneでない）
            if data.get("value") is not None:
                weighted_score = score * weight
                data["weight"] = weight
                data["weighted_score"] = weighted_score
                total_score += weighted_score
                total_weight += weight
            else:
                # データが取得できなかった指標は重み0として扱う
                data["weight"] = 0.0
                data["weighted_score"] = 0.0
                data["note"] = "データ取得失敗"
        
        # 取得できた指標の重みで正規化（オプション）
        # 現在は正規化せず、取得できた指標のみでスコアを計算

        normalized_score = max(0.0, min(100.0, 50.0 + total_score))
        level = self.get_sentiment_level(normalized_score)

        return {
            "score": normalized_score,
            "level": level,
            "indicators": indicators,
            "calculated_at": datetime.now().isoformat(),
            "weights_version": self.weights_version,
            "weights_json": json.dumps(self.weights),  # 重み付けパラメータをJSON形式で保存
            "weights_description": f"重み付けバージョン {self.weights_version}",
        }

    def get_sentiment_level(self, score: float) -> str:
        """スコアから地合いレベルを判定"""
        if score >= 80:
            return "非常に良好"
        if score >= 60:
            return "良好"
        if score >= 40:
            return "中立"
        if score >= 20:
            return "悪い"
        return "非常に悪い"

    # -----------------------------
    # 個別指標
    # -----------------------------
    def fetch_nikkei225(self) -> Dict:
        df = self._fetch_history("^N225", period="2d", interval="1d")
        value, change_pct = self._get_value_and_change(df)
        score = self._score_from_change(change_pct, [(1, 8), (0.5, 4), (-0.5, 0), (-1, -4), (-1000, -8)])
        return self._build_indicator("nikkei225", value, change_pct, score)

    def fetch_nikkei225_futures(self) -> Dict:
        """
        日経平均先物を取得（複数のシンボルを試行）
        
        注意: 
        - Yahoo Financeでは日経平均先物のデータは提供されていない可能性が高いです。
        - 調査結果では、yfinanceでは日経平均先物のデータを取得できないことが確認されています。
        - このメソッドは複数のシンボル形式を試行しますが、通常はデータが取得できません。
        - データが取得できない場合は空の指標を返し、スコア計算から自動的に除外されます。
        - 重み付けも0になるため、スコア計算には影響しません。
        
        代替案:
        - 日経平均先物のデータが必要な場合は、他のデータソース（証券会社API、JPX公式データ等）の利用を検討してください。
        - または、この指標の重みを0に設定して無効化することも可能です。
        """
        # 複数のシンボルを試行（一般的なシンボル）
        # 注意: これらのシンボルは通常、Yahoo Financeでは利用できません
        symbols = [
            "N225F=F",      # 一般的な先物シンボル形式（通常は利用不可）
            "^N225F",       # 指数形式（通常は利用不可）
            "N225F",        # シンプル形式（通常は利用不可）
            "N225F=F",      # 別の形式（通常は利用不可）
        ]
        df = None
        used_symbol = None
        last_error = None
        
        for symbol in symbols:
            try:
                # yfinanceのエラーメッセージを抑制
                with suppress_stderr():
                    df = self._fetch_history(symbol, period="2d", interval="1d")
                if df is not None and not df.empty:
                    used_symbol = symbol
                    break
            except Exception as e:
                last_error = str(e)
                continue
        
        if df is None or df.empty:
            # エラーメッセージを簡潔に（警告レベルを下げる）
            print(f"[INFO] 日経平均先物のデータが取得できませんでした。")
            print(f"       （試行したシンボル: {', '.join(symbols)}）")
            print(f"       注意: 市場が閉まっている時間や、Yahoo Financeでデータが提供されていない可能性があります。")
            return self._empty_indicator("nikkei225_futures")
        
        value, change_pct = self._get_value_and_change(df)
        score = self._score_from_change(change_pct, [(1, 10), (0.5, 5), (-0.5, 0), (-1, -5), (-1000, -10)])
        result = self._build_indicator("nikkei225_futures", value, change_pct, score)
        result["symbol_used"] = used_symbol
        return result

    def fetch_dji(self) -> Dict:
        df = self._fetch_history("^DJI", period="2d", interval="1d")
        value, change_pct = self._get_value_and_change(df)
        score = self._score_from_change(change_pct, [(1, 8), (0.5, 4), (-0.5, 0), (-1, -4), (-1000, -8)])
        return self._build_indicator("dji", value, change_pct, score)

    def fetch_nasdaq(self) -> Dict:
        df = self._fetch_history("^IXIC", period="2d", interval="1d")
        value, change_pct = self._get_value_and_change(df)
        score = self._score_from_change(change_pct, [(1, 8), (0.5, 4), (-0.5, 0), (-1, -4), (-1000, -8)])
        return self._build_indicator("nasdaq", value, change_pct, score)

    def fetch_russell2000(self) -> Dict:
        df = self._fetch_history("^RUT", period="2d", interval="1d")
        value, change_pct = self._get_value_and_change(df)
        score = self._score_from_change(change_pct, [(1, 8), (0.5, 4), (-0.5, 0), (-1, -4), (-1000, -8)])
        return self._build_indicator("russell2000", value, change_pct, score)

    def fetch_us10y(self) -> Dict:
        df = self._fetch_history("^TNX", period="5d", interval="1d")
        value, change_pct = self._get_value_and_change(df)
        # 金利上昇は株にはマイナス寄与とみなしスコアリング
        score = self._score_from_change(change_pct * -1 if change_pct is not None else None, [(1, 6), (0.5, 3), (-0.5, 0), (-1, -3), (-1000, -6)])
        return self._build_indicator("us10y", value, change_pct, score)

    def fetch_dxy(self) -> Dict:
        symbols = ["DX-Y.NYB", "UUP"]  # DXYが取れない場合はETF UUPをフォールバック
        df = None
        used = None
        for sym in symbols:
            df = self._fetch_history(sym, period="5d", interval="1d")
            if df is not None and not df.empty:
                used = sym
                break
        if df is None or df.empty:
            return self._empty_indicator("dxy")
        value, change_pct = self._get_value_and_change(df)
        # ドル高は日本株にマイナス寄与とみなしスコアリング
        score = self._score_from_change(change_pct * -1 if change_pct is not None else None, [(1, 6), (0.5, 3), (-0.5, 0), (-1, -3), (-1000, -6)])
        result = self._build_indicator("dxy", value, change_pct, score)
        result["symbol_used"] = used
        return result

    def fetch_sp500(self) -> Dict:
        df = self._fetch_history("^GSPC", period="2d", interval="1d")
        value, change_pct = self._get_value_and_change(df)
        score = self._score_from_change(change_pct, [(1, 8), (0.5, 4), (-0.5, 0), (-1, -4), (-1000, -8)])
        return self._build_indicator("sp500", value, change_pct, score)

    def fetch_sox(self) -> Dict:
        df = self._fetch_history("^SOX", period="2d", interval="1d")
        value, change_pct = self._get_value_and_change(df)
        score = self._score_from_change(change_pct, [(2, 8), (1, 5), (-1, 0), (-2, -5), (-1000, -8)])
        return self._build_indicator("sox", value, change_pct, score)

    def fetch_usdjpy(self) -> Dict:
        df = self._fetch_history("JPY=X", period="2d", interval="1d")
        value, change_abs = self._get_abs_change(df)
        # 円安(上昇)でプラス
        score = self._score_from_abs(
            change_abs,
            [
                (0.5, 5),
                (0.2, 3),
                (-0.2, 0),
                (-0.5, -3),
                (-100, -5),
            ],
        )
        return self._build_indicator("usdjpy", value, change_abs, score, field="change_abs")

    def fetch_vix(self) -> Dict:
        df = self._fetch_history("^VIX", period="5d", interval="1d")
        value = df["close"].iloc[-1] if not df.empty else None
        if value is None:
            score = 0.0
        elif value < 15:
            score = 8
        elif value < 20:
            score = 4
        elif value < 25:
            score = 0
        elif value < 30:
            score = -4
        else:
            score = -8
        return {
            "value": value,
            "score": score,
        }

    def fetch_topix_etf_volume(self, symbol: str = "1306.T") -> Dict:
        df = self._fetch_history(symbol, period="2d", interval="1d")
        if df is None or df.empty or len(df) < 2:
            return self._empty_indicator("volume")
        latest_vol = df["volume"].iloc[-1]
        prev_vol = df["volume"].iloc[-2]
        if prev_vol == 0:
            ratio = None
        else:
            ratio = latest_vol / prev_vol
        score = self._score_volume_ratio(ratio)
        return {
            "value": ratio,
            "score": score,
        }

    def calculate_advance_decline(self, symbols: List[str]) -> Dict:
        """JPX銘柄の騰落レシオ（簡易版）。symbolsは適宜サンプリング済みのリストを渡す想定。"""
        if not symbols:
            return self._empty_indicator("advance_decline")

        advances = 0
        declines = 0
        for symbol in symbols:
            df = self._fetch_history(f"{symbol}.T", period="2d", interval="1d")
            _, change_pct = self._get_value_and_change(df)
            if change_pct is None:
                continue
            if change_pct > 0:
                advances += 1
            elif change_pct < 0:
                declines += 1

        if advances == 0 and declines == 0:
            return self._empty_indicator("advance_decline")

        ratio = advances / declines if declines > 0 else float("inf")
        score = self._score_advance_decline(ratio)
        return {
            "value": ratio,
            "score": score,
        }

    def calculate_sector_strength(self, sector_map: Dict[str, List[str]]) -> Dict:
        """セクター別強弱（代表銘柄の平均変化率）。sector_map: {'IT': ['9432','9984'], ...}"""
        if not sector_map:
            return self._empty_indicator("sector")

        sector_results: List[float] = []
        for symbols in sector_map.values():
            if not symbols:
                continue
            changes: List[float] = []
            for symbol in symbols:
                df = self._fetch_history(f"{symbol}.T", period="2d", interval="1d")
                _, change_pct = self._get_value_and_change(df)
                if change_pct is not None:
                    changes.append(change_pct)
            if changes:
                sector_results.append(sum(changes) / len(changes))

        if not sector_results:
            return self._empty_indicator("sector")

        # 平均がプラスなら上昇セクターが多いとみなす
        avg_change = sum(sector_results) / len(sector_results)
        score = self._score_from_change(avg_change, [(1, 8), (0.5, 4), (-0.5, 0), (-1, -4), (-1000, -8)])
        return {
            "value": avg_change,
            "score": score,
        }

    def calculate_nikkei_technical(self) -> Dict[str, Dict]:
        df = self._fetch_history("^N225", period="1y", interval="1d")
        if df is None or df.empty:
            return {
                "rsi": self._empty_indicator("rsi"),
                "moving_average": self._empty_indicator("moving_average"),
                "bollinger_bands": self._empty_indicator("bollinger_bands"),
            }

        close = df["close"]

        # RSI(14)
        rsi_val = self._calc_rsi(close, period=14)
        if rsi_val is None:
            rsi_score = 0.0
        elif rsi_val > 70:
            rsi_score = -5
        elif rsi_val > 50:
            rsi_score = 3
        elif rsi_val >= 30:
            rsi_score = 0
        else:
            rsi_score = 5

        # 移動平均
        ma25 = close.rolling(25).mean()
        ma75 = close.rolling(75).mean()
        ma200 = close.rolling(200).mean()
        latest = len(close) - 1
        ma_score = 0.0
        if latest >= 0 and pd.notna(ma25.iloc[latest]) and pd.notna(ma75.iloc[latest]) and pd.notna(ma200.iloc[latest]):
            price = close.iloc[latest]
            cond_full_up = price > ma25.iloc[latest] > ma75.iloc[latest] > ma200.iloc[latest]
            cond_above_25 = price > ma25.iloc[latest]
            cond_full_down = price < ma25.iloc[latest] < ma75.iloc[latest] < ma200.iloc[latest]
            if cond_full_up:
                ma_score = 8
            elif cond_above_25:
                ma_score = 4
            elif cond_full_down:
                ma_score = -8
            else:
                ma_score = 0

        # ボリンジャーバンド (20, ±2σ)
        bb_mid = close.rolling(20).mean()
        bb_std = close.rolling(20).std()
        bb_upper = bb_mid + 2 * bb_std
        bb_lower = bb_mid - 2 * bb_std
        bb_score = 0.0
        if latest >= 0 and pd.notna(bb_upper.iloc[latest]) and pd.notna(bb_lower.iloc[latest]):
            price = close.iloc[latest]
            if price > bb_upper.iloc[latest]:
                bb_score = -3
            elif price > bb_mid.iloc[latest]:
                bb_score = 0
            elif price > bb_lower.iloc[latest]:
                bb_score = 3
            else:
                bb_score = 5

        return {
            "rsi": {"value": rsi_val, "score": rsi_score},
            "moving_average": {"value": ma_score, "score": ma_score},
            "bollinger_bands": {"value": bb_score, "score": bb_score},
        }

    # -----------------------------
    # 補助
    # -----------------------------
    def _fetch_history(self, symbol: str, period: str, interval: str) -> Optional[pd.DataFrame]:
        try:
            ticker = yf.Ticker(symbol)
            # yfinanceのエラーメッセージを抑制（呼び出し元で抑制する場合は二重に抑制されるが問題なし）
            with suppress_stderr():
                df = ticker.history(period=period, interval=interval)
            if df is None or df.empty:
                return None
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            df.columns = [c.lower() for c in df.columns]
            if df.index.tz is not None:
                df.index = df.index.tz_localize(None)
            return df
        except Exception as e:  # pragma: no cover - 通信依存
            # エラーメッセージは抑制されているため、ログのみ出力
            return None

    def _get_value_and_change(self, df: Optional[pd.DataFrame]) -> Tuple[Optional[float], Optional[float]]:
        if df is None or df.empty or len(df) < 2:
            return None, None
        value = df["close"].iloc[-1]
        prev = df["close"].iloc[-2]
        if prev == 0:
            return value, None
        change_pct = (value / prev - 1) * 100
        return value, change_pct

    def _get_abs_change(self, df: Optional[pd.DataFrame]) -> Tuple[Optional[float], Optional[float]]:
        if df is None or df.empty or len(df) < 2:
            return None, None
        value = df["close"].iloc[-1]
        prev = df["close"].iloc[-2]
        return value, value - prev

    def _score_from_change(self, change_pct: Optional[float], rules: List[Tuple[float, float]]) -> float:
        """
        rules: [(threshold, score)] を降順で評価。threshold以上ならscore。
        最終要素は下限用に設定。
        """
        if change_pct is None:
            return 0.0
        for threshold, score in rules:
            if change_pct >= threshold:
                return score
        return rules[-1][1] if rules else 0.0

    def _score_from_abs(self, change_abs: Optional[float], rules: List[Tuple[float, float]]) -> float:
        if change_abs is None:
            return 0.0
        for threshold, score in rules:
            if change_abs >= threshold:
                return score
        return rules[-1][1] if rules else 0.0

    def _score_volume_ratio(self, ratio: Optional[float]) -> float:
        if ratio is None:
            return 0.0
        if ratio >= 1.5:
            return 5
        if ratio >= 1.2:
            return 3
        if ratio >= 0.8:
            return 0
        if ratio >= 0.5:
            return -3
        return -5

    def _score_advance_decline(self, ratio: float) -> float:
        if ratio > 2.0:
            return 10
        if ratio >= 1.5:
            return 5
        if ratio >= 0.67:
            return 0
        if ratio >= 0.5:
            return -5
        return -10

    def _calc_rsi(self, series: pd.Series, period: int = 14) -> Optional[float]:
        if len(series) < period + 1:
            return None
        delta = series.diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.rolling(period).mean()
        avg_loss = loss.rolling(period).mean()
        rs = avg_gain / avg_loss.replace(0, pd.NA)
        rsi = 100 - (100 / (1 + rs))
        return float(rsi.iloc[-1]) if pd.notna(rsi.iloc[-1]) else None

    def _build_indicator(
        self,
        name: str,
        value: Optional[float],
        change: Optional[float],
        score: float,
        field: str = "change_pct",
    ) -> Dict:
        return {
            "value": value,
            field: change,
            "score": score,
        }

    def _empty_indicator(self, name: str) -> Dict:
        return {"value": None, "score": 0.0}

    def _ensure_yfinance(self):
        if yf is None:
            raise ImportError("yfinance がインストールされていません。pip install yfinance を実行してください。")


