"""
JPX400銘柄スクリーニングモジュール

JPX400銘柄をスクリーニングして、条件を満たす銘柄を抽出します。

Copyright (c) 2025 JPX400スクリーニングシステム

This software is licensed under the MIT License.
See LICENSE file for details.
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from src.utils.console import setup_console_encoding
setup_console_encoding()

import pandas as pd
from datetime import datetime, date
from typing import List, Dict, Optional
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

from src.data_collector.ohlcv_data_manager import OHLCVDataManager
from src.screening.jpx400_manager import JPX400Manager
from src.screening.data_collector import JPX400DataCollector

# PyYAMLがない環境向けに一度だけ判定し、過剰ログを抑止
try:
    import yaml  # type: ignore
    _YAML_AVAILABLE = True
except ImportError:
    _YAML_AVAILABLE = False


class JPX400Screener:
    """JPX400銘柄スクリーナークラス"""
    
    def __init__(self, db_path: str = "data/tick_data.db"):
        """
        初期化
        
        Args:
            db_path: データベースパス
        """
        self.db_path = db_path
        self.ohlcv_manager = OHLCVDataManager(db_path)
        self.jpx400_manager = JPX400Manager()
        self.data_collector = JPX400DataCollector(db_path)
        self.config = self._load_config()

    def _default_config(self) -> Dict:
        """MACD/KD設定のデフォルト値"""
        return {
            "screening": {
                "indicators": {
                    "macd": {
                        "short_period": 6,
                        "long_period": 13,
                        "signal_period": 5
                    },
                    "stochastic": {
                        "k_period": 9,
                        "smooth_k": 3,
                        "d_period": 3,
                        "oversold_threshold": 20
                    }
                },
                "proximity": {
                    "enable_macd_kd": True,
                    "window_days": 1
                },
                "performance": {
                    "use_parallel": True,
                    "max_workers": None
                }
            }
        }

    def _merge_dicts(self, base: Dict, updates: Dict) -> Dict:
        """ネストしたdictをマージ"""
        result = base.copy()
        for key, value in updates.items():
            if isinstance(value, dict) and isinstance(result.get(key), dict):
                result[key] = self._merge_dicts(result[key], value)
            else:
                result[key] = value
        return result

    def _load_config(self) -> Dict:
        """
        config/screening.yaml を読み込み（存在しない/パース失敗時はデフォルト）
        """
        default_cfg = self._default_config()
        config_path = Path("config") / "screening.yaml"
        if not config_path.exists():
            return default_cfg

        try:
            if not _YAML_AVAILABLE:
                # PyYAMLなし: サイレントでデフォルトを使う（ログ連打を避ける）
                return default_cfg

            with open(config_path, "r", encoding="utf-8") as f:
                loaded = yaml.safe_load(f) or {}
            return self._merge_dicts(default_cfg, loaded)
        except Exception as e:
            print(f"[JPX400Screener] 設定ファイル読み込みエラー: {e}（デフォルトを使用）")
            return default_cfg

    def _calculate_macd(self, df: pd.DataFrame, params: Dict) -> pd.DataFrame:
        """MACDを計算"""
        df = df.copy()
        short = params.get("short_period", 6)
        long = params.get("long_period", 13)
        signal = params.get("signal_period", 5)

        ema_short = df["close"].ewm(span=short, adjust=False, min_periods=short).mean()
        ema_long = df["close"].ewm(span=long, adjust=False, min_periods=long).mean()
        df["macd"] = ema_short - ema_long
        df["macd_signal"] = df["macd"].ewm(span=signal, adjust=False, min_periods=signal).mean()
        df["macd_hist"] = df["macd"] - df["macd_signal"]
        return df

    def _calculate_stochastic(self, df: pd.DataFrame, params: Dict) -> pd.DataFrame:
        """Stochastic Slow (K,D) を計算"""
        df = df.copy()
        k_period = params.get("k_period", 9)
        smooth_k = params.get("smooth_k", 3)
        d_period = params.get("d_period", 3)

        lowest_low = df["low"].rolling(window=k_period, min_periods=k_period).min()
        highest_high = df["high"].rolling(window=k_period, min_periods=k_period).max()
        raw_k = (df["close"] - lowest_low) / (highest_high - lowest_low) * 100

        df["stoch_k"] = raw_k.rolling(window=smooth_k, min_periods=smooth_k).mean()
        df["stoch_d"] = df["stoch_k"].rolling(window=d_period, min_periods=d_period).mean()
        return df

    def _detect_macd_bullish_signals(self, df: pd.DataFrame) -> List[Dict]:
        """
        MACD上昇サイン（MACDがシグナルを上抜け）を検出
        欠損がある日はスキップ
        """
        signals = []
        if len(df) < 2:
            return signals

        macd = df["macd"]
        signal = df["macd_signal"]

        for i in range(1, len(df)):
            if pd.isna(macd.iloc[i]) or pd.isna(signal.iloc[i]) or pd.isna(macd.iloc[i - 1]) or pd.isna(signal.iloc[i - 1]):
                continue
            if macd.iloc[i] > signal.iloc[i] and macd.iloc[i - 1] <= signal.iloc[i - 1]:
                signals.append({
                    "date": df.index[i].date(),
                    "idx": i
                })
        return signals

    def _detect_kd_bullish_signals(self, df: pd.DataFrame, oversold_threshold: float) -> List[Dict]:
        """
        KD上昇サイン（slowKがslowDを上抜け、かつ20以下からの上抜け）を検出
        """
        signals = []
        if len(df) < 2:
            return signals

        k = df["stoch_k"]
        d = df["stoch_d"]

        for i in range(1, len(df)):
            if pd.isna(k.iloc[i]) or pd.isna(d.iloc[i]) or pd.isna(k.iloc[i - 1]) or pd.isna(d.iloc[i - 1]):
                continue
            crossed_up = k.iloc[i] > d.iloc[i] and k.iloc[i - 1] <= d.iloc[i - 1]
            from_oversold = k.iloc[i - 1] <= oversold_threshold and k.iloc[i] >= oversold_threshold
            if crossed_up and from_oversold:
                signals.append({
                    "date": df.index[i].date(),
                    "idx": i
                })
        return signals

    def _check_macd_kd_proximity(
        self,
        macd_signals: List[Dict],
        kd_signals: List[Dict],
        window: int,
        latest_idx: int,
        is_macd_bullish_now: bool,
        is_kd_bullish_now: bool
    ) -> Dict:
        """
        直近シグナル同士が指定営業日以内か、かつ「現在も」強気状態が維持されているかを判定。
        - 最新MACD/KD上昇シグナルが揃っていること
        - シグナル同士の差がwindow以内
        - 両シグナルが最新足からwindow以内に発生
        - 現在もMACD>シグナル、slowK>slowDで強気継続
        """
        if not macd_signals or not kd_signals:
            return {"has_proximity": False, "macd_date": None, "kd_date": None, "gap_days": None}

        macd_latest = macd_signals[-1]
        kd_latest = kd_signals[-1]
        gap = abs(macd_latest["idx"] - kd_latest["idx"])

        # シグナル同士の近接
        if gap > window:
            return {"has_proximity": False, "macd_date": macd_latest["date"], "kd_date": kd_latest["date"], "gap_days": gap}

        # 最新足からの経過日数（営業日インデックス差）
        macd_age = latest_idx - macd_latest["idx"]
        kd_age = latest_idx - kd_latest["idx"]
        if macd_age > window or kd_age > window:
            return {"has_proximity": False, "macd_date": macd_latest["date"], "kd_date": kd_latest["date"], "gap_days": gap}

        # 現在も強気でなければ除外
        if not (is_macd_bullish_now and is_kd_bullish_now):
            return {"has_proximity": False, "macd_date": macd_latest["date"], "kd_date": kd_latest["date"], "gap_days": gap}

        return {
            "has_proximity": True,
            "macd_date": macd_latest["date"],
            "kd_date": kd_latest["date"],
            "gap_days": gap
        }
    
    def calculate_moving_averages(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        移動平均線を計算
        
        Args:
            df: 日足データ
            
        Returns:
            pd.DataFrame: 移動平均線を追加したデータ
        """
        df = df.copy()
        
        # 移動平均線を計算
        df['ma5'] = df['close'].rolling(window=5, min_periods=1).mean()
        df['ma25'] = df['close'].rolling(window=25, min_periods=1).mean()
        df['ma75'] = df['close'].rolling(window=75, min_periods=1).mean()
        df['ma200'] = df['close'].rolling(window=200, min_periods=1).mean()
        
        return df
    
    def check_condition1_ma_order(self, df: pd.DataFrame) -> bool:
        """
        条件1: 5MA、25MA、75MA、200MAが上から順に並んでいる
        
        Args:
            df: 日足データ（移動平均線を含む）
            
        Returns:
            bool: 条件を満たすか
        """
        if len(df) < 200:
            return False
        
        latest = df.iloc[-1]
        
        # 移動平均線が上から順に並んでいるか
        condition = (
            latest['ma5'] > latest['ma25'] and
            latest['ma25'] > latest['ma75'] and
            latest['ma75'] > latest['ma200']
        )
        
        return condition
    
    def check_condition2_positive_candles(self, df: pd.DataFrame) -> bool:
        """
        条件2: 5MA線の上で2回続けて陽線が出ている
        
        注意: 今日のデータが仮終値として存在する場合、それが陰線なら条件を満たしません。
        今日のデータが仮終値で陽線の場合、今日と昨日で判定します。
        今日のデータが仮終値でない場合、正式データの最新2日で判定します。
        
        Args:
            df: 日足データ（移動平均線を含む）
            
        Returns:
            bool: 条件を満たすか
        """
        if len(df) < 2:
            return False
        
        today = date.today()
        
        # 今日のデータが存在するか確認
        has_today_data = False
        today_row = None
        if not df.empty:
            latest_date = df.index[-1].date()
            if latest_date == today:
                today_row = df.iloc[-1]
                has_today_data = True
        
        # 今日のデータが仮終値として存在する場合
        if has_today_data and today_row is not None:
            is_temporary = today_row.get('is_temporary_close', 0) == 1
            
            if is_temporary:
                # 今日が陰線なら条件を満たさない
                if today_row['close'] <= today_row['open']:
                    return False
                
                # 今日が陽線の場合、昨日のデータを取得
                if len(df) < 2:
                    return False
                
                # 昨日のデータを取得（仮終値を除外）
                if 'is_temporary_close' in df.columns:
                    df_excluding_temporary = df[df['is_temporary_close'] == 0]
                else:
                    df_excluding_temporary = df
                
                # 昨日のデータが存在するか確認
                if len(df_excluding_temporary) < 1:
                    return False
                
                # 昨日のデータを取得（今日より前の最新データ）
                prev_date = df_excluding_temporary.index[-1].date()
                if prev_date >= today:
                    # 今日より前のデータを取得
                    df_before_today = df_excluding_temporary[df_excluding_temporary.index.date < today]
                    if len(df_before_today) < 1:
                        return False
                    prev = df_before_today.iloc[-1]
                else:
                    prev = df_excluding_temporary.iloc[-1]
                
                # 今日が陽線で、昨日も陽線か確認
                is_positive_today = today_row['close'] > today_row['open']
                is_positive_prev = prev['close'] > prev['open']
                
                if not (is_positive_today and is_positive_prev):
                    return False
                
                # 両方の終値が5MAより上か
                condition = (
                    today_row['close'] > today_row['ma5'] and
                    prev['close'] > prev['ma5']
                )
                
                return condition
        
        # 今日のデータが仮終値でない場合、既存のロジックで判定
        # 仮終値データ（is_temporary_close=1）を除外
        if 'is_temporary_close' in df.columns:
            df_excluding_temporary = df[df['is_temporary_close'] == 0]
        else:
            df_excluding_temporary = df
        
        # 仮終値データを除外した後、データが2日未満の場合はFalseを返す
        if len(df_excluding_temporary) < 2:
            return False
        
        # DBの最新日付（仮終値を除外した後の最新、正式データ）とその前日で判定
        latest = df_excluding_temporary.iloc[-1]
        prev = df_excluding_temporary.iloc[-2]
        
        # 最新2本が陽線か（DBの最新日付とその前日）
        is_positive_latest = latest['close'] > latest['open']
        is_positive_prev = prev['close'] > prev['open']
        
        if not (is_positive_latest and is_positive_prev):
            return False
        
        # 両方の終値が5MAより上か
        condition = (
            latest['close'] > latest['ma5'] and
            prev['close'] > prev['ma5']
        )
        
        return condition
    
    def check_condition3_ma5_upward(self, df: pd.DataFrame) -> bool:
        """
        条件3: 5MAが上向き
        
        Args:
            df: 日足データ（移動平均線を含む）
            
        Returns:
            bool: 条件を満たすか
        """
        if len(df) < 2:
            return False
        
        latest = df.iloc[-1]
        prev = df.iloc[-2]
        
        # 5MAが前日より上昇しているか
        return latest['ma5'] > prev['ma5']
    
    def check_condition4_ma25_upward(self, df: pd.DataFrame) -> bool:
        """
        条件4: 25MAが上向き
        
        Args:
            df: 日足データ（移動平均線を含む）
            
        Returns:
            bool: 条件を満たすか
        """
        if len(df) < 2:
            return False
        
        latest = df.iloc[-1]
        prev = df.iloc[-2]
        
        # 25MAが前日より上昇しているか
        return latest['ma25'] > prev['ma25']
    
    def check_condition5_ma75_upward(self, df: pd.DataFrame) -> bool:
        """
        条件5: 75MAが上向き
        
        Args:
            df: 日足データ（移動平均線を含む）
            
        Returns:
            bool: 条件を満たすか
        """
        if len(df) < 2:
            return False
        
        latest = df.iloc[-1]
        prev = df.iloc[-2]
        
        # 75MAが前日より上昇しているか
        return latest['ma75'] > prev['ma75']
    
    def check_condition6_ma200_upward(self, df: pd.DataFrame) -> bool:
        """
        条件6: 200MAが上向き
        
        Args:
            df: 日足データ（移動平均線を含む）
            
        Returns:
            bool: 条件を満たすか
        """
        if len(df) < 2:
            return False
        
        latest = df.iloc[-1]
        prev = df.iloc[-2]
        
        # 200MAが前日より上昇しているか
        return latest['ma200'] > prev['ma200']
    
    def check_golden_cross_5_25(self, df: pd.DataFrame, days_ago: int = 0) -> Dict:
        """
        5MA/25MAのゴールデンクロスをチェック
        
        Args:
            df: 日足データ（移動平均線を含む）
            days_ago: 何日前のクロスをチェックするか（0=最新、1=1日前など）
            
        Returns:
            dict: {
                'has_crossed': bool,  # クロスしているか（現在5MA > 25MA）
                'just_crossed': bool,  # 直近でクロスしたか（前日は5MA < 25MA、今日は5MA > 25MA）
                'cross_date': Optional[date],  # クロスした日付（just_crossedがTrueの場合）
                'ma5': float,  # 現在の5MA
                'ma25': float  # 現在の25MA
            }
        """
        if len(df) < max(25, days_ago + 1):
            return {
                'has_crossed': False,
                'just_crossed': False,
                'cross_date': None,
                'ma5': None,
                'ma25': None
            }
        
        latest_idx = len(df) - 1 - days_ago
        if latest_idx < 1:
            return {
                'has_crossed': False,
                'just_crossed': False,
                'cross_date': None,
                'ma5': None,
                'ma25': None
            }
        
        latest = df.iloc[latest_idx]
        prev = df.iloc[latest_idx - 1]
        
        # 現在の状態
        has_crossed = latest['ma5'] > latest['ma25']
        
        # 直近でクロスしたか（前日は5MA < 25MA、今日は5MA > 25MA）
        just_crossed = (
            has_crossed and
            prev['ma5'] <= prev['ma25']
        )
        
        cross_date = None
        if just_crossed:
            cross_date = df.index[latest_idx].date()
        
        return {
            'has_crossed': has_crossed,
            'just_crossed': just_crossed,
            'cross_date': cross_date,
            'ma5': float(latest['ma5']),
            'ma25': float(latest['ma25'])
        }
    
    def check_golden_cross_25_75(self, df: pd.DataFrame, days_ago: int = 0) -> Dict:
        """
        25MA/75MAのゴールデンクロスをチェック
        
        Args:
            df: 日足データ（移動平均線を含む）
            days_ago: 何日前のクロスをチェックするか（0=最新、1=1日前など）
            
        Returns:
            dict: {
                'has_crossed': bool,  # クロスしているか（現在25MA > 75MA）
                'just_crossed': bool,  # 直近でクロスしたか（前日は25MA < 75MA、今日は25MA > 75MA）
                'cross_date': Optional[date],  # クロスした日付（just_crossedがTrueの場合）
                'ma25': float,  # 現在の25MA
                'ma75': float  # 現在の75MA
            }
        """
        if len(df) < max(75, days_ago + 1):
            return {
                'has_crossed': False,
                'just_crossed': False,
                'cross_date': None,
                'ma25': None,
                'ma75': None
            }
        
        latest_idx = len(df) - 1 - days_ago
        if latest_idx < 1:
            return {
                'has_crossed': False,
                'just_crossed': False,
                'cross_date': None,
                'ma25': None,
                'ma75': None
            }
        
        latest = df.iloc[latest_idx]
        prev = df.iloc[latest_idx - 1]
        
        # 現在の状態
        has_crossed = latest['ma25'] > latest['ma75']
        
        # 直近でクロスしたか（前日は25MA < 75MA、今日は25MA > 75MA）
        just_crossed = (
            has_crossed and
            prev['ma25'] <= prev['ma75']
        )
        
        cross_date = None
        if just_crossed:
            cross_date = df.index[latest_idx].date()
        
        return {
            'has_crossed': has_crossed,
            'just_crossed': just_crossed,
            'cross_date': cross_date,
            'ma25': float(latest['ma25']),
            'ma75': float(latest['ma75'])
        }
    
    def check_golden_cross_5_200(self, df: pd.DataFrame, days_ago: int = 0) -> Dict:
        """
        5MA/200MAのゴールデンクロスをチェック
        
        Args:
            df: 日足データ（移動平均線を含む）
            days_ago: 何日前のクロスをチェックするか（0=最新、1=1日前など）
            
        Returns:
            dict: {
                'has_crossed': bool,  # クロスしているか（現在5MA > 200MA）
                'just_crossed': bool,  # 直近でクロスしたか（前日は5MA < 200MA、今日は5MA > 200MA）
                'cross_date': Optional[date],  # クロスした日付（just_crossedがTrueの場合）
                'ma5': float,  # 現在の5MA
                'ma200': float  # 現在の200MA
            }
        """
        if len(df) < max(200, days_ago + 1):
            return {
                'has_crossed': False,
                'just_crossed': False,
                'cross_date': None,
                'ma5': None,
                'ma200': None
            }
        
        latest_idx = len(df) - 1 - days_ago
        if latest_idx < 1:
            return {
                'has_crossed': False,
                'just_crossed': False,
                'cross_date': None,
                'ma5': None,
                'ma200': None
            }
        
        latest = df.iloc[latest_idx]
        prev = df.iloc[latest_idx - 1]
        
        # 現在の状態
        has_crossed = latest['ma5'] > latest['ma200']
        
        # 直近でクロスしたか（前日は5MA < 200MA、今日は5MA > 200MA）
        just_crossed = (
            has_crossed and
            prev['ma5'] <= prev['ma200']
        )
        
        cross_date = None
        if just_crossed:
            cross_date = df.index[latest_idx].date()
        
        return {
            'has_crossed': has_crossed,
            'just_crossed': just_crossed,
            'cross_date': cross_date,
            'ma5': float(latest['ma5']),
            'ma200': float(latest['ma200'])
        }
    
    def screen_symbol(
        self,
        symbol: str,
        complement_today: bool = True,
        check_condition1: bool = True,
        check_condition2: bool = True,
        check_condition3: bool = False,
        check_condition4: bool = False,
        check_condition5: bool = False,
        check_condition6: bool = False,
        check_golden_cross_5_25: bool = False,
        check_golden_cross_25_75: bool = False,
        check_golden_cross_5_200: bool = False,
        golden_cross_mode: str = 'just_crossed',  # 'just_crossed': 直近でクロス, 'has_crossed': クロス中
        use_macd_kd_filter: Optional[bool] = None,
        macd_kd_window: Optional[int] = None
    ) -> Optional[Dict]:
        """
        1銘柄をスクリーニング
        
        Args:
            symbol: 銘柄コード
            complement_today: 当日データがない場合に補完するか
            check_condition1: 条件1（移動平均線の順序）をチェックするか
            check_condition2: 条件2（陽線の連続）をチェックするか
            check_condition3: 条件3（5MAが上向き）をチェックするか
            check_condition4: 条件4（25MAが上向き）をチェックするか
            check_condition5: 条件5（75MAが上向き）をチェックするか
            check_condition6: 条件6（200MAが上向き）をチェックするか
            check_golden_cross_5_25: 5MA/25MAのゴールデンクロスをチェックするか
            check_golden_cross_25_75: 25MA/75MAのゴールデンクロスをチェックするか
            check_golden_cross_5_200: 5MA/200MAのゴールデンクロスをチェックするか
            golden_cross_mode: ゴールデンクロスの判定モード
                - 'just_crossed': 直近でクロスした銘柄のみ（推奨）
                - 'has_crossed': 現在クロスしている銘柄（既にクロス済みも含む）
            use_macd_kd_filter: MACD/KD近接フィルタを有効にするか（Noneなら設定ファイルの値）
            macd_kd_window: 近接判定の営業日幅（Noneなら設定ファイルの値）
            
        Returns:
            dict: スクリーニング結果（条件を満たす場合）、またはNone
        """
        try:
            # DBから日足データを取得（仮終値フラグを含む）
            df_daily = self.ohlcv_manager.get_ohlcv_data_with_temporary_flag(
                symbol=symbol,
                timeframe='1d',
                source='yahoo',
                include_temporary=True
            )
            
            if df_daily.empty or len(df_daily) < 200:
                # データが不足している場合、補完を試みる
                if complement_today:
                    df_daily = self.data_collector.complement_today_data(symbol, df_daily)
                    # 補完後もデータが不足している場合はスキップ
                    if df_daily.empty or len(df_daily) < 200:
                        return None
                else:
                    return None
            
            # 当日データの確認と補完
            today = date.today()
            latest_date = df_daily.index[-1].date()
            
            if latest_date < today or (latest_date == today and df_daily.iloc[-1].get('is_temporary_close', 0) == 1):
                # 当日データがない、または仮終値の場合、補完を試みる
                if complement_today:
                    df_daily = self.data_collector.complement_today_data(symbol, df_daily)
            
            # 移動平均線を計算
            df_daily = self.calculate_moving_averages(df_daily)

            # 設定値取得
            screening_cfg = self.config.get("screening", {})
            indicator_cfg = screening_cfg.get("indicators", {})
            macd_cfg = indicator_cfg.get("macd", {})
            stoch_cfg = indicator_cfg.get("stochastic", {})
            proximity_cfg = screening_cfg.get("proximity", {})

            # MACD/KDフィルタが有効な場合のみ計算（最適化）
            macd_kd_filter_enabled = proximity_cfg.get("enable_macd_kd", True) if use_macd_kd_filter is None else use_macd_kd_filter
            macd_kd_window_days = proximity_cfg.get("window_days", 3) if macd_kd_window is None else macd_kd_window

            macd_signals = []
            kd_signals = []
            macd_kd_proximity_result = None
            
            if macd_kd_filter_enabled:
                # MACD / Stochasticを計算（欠損はそのままにし、サイン判定時にスキップ）
                df_daily = self._calculate_macd(df_daily, macd_cfg)
                df_daily = self._calculate_stochastic(df_daily, stoch_cfg)

                macd_signals = self._detect_macd_bullish_signals(df_daily)
                kd_signals = self._detect_kd_bullish_signals(df_daily, stoch_cfg.get("oversold_threshold", 20))

                latest_idx = len(df_daily) - 1
                latest_macd = df_daily["macd"].iloc[-1]
                latest_macd_signal = df_daily["macd_signal"].iloc[-1]
                latest_k = df_daily["stoch_k"].iloc[-1]
                latest_d = df_daily["stoch_d"].iloc[-1]
                is_macd_bullish_now = pd.notna(latest_macd) and pd.notna(latest_macd_signal) and latest_macd > latest_macd_signal
                is_kd_bullish_now = pd.notna(latest_k) and pd.notna(latest_d) and latest_k > latest_d
                macd_kd_proximity_result = self._check_macd_kd_proximity(
                    macd_signals,
                    kd_signals,
                    macd_kd_window_days,
                    latest_idx,
                    is_macd_bullish_now,
                    is_kd_bullish_now
                )
                if not macd_kd_proximity_result["has_proximity"]:
                    return None
            
            # 条件をチェック（選択的に）
            condition1_result = True
            condition2_result = True
            condition3_result = True
            condition4_result = True
            condition5_result = True
            condition6_result = True
            golden_cross_5_25_result = None
            golden_cross_25_75_result = None
            golden_cross_5_200_result = None
            
            if check_condition1:
                condition1_result = self.check_condition1_ma_order(df_daily)
            
            if check_condition2:
                condition2_result = self.check_condition2_positive_candles(df_daily)
            
            if check_condition3:
                condition3_result = self.check_condition3_ma5_upward(df_daily)
            
            if check_condition4:
                condition4_result = self.check_condition4_ma25_upward(df_daily)
            
            if check_condition5:
                condition5_result = self.check_condition5_ma75_upward(df_daily)
            
            if check_condition6:
                condition6_result = self.check_condition6_ma200_upward(df_daily)
            
            # ゴールデンクロスをチェック
            if check_golden_cross_5_25:
                golden_cross_5_25_result = self.check_golden_cross_5_25(df_daily)
                if golden_cross_mode == 'just_crossed':
                    if not golden_cross_5_25_result['just_crossed']:
                        return None
                elif golden_cross_mode == 'has_crossed':
                    if not golden_cross_5_25_result['has_crossed']:
                        return None
            
            if check_golden_cross_25_75:
                golden_cross_25_75_result = self.check_golden_cross_25_75(df_daily)
                if golden_cross_mode == 'just_crossed':
                    if not golden_cross_25_75_result['just_crossed']:
                        return None
                elif golden_cross_mode == 'has_crossed':
                    if not golden_cross_25_75_result['has_crossed']:
                        return None
            
            if check_golden_cross_5_200:
                golden_cross_5_200_result = self.check_golden_cross_5_200(df_daily)
                if golden_cross_mode == 'just_crossed':
                    if not golden_cross_5_200_result['just_crossed']:
                        return None
                elif golden_cross_mode == 'has_crossed':
                    if not golden_cross_5_200_result['has_crossed']:
                        return None
            
            # 選択された条件をすべて満たす場合
            if (condition1_result and condition2_result and 
                condition3_result and condition4_result and 
                condition5_result and condition6_result):
                latest = df_daily.iloc[-1]
                prev = df_daily.iloc[-2]
                
                result = {
                    'symbol': symbol,
                    'current_price': float(latest['close']),
                    'latest_volume': int(latest['volume']) if 'volume' in latest and pd.notna(latest['volume']) else None,
                    'is_temporary_close': int(latest.get('is_temporary_close', 0)),
                    'ma5': float(latest['ma5']),
                    'ma25': float(latest['ma25']),
                    'ma75': float(latest['ma75']),
                    'ma200': float(latest['ma200']),
                    'condition1': condition1_result if check_condition1 else None,
                    'condition2': condition2_result if check_condition2 else None,
                    'latest_candle': {
                        'open': float(latest['open']),
                        'high': float(latest['high']),
                        'low': float(latest['low']),
                        'close': float(latest['close']),
                        'is_positive': latest['close'] > latest['open']
                    },
                    'prev_candle': {
                        'open': float(prev['open']),
                        'high': float(prev['high']),
                        'low': float(prev['low']),
                        'close': float(prev['close']),
                        'is_positive': prev['close'] > prev['open']
                    },
                    'macd': {
                        'value': float(latest['macd']) if pd.notna(latest.get('macd')) else None,
                        'signal': float(latest['macd_signal']) if pd.notna(latest.get('macd_signal')) else None,
                        'hist': float(latest['macd_hist']) if pd.notna(latest.get('macd_hist')) else None,
                        'last_bullish_cross_date': macd_signals[-1]['date'] if macd_signals else None
                    },
                    'stochastic': {
                        'stoch_k': float(latest['stoch_k']) if pd.notna(latest.get('stoch_k')) else None,
                        'stoch_d': float(latest['stoch_d']) if pd.notna(latest.get('stoch_d')) else None,
                        'last_bullish_cross_date': kd_signals[-1]['date'] if kd_signals else None
                    }
                }
                
                # ゴールデンクロス情報を追加
                if check_golden_cross_5_25 and golden_cross_5_25_result:
                    result['golden_cross_5_25'] = golden_cross_5_25_result
                
                if check_golden_cross_25_75 and golden_cross_25_75_result:
                    result['golden_cross_25_75'] = golden_cross_25_75_result
                
                if check_golden_cross_5_200 and golden_cross_5_200_result:
                    result['golden_cross_5_200'] = golden_cross_5_200_result

                # MACD/KD近接情報を追加
                if macd_kd_proximity_result is not None:
                    result['macd_kd_proximity'] = macd_kd_proximity_result
                
                return result
            
            return None
        
        except Exception as e:
            print(f"[{symbol}] スクリーニングエラー: {e}")
            return None
    
    def screen_all(
        self,
        complement_today: bool = True,
        progress_callback: Optional[callable] = None,
        check_condition1: bool = True,
        check_condition2: bool = True,
        check_condition3: bool = False,
        check_condition4: bool = False,
        check_condition5: bool = False,
        check_condition6: bool = False,
        check_golden_cross_5_25: bool = False,
        check_golden_cross_25_75: bool = False,
        check_golden_cross_5_200: bool = False,
        golden_cross_mode: str = 'just_crossed',
        use_macd_kd_filter: Optional[bool] = None,
        macd_kd_window: Optional[int] = None,
        use_parallel: Optional[bool] = None,
        max_workers: Optional[int] = None
    ) -> List[Dict]:
        """
        JPX400銘柄を全てスクリーニング
        
        Args:
            complement_today: 当日データがない場合に補完するか
            progress_callback: 進捗コールバック関数（symbol, current, total, result）
            check_condition1: 条件1（移動平均線の順序）をチェックするか
            check_condition2: 条件2（陽線の連続）をチェックするか
            check_condition3: 条件3（5MAが上向き）をチェックするか
            check_condition4: 条件4（25MAが上向き）をチェックするか
            check_condition5: 条件5（75MAが上向き）をチェックするか
            check_condition6: 条件6（200MAが上向き）をチェックするか
            check_golden_cross_5_25: 5MA/25MAのゴールデンクロスをチェックするか
            check_golden_cross_25_75: 25MA/75MAのゴールデンクロスをチェックするか
            check_golden_cross_5_200: 5MA/200MAのゴールデンクロスをチェックするか
            golden_cross_mode: ゴールデンクロスの判定モード
                - 'just_crossed': 直近でクロスした銘柄のみ（推奨）
                - 'has_crossed': 現在クロスしている銘柄（既にクロス済みも含む）
            use_macd_kd_filter: MACD/KD近接フィルタを有効にするか（Noneなら設定ファイルの値）
            macd_kd_window: 近接判定の営業日幅（Noneなら設定ファイルの値）
            use_parallel: 並列処理を使用するか（Noneなら設定ファイルの値）
            max_workers: 並列処理の最大スレッド数（NoneならCPUコア数）
            
        Returns:
            List[Dict]: 条件を満たす銘柄のリスト
        """
        # JPX400銘柄リストを読み込み
        symbols = self.jpx400_manager.load_symbols()
        
        if not symbols:
            print("[JPX400Screener] JPX400銘柄リストが空です")
            return []
        
        # 並列処理の設定を取得
        screening_cfg = self.config.get("screening", {})
        performance_cfg = screening_cfg.get("performance", {})
        use_parallel_setting = performance_cfg.get("use_parallel", True) if use_parallel is None else use_parallel
        max_workers_setting = performance_cfg.get("max_workers") if max_workers is None else max_workers
        
        if max_workers_setting is None:
            max_workers_setting = min(os.cpu_count() or 4, len(symbols))
        
        print(f"\n{'='*80}")
        print(f"JPX400銘柄スクリーニングを開始します")
        print(f"{'='*80}")
        print(f"対象銘柄数: {len(symbols)}")
        print(f"当日データ補完: {'有効' if complement_today else '無効'}")
        print(f"並列処理: {'有効' if use_parallel_setting else '無効'}" + (f" ({max_workers_setting}スレッド)" if use_parallel_setting else ""))
        print(f"{'='*80}\n")
        
        # 並列処理を使用する場合
        if use_parallel_setting and len(symbols) > 1:
            return self._screen_all_parallel(
                symbols=symbols,
                complement_today=complement_today,
                progress_callback=progress_callback,
                check_condition1=check_condition1,
                check_condition2=check_condition2,
                check_condition3=check_condition3,
                check_condition4=check_condition4,
                check_condition5=check_condition5,
                check_condition6=check_condition6,
                check_golden_cross_5_25=check_golden_cross_5_25,
                check_golden_cross_25_75=check_golden_cross_25_75,
                check_golden_cross_5_200=check_golden_cross_5_200,
                golden_cross_mode=golden_cross_mode,
                use_macd_kd_filter=use_macd_kd_filter,
                macd_kd_window=macd_kd_window,
                max_workers=max_workers_setting
            )
        
        # 逐次処理（既存の実装）
        results = []
        yahoo_access_count = 0
        
        for i, symbol in enumerate(symbols, 1):
            # DBからデータを取得
            df_daily = self.ohlcv_manager.get_ohlcv_data_with_temporary_flag(
                symbol=symbol,
                timeframe='1d',
                source='yahoo',
                include_temporary=True
            )
            
            # 当日データの確認
            need_complement = False
            if df_daily.empty or len(df_daily) < 200:
                need_complement = True
            else:
                today = date.today()
                latest_date = df_daily.index[-1].date()
                if latest_date < today:
                    need_complement = True
                elif latest_date == today:
                    latest_row = df_daily.iloc[-1]
                    if latest_row.get('is_temporary_close', 0) == 1:
                        need_complement = True
            
            if need_complement and complement_today:
                # Yahoo Financeから補完
                df_daily = self.data_collector.complement_today_data(symbol, df_daily)
                yahoo_access_count += 1
            
            # スクリーニング実行
            result = self.screen_symbol(
                symbol, 
                complement_today=False,  # 既に補完済みなのでFalse
                check_condition1=check_condition1,
                check_condition2=check_condition2,
                check_condition3=check_condition3,
                check_condition4=check_condition4,
                check_condition5=check_condition5,
                check_condition6=check_condition6,
                check_golden_cross_5_25=check_golden_cross_5_25,
                check_golden_cross_25_75=check_golden_cross_25_75,
                check_golden_cross_5_200=check_golden_cross_5_200,
                golden_cross_mode=golden_cross_mode,
                use_macd_kd_filter=use_macd_kd_filter,
                macd_kd_window=macd_kd_window
            )
            
            if result:
                results.append(result)
                print(f"[{i}/{len(symbols)}] {symbol}: ✓ 条件を満たす")
            else:
                if i % 50 == 0:  # 50銘柄ごとに進捗表示
                    print(f"[{i}/{len(symbols)}] 処理中...")
            
            # 進捗コールバック
            if progress_callback:
                progress_callback(symbol, i, len(symbols), result)
        
        print(f"\n{'='*80}")
        print(f"スクリーニング完了")
        print(f"{'='*80}")
        print(f"条件を満たす銘柄: {len(results)}件")
        print(f"Yahoo Financeアクセス回数: {yahoo_access_count}回")
        print(f"{'='*80}\n")
        
        return results
    
    def _screen_all_parallel(
        self,
        symbols: List[str],
        complement_today: bool = True,
        progress_callback: Optional[callable] = None,
        check_condition1: bool = True,
        check_condition2: bool = True,
        check_condition3: bool = False,
        check_condition4: bool = False,
        check_condition5: bool = False,
        check_condition6: bool = False,
        check_golden_cross_5_25: bool = False,
        check_golden_cross_25_75: bool = False,
        check_golden_cross_5_200: bool = False,
        golden_cross_mode: str = 'just_crossed',
        use_macd_kd_filter: Optional[bool] = None,
        macd_kd_window: Optional[int] = None,
        max_workers: int = 4
    ) -> List[Dict]:
        """
        並列処理でJPX400銘柄をスクリーニング
        
        Args:
            symbols: スクリーニング対象銘柄リスト
            complement_today: 当日データがない場合に補完するか
            progress_callback: 進捗コールバック関数（symbol, current, total, result）
            check_condition1～check_condition6: 各種条件フラグ
            check_golden_cross_5_25, check_golden_cross_25_75, check_golden_cross_5_200: ゴールデンクロス条件
            golden_cross_mode: ゴールデンクロス判定モード
            use_macd_kd_filter: MACD/KD近接フィルタを有効にするか
            macd_kd_window: 近接判定の営業日幅
            max_workers: 最大スレッド数
            
        Returns:
            List[Dict]: 条件を満たす銘柄のリスト
        """
        results = []
        yahoo_access_count = 0
        completed_count = 0
        total = len(symbols)
        
        def process_symbol(symbol: str) -> tuple:
            """1銘柄を処理する関数（並列実行用）"""
            nonlocal yahoo_access_count
            
            try:
                # DBからデータを取得
                df_daily = self.ohlcv_manager.get_ohlcv_data_with_temporary_flag(
                    symbol=symbol,
                    timeframe='1d',
                    source='yahoo',
                    include_temporary=True
                )
                
                # 当日データの確認
                need_complement = False
                if df_daily.empty or len(df_daily) < 200:
                    need_complement = True
                else:
                    today = date.today()
                    latest_date = df_daily.index[-1].date()
                    if latest_date < today:
                        need_complement = True
                    elif latest_date == today:
                        latest_row = df_daily.iloc[-1]
                        if latest_row.get('is_temporary_close', 0) == 1:
                            need_complement = True
                
                if need_complement and complement_today:
                    # Yahoo Financeから補完
                    df_daily = self.data_collector.complement_today_data(symbol, df_daily)
                    yahoo_access_count += 1
                
                # スクリーニング実行
                result = self.screen_symbol(
                    symbol, 
                    complement_today=False,  # 既に補完済みなのでFalse
                    check_condition1=check_condition1,
                    check_condition2=check_condition2,
                    check_condition3=check_condition3,
                    check_condition4=check_condition4,
                    check_condition5=check_condition5,
                    check_condition6=check_condition6,
                    check_golden_cross_5_25=check_golden_cross_5_25,
                    check_golden_cross_25_75=check_golden_cross_25_75,
                    check_golden_cross_5_200=check_golden_cross_5_200,
                    golden_cross_mode=golden_cross_mode,
                    use_macd_kd_filter=use_macd_kd_filter,
                    macd_kd_window=macd_kd_window
                )
                
                return (symbol, result, None)
            except Exception as e:
                return (symbol, None, str(e))
        
        # 並列処理実行
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 各銘柄のスクリーニングを並列実行
            future_to_symbol = {
                executor.submit(process_symbol, symbol): symbol
                for symbol in symbols
            }
            
            # 完了したタスクから順に処理
            for future in as_completed(future_to_symbol):
                symbol, result, error = future.result()
                completed_count += 1
                
                if error:
                    print(f"[{completed_count}/{total}] {symbol}: ✗ エラー - {error}")
                elif result:
                    results.append(result)
                    print(f"[{completed_count}/{total}] {symbol}: ✓ 条件を満たす")
                else:
                    # 進捗表示（50銘柄ごと）
                    if completed_count % 50 == 0 or completed_count == total:
                        print(f"[{completed_count}/{total}] 処理中...")
                
                # 進捗コールバック
                if progress_callback:
                    progress_callback(symbol, completed_count, total, result)
        
        print(f"\n{'='*80}")
        print(f"スクリーニング完了")
        print(f"{'='*80}")
        print(f"条件を満たす銘柄: {len(results)}件")
        print(f"Yahoo Financeアクセス回数: {yahoo_access_count}回")
        print(f"{'='*80}\n")
        
        return results
    
    def display_results(self, results: List[Dict]):
        """
        スクリーニング結果を表示
        
        Args:
            results: スクリーニング結果のリスト
        """
        if not results:
            print("条件を満たす銘柄はありませんでした。")
            return
        
        print(f"\n{'='*80}")
        print(f"スクリーニング結果: {len(results)}銘柄")
        print(f"{'='*80}\n")
        
        for i, result in enumerate(results, 1):
            symbol = result['symbol']
            price = result['current_price']
            is_temporary = result['is_temporary_close']
            
            print(f"{i}. {symbol}")
            print(f"   現在価格: {price:.2f}円", end="")
            if is_temporary == 1:
                print(" ⚠️（仮終値）")
            else:
                print(" ✅（正式）")
            
            print(f"   5MA: {result['ma5']:.2f}円")
            print(f"   25MA: {result['ma25']:.2f}円")
            print(f"   75MA: {result['ma75']:.2f}円")
            print(f"   200MA: {result['ma200']:.2f}円")
            
            # ゴールデンクロス情報を表示
            if 'golden_cross_5_25' in result:
                gc_5_25 = result['golden_cross_5_25']
                if gc_5_25['just_crossed']:
                    print(f"   ✓ 5MA/25MAゴールデンクロス: {gc_5_25['cross_date']}に発生")
                elif gc_5_25['has_crossed']:
                    print(f"   ✓ 5MA/25MAゴールデンクロス: クロス中（5MA={gc_5_25['ma5']:.2f} > 25MA={gc_5_25['ma25']:.2f}）")
            
            if 'golden_cross_25_75' in result:
                gc_25_75 = result['golden_cross_25_75']
                if gc_25_75['just_crossed']:
                    print(f"   ✓ 25MA/75MAゴールデンクロス: {gc_25_75['cross_date']}に発生")
                elif gc_25_75['has_crossed']:
                    print(f"   ✓ 25MA/75MAゴールデンクロス: クロス中（25MA={gc_25_75['ma25']:.2f} > 75MA={gc_25_75['ma75']:.2f}）")
            
            if 'golden_cross_5_200' in result:
                gc_5_200 = result['golden_cross_5_200']
                if gc_5_200['just_crossed']:
                    print(f"   ✓ 5MA/200MAゴールデンクロス: {gc_5_200['cross_date']}に発生")
                elif gc_5_200['has_crossed']:
                    print(f"   ✓ 5MA/200MAゴールデンクロス: クロス中（5MA={gc_5_200['ma5']:.2f} > 200MA={gc_5_200['ma200']:.2f}）")
            
            latest = result['latest_candle']
            prev = result['prev_candle']
            print(f"   最新2本: 陽線、陽線（両方5MAより上）")
            print()


if __name__ == '__main__':
    # テスト実行
    screener = JPX400Screener()
    results = screener.screen_all(complement_today=True)
    screener.display_results(results)

