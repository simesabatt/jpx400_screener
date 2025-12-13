"""
Yahoo Financeからデータを取得するモジュール

yfinanceライブラリを使用して日本株の時系列データを取得します。

Copyright (c) 2025 JPX400スクリーニングシステム

This software is licensed under the MIT License.
See LICENSE file for details.
"""

import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, Literal
import sqlite3
import os
from src.data_collector.ohlcv_data_manager import OHLCVDataManager


class YahooFinanceFetcher:
    """Yahoo Financeデータ取得クラス"""
    
    def __init__(self, db_path: str):
        """
        初期化
        
        Args:
            db_path: SQLiteデータベースのパス
        """
        self.db_path = db_path
        self.ohlcv_manager = OHLCVDataManager(db_path)
    
    def fetch_data(
        self,
        symbol: str,
        period: str = "7d",
        interval: Literal["1m", "5m", "15m", "1h", "1d"] = "1m"
    ) -> pd.DataFrame:
        """
        Yahoo Financeからデータを取得
        
        Args:
            symbol: 銘柄コード（例: "9501"）
            period: 取得期間
                - "1d": 1日
                - "5d": 5日
                - "1mo": 1ヶ月
                - "3mo": 3ヶ月
                - "6mo": 6ヶ月
                - "1y": 1年
                - "2y": 2年
                - "5y": 5年
                - "max": 全期間
            interval: 時間足
                - "1m": 1分足（最大7日間）
                - "5m": 5分足
                - "15m": 15分足
                - "1h": 1時間足
                - "1d": 1日足
        
        Returns:
            pd.DataFrame: OHLCV データ（列: Open, High, Low, Close, Volume）
        """
        # 日本株の場合は.Tを付ける
        ticker_symbol = f"{symbol}.T"
        
        print(f"[Yahoo Finance] {symbol} のデータ取得開始...")
        print(f"  期間: {period}, 間隔: {interval}")
        
        try:
            ticker = yf.Ticker(ticker_symbol)
            df = ticker.history(period=period, interval=interval)
            
            if df.empty:
                print(f"[Yahoo Finance] データが見つかりませんでした: {symbol}")
                return pd.DataFrame()
            
            # 列名を小文字に統一
            df.columns = [col.lower() for col in df.columns]
            
            # 必要な列のみ抽出
            required_cols = ['open', 'high', 'low', 'close', 'volume']
            df = df[required_cols]
            
            # タイムゾーン情報を削除（ナイーブにする）
            if df.index.tz is not None:
                df.index = df.index.tz_localize(None)
            
            print(f"[Yahoo Finance] {len(df)}件のデータを取得しました")
            if not df.empty:
                print(f"  期間: {df.index.min()} ～ {df.index.max()}")
            
            return df
        
        except Exception as e:
            print(f"[Yahoo Finance] エラー: {e}")
            return pd.DataFrame()
    
    
    def fetch_and_save(
        self,
        symbol: str,
        period: str = "7d",
        interval: Literal["1m", "5m", "15m", "1h", "1d"] = "1m",
        overwrite: bool = False
    ) -> dict:
        """
        データを取得してデータベースに保存（ワンステップ）
        
        Args:
            symbol: 銘柄コード
            period: 取得期間
            interval: 時間足
            overwrite: 既存データを上書きするか
        
        Returns:
            dict: 保存結果（saved_count, skipped_count, updated_count）
        """
        df = self.fetch_data(symbol, period, interval)
        if df.empty:
            return {'saved_count': 0, 'skipped_count': 0, 'updated_count': 0, 'total_count': 0}
        
        result = self.ohlcv_manager.save_ohlcv_data_with_stats(symbol, df, interval, source="yahoo", overwrite=overwrite)
        print(f"[Yahoo Finance] DB保存完了: {result['saved_count']}件新規保存, {result['updated_count']}件更新, {result['skipped_count']}件スキップ")
        return result
    
    def get_ohlcv_data(
        self,
        symbol: str,
        timeframe: str = "1m",
        start_datetime: Optional[datetime] = None,
        end_datetime: Optional[datetime] = None,
        source: str = "yahoo"
    ) -> pd.DataFrame:
        """
        データベースからOHLCVデータを取得
        
        Args:
            symbol: 銘柄コード
            timeframe: 時間足
            start_datetime: 開始日時
            end_datetime: 終了日時
            source: データソース
        
        Returns:
            pd.DataFrame: OHLCVデータ
        """
        return self.ohlcv_manager.get_ohlcv_data(
            symbol, timeframe, start_datetime, end_datetime, source
        )
    
    def get_data_stats(self, symbol: str, timeframe: str = "1m") -> dict:
        """
        データベース内のデータ統計を取得
        
        Args:
            symbol: 銘柄コード
            timeframe: 時間足
        
        Returns:
            dict: 統計情報（件数、期間など）
        """
        return self.ohlcv_manager.get_data_stats(symbol, timeframe, source="yahoo")


def main():
    """テスト用のメイン関数"""
    import sys
    
    # データベースパス
    db_path = os.path.join(os.path.dirname(__file__), "..", "..", "data", "tick_data.db")
    
    # Fetcherインスタンス作成
    fetcher = YahooFinanceFetcher(db_path)
    
    # テスト銘柄
    test_symbols = ["9501", "9432", "1306", "1475"]
    
    print("=" * 60)
    print("Yahoo Finance データ取得テスト")
    print("=" * 60)
    
    for symbol in test_symbols:
        print(f"\n{'='*60}")
        print(f"銘柄: {symbol}")
        print(f"{'='*60}")
        
        # 1分足データを7日分取得して保存
        saved_count = fetcher.fetch_and_save(
            symbol=symbol,
            period="7d",
            interval="1m"
        )
        
        if saved_count > 0:
            # 統計情報を表示
            stats = fetcher.get_data_stats(symbol, "1m")
            print(f"\n[統計情報]")
            print(f"  総件数: {stats['total_count']:,}件")
            print(f"  期間: {stats['start_date']} ～ {stats['end_date']}")
    
    print(f"\n{'='*60}")
    print("完了")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()

