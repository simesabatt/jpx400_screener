"""
セクター資金流動分析モジュール

セクターごとの売買代金（終値 × 出来高）を計算・分析する機能を提供します。

Copyright (c) 2025 JPX400スクリーニングシステム

This software is licensed under the MIT License.
See LICENSE file for details.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import sqlite3
from src.data_collector.ohlcv_data_manager import OHLCVDataManager
from src.screening.jpx400_manager import JPX400Manager


class SectorFlowAnalyzer:
    """セクター資金流動分析クラス"""
    
    def __init__(self, db_path: str):
        """
        初期化
        
        Args:
            db_path: SQLiteデータベースのパス
        """
        self.db_path = db_path
        self.ohlcv_manager = OHLCVDataManager(db_path)
        self.jpx400_manager = JPX400Manager()
    
    def get_oldest_date(self) -> Optional[datetime]:
        """
        DB内の日足データの最も古い日付を取得
        
        Returns:
            Optional[datetime]: 最も古い日付、データがない場合はNone
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT MIN(datetime)
                    FROM ohlcv_data
                    WHERE timeframe = ? AND source = ?
                ''', ('1d', 'yahoo'))
                
                result = cursor.fetchone()
                if result and result[0]:
                    try:
                        return pd.to_datetime(result[0])
                    except:
                        return None
                return None
        except Exception as e:
            print(f"[SectorFlowAnalyzer] 最古日付取得エラー: {e}")
            return None
    
    def calculate_sector_flow(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        days: Optional[int] = None
    ) -> pd.DataFrame:
        """
        セクターごとの売買代金（終値 × 出来高）を計算
        
        Args:
            start_date: 開始日（Noneの場合はdaysから計算）
            end_date: 終了日（Noneの場合は今日）
            days: 過去何日分を取得するか（start_dateがNoneの場合に使用）
        
        Returns:
            pd.DataFrame: 日付をインデックス、セクターを列とする売買代金データ
                列: 各セクター名
                値: 売買代金（億円単位）
        """
        # 日付の設定
        if end_date is None:
            end_date = datetime.now()
        
        if start_date is None:
            if days is None:
                # daysがNoneの場合は、DBから最も古い日付を取得
                oldest_date = self.get_oldest_date()
                if oldest_date:
                    start_date = oldest_date
                else:
                    # データがない場合は30日前から
                    start_date = end_date - timedelta(days=30)
            else:
                start_date = end_date - timedelta(days=days)
        
        # JPX400銘柄リストを取得
        symbols = self.jpx400_manager.load_symbols()
        if not symbols:
            print("[SectorFlowAnalyzer] JPX400銘柄リストが空です")
            return pd.DataFrame()
        
        # セクター情報を一括取得
        sectors_dict = self.ohlcv_manager.get_symbol_sectors(symbols)
        
        # 日足データを取得して売買代金を計算
        sector_flow_data: Dict[str, Dict[str, float]] = {}
        
        print(f"[SectorFlowAnalyzer] {len(symbols)}銘柄のデータを処理中...")
        
        for i, symbol in enumerate(symbols):
            if (i + 1) % 50 == 0:
                print(f"  進捗: {i + 1}/{len(symbols)}銘柄処理完了")
            
            # セクター情報を取得
            sector = sectors_dict.get(symbol)
            if not sector:
                continue  # セクター情報がない銘柄はスキップ
            
            # 日足データを取得
            df = self.ohlcv_manager.get_ohlcv_data(
                symbol=symbol,
                timeframe="1d",
                start_datetime=start_date,
                end_datetime=end_date,
                source="yahoo"
            )
            
            if df.empty:
                continue
            
            # 売買代金を計算（終値 × 出来高）
            df['turnover'] = df['close'] * df['volume']
            
            # セクターごとに集計
            for date, row in df.iterrows():
                date_str = date.strftime('%Y-%m-%d') if isinstance(date, pd.Timestamp) else str(date)
                
                if date_str not in sector_flow_data:
                    sector_flow_data[date_str] = {}
                
                if sector not in sector_flow_data[date_str]:
                    sector_flow_data[date_str][sector] = 0.0
                
                # 売買代金を加算（億円単位に変換）
                sector_flow_data[date_str][sector] += row['turnover'] / 100000000
        
        # DataFrameに変換
        if not sector_flow_data:
            print("[SectorFlowAnalyzer] データが取得できませんでした")
            return pd.DataFrame()
        
        # 日付でソート
        sorted_dates = sorted(sector_flow_data.keys())
        
        # 全セクターを取得
        all_sectors = set()
        for date_data in sector_flow_data.values():
            all_sectors.update(date_data.keys())
        all_sectors = sorted(list(all_sectors))
        
        # DataFrameを作成
        data_dict = {}
        for sector in all_sectors:
            data_dict[sector] = [
                sector_flow_data.get(date, {}).get(sector, 0.0)
                for date in sorted_dates
            ]
        
        result_df = pd.DataFrame(data_dict, index=pd.to_datetime(sorted_dates))
        result_df.index.name = 'date'
        
        print(f"[SectorFlowAnalyzer] {len(result_df)}日分のデータを取得しました")
        print(f"[SectorFlowAnalyzer] セクター数: {len(all_sectors)}")
        
        return result_df
    
    def calculate_sector_flow_with_change(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        days: Optional[int] = None
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        セクターごとの売買代金と前日比を計算
        
        Args:
            start_date: 開始日
            end_date: 終了日
            days: 過去何日分を取得するか
        
        Returns:
            Tuple[pd.DataFrame, pd.DataFrame]: 
                - 売買代金データ（億円単位）
                - 前日比データ（%）
        """
        flow_df = self.calculate_sector_flow(start_date, end_date, days)
        
        if flow_df.empty:
            return flow_df, pd.DataFrame()
        
        # 前日比を計算
        change_df = flow_df.pct_change() * 100
        change_df = change_df.fillna(0.0)
        
        return flow_df, change_df
    
    def calculate_sector_share(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        days: Optional[int] = None
    ) -> pd.DataFrame:
        """
        セクターごとの売買代金シェア（全体に占める割合）を計算
        
        Args:
            start_date: 開始日
            end_date: 終了日
            days: 過去何日分を取得するか
        
        Returns:
            pd.DataFrame: 日付をインデックス、セクターを列とするシェアデータ（%）
        """
        flow_df = self.calculate_sector_flow(start_date, end_date, days)
        
        if flow_df.empty:
            return pd.DataFrame()
        
        # 各日の合計を計算
        daily_total = flow_df.sum(axis=1)
        
        # シェアを計算（%）
        share_df = flow_df.div(daily_total, axis=0) * 100
        share_df = share_df.fillna(0.0)
        
        return share_df
    
    def get_top_sectors(
        self,
        date: Optional[datetime] = None,
        top_n: int = 5
    ) -> pd.DataFrame:
        """
        指定日の売買代金上位セクターを取得
        
        Args:
            date: 対象日（Noneの場合は最新日）
            top_n: 上位何件を取得するか
        
        Returns:
            pd.DataFrame: セクター名と売買代金（億円単位）
        """
        flow_df = self.calculate_sector_flow(days=1)
        
        if flow_df.empty:
            return pd.DataFrame()
        
        # 最新日を取得
        if date is None:
            target_date = flow_df.index[-1]
        else:
            target_date = pd.to_datetime(date)
            if target_date not in flow_df.index:
                # 最も近い日を取得
                target_date = flow_df.index[flow_df.index <= target_date][-1] if len(flow_df.index[flow_df.index <= target_date]) > 0 else flow_df.index[-1]
        
        # その日のデータを取得
        daily_data = flow_df.loc[target_date]
        
        # 上位N件を取得
        top_data = daily_data.nlargest(top_n)
        
        result_df = pd.DataFrame({
            'sector': top_data.index,
            'turnover': top_data.values
        })
        
        return result_df
    
    def get_sector_stock_counts(self) -> pd.DataFrame:
        """
        セクターごとの登録銘柄数を取得
        
        Returns:
            pd.DataFrame: セクター名と銘柄数のデータ
                列: 'sector'（セクター名）, 'count'（銘柄数）
        """
        # JPX400銘柄リストを取得
        symbols = self.jpx400_manager.load_symbols()
        if not symbols:
            print("[SectorFlowAnalyzer] JPX400銘柄リストが空です")
            return pd.DataFrame()
        
        # セクター情報を一括取得
        sectors_dict = self.ohlcv_manager.get_symbol_sectors(symbols)
        
        # セクターごとに銘柄数をカウント
        sector_counts: Dict[str, int] = {}
        for symbol, sector in sectors_dict.items():
            if sector:  # セクター情報がある場合のみ
                sector_counts[sector] = sector_counts.get(sector, 0) + 1
        
        if not sector_counts:
            print("[SectorFlowAnalyzer] セクター情報が見つかりませんでした")
            return pd.DataFrame()
        
        # DataFrameに変換（銘柄数でソート）
        result_data = [
            {'sector': sector, 'count': count}
            for sector, count in sorted(sector_counts.items(), key=lambda x: x[1], reverse=True)
        ]
        
        result_df = pd.DataFrame(result_data)
        
        print(f"[SectorFlowAnalyzer] セクター別銘柄数: {len(result_df)}セクター")
        
        return result_df
    
    def get_sector_industry_stock_counts(self) -> pd.DataFrame:
        """
        セクター・業種ごとの登録銘柄数を取得
        
        Returns:
            pd.DataFrame: セクター名、業種名、銘柄数のデータ
                列: 'sector'（セクター名）, 'industry'（業種名）, 'count'（銘柄数）
        """
        # JPX400銘柄リストを取得
        symbols = self.jpx400_manager.load_symbols()
        if not symbols:
            print("[SectorFlowAnalyzer] JPX400銘柄リストが空です")
            return pd.DataFrame()
        
        # セクター情報と業種情報を一括取得
        sectors_dict = self.ohlcv_manager.get_symbol_sectors(symbols)
        industries_dict = self.ohlcv_manager.get_symbol_industries(symbols)
        
        # セクター・業種ごとに銘柄数をカウント
        sector_industry_counts: Dict[Tuple[str, str], int] = {}
        for symbol in symbols:
            sector = sectors_dict.get(symbol)
            industry = industries_dict.get(symbol)
            
            if sector and industry:  # セクター情報と業種情報がある場合のみ
                key = (sector, industry)
                sector_industry_counts[key] = sector_industry_counts.get(key, 0) + 1
        
        if not sector_industry_counts:
            print("[SectorFlowAnalyzer] セクター・業種情報が見つかりませんでした")
            return pd.DataFrame()
        
        # DataFrameに変換（セクター、銘柄数（降順）、業種名でソート）
        result_data = [
            {'sector': sector, 'industry': industry, 'count': count}
            for (sector, industry), count in sorted(
                sector_industry_counts.items(), 
                key=lambda x: (x[0][0], -x[1], x[0][1])  # セクター名、銘柄数（降順）、業種名でソート
            )
        ]
        
        result_df = pd.DataFrame(result_data)
        
        print(f"[SectorFlowAnalyzer] セクター・業種別銘柄数: {len(result_df)}件")
        
        return result_df
    
    def calculate_sector_flow_per_stock(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        days: Optional[int] = None
    ) -> pd.DataFrame:
        """
        セクターごとの1銘柄あたりの売買代金を計算
        
        Args:
            start_date: 開始日（Noneの場合はdaysから計算）
            end_date: 終了日（Noneの場合は今日）
            days: 過去何日分を取得するか（start_dateがNoneの場合に使用）
        
        Returns:
            pd.DataFrame: 日付をインデックス、セクターを列とする1銘柄あたり売買代金データ
                列: 各セクター名
                値: 1銘柄あたりの売買代金（億円単位）
        """
        # セクターごとの売買代金を取得
        flow_df = self.calculate_sector_flow(start_date, end_date, days)
        
        if flow_df.empty:
            return pd.DataFrame()
        
        # セクターごとの銘柄数を取得
        sector_counts_df = self.get_sector_stock_counts()
        
        if sector_counts_df.empty:
            print("[SectorFlowAnalyzer] セクター別銘柄数が取得できませんでした")
            return pd.DataFrame()
        
        # セクター名をキーとする銘柄数の辞書を作成
        sector_counts_dict = dict(zip(sector_counts_df['sector'], sector_counts_df['count']))
        
        # 1銘柄あたりの売買代金を計算
        result_df = flow_df.copy()
        
        for sector in result_df.columns:
            count = sector_counts_dict.get(sector, 1)  # 銘柄数が取得できない場合は1で割る
            if count > 0:
                result_df[sector] = result_df[sector] / count
            else:
                result_df[sector] = 0.0
        
        print(f"[SectorFlowAnalyzer] 1銘柄あたり売買代金を計算しました")
        
        return result_df

