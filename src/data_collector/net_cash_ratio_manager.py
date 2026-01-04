"""
ネットキャッシュ比率管理モジュール

Yahoo Financeから取得した貸借対照表データを使用して、
清原スコアのネットキャッシュ比率を算出します。

計算式: (流動資産 + 投資有価証券 × 70% - 有利子負債) ÷ 時価総額

Copyright (c) 2025 JPX400スクリーニングシステム

This software is licensed under the MIT License.
See LICENSE file for details.
"""

import sqlite3
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Callable
import os
import time
import random
import json
import yfinance as yf
import pandas as pd


class NetCashRatioManager:
    """ネットキャッシュ比率管理クラス"""
    
    def __init__(self, db_path: str):
        """
        初期化
        
        Args:
            db_path: SQLiteデータベースのパス
        """
        self.db_path = db_path
        self._ensure_columns()
    
    def _ensure_columns(self):
        """テーブルと列が存在することを確認（なければ作成・追加）"""
        db_dir = os.path.dirname(self.db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # 貸借対照表キャッシュテーブルを作成
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS balance_sheet_cache (
                    symbol TEXT NOT NULL,
                    year TEXT NOT NULL,
                    data_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (symbol, year)
                )
            ''')
            
            # 時価総額キャッシュテーブルを作成
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS market_cap_cache (
                    symbol TEXT PRIMARY KEY,
                    market_cap REAL NOT NULL,
                    updated_at TEXT NOT NULL
                )
            ''')
            
            # インデックスを作成
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_balance_sheet_symbol ON balance_sheet_cache(symbol)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_market_cap_symbol ON market_cap_cache(symbol)')
            
            # symbolsテーブルにネットキャッシュ比率列を追加（既存テーブル用）
            columns_to_add = [
                ('net_cash_ratio', 'REAL'),
                ('net_cash_ratio_updated_at', 'TEXT')
            ]
            
            for col_name, col_type in columns_to_add:
                try:
                    cursor.execute(f'ALTER TABLE symbols ADD COLUMN {col_name} {col_type}')
                    print(f"[NetCashRatioManager] symbolsテーブルに{col_name}列を追加しました")
                except sqlite3.OperationalError:
                    # 既に存在する場合はスキップ
                    pass
            
            conn.commit()
    
    def _get_balance_sheet_value(
        self,
        balance_sheet: pd.DataFrame,
        candidates: List[str],
        latest_year: Optional[pd.Timestamp] = None
    ) -> Optional[float]:
        """
        貸借対照表から値を取得（複数の候補項目名を試行）
        
        検索順序:
        1. 完全一致（大文字小文字を区別する）
        2. 完全一致（大文字小文字を区別しない）
        3. 部分一致（大文字小文字を区別しない）
        
        Args:
            balance_sheet: 貸借対照表のDataFrame
            candidates: 試行する項目名のリスト（優先順位順）
            latest_year: 取得する年度（Noneの場合は最新年度）
        
        Returns:
            取得できた値、またはNone
        """
        if balance_sheet is None or balance_sheet.empty:
            return None
        
        # 年度を決定
        if latest_year is None and len(balance_sheet.columns) > 0:
            latest_year = balance_sheet.columns[0]
        elif latest_year is None:
            return None
        
        # インデックスを文字列のリストに変換（検索効率化のため）
        index_list = list(balance_sheet.index)
        index_lower = [str(idx).lower() for idx in index_list]
        
        # 候補項目名を順に試行
        for candidate in candidates:
            candidate_lower = candidate.lower()
            
            # 1. 完全一致（大文字小文字を区別する）
            try:
                if candidate in balance_sheet.index:
                    value = balance_sheet.loc[candidate, latest_year]
                    if pd.notna(value) and value != 0:
                        return float(value)
            except (KeyError, IndexError):
                pass
            
            # 2. 完全一致（大文字小文字を区別しない）
            try:
                if candidate_lower in index_lower:
                    idx_pos = index_lower.index(candidate_lower)
                    actual_index = index_list[idx_pos]
                    value = balance_sheet.loc[actual_index, latest_year]
                    if pd.notna(value) and value != 0:
                        return float(value)
            except (KeyError, IndexError, ValueError):
                pass
            
            # 3. 部分一致（大文字小文字を区別しない）
            try:
                for idx, idx_lower in zip(index_list, index_lower):
                    if candidate_lower in idx_lower:
                        value = balance_sheet.loc[idx, latest_year]
                        if pd.notna(value) and value != 0:
                            return float(value)
            except (KeyError, IndexError):
                continue
        
        return None
    
    def _get_balance_sheet_from_cache(self, symbol: str, year: str) -> Optional[pd.DataFrame]:
        """
        キャッシュから貸借対照表データを取得
        
        Args:
            symbol: 銘柄コード
            year: 年度（文字列）
        
        Returns:
            pd.DataFrame: 貸借対照表データ、またはNone
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT data_json FROM balance_sheet_cache
                WHERE symbol = ? AND year = ?
            ''', (symbol, year))
            
            row = cursor.fetchone()
            if row:
                try:
                    data_dict = json.loads(row[0])
                    # JSONからDataFrameに復元
                    # data_dict['data']は辞書形式 {項目名: 値}
                    series_data = pd.Series(data_dict['data'])
                    df = pd.DataFrame({data_dict['columns'][0]: series_data})
                    df.index = data_dict['index']
                    # 列名を日付型に変換
                    try:
                        df.columns = pd.to_datetime(df.columns)
                    except:
                        # 日付変換に失敗した場合は文字列のまま
                        pass
                    return df
                except Exception as e:
                    print(f"[NetCashRatioManager] キャッシュデータの復元エラー {symbol}: {e}")
                    return None
            return None
    
    def _save_balance_sheet_to_cache(self, symbol: str, balance_sheet: pd.DataFrame) -> bool:
        """
        貸借対照表データをキャッシュに保存
        
        Args:
            symbol: 銘柄コード
            balance_sheet: 貸借対照表データ
        
        Returns:
            bool: 保存成功したかどうか
        """
        if balance_sheet is None or balance_sheet.empty:
            return False
        
        try:
            now = datetime.now().isoformat()
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # 各年度のデータを保存
                for year in balance_sheet.columns:
                    year_str = str(year)
                    # DataFrameをJSONに変換
                    data_dict = {
                        'data': balance_sheet[year].to_dict(),
                        'index': balance_sheet.index.tolist(),
                        'columns': [year_str]
                    }
                    data_json = json.dumps(data_dict, default=str)
                    
                    cursor.execute('''
                        INSERT OR REPLACE INTO balance_sheet_cache
                        (symbol, year, data_json, updated_at)
                        VALUES (?, ?, ?, ?)
                    ''', (symbol, year_str, data_json, now))
                
                conn.commit()
                return True
        except Exception as e:
            print(f"[NetCashRatioManager] キャッシュ保存エラー {symbol}: {e}")
            return False
    
    def _get_market_cap_from_cache(self, symbol: str) -> Optional[float]:
        """
        キャッシュから時価総額を取得
        
        Args:
            symbol: 銘柄コード
        
        Returns:
            float: 時価総額、またはNone
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT market_cap FROM market_cap_cache
                WHERE symbol = ?
            ''', (symbol,))
            
            row = cursor.fetchone()
            if row and row[0] is not None:
                return float(row[0])
            return None
    
    def _save_market_cap_to_cache(self, symbol: str, market_cap: float) -> bool:
        """
        時価総額をキャッシュに保存
        
        Args:
            symbol: 銘柄コード
            market_cap: 時価総額
        
        Returns:
            bool: 保存成功したかどうか
        """
        try:
            now = datetime.now().isoformat()
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR REPLACE INTO market_cap_cache
                    (symbol, market_cap, updated_at)
                    VALUES (?, ?, ?)
                ''', (symbol, market_cap, now))
                conn.commit()
                return True
        except Exception as e:
            print(f"[NetCashRatioManager] 時価総額キャッシュ保存エラー {symbol}: {e}")
            return False
    
    def fetch_balance_sheet_data(
        self,
        symbol: str,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        use_cache: bool = True,
        force_update: bool = False,
        cache_only: bool = False
    ) -> Optional[pd.DataFrame]:
        """
        貸借対照表データを取得（キャッシュ優先）
        
        Args:
            symbol: 銘柄コード（例: "7203"）
            max_retries: 最大リトライ回数
            retry_delay: リトライ時の基本待機時間（秒）
            use_cache: キャッシュを使用するか（Trueの場合、キャッシュがあればそれを使用）
            force_update: 強制更新（キャッシュを無視してYahoo Financeから取得）
            cache_only: キャッシュのみを使用（ネットワークアクセスなし）
        
        Returns:
            pd.DataFrame: 貸借対照表データ、またはNone
        """
        # キャッシュから取得を試行（force_update=Falseの場合のみ）
        if use_cache and not force_update:
            # 最新年度を取得
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT year FROM balance_sheet_cache
                    WHERE symbol = ?
                    ORDER BY year DESC
                    LIMIT 1
                ''', (symbol,))
                row = cursor.fetchone()
                if row:
                    cached_data = self._get_balance_sheet_from_cache(symbol, row[0])
                    if cached_data is not None:
                        # キャッシュから全年度のデータを再構築
                        cursor.execute('''
                            SELECT year, data_json FROM balance_sheet_cache
                            WHERE symbol = ?
                            ORDER BY year DESC
                        ''', (symbol,))
                        rows = cursor.fetchall()
                        if rows:
                            try:
                                # 全年度のデータを結合
                                all_data = {}
                                all_index = None
                                for year_str, data_json in rows:
                                    data_dict = json.loads(data_json)
                                    if all_index is None:
                                        all_index = data_dict['index']
                                    all_data[year_str] = data_dict['data']
                                
                                # DataFrameに変換
                                df = pd.DataFrame(all_data)
                                df.index = all_index
                                # 列名を日付型に変換
                                try:
                                    df.columns = pd.to_datetime(df.columns)
                                except:
                                    # 日付変換に失敗した場合は文字列のまま
                                    pass
                                return df
                            except Exception as e:
                                print(f"[NetCashRatioManager] キャッシュデータの復元エラー {symbol}: {e}")
        
        # cache_only=Trueの場合、キャッシュにデータがない場合はNoneを返す
        if cache_only:
            return None
        
        # キャッシュにない、またはforce_update=Trueの場合はYahoo Financeから取得
        ticker_symbol = f"{symbol}.T"
        last_error = None
        
        for attempt in range(max_retries):
            try:
                ticker = yf.Ticker(ticker_symbol)
                balance_sheet = ticker.balance_sheet
                
                if balance_sheet is None or balance_sheet.empty:
                    return None
                
                # 取得したデータをキャッシュに保存
                self._save_balance_sheet_to_cache(symbol, balance_sheet)
                
                return balance_sheet
            
            except Exception as e:
                last_error = e
                error_str = str(e)
                
                # 404エラー（銘柄が見つからない）の場合は即座に返す（リトライ不要）
                if '404' in error_str or 'Not Found' in error_str or 'Quote not found' in error_str:
                    print(f"[ネットキャッシュ比率取得] {symbol}: 銘柄が見つかりません（404エラー）- スキップします")
                    return None
                
                # 接続エラーの場合はリトライ
                if '10061' in error_str or 'Connection refused' in error_str or 'urlopen error' in error_str:
                    if attempt < max_retries - 1:
                        wait_time = retry_delay * (2 ** attempt) + random.uniform(0, 0.5)
                        print(f"[ネットキャッシュ比率取得] {symbol}: 接続エラー、{wait_time:.1f}秒後にリトライ ({attempt + 1}/{max_retries})")
                        time.sleep(wait_time)
                        continue
                    else:
                        break
                else:
                    # その他のエラーは即座に返す
                    break
        
        # エラーが発生した場合
        error_msg = str(last_error) if last_error else '不明なエラー'
        print(f"[ネットキャッシュ比率取得] {symbol}: エラー - {error_msg}")
        return None
    
    def calculate_net_cash_ratio(
        self,
        symbol: str,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        use_cache: bool = True,
        force_update: bool = False,
        cache_only: bool = False
    ) -> Optional[float]:
        """
        ネットキャッシュ比率を計算（キャッシュ優先）
        
        計算式: (流動資産 + 投資有価証券 × 70% - 有利子負債) ÷ 時価総額
        
        Args:
            symbol: 銘柄コード（例: "7203"）
            max_retries: 最大リトライ回数
            retry_delay: リトライ時の基本待機時間（秒）
            use_cache: キャッシュを使用するか
            force_update: 強制更新（キャッシュを無視）
            cache_only: キャッシュのみを使用（ネットワークアクセスなし）
        
        Returns:
            float: ネットキャッシュ比率、またはNone（データ不足時）
        """
        ticker_symbol = f"{symbol}.T"
        
        try:
            # 貸借対照表を取得（キャッシュ優先）
            balance_sheet = self.fetch_balance_sheet_data(
                symbol, max_retries, retry_delay, use_cache=use_cache, force_update=force_update, cache_only=cache_only
            )
            if balance_sheet is None or balance_sheet.empty:
                if cache_only:
                    print(f"[ネットキャッシュ比率計算] {symbol}: キャッシュに貸借対照表データがありません")
                else:
                    print(f"[ネットキャッシュ比率計算] {symbol}: 貸借対照表データが取得できませんでした")
                return None
            
            # 時価総額を取得（キャッシュ優先）
            market_cap = None
            if use_cache and not force_update:
                market_cap = self._get_market_cap_from_cache(symbol)
            
            if market_cap is None:
                if cache_only:
                    print(f"[ネットキャッシュ比率計算] {symbol}: キャッシュに時価総額データがありません")
                    return None
                # キャッシュにない、またはforce_update=Trueの場合はYahoo Financeから取得
                ticker = yf.Ticker(ticker_symbol)
                info = ticker.info
                if not info or len(info) <= 1:
                    print(f"[ネットキャッシュ比率計算] {symbol}: 銘柄情報が取得できませんでした")
                    return None
                
                market_cap = info.get('marketCap')
                if market_cap is None or market_cap <= 0:
                    print(f"[ネットキャッシュ比率計算] {symbol}: 時価総額（marketCap）が取得できませんでした")
                    return None
                
                # 取得した時価総額をキャッシュに保存
                self._save_market_cap_to_cache(symbol, market_cap)
            
            # 最新年度を取得
            if len(balance_sheet.columns) == 0:
                print(f"[ネットキャッシュ比率計算] {symbol}: 貸借対照表に年度データがありません")
                return None
            latest_year = balance_sheet.columns[0]
            
            # 流動資産を取得
            # 部分一致検索により「Current Assets」を含む項目も検索される
            current_assets_candidates = [
                'Current Assets',
                'Total Current Assets',
                # 部分一致検索で「Current Assets」を含む項目も検索される
            ]
            current_assets = self._get_balance_sheet_value(
                balance_sheet, current_assets_candidates, latest_year
            )
            if current_assets is None:
                print(f"[ネットキャッシュ比率計算] {symbol}: 流動資産（Current Assets）が取得できませんでした")
                return None
            
            # 投資有価証券を取得（複数の候補を合計）
            investment_candidates = [
                'Long Term Equity Investment',
                'Other Short Term Investments',
                'Investments And Advances',
                'Investmentin Financial Assets',
                'Available For Sale Securities',
                'Held To Maturity Securities',
                'Trading Securities',
                'Investments In Other Ventures Under Equity Method',
                'Investmentsin Associatesat Cost',
                'Investmentsin Joint Venturesat Cost',
                'Investmentsin Subsidiariesat Cost',
                'Other Investments',
                'Investment Properties'
            ]
            
            investment_securities = 0.0
            for candidate in investment_candidates:
                value = self._get_balance_sheet_value(
                    balance_sheet, [candidate], latest_year
                )
                if value is not None:
                    investment_securities += value
            
            # 有利子負債を取得
            # 優先順位: Total Debt > (Long Term Debt + Current Debt) > (Long Term Debt And Capital Lease Obligation + Current Debt And Capital Lease Obligation)
            debt_candidates = [
                'Total Debt'
            ]
            total_debt = self._get_balance_sheet_value(
                balance_sheet, debt_candidates, latest_year
            )
            
            if total_debt is None:
                # Total Debtが取得できない場合は、Long Term Debt + Current Debtを試行
                long_term_debt_candidates = [
                    'Long Term Debt',
                    'Long Term Debt And Capital Lease Obligation'
                ]
                long_term_debt = self._get_balance_sheet_value(
                    balance_sheet, long_term_debt_candidates, latest_year
                ) or 0.0
                
                current_debt_candidates = [
                    'Current Debt',
                    'Current Debt And Capital Lease Obligation'
                ]
                current_debt = self._get_balance_sheet_value(
                    balance_sheet, current_debt_candidates, latest_year
                ) or 0.0
                
                total_debt = long_term_debt + current_debt
                if total_debt == 0:
                    print(f"[ネットキャッシュ比率計算] {symbol}: 有利子負債（Total Debt/Long Term Debt/Current Debt）が取得できませんでした")
                    return None
            
            # ネットキャッシュ比率を計算
            # (流動資産 + 投資有価証券 × 70% - 有利子負債) ÷ 時価総額
            numerator = current_assets + (investment_securities * 0.7) - total_debt
            net_cash_ratio = numerator / market_cap if market_cap > 0 else None
            
            return net_cash_ratio
        
        except Exception as e:
            print(f"[ネットキャッシュ比率計算] {symbol}: エラー - {e}")
            return None
    
    def save_net_cash_ratio(
        self,
        symbol: str,
        net_cash_ratio: Optional[float]
    ) -> bool:
        """
        ネットキャッシュ比率をデータベースに保存
        
        Args:
            symbol: 銘柄コード
            net_cash_ratio: ネットキャッシュ比率
        
        Returns:
            bool: 保存成功したかどうか
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                now = datetime.now().isoformat()
                
                cursor.execute('''
                    UPDATE symbols
                    SET net_cash_ratio = ?,
                        net_cash_ratio_updated_at = ?
                    WHERE symbol = ?
                ''', (
                    net_cash_ratio,
                    now,
                    symbol
                ))
                
                conn.commit()
                return True
        
        except Exception as e:
            print(f"[ネットキャッシュ比率保存] {symbol}: エラー - {e}")
            return False
    
    def fetch_and_save_net_cash_ratio(
        self,
        symbol: str,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        use_cache: bool = True,
        force_update: bool = False,
        cache_only: bool = False
    ) -> Dict[str, any]:
        """
        ネットキャッシュ比率を取得してデータベースに保存（ワンステップ）
        
        Args:
            symbol: 銘柄コード
            max_retries: 最大リトライ回数
            retry_delay: リトライ時の基本待機時間（秒）
            use_cache: キャッシュを使用するか
            force_update: 強制更新（キャッシュを無視）
            cache_only: キャッシュのみを使用（ネットワークアクセスなし）
        
        Returns:
            Dict: 結果
                - success: 成功したかどうか
                - net_cash_ratio: 取得したネットキャッシュ比率
                - error: エラーメッセージ（失敗時）
        """
        net_cash_ratio = self.calculate_net_cash_ratio(
            symbol, max_retries, retry_delay, use_cache=use_cache, force_update=force_update, cache_only=cache_only
        )
        
        if net_cash_ratio is not None:
            success = self.save_net_cash_ratio(symbol, net_cash_ratio)
            if success:
                return {
                    'success': True,
                    'net_cash_ratio': net_cash_ratio
                }
            else:
                return {
                    'success': False,
                    'error': 'データベース保存に失敗しました'
                }
        else:
            return {
                'success': False,
                'error': 'ネットキャッシュ比率データが取得できませんでした'
            }
    
    def calculate_from_cache_batch(
        self,
        symbols: List[str],
        progress_callback: Optional[Callable] = None
    ) -> Dict[str, Dict]:
        """
        キャッシュからネットキャッシュ比率を一括計算して保存（ネットワークアクセスなし）
        
        Args:
            symbols: 銘柄コードのリスト
            progress_callback: 進捗コールバック関数（symbol, success, current, total）を受け取る
        
        Returns:
            Dict[str, Dict]: 結果の辞書
                - success_count: 成功数
                - error_count: エラー数
                - results: 各銘柄の結果
        """
        results = {
            'success_count': 0,
            'error_count': 0,
            'results': {}
        }
        
        total = len(symbols)
        print(f"[ネットキャッシュ比率計算] 開始: {total}銘柄のネットキャッシュ比率をキャッシュから計算します")
        
        for i, symbol in enumerate(symbols, 1):
            # 進捗ログを出力
            if i % 10 == 0 or i == 1 or i == total:
                print(f"[ネットキャッシュ比率計算] 進捗: {i}/{total} ({symbol})")
            
            result = self.fetch_and_save_net_cash_ratio(
                symbol, cache_only=True
            )
            
            if result['success']:
                results['success_count'] += 1
                results['results'][symbol] = result
                if i % 10 == 0 or i == 1 or i == total:
                    print(f"  → ネットキャッシュ比率: {result['net_cash_ratio']:.4f}")
            else:
                results['error_count'] += 1
                results['results'][symbol] = result
                if i % 10 == 0 or i == 1 or i == total:
                    print(f"  → エラー: {result.get('error', '不明なエラー')}")
            
            # 進捗コールバックを呼び出し
            if progress_callback:
                progress_callback(symbol, result['success'], i, total)
        
        print(f"[ネットキャッシュ比率計算] 完了: 成功 {results['success_count']}件、エラー {results['error_count']}件")
        return results
    
    def fetch_and_save_batch(
        self,
        symbols: List[str],
        progress_callback: Optional[Callable] = None,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        use_cache: bool = True,
        force_update: bool = False,
        cache_only: bool = False
    ) -> Dict[str, Dict]:
        """
        複数銘柄のネットキャッシュ比率を一括取得して保存
        
        Args:
            symbols: 銘柄コードのリスト
            progress_callback: 進捗コールバック関数（symbol, success, current, total）を受け取る
            max_retries: 最大リトライ回数
            retry_delay: リトライ時の基本待機時間（秒）
            use_cache: キャッシュを使用するか
            force_update: 強制更新（キャッシュを無視）
            cache_only: キャッシュのみを使用（ネットワークアクセスなし）
        
        Returns:
            Dict[str, Dict]: 結果の辞書
                - success_count: 成功数
                - error_count: エラー数
                - results: 各銘柄の結果
        """
        results = {
            'success_count': 0,
            'error_count': 0,
            'results': {}
        }
        
        total = len(symbols)
        if cache_only:
            print(f"[ネットキャッシュ比率計算] 開始: {total}銘柄のネットキャッシュ比率をキャッシュから計算します")
        else:
            print(f"[ネットキャッシュ比率取得] 開始: {total}銘柄のネットキャッシュ比率を取得します")
        
        for i, symbol in enumerate(symbols, 1):
            # 進捗ログを出力
            if i % 10 == 0 or i == 1 or i == total:
                if cache_only:
                    print(f"[ネットキャッシュ比率計算] 進捗: {i}/{total} ({symbol})")
                else:
                    print(f"[ネットキャッシュ比率取得] 進捗: {i}/{total} ({symbol})")
            
            result = self.fetch_and_save_net_cash_ratio(
                symbol, max_retries, retry_delay, use_cache=use_cache, force_update=force_update, cache_only=cache_only
            )
            
            if result['success']:
                results['success_count'] += 1
                results['results'][symbol] = result
                if i % 10 == 0 or i == 1 or i == total:
                    print(f"  → ネットキャッシュ比率: {result['net_cash_ratio']:.4f}")
            else:
                results['error_count'] += 1
                results['results'][symbol] = result
                # エラー詳細をログに出力（404エラーは除く）
                error_msg = result.get('error', '')
                if error_msg and '404' not in error_msg and 'Not Found' not in error_msg:
                    print(f"[ネットキャッシュ比率取得] {symbol}: {error_msg}")
            
            if progress_callback:
                progress_callback(symbol, result['success'], i, total)
            
            # レート制限対策：ランダムな遅延
            if i < len(symbols):
                time.sleep(random.uniform(0.5, 1.0))
        
        print(f"[ネットキャッシュ比率取得] 完了: 成功 {results['success_count']}件, エラー {results['error_count']}件")
        
        return results
    
    def get_net_cash_ratio(self, symbol: str) -> Optional[float]:
        """
        データベースからネットキャッシュ比率を取得
        
        Args:
            symbol: 銘柄コード
        
        Returns:
            Optional[float]: ネットキャッシュ比率、またはNone
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT net_cash_ratio
                FROM symbols
                WHERE symbol = ?
            ''', (symbol,))
            
            row = cursor.fetchone()
            if row and row[0] is not None:
                return float(row[0])
            return None
    
    def get_net_cash_ratio_batch(self, symbols: List[str]) -> Dict[str, Optional[float]]:
        """
        複数銘柄のネットキャッシュ比率を一括取得
        
        Args:
            symbols: 銘柄コードのリスト
        
        Returns:
            Dict[str, Optional[float]]: 銘柄コードをキー、ネットキャッシュ比率を値とする辞書
        """
        if not symbols:
            return {}
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            placeholders = ','.join(['?'] * len(symbols))
            cursor.execute(f'''
                SELECT symbol, net_cash_ratio
                FROM symbols
                WHERE symbol IN ({placeholders})
            ''', symbols)
            
            rows = cursor.fetchall()
            result = {}
            for row in rows:
                result[row[0]] = float(row[1]) if row[1] is not None else None
            
            return result

