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
from datetime import datetime
from typing import Optional, List, Dict, Callable
import os
import time
import random
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
        """symbolsテーブルにネットキャッシュ比率列が存在することを確認（なければ追加）"""
        db_dir = os.path.dirname(self.db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # ネットキャッシュ比率列を追加（既存テーブル用）
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
        
        # 候補項目名を順に試行
        for candidate in candidates:
            try:
                if candidate in balance_sheet.index:
                    value = balance_sheet.loc[candidate, latest_year]
                    if pd.notna(value) and value != 0:
                        return float(value)
            except (KeyError, IndexError):
                continue
        
        return None
    
    def fetch_balance_sheet_data(
        self,
        symbol: str,
        max_retries: int = 3,
        retry_delay: float = 1.0
    ) -> Optional[pd.DataFrame]:
        """
        貸借対照表データを取得
        
        Args:
            symbol: 銘柄コード（例: "7203"）
            max_retries: 最大リトライ回数
            retry_delay: リトライ時の基本待機時間（秒）
        
        Returns:
            pd.DataFrame: 貸借対照表データ、またはNone
        """
        ticker_symbol = f"{symbol}.T"
        last_error = None
        
        for attempt in range(max_retries):
            try:
                ticker = yf.Ticker(ticker_symbol)
                balance_sheet = ticker.balance_sheet
                
                if balance_sheet is None or balance_sheet.empty:
                    return None
                
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
        retry_delay: float = 1.0
    ) -> Optional[float]:
        """
        ネットキャッシュ比率を計算
        
        計算式: (流動資産 + 投資有価証券 × 70% - 有利子負債) ÷ 時価総額
        
        Args:
            symbol: 銘柄コード（例: "7203"）
            max_retries: 最大リトライ回数
            retry_delay: リトライ時の基本待機時間（秒）
        
        Returns:
            float: ネットキャッシュ比率、またはNone（データ不足時）
        """
        ticker_symbol = f"{symbol}.T"
        
        try:
            ticker = yf.Ticker(ticker_symbol)
            
            # 貸借対照表を取得
            balance_sheet = self.fetch_balance_sheet_data(symbol, max_retries, retry_delay)
            if balance_sheet is None or balance_sheet.empty:
                print(f"[ネットキャッシュ比率計算] {symbol}: 貸借対照表データが取得できませんでした")
                return None
            
            # 時価総額を取得
            info = ticker.info
            if not info or len(info) <= 1:
                print(f"[ネットキャッシュ比率計算] {symbol}: 銘柄情報が取得できませんでした")
                return None
            
            market_cap = info.get('marketCap')
            if market_cap is None or market_cap <= 0:
                print(f"[ネットキャッシュ比率計算] {symbol}: 時価総額（marketCap）が取得できませんでした")
                return None
            
            # 最新年度を取得
            if len(balance_sheet.columns) == 0:
                print(f"[ネットキャッシュ比率計算] {symbol}: 貸借対照表に年度データがありません")
                return None
            latest_year = balance_sheet.columns[0]
            
            # 流動資産を取得
            current_assets_candidates = [
                'Current Assets',
                'Total Current Assets'
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
            debt_candidates = [
                'Total Debt'
            ]
            total_debt = self._get_balance_sheet_value(
                balance_sheet, debt_candidates, latest_year
            )
            if total_debt is None:
                # Total Debtが取得できない場合は、Long Term Debt + Current Debtを試行
                long_term_debt = self._get_balance_sheet_value(
                    balance_sheet, ['Long Term Debt'], latest_year
                ) or 0.0
                current_debt = self._get_balance_sheet_value(
                    balance_sheet, ['Current Debt'], latest_year
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
        retry_delay: float = 1.0
    ) -> Dict[str, any]:
        """
        ネットキャッシュ比率を取得してデータベースに保存（ワンステップ）
        
        Args:
            symbol: 銘柄コード
            max_retries: 最大リトライ回数
            retry_delay: リトライ時の基本待機時間（秒）
        
        Returns:
            Dict: 結果
                - success: 成功したかどうか
                - net_cash_ratio: 取得したネットキャッシュ比率
                - error: エラーメッセージ（失敗時）
        """
        net_cash_ratio = self.calculate_net_cash_ratio(symbol, max_retries, retry_delay)
        
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
    
    def fetch_and_save_batch(
        self,
        symbols: List[str],
        progress_callback: Optional[Callable] = None,
        max_retries: int = 3,
        retry_delay: float = 1.0
    ) -> Dict[str, Dict]:
        """
        複数銘柄のネットキャッシュ比率を一括取得して保存
        
        Args:
            symbols: 銘柄コードのリスト
            progress_callback: 進捗コールバック関数（symbol, success, current, total）を受け取る
            max_retries: 最大リトライ回数
            retry_delay: リトライ時の基本待機時間（秒）
        
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
        print(f"[ネットキャッシュ比率取得] 開始: {total}銘柄のネットキャッシュ比率を取得します")
        
        for i, symbol in enumerate(symbols, 1):
            # 進捗ログを出力
            if i % 10 == 0 or i == 1 or i == total:
                print(f"[ネットキャッシュ比率取得] 進捗: {i}/{total} ({symbol})")
            
            result = self.fetch_and_save_net_cash_ratio(symbol, max_retries, retry_delay)
            
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

