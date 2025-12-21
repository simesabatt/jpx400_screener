"""
財務指標管理モジュール

Yahoo Financeから財務指標（PER、PBR、利回りなど）を取得し、データベースに保存・管理します。

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


class FinancialMetricsManager:
    """財務指標管理クラス"""
    
    def __init__(self, db_path: str):
        """
        初期化
        
        Args:
            db_path: SQLiteデータベースのパス
        """
        self.db_path = db_path
        self._ensure_columns()
    
    def _ensure_columns(self):
        """symbolsテーブルに財務指標列が存在することを確認（なければ追加）"""
        db_dir = os.path.dirname(self.db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # 財務指標列を追加（既存テーブル用）
            columns_to_add = [
                ('per', 'REAL'),
                ('pbr', 'REAL'),
                ('dividend_yield', 'REAL'),
                ('roe', 'REAL'),
                ('roa', 'REAL'),
                ('profit_margin', 'REAL'),
                ('financial_metrics_updated_at', 'TEXT')
            ]
            
            for col_name, col_type in columns_to_add:
                try:
                    cursor.execute(f'ALTER TABLE symbols ADD COLUMN {col_name} {col_type}')
                    print(f"[FinancialMetricsManager] symbolsテーブルに{col_name}列を追加しました")
                except sqlite3.OperationalError:
                    # 既に存在する場合はスキップ
                    pass
            
            # 設定テーブルを作成（最新実施日時を保存）
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS financial_metrics_settings (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            ''')
            
            conn.commit()
    
    def fetch_financial_metrics(
        self,
        symbol: str,
        max_retries: int = 3,
        retry_delay: float = 1.0
    ) -> Dict[str, Optional[float]]:
        """
        単一銘柄の財務指標をYahoo Financeから取得
        
        Args:
            symbol: 銘柄コード（例: "7203"）
            max_retries: 最大リトライ回数
            retry_delay: リトライ時の基本待機時間（秒）
        
        Returns:
            Dict[str, Optional[float]]: 財務指標の辞書
                - per: PER（過去12ヶ月）
                - pbr: PBR
                - dividend_yield: 配当利回り（%）
                - roe: ROE（%）
                - roa: ROA（%）
                - profit_margin: 売上高利益率（%）
        """
        ticker_symbol = f"{symbol}.T"
        last_error = None
        
        for attempt in range(max_retries):
            try:
                ticker = yf.Ticker(ticker_symbol)
                info = ticker.info
                
                if not info or len(info) <= 1:
                    return {
                        'per': None,
                        'pbr': None,
                        'dividend_yield': None,
                        'roe': None,
                        'roa': None,
                        'profit_margin': None
                    }
                
                # 財務指標を取得
                metrics = {
                    'per': info.get('trailingPE'),
                    'pbr': info.get('priceToBook'),
                    'dividend_yield': info.get('dividendYield'),
                    'roe': info.get('returnOnEquity'),
                    'roa': info.get('returnOnAssets'),
                    'profit_margin': info.get('profitMargins')
                }
                
                # dividend_yieldはパーセント値（例: 2.77）なのでそのまま使用
                # roe, roa, profit_marginは小数値（例: 0.12938）なので100倍してパーセントに変換
                if metrics['roe'] is not None:
                    metrics['roe'] = metrics['roe'] * 100
                if metrics['roa'] is not None:
                    metrics['roa'] = metrics['roa'] * 100
                if metrics['profit_margin'] is not None:
                    metrics['profit_margin'] = metrics['profit_margin'] * 100
                
                return metrics
            
            except Exception as e:
                last_error = e
                error_str = str(e)
                
                # 404エラー（銘柄が見つからない）の場合は即座に返す（リトライ不要）
                if '404' in error_str or 'Not Found' in error_str or 'Quote not found' in error_str:
                    print(f"[財務指標取得] {symbol}: 銘柄が見つかりません（404エラー）- スキップします")
                    return {
                        'per': None,
                        'pbr': None,
                        'dividend_yield': None,
                        'roe': None,
                        'roa': None,
                        'profit_margin': None
                    }
                
                # 接続エラーの場合はリトライ
                if '10061' in error_str or 'Connection refused' in error_str or 'urlopen error' in error_str:
                    if attempt < max_retries - 1:
                        wait_time = retry_delay * (2 ** attempt) + random.uniform(0, 0.5)
                        print(f"[財務指標取得] {symbol}: 接続エラー、{wait_time:.1f}秒後にリトライ ({attempt + 1}/{max_retries})")
                        time.sleep(wait_time)
                        continue
                    else:
                        break
                else:
                    # その他のエラーは即座に返す
                    break
        
        # エラーが発生した場合
        error_msg = str(last_error) if last_error else '不明なエラー'
        print(f"[財務指標取得] {symbol}: エラー - {error_msg}")
        return {
            'per': None,
            'pbr': None,
            'dividend_yield': None,
            'roe': None,
            'roa': None,
            'profit_margin': None
        }
    
    def save_financial_metrics(
        self,
        symbol: str,
        metrics: Dict[str, Optional[float]]
    ) -> bool:
        """
        財務指標をデータベースに保存
        
        Args:
            symbol: 銘柄コード
            metrics: 財務指標の辞書
        
        Returns:
            bool: 保存成功したかどうか
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                now = datetime.now().isoformat()
                
                cursor.execute('''
                    UPDATE symbols
                    SET per = ?,
                        pbr = ?,
                        dividend_yield = ?,
                        roe = ?,
                        roa = ?,
                        profit_margin = ?,
                        financial_metrics_updated_at = ?
                    WHERE symbol = ?
                ''', (
                    metrics.get('per'),
                    metrics.get('pbr'),
                    metrics.get('dividend_yield'),
                    metrics.get('roe'),
                    metrics.get('roa'),
                    metrics.get('profit_margin'),
                    now,
                    symbol
                ))
                
                conn.commit()
                return True
        
        except Exception as e:
            print(f"[財務指標保存] {symbol}: エラー - {e}")
            return False
    
    def fetch_and_save_financial_metrics(
        self,
        symbol: str,
        max_retries: int = 3,
        retry_delay: float = 1.0
    ) -> Dict[str, any]:
        """
        財務指標を取得してデータベースに保存（ワンステップ）
        
        Args:
            symbol: 銘柄コード
            max_retries: 最大リトライ回数
            retry_delay: リトライ時の基本待機時間（秒）
        
        Returns:
            Dict: 結果
                - success: 成功したかどうか
                - metrics: 取得した財務指標
                - error: エラーメッセージ（失敗時）
        """
        metrics = self.fetch_financial_metrics(symbol, max_retries, retry_delay)
        
        # データが取得できたかチェック（少なくとも1つでも値があれば成功とみなす）
        has_data = any(v is not None for v in metrics.values())
        
        if has_data:
            success = self.save_financial_metrics(symbol, metrics)
            if success:
                return {
                    'success': True,
                    'metrics': metrics
                }
            else:
                return {
                    'success': False,
                    'error': 'データベース保存に失敗しました'
                }
        else:
            return {
                'success': False,
                'error': '財務指標データが取得できませんでした'
            }
    
    def fetch_and_save_batch(
        self,
        symbols: List[str],
        progress_callback: Optional[Callable] = None,
        max_retries: int = 3,
        retry_delay: float = 1.0
    ) -> Dict[str, Dict]:
        """
        複数銘柄の財務指標を一括取得して保存
        
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
        print(f"[財務指標取得] 開始: {total}銘柄の財務指標を取得します")
        
        for i, symbol in enumerate(symbols, 1):
            # 進捗ログを出力
            if i % 10 == 0 or i == 1 or i == total:
                print(f"[財務指標取得] 進捗: {i}/{total} ({symbol})")
            
            result = self.fetch_and_save_financial_metrics(symbol, max_retries, retry_delay)
            
            if result['success']:
                results['success_count'] += 1
                results['results'][symbol] = result
            else:
                results['error_count'] += 1
                results['results'][symbol] = result
                # エラー詳細をログに出力（404エラーは除く）
                error_msg = result.get('error', '')
                if error_msg and '404' not in error_msg and 'Not Found' not in error_msg:
                    print(f"[財務指標取得] {symbol}: {error_msg}")
            
            if progress_callback:
                progress_callback(symbol, result['success'], i, total)
            
            # レート制限対策：ランダムな遅延
            if i < len(symbols):
                time.sleep(random.uniform(0.5, 1.0))
        
        print(f"[財務指標取得] 完了: 成功 {results['success_count']}件, エラー {results['error_count']}件")
        
        # 最新の取得日時を保存
        self.save_last_fetch_time()
        
        return results
    
    def get_financial_metrics(self, symbol: str) -> Dict[str, Optional[float]]:
        """
        データベースから財務指標を取得
        
        Args:
            symbol: 銘柄コード
        
        Returns:
            Dict[str, Optional[float]]: 財務指標の辞書
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT per, pbr, dividend_yield, roe, roa, profit_margin
                FROM symbols
                WHERE symbol = ?
            ''', (symbol,))
            
            row = cursor.fetchone()
            if row:
                return {
                    'per': row[0],
                    'pbr': row[1],
                    'dividend_yield': row[2],
                    'roe': row[3],
                    'roa': row[4],
                    'profit_margin': row[5]
                }
            else:
                return {
                    'per': None,
                    'pbr': None,
                    'dividend_yield': None,
                    'roe': None,
                    'roa': None,
                    'profit_margin': None
                }
    
    def get_financial_metrics_batch(self, symbols: List[str]) -> Dict[str, Dict[str, Optional[float]]]:
        """
        複数銘柄の財務指標を一括取得
        
        Args:
            symbols: 銘柄コードのリスト
        
        Returns:
            Dict[str, Dict]: 銘柄コードをキー、財務指標を値とする辞書
        """
        if not symbols:
            return {}
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            placeholders = ','.join(['?'] * len(symbols))
            cursor.execute(f'''
                SELECT symbol, per, pbr, dividend_yield, roe, roa, profit_margin
                FROM symbols
                WHERE symbol IN ({placeholders})
            ''', symbols)
            
            rows = cursor.fetchall()
            result = {}
            for row in rows:
                result[row[0]] = {
                    'per': row[1],
                    'pbr': row[2],
                    'dividend_yield': row[3],
                    'roe': row[4],
                    'roa': row[5],
                    'profit_margin': row[6]
                }
            
            return result
    
    def save_last_fetch_time(self, fetch_time: Optional[datetime] = None):
        """
        最新の財務指標取得日時を保存
        
        Args:
            fetch_time: 取得日時（Noneの場合は現在時刻）
        """
        if fetch_time is None:
            fetch_time = datetime.now()
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO financial_metrics_settings (key, value)
                VALUES (?, ?)
            ''', ('last_fetch_time', fetch_time.strftime('%Y-%m-%d %H:%M:%S')))
            conn.commit()
    
    def get_last_fetch_time(self) -> Optional[str]:
        """
        最新の財務指標取得日時を取得
        
        Returns:
            取得日時の文字列（YYYY-MM-DD HH:MM:SS形式）、未取得の場合はNone
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT value FROM financial_metrics_settings
                WHERE key = ?
            ''', ('last_fetch_time',))
            
            row = cursor.fetchone()
            if row:
                return row[0]
            return None

