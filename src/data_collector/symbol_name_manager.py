"""
銘柄名管理モジュール

Yahoo Financeから銘柄名（和名）を取得し、データベースに保存・管理します。

Copyright (c) 2025 JPX400スクリーニングシステム

This software is licensed under the MIT License.
See LICENSE file for details.
"""
import sqlite3
from datetime import datetime
from typing import Optional, List, Dict, Callable
import os
import re


# セクター名の英語→日本語変換辞書
SECTOR_NAME_JP = {
    "Basic Materials": "素材",
    "Communication Services": "通信サービス",
    "Consumer Cyclical": "一般消費財",
    "Consumer Defensive": "生活必需品",
    "Energy": "エネルギー",
    "Financial Services": "金融",
    "Healthcare": "ヘルスケア",
    "Industrials": "資本財",
    "Real Estate": "不動産",
    "Technology": "テクノロジー",
    "Utilities": "公益事業",
    # 業種（industry）も含める
    "Consumer Staples": "生活必需品",
    "Consumer Discretionary": "一般消費財",
    "Information Technology": "情報技術",
    "Telecommunication Services": "通信サービス",
}


class SymbolNameManager:
    """銘柄名管理クラス"""
    
    def __init__(self, db_path: str):
        """
        初期化
        
        Args:
            db_path: SQLiteデータベースのパス
        """
        self.db_path = db_path
        self._ensure_table()
    
    def _ensure_table(self):
        """symbolsテーブルが存在することを確認（なければ作成）"""
        # データベースディレクトリの存在確認
        db_dir = os.path.dirname(self.db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # 銘柄情報テーブルを作成（銘柄名を保存）
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS symbols (
                    symbol TEXT PRIMARY KEY,
                    name TEXT,
                    sector TEXT,
                    updated_at TEXT,
                    created_at TEXT NOT NULL
                )
            ''')
            
            # マイグレーション: created_at列を追加（既存テーブル用）
            try:
                cursor.execute('ALTER TABLE symbols ADD COLUMN created_at TEXT')
                # 既存レコードのcreated_atを現在時刻で埋める
                cursor.execute('UPDATE symbols SET created_at = ? WHERE created_at IS NULL', (datetime.now().isoformat(),))
                print("[SymbolNameManager] symbolsテーブルにcreated_at列を追加しました")
            except sqlite3.OperationalError:
                # 既に存在する場合はスキップ
                pass
            
            # マイグレーション: updated_at列を追加（既存テーブル用）
            try:
                cursor.execute('ALTER TABLE symbols ADD COLUMN updated_at TEXT')
                print("[SymbolNameManager] symbolsテーブルにupdated_at列を追加しました")
            except sqlite3.OperationalError:
                # 既に存在する場合はスキップ
                pass
            
            # マイグレーション: sector列を追加（既存テーブル用）
            try:
                cursor.execute('ALTER TABLE symbols ADD COLUMN sector TEXT')
                print("[SymbolNameManager] symbolsテーブルにsector列を追加しました")
            except sqlite3.OperationalError:
                # 既に存在する場合はスキップ
                pass
            
            # マイグレーション: industry列を追加（既存テーブル用）
            try:
                cursor.execute('ALTER TABLE symbols ADD COLUMN industry TEXT')
                print("[SymbolNameManager] symbolsテーブルにindustry列を追加しました")
            except sqlite3.OperationalError:
                # 既に存在する場合はスキップ
                pass
            
            # 銘柄情報テーブルのインデックス
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_symbols_name 
                ON symbols(name)
            ''')
            
            conn.commit()
    
    def save_symbol_name(
        self, 
        symbol: str, 
        name: str, 
        sector: Optional[str] = None, 
        industry: Optional[str] = None,
        preserve_existing_sector: bool = True,
        preserve_existing_industry: bool = True
    ):
        """
        銘柄名を保存
        
        Args:
            symbol: 銘柄コード
            name: 銘柄名
            sector: セクター（任意）
            industry: 業種（任意）
            preserve_existing_sector: 既存のセクター情報を保持するか（デフォルト: True）
            preserve_existing_industry: 既存の業種情報を保持するか（デフォルト: True）
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            now = datetime.now().isoformat()
            
            # テーブル構造を確認してから適切なSQLを実行
            cursor.execute("PRAGMA table_info(symbols)")
            columns = [row[1] for row in cursor.fetchall()]
            has_created_at = 'created_at' in columns
            has_updated_at = 'updated_at' in columns
            has_sector = 'sector' in columns
            has_industry = 'industry' in columns
            
            # 既存のセクター情報を取得（保持する場合）
            existing_sector = None
            if preserve_existing_sector and has_sector:
                existing_sector = self.get_symbol_sector(symbol)
                # 既存のセクター情報がある場合は、それを使用
                if existing_sector and not sector:
                    sector = existing_sector
            
            # 既存の業種情報を取得（保持する場合）
            existing_industry = None
            if preserve_existing_industry and has_industry:
                existing_industry = self.get_symbol_industry(symbol)
                # 既存の業種情報がある場合は、それを使用
                if existing_industry and not industry:
                    industry = existing_industry
            
            # テーブル構造に応じて適切なSQLを実行
            if has_created_at and has_updated_at and has_sector and has_industry:
                # created_at、updated_at、sector、industryの全てがある場合
                cursor.execute('''
                    INSERT OR REPLACE INTO symbols (symbol, name, sector, industry, updated_at, created_at)
                    VALUES (?, ?, ?, ?, ?, COALESCE((SELECT created_at FROM symbols WHERE symbol = ?), ?))
                ''', (symbol, name, sector, industry, now, symbol, now))
            elif has_created_at and has_updated_at and has_sector:
                # created_at、updated_at、sectorがある場合（industryなし）
                cursor.execute('''
                    INSERT OR REPLACE INTO symbols (symbol, name, sector, updated_at, created_at)
                    VALUES (?, ?, ?, ?, COALESCE((SELECT created_at FROM symbols WHERE symbol = ?), ?))
                ''', (symbol, name, sector, now, symbol, now))
            elif has_created_at and has_updated_at:
                # created_atとupdated_atの両方がある場合（sectorなし）
                cursor.execute('''
                    INSERT OR REPLACE INTO symbols (symbol, name, updated_at, created_at)
                    VALUES (?, ?, ?, COALESCE((SELECT created_at FROM symbols WHERE symbol = ?), ?))
                ''', (symbol, name, now, symbol, now))
            elif has_updated_at:
                # updated_atのみある場合
                cursor.execute('''
                    INSERT OR REPLACE INTO symbols (symbol, name, updated_at)
                    VALUES (?, ?, ?)
                ''', (symbol, name, now))
            else:
                # どちらもない場合（最小限の構造）
                cursor.execute('''
                    INSERT OR REPLACE INTO symbols (symbol, name)
                    VALUES (?, ?)
                ''', (symbol, name))
            
            conn.commit()
    
    def get_symbol_name(self, symbol: str) -> Optional[str]:
        """
        銘柄名を取得
        
        Args:
            symbol: 銘柄コード
            
        Returns:
            Optional[str]: 銘柄名（存在しない場合はNone）
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT name FROM symbols WHERE symbol = ?', (symbol,))
            row = cursor.fetchone()
            return row[0] if row else None
    
    def get_symbol_names(self, symbols: List[str]) -> Dict[str, str]:
        """
        複数の銘柄名を一括取得
        
        Args:
            symbols: 銘柄コードのリスト
            
        Returns:
            Dict[str, str]: 銘柄コードをキー、銘柄名を値とする辞書
        """
        if not symbols:
            return {}
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            placeholders = ','.join(['?'] * len(symbols))
            cursor.execute(
                f'SELECT symbol, name FROM symbols WHERE symbol IN ({placeholders})',
                symbols
            )
            rows = cursor.fetchall()
            return {row[0]: row[1] for row in rows if row[1]}
    
    @staticmethod
    def _translate_sector_to_japanese(sector: Optional[str]) -> Optional[str]:
        """
        セクター名を英語から日本語に変換
        
        まず辞書を確認し、辞書にない場合は翻訳モジュールを使用
        
        Args:
            sector: 英語のセクター名
            
        Returns:
            Optional[str]: 日本語のセクター名（変換できない場合は元の値）
        """
        if not sector:
            return None
        
        # まず辞書を確認（高速で一貫性がある）
        if sector in SECTOR_NAME_JP:
            return SECTOR_NAME_JP[sector]
        
        # 辞書にない場合は翻訳モジュールを使用
        try:
            from deep_translator import GoogleTranslator
            
            translator = GoogleTranslator(source='en', target='ja')
            translated = translator.translate(sector)
            
            if translated and translated.strip():
                return translated.strip()
            else:
                # 翻訳に失敗した場合は元の値を返す
                return sector
        
        except ImportError:
            # deep-translatorがインストールされていない場合は元の値を返す
            return sector
        
        except Exception:
            # 翻訳エラーが発生した場合は元の値を返す
            return sector
    
    @staticmethod
    def _translate_industry_to_japanese(industry: Optional[str]) -> Optional[str]:
        """
        業種名を英語から日本語に変換
        
        まず辞書を確認し、辞書にない場合は翻訳モジュールを使用
        
        Args:
            industry: 英語の業種名
            
        Returns:
            Optional[str]: 日本語の業種名（変換できない場合は元の値）
        """
        if not industry:
            return None
        
        # まず辞書を確認（セクター辞書も業種に使える場合がある）
        if industry in SECTOR_NAME_JP:
            return SECTOR_NAME_JP[industry]
        
        # 辞書にない場合は翻訳モジュールを使用
        try:
            from deep_translator import GoogleTranslator
            
            translator = GoogleTranslator(source='en', target='ja')
            translated = translator.translate(industry)
            
            if translated and translated.strip():
                return translated.strip()
            else:
                # 翻訳に失敗した場合は元の値を返す
                return industry
        
        except ImportError:
            # deep-translatorがインストールされていない場合は元の値を返す
            return industry
        
        except Exception:
            # 翻訳エラーが発生した場合は元の値を返す
            return industry
    
    def get_symbol_sector(self, symbol: str) -> Optional[str]:
        """
        セクター情報を取得
        
        Args:
            symbol: 銘柄コード
            
        Returns:
            Optional[str]: セクター（存在しない場合はNone）
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT sector FROM symbols WHERE symbol = ?', (symbol,))
            row = cursor.fetchone()
            return row[0] if row and row[0] else None
    
    def get_symbol_sectors(self, symbols: List[str]) -> Dict[str, str]:
        """
        複数のセクター情報を一括取得
        
        Args:
            symbols: 銘柄コードのリスト
            
        Returns:
            Dict[str, str]: 銘柄コードをキー、セクターを値とする辞書
        """
        if not symbols:
            return {}
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            placeholders = ','.join(['?'] * len(symbols))
            cursor.execute(
                f'SELECT symbol, sector FROM symbols WHERE symbol IN ({placeholders})',
                symbols
            )
            rows = cursor.fetchall()
            return {row[0]: row[1] for row in rows if row[1]}
    
    def get_symbol_industry(self, symbol: str) -> Optional[str]:
        """
        業種情報を取得
        
        Args:
            symbol: 銘柄コード
            
        Returns:
            Optional[str]: 業種（存在しない場合はNone）
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT industry FROM symbols WHERE symbol = ?', (symbol,))
            row = cursor.fetchone()
            return row[0] if row and row[0] else None
    
    def get_symbol_industries(self, symbols: List[str]) -> Dict[str, str]:
        """
        複数の業種情報を一括取得
        
        Args:
            symbols: 銘柄コードのリスト
            
        Returns:
            Dict[str, str]: 銘柄コードをキー、業種を値とする辞書
        """
        if not symbols:
            return {}
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            placeholders = ','.join(['?'] * len(symbols))
            cursor.execute(
                f'SELECT symbol, industry FROM symbols WHERE symbol IN ({placeholders})',
                symbols
            )
            rows = cursor.fetchall()
            return {row[0]: row[1] for row in rows if row[1]}
    
    def convert_sectors_to_japanese(self, symbols: Optional[List[str]] = None) -> Dict[str, int]:
        """
        既存のセクター情報を英語から日本語に変換
        
        Args:
            symbols: 変換対象の銘柄コードリスト（Noneの場合は全銘柄）
            
        Returns:
            Dict[str, int]: 変換結果
                - converted_count: 変換した銘柄数
                - skipped_count: スキップした銘柄数（既に日本語またはセクター情報なし）
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            if symbols:
                placeholders = ','.join(['?'] * len(symbols))
                cursor.execute(
                    f'SELECT symbol, sector, name FROM symbols WHERE symbol IN ({placeholders})',
                    symbols
                )
            else:
                cursor.execute('SELECT symbol, sector, name FROM symbols WHERE sector IS NOT NULL')
            
            rows = cursor.fetchall()
            
            converted_count = 0
            skipped_count = 0
            
            for symbol, sector, name in rows:
                if not sector:
                    skipped_count += 1
                    continue
                
                # 既に日本語かどうかチェック（辞書に含まれていない場合は既に日本語の可能性）
                if sector in SECTOR_NAME_JP.values():
                    # 既に日本語
                    skipped_count += 1
                    continue
                
                # 英語から日本語に変換
                sector_jp = self._translate_sector_to_japanese(sector)
                
                if sector_jp != sector:
                    # 変換された場合のみ更新
                    self.save_symbol_name(symbol, name, sector_jp)
                    converted_count += 1
                    print(f"[セクター変換] {symbol}: {sector} → {sector_jp}")
                else:
                    # 変換できない（辞書にない）場合はスキップ
                    skipped_count += 1
            
            return {
                'converted_count': converted_count,
                'skipped_count': skipped_count
            }
    
    def fetch_and_save_symbol_names(
        self,
        symbols: List[str],
        progress_callback: Optional[Callable] = None,
        max_retries: int = 3,
        retry_delay: float = 1.0
    ) -> Dict[str, Dict]:
        """
        複数の銘柄名をYahoo Financeから取得してDBに保存
        
        Args:
            symbols: 銘柄コードのリスト
            progress_callback: 進捗コールバック関数（symbol, success, name, error, current, total）を受け取る
            max_retries: 最大リトライ回数
            retry_delay: リトライ時の基本待機時間（秒）
            
        Returns:
            Dict[str, Dict]: 結果の辞書
                - success_count: 成功数
                - error_count: エラー数
                - skipped_count: スキップ数（既に名前がある）
                - results: 各銘柄の結果
        """
        import yfinance as yf
        import time
        import random
        
        results = {
            'success_count': 0,
            'error_count': 0,
            'skipped_count': 0,
            'results': {}
        }
        
        for i, symbol in enumerate(symbols, 1):
            try:
                # 既に銘柄名、セクター情報、業種情報が保存されているか確認
                existing_name = self.get_symbol_name(symbol)
                existing_sector = self.get_symbol_sector(symbol)
                existing_industry = self.get_symbol_industry(symbol)
                
                # 銘柄名があり、かつセクター情報と業種情報もある場合はスキップ
                if existing_name and existing_name.strip() and existing_sector and existing_industry:
                    results['skipped_count'] += 1
                    results['results'][symbol] = {
                        'success': True,
                        'name': existing_name,
                        'sector': existing_sector,
                        'industry': existing_industry,
                        'skipped': True
                    }
                    if progress_callback:
                        progress_callback(symbol, True, existing_name, None, i, len(symbols))
                    continue
                
                # 銘柄名はあるがセクター情報または業種情報がない場合
                if existing_name and existing_name.strip():
                    if not existing_sector:
                        print(f"[銘柄名取得] {symbol}: 銘柄名あり、セクター情報を取得します")
                    elif not existing_industry:
                        print(f"[銘柄名取得] {symbol}: 銘柄名あり、業種情報を取得します")
                    else:
                        print(f"[銘柄名取得] {symbol}: 銘柄名あり、セクター・業種情報を更新します")
                
                # Yahoo Financeから銘柄名を取得（リトライロジック付き）
                ticker_symbol = f"{symbol}.T"
                last_error = None
                success = False
                
                for attempt in range(max_retries):
                    try:
                        ticker = yf.Ticker(ticker_symbol)
                        info = ticker.info
                        
                        if not info or len(info) <= 1:
                            error_msg = '銘柄情報が見つかりません（上場廃止の可能性）'
                            last_error = error_msg
                            # 上場廃止の場合はリトライしない
                            break
                        
                        # 銘柄名を取得（既存の銘柄名がある場合はそれを使用）
                        if existing_name and existing_name.strip():
                            company_name = existing_name
                        else:
                            # 銘柄名を取得（和名を優先）
                            company_name = self._extract_japanese_name(info, symbol)
                        
                        # セクター情報を取得（英語）
                        sector_en = info.get('sector') or None
                        # 日本語に変換
                        sector = self._translate_sector_to_japanese(sector_en)
                        
                        # 業種情報を取得（英語）
                        industry_en = info.get('industry') or None
                        # 日本語に変換
                        industry = self._translate_industry_to_japanese(industry_en)
                        
                        if company_name:
                            # DBに保存（セクター情報と業種情報も含む）
                            # 既存の情報を保持する設定で保存（既存のセクター/業種がある場合は保持）
                            self.save_symbol_name(
                                symbol, 
                                company_name, 
                                sector, 
                                industry,
                                preserve_existing_sector=True,
                                preserve_existing_industry=True
                            )
                            results['success_count'] += 1
                            result_info = {
                                'success': True,
                                'name': company_name
                            }
                            if sector:
                                result_info['sector'] = sector
                                print(f"[銘柄名取得] {symbol}: セクター情報を更新 - {sector}")
                            if industry:
                                result_info['industry'] = industry
                                print(f"[銘柄名取得] {symbol}: 業種情報を更新 - {industry}")
                            results['results'][symbol] = result_info
                            if progress_callback:
                                progress_callback(symbol, True, company_name, None, i, len(symbols))
                            success = True
                            break
                        else:
                            error_msg = '銘柄名が取得できませんでした'
                            last_error = error_msg
                            # 銘柄名が取得できない場合はリトライしない
                            break
                    
                    except Exception as e:
                        last_error = e
                        error_str = str(e)
                        
                        # 接続エラー（WinError 10061など）の場合はリトライ
                        if '10061' in error_str or 'Connection refused' in error_str or 'urlopen error' in error_str:
                            if attempt < max_retries - 1:
                                # 指数バックオフで待機（1秒、2秒、4秒...）
                                wait_time = retry_delay * (2 ** attempt) + random.uniform(0, 0.5)
                                print(f"[銘柄名取得] {symbol}: 接続エラー、{wait_time:.1f}秒後にリトライ ({attempt + 1}/{max_retries})")
                                time.sleep(wait_time)
                                continue
                            else:
                                error_msg = f'接続エラー（リトライ{max_retries}回失敗）: {error_str}'
                                last_error = error_msg
                                break
                        else:
                            # その他のエラーは即座に返す
                            error_msg = error_str
                            last_error = error_msg
                            break
                
                # エラーが発生した場合
                if not success:
                    error_msg = str(last_error) if last_error else '不明なエラー'
                    print(f"[銘柄名取得] {symbol}: エラー - {error_msg}")
                    results['error_count'] += 1
                    results['results'][symbol] = {
                        'success': False,
                        'error': error_msg
                    }
                    if progress_callback:
                        progress_callback(symbol, False, None, error_msg, i, len(symbols))
                
                # レート制限対策：ランダムな遅延
                if i < len(symbols):
                    time.sleep(random.uniform(0.5, 1.0))  # 遅延を少し長くする
            
            except Exception as e:
                error_msg = f"予期しないエラー: {str(e)}"
                print(f"[銘柄名取得] {symbol}: 予期しないエラー - {error_msg}")
                results['error_count'] += 1
                results['results'][symbol] = {
                    'success': False,
                    'error': error_msg
                }
                if progress_callback:
                    progress_callback(symbol, False, None, error_msg, i, len(symbols))
        
        return results
    
    def fetch_and_save_industries(
        self,
        symbols: List[str],
        progress_callback: Optional[Callable] = None,
        max_retries: int = 3,
        retry_delay: float = 1.0
    ) -> Dict[str, Dict]:
        """
        複数の銘柄の業種情報をYahoo Financeから取得してDBに保存
        
        Args:
            symbols: 銘柄コードのリスト
            progress_callback: 進捗コールバック関数（symbol, success, industry, error, current, total）
            max_retries: 最大リトライ回数
            retry_delay: リトライ間隔（秒）
        
        Returns:
            Dict: 結果サマリー
        """
        import yfinance as yf
        import random
        import time
        
        results = {
            'success_count': 0,
            'error_count': 0,
            'skipped_count': 0,
            'results': {}
        }
        
        for i, symbol in enumerate(symbols):
            # 既存の銘柄名を取得（業種情報だけを取得するため）
            existing_name = self.get_symbol_name(symbol)
            if not existing_name:
                results['skipped_count'] += 1
                results['results'][symbol] = {
                    'success': False,
                    'error': '銘柄名が登録されていません。先に銘柄名を取得してください。'
                }
                continue
            
            # 既に業種情報がある場合はスキップ
            existing_industry = self.get_symbol_industry(symbol)
            if existing_industry:
                results['skipped_count'] += 1
                results['results'][symbol] = {
                    'success': True,
                    'industry': existing_industry,
                    'skipped': True
                }
                continue
            
            ticker_symbol = f"{symbol}.T"
            last_error = None
            success = False
            
            for attempt in range(max_retries):
                try:
                    ticker = yf.Ticker(ticker_symbol)
                    info = ticker.info
                    
                    if not info or len(info) <= 1:
                        error_msg = '銘柄情報が見つかりません（上場廃止の可能性）'
                        last_error = error_msg
                        break
                    
                    # 業種情報を取得（英語）
                    industry_en = info.get('industry') or None
                    # 日本語に変換
                    industry = self._translate_industry_to_japanese(industry_en)
                    
                    if industry:
                        # DBに保存（既存の銘柄名とセクターを使用）
                        existing_sector = self.get_symbol_sector(symbol)
                        self.save_symbol_name(symbol, existing_name, existing_sector, industry)
                        results['success_count'] += 1
                        results['results'][symbol] = {
                            'success': True,
                            'industry': industry
                        }
                        print(f"[業種取得] {symbol}: {industry}")
                        if progress_callback:
                            progress_callback(symbol, True, industry, None, i, len(symbols))
                        success = True
                        break
                    else:
                        error_msg = '業種情報が取得できませんでした'
                        last_error = error_msg
                        break
                
                except Exception as e:
                    last_error = e
                    error_str = str(e)
                    
                    # 接続エラー（WinError 10061など）の場合はリトライ
                    if '10061' in error_str or 'Connection refused' in error_str or 'urlopen error' in error_str:
                        if attempt < max_retries - 1:
                            # 指数バックオフで待機（1秒、2秒、4秒...）
                            wait_time = retry_delay * (2 ** attempt) + random.uniform(0, 0.5)
                            print(f"[業種取得] {symbol}: 接続エラー、{wait_time:.1f}秒後にリトライ ({attempt + 1}/{max_retries})")
                            time.sleep(wait_time)
                            continue
                    
                    # その他のエラーはリトライしない
                    break
            
            if not success:
                results['error_count'] += 1
                error_msg = str(last_error) if last_error else '不明なエラー'
                results['results'][symbol] = {
                    'success': False,
                    'error': error_msg
                }
                print(f"[業種取得] {symbol}: エラー - {error_msg}")
                if progress_callback:
                    progress_callback(symbol, False, None, error_msg, i, len(symbols))
            
            # レート制限対策（0.1秒待機）
            if i < len(symbols) - 1:
                time.sleep(0.1)
        
        return results
    
    def fetch_and_save_sectors(
        self,
        symbols: List[str],
        progress_callback: Optional[Callable] = None,
        max_retries: int = 3,
        retry_delay: float = 1.0
    ) -> Dict[str, Dict]:
        """
        複数の銘柄のセクター情報をYahoo Financeから取得してDBに保存
        （既に銘柄名が保存されている銘柄のセクター情報を補完する用途）
        
        Args:
            symbols: 銘柄コードのリスト
            progress_callback: 進捗コールバック関数（symbol, success, sector, error, current, total）を受け取る
            max_retries: 最大リトライ回数
            retry_delay: リトライ時の基本待機時間（秒）
            
        Returns:
            Dict[str, Dict]: 結果の辞書
                - success_count: 成功数
                - error_count: エラー数
                - skipped_count: スキップ数（既にセクター情報あり）
                - results: 各銘柄の結果
        """
        import yfinance as yf
        import time
        import random
        
        results = {
            'success_count': 0,
            'error_count': 0,
            'skipped_count': 0,
            'results': {}
        }
        
        for i, symbol in enumerate(symbols, 1):
            try:
                # 既にセクター情報が保存されているか確認
                existing_sector = self.get_symbol_sector(symbol)
                if existing_sector:
                    results['skipped_count'] += 1
                    results['results'][symbol] = {
                        'success': True,
                        'sector': existing_sector,
                        'skipped': True
                    }
                    if progress_callback:
                        progress_callback(symbol, True, existing_sector, None, i, len(symbols))
                    continue
                
                # 銘柄名を取得（セクター更新時に必要）
                existing_name = self.get_symbol_name(symbol)
                if not existing_name or not existing_name.strip():
                    error_msg = '銘柄名が未登録です。先に銘柄名を取得してください。'
                    print(f"[セクター取得] {symbol}: {error_msg}")
                    results['error_count'] += 1
                    results['results'][symbol] = {
                        'success': False,
                        'error': error_msg
                    }
                    if progress_callback:
                        progress_callback(symbol, False, None, error_msg, i, len(symbols))
                    continue
                
                # Yahoo Financeからセクター情報を取得（リトライロジック付き）
                ticker_symbol = f"{symbol}.T"
                last_error = None
                success = False
                
                for attempt in range(max_retries):
                    try:
                        ticker = yf.Ticker(ticker_symbol)
                        info = ticker.info
                        
                        if not info or len(info) <= 1:
                            error_msg = '銘柄情報が見つかりません（上場廃止の可能性）'
                            last_error = error_msg
                            break
                        
                        # セクター情報を取得（英語）
                        sector_en = info.get('sector') or info.get('industry') or None
                        # 日本語に変換
                        sector = self._translate_sector_to_japanese(sector_en)
                        
                        if sector:
                            # DBに保存（既存の銘柄名を使用）
                            self.save_symbol_name(symbol, existing_name, sector)
                            results['success_count'] += 1
                            results['results'][symbol] = {
                                'success': True,
                                'sector': sector
                            }
                            print(f"[セクター取得] {symbol}: {sector}")
                            if progress_callback:
                                progress_callback(symbol, True, sector, None, i, len(symbols))
                            success = True
                            break
                        else:
                            error_msg = 'セクター情報が取得できませんでした'
                            last_error = error_msg
                            break
                    
                    except Exception as e:
                        last_error = e
                        error_str = str(e)
                        
                        # 接続エラーの場合はリトライ
                        if '10061' in error_str or 'Connection refused' in error_str or 'urlopen error' in error_str:
                            if attempt < max_retries - 1:
                                wait_time = retry_delay * (2 ** attempt) + random.uniform(0, 0.5)
                                print(f"[セクター取得] {symbol}: 接続エラー、{wait_time:.1f}秒後にリトライ ({attempt + 1}/{max_retries})")
                                time.sleep(wait_time)
                                continue
                            else:
                                error_msg = f'接続エラー（リトライ{max_retries}回失敗）: {error_str}'
                                last_error = error_msg
                                break
                        else:
                            error_msg = error_str
                            last_error = error_msg
                            break
                
                # エラーが発生した場合
                if not success:
                    error_msg = str(last_error) if last_error else '不明なエラー'
                    print(f"[セクター取得] {symbol}: エラー - {error_msg}")
                    results['error_count'] += 1
                    results['results'][symbol] = {
                        'success': False,
                        'error': error_msg
                    }
                    if progress_callback:
                        progress_callback(symbol, False, None, error_msg, i, len(symbols))
                
                # レート制限対策：ランダムな遅延
                if i < len(symbols):
                    time.sleep(random.uniform(0.5, 1.0))
            
            except Exception as e:
                error_msg = f"予期しないエラー: {str(e)}"
                print(f"[セクター取得] {symbol}: 予期しないエラー - {error_msg}")
                results['error_count'] += 1
                results['results'][symbol] = {
                    'success': False,
                    'error': error_msg
                }
                if progress_callback:
                    progress_callback(symbol, False, None, error_msg, i, len(symbols))
        
        return results
    
    def _extract_japanese_name(self, info: dict, symbol: str) -> Optional[str]:
        """
        Yahoo Financeから取得した英語名を日本語に翻訳して取得
        
        Args:
            info: ticker.infoの結果
            symbol: 銘柄コード
            
        Returns:
            Optional[str]: 日本語名（翻訳失敗時は英語名、見つからない場合はNone）
        """
        if not info:
            return None
        
        # ticker.infoから英語名を取得
        long_name = info.get('longName', '').strip()
        short_name = info.get('shortName', '').strip()
        
        # 優先順位: longName > shortName
        english_name = long_name if long_name else short_name
        
        if not english_name:
            return None
        
        # 既に日本語が含まれている場合はそのまま返す
        japanese_pattern = re.compile(r'[\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FAF]')
        if japanese_pattern.search(english_name):
            return english_name
        
        # 英語名を日本語に翻訳
        try:
            from deep_translator import GoogleTranslator
            
            translator = GoogleTranslator(source='en', target='ja')
            japanese_name = translator.translate(english_name)
            
            if japanese_name and japanese_name.strip():
                print(f"[銘柄名取得] {symbol}: {english_name} → {japanese_name}")
                return japanese_name.strip()
            else:
                # 翻訳に失敗した場合は英語名を返す
                print(f"[銘柄名取得] {symbol}: 翻訳失敗、英語名を使用 - {english_name}")
                return english_name
        
        except ImportError:
            # deep-translatorがインストールされていない場合
            print(f"[銘柄名取得] {symbol}: deep-translatorがインストールされていません。英語名を使用 - {english_name}")
            print("  日本語名を取得するには: pip install deep-translator")
            return english_name
        
        except Exception as e:
            # 翻訳エラーが発生した場合は英語名を返す
            error_msg = str(e)[:100]  # エラーメッセージを短縮
            print(f"[銘柄名取得] {symbol}: 翻訳エラー、英語名を使用 - {english_name} (エラー: {error_msg})")
            return english_name
    

