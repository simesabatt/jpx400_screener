"""
OHLCVデータ管理の共通モジュール

OHLCVデータの保存・取得・テーブル管理を統一して提供します。

Copyright (c) 2025 JPX400スクリーニングシステム

This software is licensed under the MIT License.
See LICENSE file for details.
"""

import sqlite3
import pandas as pd
from datetime import datetime
from typing import Optional, List, Dict, Callable
import os


class OHLCVDataManager:
    """OHLCVデータ管理クラス（共通機能）"""
    
    def __init__(self, db_path: str):
        """
        初期化
        
        Args:
            db_path: SQLiteデータベースのパス
        """
        self.db_path = db_path
        self._ensure_table()
        # 銘柄名管理は別モジュールに分離（後方互換性のため）
        from src.data_collector.symbol_name_manager import SymbolNameManager
        self._symbol_name_manager = SymbolNameManager(db_path)
    
    def _ensure_table(self):
        """ohlcv_dataテーブルが存在することを確認（なければ作成）"""
        # データベースディレクトリの存在確認
        db_dir = os.path.dirname(self.db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS ohlcv_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    datetime TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    open REAL NOT NULL,
                    high REAL NOT NULL,
                    low REAL NOT NULL,
                    close REAL NOT NULL,
                    volume INTEGER NOT NULL,
                    source TEXT DEFAULT 'kabu',
                    created_at TEXT NOT NULL,
                    UNIQUE(symbol, datetime, timeframe, source)
                )
            ''')
            
            # マイグレーション: is_temporary_close列を追加（既存テーブル用）
            try:
                cursor.execute('ALTER TABLE ohlcv_data ADD COLUMN is_temporary_close INTEGER DEFAULT 0')
                print("[OHLCVDataManager] is_temporary_close列を追加しました")
            except sqlite3.OperationalError:
                # 既に存在する場合はスキップ
                pass
            
            # マイグレーション: updated_at列を追加（既存テーブル用）
            try:
                cursor.execute('ALTER TABLE ohlcv_data ADD COLUMN updated_at TEXT')
                print("[OHLCVDataManager] updated_at列を追加しました")
            except sqlite3.OperationalError:
                # 既に存在する場合はスキップ
                pass
            
            # インデックスを作成（高速検索用）
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_ohlcv_symbol_datetime 
                ON ohlcv_data(symbol, datetime)
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_ohlcv_timeframe 
                ON ohlcv_data(timeframe)
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_ohlcv_source 
                ON ohlcv_data(source)
            ''')
            
            conn.commit()
    
    def save_ohlcv_data(
        self,
        symbol: str,
        df: pd.DataFrame,
        timeframe: str,
        source: str = "kabu",
        overwrite: bool = False
    ) -> int:
        """
        OHLCVデータをデータベースに保存
        
        Args:
            symbol: 銘柄コード
            df: OHLCVデータ（インデックスがdatetime、列: open, high, low, close, volume）
            timeframe: 時間足（"1s", "1m", "5m"など）
            source: データソース（"kabu", "yahoo"など）
            overwrite: 既存データを上書きするか
        
        Returns:
            int: 保存した件数
        """
        if df.empty:
            return 0
        
        saved_count = 0
        skipped_count = 0
        updated_count = 0
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            for dt, row in df.iterrows():
                try:
                    # NaN値を安全に処理
                    def safe_float(val):
                        try:
                            if val is None or (isinstance(val, float) and pd.isna(val)):
                                return 0.0
                            return float(val)
                        except (ValueError, TypeError):
                            return 0.0
                    
                    def safe_int(val):
                        try:
                            # Noneチェック
                            if val is None:
                                return 0
                            
                            # 文字列の'nan'チェック
                            if isinstance(val, str) and val.lower() == 'nan':
                                return 0
                            
                            # float型のNaNチェック（math.isnanを使用）
                            try:
                                import math
                                if isinstance(val, float) and math.isnan(val):
                                    return 0
                            except (ImportError, TypeError):
                                pass
                            
                            # NaNチェック（numpy.nanやpandas.NAなども含む）
                            # pd.isna()はnumpy.nan、pandas.NA、Noneなどを検出
                            try:
                                if pd.isna(val):
                                    return 0
                            except (TypeError, ValueError):
                                pass
                            
                            # 型変換（最後の手段としてtry-exceptで囲む）
                            try:
                                return int(float(val))
                            except (ValueError, TypeError, OverflowError) as e:
                                # デバッグ用: エラーが発生した場合の情報を出力
                                print(f"[DEBUG] safe_int変換エラー: val={val}, type={type(val)}, error={e}")
                                return 0
                        except Exception as e:
                            # 予期しないエラーもキャッチ
                            print(f"[DEBUG] safe_int予期しないエラー: val={val}, type={type(val)}, error={e}")
                            return 0
                    
                    # rowから直接取得（NaNの可能性がある）
                    # pandas Seriesの場合、'key' in row は動作するが、より安全に取得
                    try:
                        # まず値を取得してからsafe_int/safe_floatに渡す
                        volume_raw = row['volume'] if 'volume' in row.index else 0
                        # デバッグ: NaNの場合にログを出力
                        if pd.isna(volume_raw):
                            print(f"[DEBUG] volumeがNaN: symbol={symbol}, datetime={dt}, volume_raw={volume_raw}, type={type(volume_raw)}")
                        volume_val = safe_int(volume_raw)
                        
                        open_val = safe_float(row['open'] if 'open' in row.index else 0.0)
                        high_val = safe_float(row['high'] if 'high' in row.index else 0.0)
                        low_val = safe_float(row['low'] if 'low' in row.index else 0.0)
                        close_val = safe_float(row['close'] if 'close' in row.index else 0.0)
                    except (KeyError, AttributeError):
                        # フォールバック: get()メソッドを使用
                        volume_raw = row.get('volume', 0)
                        # デバッグ: NaNの場合にログを出力
                        if pd.isna(volume_raw):
                            print(f"[DEBUG] volumeがNaN (get): symbol={symbol}, datetime={dt}, volume_raw={volume_raw}, type={type(volume_raw)}")
                        volume_val = safe_int(volume_raw)
                        
                        open_val = safe_float(row.get('open', 0.0))
                        high_val = safe_float(row.get('high', 0.0))
                        low_val = safe_float(row.get('low', 0.0))
                        close_val = safe_float(row.get('close', 0.0))
                    
                    # 既存データをチェック
                    cursor.execute('''
                        SELECT COUNT(*) FROM ohlcv_data
                        WHERE symbol = ? AND datetime = ? AND timeframe = ? AND source = ?
                    ''', (
                        symbol,
                        dt.isoformat(),
                        timeframe,
                        source
                    ))
                    
                    exists = cursor.fetchone()[0] > 0
                    
                    if exists and not overwrite:
                        skipped_count += 1
                        continue
                    
                    if exists and overwrite:
                        # 既存データを更新
                        cursor.execute('''
                            UPDATE ohlcv_data
                            SET open = ?, high = ?, low = ?, close = ?, volume = ?, created_at = ?
                            WHERE symbol = ? AND datetime = ? AND timeframe = ? AND source = ?
                        ''', (
                            open_val,
                            high_val,
                            low_val,
                            close_val,
                            volume_val,
                            datetime.now().isoformat(),
                            symbol,
                            dt.isoformat(),
                            timeframe,
                            source
                        ))
                        updated_count += 1
                    else:
                        # 新規データを挿入
                        cursor.execute('''
                            INSERT INTO ohlcv_data
                            (symbol, datetime, timeframe, open, high, low, close, volume, source, created_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            symbol,
                            dt.isoformat(),
                            timeframe,
                            open_val,
                            high_val,
                            low_val,
                            close_val,
                            volume_val,
                            source,
                            datetime.now().isoformat()
                        ))
                        saved_count += 1
                
                except Exception as e:
                    # エラーの詳細を表示（デバッグ用）
                    error_msg = str(e)
                    symbol_info = f"symbol={symbol}, datetime={dt}"
                    print(f"[OHLCVDataManager] データ保存エラー: {error_msg} ({symbol_info})")
                    skipped_count += 1
            
            conn.commit()
        
        return saved_count + updated_count
    
    def save_ohlcv_data_with_stats(
        self,
        symbol: str,
        df: pd.DataFrame,
        timeframe: str,
        source: str = "kabu",
        overwrite: bool = False
    ) -> dict:
        """
        OHLCVデータをデータベースに保存（統計情報付き）
        
        Args:
            symbol: 銘柄コード
            df: OHLCVデータ（インデックスがdatetime、列: open, high, low, close, volume）
            timeframe: 時間足（"1s", "1m", "5m"など）
            source: データソース（"kabu", "yahoo"など）
            overwrite: 既存データを上書きするか
        
        Returns:
            dict: 保存結果（saved_count, skipped_count, updated_count, total_count）
        """
        if df.empty:
            return {'saved_count': 0, 'skipped_count': 0, 'updated_count': 0, 'total_count': 0}
        
        saved_count = 0
        skipped_count = 0
        updated_count = 0
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            for dt, row in df.iterrows():
                try:
                    # NaN値を安全に処理
                    def safe_float(val):
                        try:
                            if val is None or (isinstance(val, float) and pd.isna(val)):
                                return 0.0
                            return float(val)
                        except (ValueError, TypeError):
                            return 0.0
                    
                    def safe_int(val):
                        try:
                            # Noneチェック
                            if val is None:
                                return 0
                            
                            # 文字列の'nan'チェック
                            if isinstance(val, str) and val.lower() == 'nan':
                                return 0
                            
                            # float型のNaNチェック（math.isnanを使用）
                            try:
                                import math
                                if isinstance(val, float) and math.isnan(val):
                                    return 0
                            except (ImportError, TypeError):
                                pass
                            
                            # NaNチェック（numpy.nanやpandas.NAなども含む）
                            # pd.isna()はnumpy.nan、pandas.NA、Noneなどを検出
                            try:
                                if pd.isna(val):
                                    return 0
                            except (TypeError, ValueError):
                                pass
                            
                            # 型変換（最後の手段としてtry-exceptで囲む）
                            try:
                                return int(float(val))
                            except (ValueError, TypeError, OverflowError) as e:
                                # デバッグ用: エラーが発生した場合の情報を出力
                                print(f"[DEBUG] safe_int変換エラー: val={val}, type={type(val)}, error={e}")
                                return 0
                        except Exception as e:
                            # 予期しないエラーもキャッチ
                            print(f"[DEBUG] safe_int予期しないエラー: val={val}, type={type(val)}, error={e}")
                            return 0
                    
                    # rowから直接取得（NaNの可能性がある）
                    # pandas Seriesの場合、'key' in row は動作するが、より安全に取得
                    try:
                        # まず値を取得してからsafe_int/safe_floatに渡す
                        volume_raw = row['volume'] if 'volume' in row.index else 0
                        # デバッグ: NaNの場合にログを出力
                        if pd.isna(volume_raw):
                            print(f"[DEBUG] volumeがNaN: symbol={symbol}, datetime={dt}, volume_raw={volume_raw}, type={type(volume_raw)}")
                        volume_val = safe_int(volume_raw)
                        
                        open_val = safe_float(row['open'] if 'open' in row.index else 0.0)
                        high_val = safe_float(row['high'] if 'high' in row.index else 0.0)
                        low_val = safe_float(row['low'] if 'low' in row.index else 0.0)
                        close_val = safe_float(row['close'] if 'close' in row.index else 0.0)
                    except (KeyError, AttributeError):
                        # フォールバック: get()メソッドを使用
                        volume_raw = row.get('volume', 0)
                        # デバッグ: NaNの場合にログを出力
                        if pd.isna(volume_raw):
                            print(f"[DEBUG] volumeがNaN (get): symbol={symbol}, datetime={dt}, volume_raw={volume_raw}, type={type(volume_raw)}")
                        volume_val = safe_int(volume_raw)
                        
                        open_val = safe_float(row.get('open', 0.0))
                        high_val = safe_float(row.get('high', 0.0))
                        low_val = safe_float(row.get('low', 0.0))
                        close_val = safe_float(row.get('close', 0.0))
                    
                    # 既存データをチェック
                    cursor.execute('''
                        SELECT COUNT(*) FROM ohlcv_data
                        WHERE symbol = ? AND datetime = ? AND timeframe = ? AND source = ?
                    ''', (
                        symbol,
                        dt.isoformat(),
                        timeframe,
                        source
                    ))
                    
                    exists = cursor.fetchone()[0] > 0
                    
                    if exists and not overwrite:
                        skipped_count += 1
                        continue
                    
                    if exists and overwrite:
                        # 既存データを更新
                        cursor.execute('''
                            UPDATE ohlcv_data
                            SET open = ?, high = ?, low = ?, close = ?, volume = ?, created_at = ?
                            WHERE symbol = ? AND datetime = ? AND timeframe = ? AND source = ?
                        ''', (
                            open_val,
                            high_val,
                            low_val,
                            close_val,
                            volume_val,
                            datetime.now().isoformat(),
                            symbol,
                            dt.isoformat(),
                            timeframe,
                            source
                        ))
                        updated_count += 1
                    else:
                        # 新規データを挿入
                        cursor.execute('''
                            INSERT INTO ohlcv_data
                            (symbol, datetime, timeframe, open, high, low, close, volume, source, created_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            symbol,
                            dt.isoformat(),
                            timeframe,
                            open_val,
                            high_val,
                            low_val,
                            close_val,
                            volume_val,
                            source,
                            datetime.now().isoformat()
                        ))
                        saved_count += 1
                
                except Exception as e:
                    # エラーの詳細を表示（デバッグ用）
                    error_msg = str(e)
                    symbol_info = f"symbol={symbol}, datetime={dt}"
                    print(f"[OHLCVDataManager] データ保存エラー: {error_msg} ({symbol_info})")
                    skipped_count += 1
            
            conn.commit()
        
        return {
            'saved_count': saved_count,
            'skipped_count': skipped_count,
            'updated_count': updated_count,
            'total_count': len(df)
        }
    
    def get_ohlcv_data(
        self,
        symbol: str,
        timeframe: str = "1s",
        start_datetime: Optional[datetime] = None,
        end_datetime: Optional[datetime] = None,
        source: Optional[str] = None
    ) -> pd.DataFrame:
        """
        データベースからOHLCVデータを取得
        
        Args:
            symbol: 銘柄コード
            timeframe: 時間足（"1s", "1m", "5m"など）
            start_datetime: 開始日時（Noneの場合は全期間）
            end_datetime: 終了日時（Noneの場合は全期間）
            source: データソース（Noneの場合は全ソース）
        
        Returns:
            pd.DataFrame: OHLCVデータ（インデックスがdatetime）
        """
        with sqlite3.connect(self.db_path) as conn:
            query = '''
                SELECT datetime, open, high, low, close, volume
                FROM ohlcv_data
                WHERE symbol = ? AND timeframe = ?
            '''
            params = [symbol, timeframe]
            
            if source:
                query += ' AND source = ?'
                params.append(source)
            
            if start_datetime:
                query += ' AND datetime >= ?'
                params.append(start_datetime.isoformat())
            
            if end_datetime:
                query += ' AND datetime <= ?'
                params.append(end_datetime.isoformat())
            
            query += ' ORDER BY datetime'
            
            df = pd.read_sql_query(query, conn, params=params)
            
            if df.empty:
                return pd.DataFrame()
            
            # datetimeをインデックスに設定
            df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce', utc=True)
            df = df.dropna(subset=['datetime'])
            
            if df.empty:
                return pd.DataFrame()
            
            # タイムゾーン情報を削除
            if df['datetime'].dt.tz is not None:
                df['datetime'] = df['datetime'].dt.tz_localize(None)
            
            df = df.set_index('datetime').sort_index()
            
            # 重複インデックスを削除
            df = df[~df.index.duplicated(keep='first')]
            
            # 欠損値を削除
            df = df.dropna(subset=['open', 'high', 'low', 'close', 'volume'])
            
            return df
    
    def save_ohlcv_data_with_temporary_flag(
        self,
        symbol: str,
        df: pd.DataFrame,
        timeframe: str,
        source: str = "yahoo",
        overwrite: bool = False
    ) -> dict:
        """
        OHLCVデータを保存（仮終値フラグを考慮）
        
        Args:
            symbol: 銘柄コード
            df: OHLCVデータ（is_temporary_close列を含む可能性がある）
            timeframe: 時間足
            source: データソース
            overwrite: 既存データを上書きするか
            
        Returns:
            dict: 保存結果（saved_count, skipped_count, updated_count, total_count）
        """
        if df.empty:
            return {'saved_count': 0, 'skipped_count': 0, 'updated_count': 0, 'total_count': 0}
        
        saved_count = 0
        skipped_count = 0
        updated_count = 0
        
        # DBから最新日を取得（過去データの更新を避けるため）
        stats = self.get_data_stats(symbol, timeframe, source)
        latest_date_str = stats.get('end_date')
        latest_date = None
        if latest_date_str:
            try:
                # 文字列をdatetimeに変換
                if isinstance(latest_date_str, str):
                    latest_date = pd.to_datetime(latest_date_str)
                else:
                    latest_date = pd.to_datetime(latest_date_str)
            except (ValueError, TypeError):
                latest_date = None
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            for dt, row in df.iterrows():
                try:
                    # NaN値を安全に処理
                    def safe_float(val):
                        try:
                            if val is None or (isinstance(val, float) and pd.isna(val)):
                                return 0.0
                            return float(val)
                        except (ValueError, TypeError):
                            return 0.0
                    
                    def safe_int(val):
                        try:
                            # Noneチェック
                            if val is None:
                                return 0
                            
                            # 文字列の'nan'チェック
                            if isinstance(val, str) and val.lower() == 'nan':
                                return 0
                            
                            # float型のNaNチェック（math.isnanを使用）
                            try:
                                import math
                                if isinstance(val, float) and math.isnan(val):
                                    return 0
                            except (ImportError, TypeError):
                                pass
                            
                            # NaNチェック（numpy.nanやpandas.NAなども含む）
                            # pd.isna()はnumpy.nan、pandas.NA、Noneなどを検出
                            try:
                                if pd.isna(val):
                                    return 0
                            except (TypeError, ValueError):
                                pass
                            
                            # 型変換（最後の手段としてtry-exceptで囲む）
                            try:
                                return int(float(val))
                            except (ValueError, TypeError, OverflowError) as e:
                                # デバッグ用: エラーが発生した場合の情報を出力
                                print(f"[DEBUG] safe_int変換エラー: val={val}, type={type(val)}, error={e}")
                                return 0
                        except Exception as e:
                            # 予期しないエラーもキャッチ
                            print(f"[DEBUG] safe_int予期しないエラー: val={val}, type={type(val)}, error={e}")
                            return 0
                    
                    # rowから直接取得（NaNの可能性がある）
                    # pandas Seriesの場合、'key' in row は動作するが、より安全に取得
                    try:
                        # まず値を取得してからsafe_int/safe_floatに渡す
                        volume_raw = row['volume'] if 'volume' in row.index else 0
                        # デバッグ: NaNの場合にログを出力
                        if pd.isna(volume_raw):
                            print(f"[DEBUG] volumeがNaN: symbol={symbol}, datetime={dt}, volume_raw={volume_raw}, type={type(volume_raw)}")
                        volume_val = safe_int(volume_raw)
                        
                        open_val = safe_float(row['open'] if 'open' in row.index else 0.0)
                        high_val = safe_float(row['high'] if 'high' in row.index else 0.0)
                        low_val = safe_float(row['low'] if 'low' in row.index else 0.0)
                        close_val = safe_float(row['close'] if 'close' in row.index else 0.0)
                    except (KeyError, AttributeError):
                        # フォールバック: get()メソッドを使用
                        volume_raw = row.get('volume', 0)
                        # デバッグ: NaNの場合にログを出力
                        if pd.isna(volume_raw):
                            print(f"[DEBUG] volumeがNaN (get): symbol={symbol}, datetime={dt}, volume_raw={volume_raw}, type={type(volume_raw)}")
                        volume_val = safe_int(volume_raw)
                        
                        open_val = safe_float(row.get('open', 0.0))
                        high_val = safe_float(row.get('high', 0.0))
                        low_val = safe_float(row.get('low', 0.0))
                        close_val = safe_float(row.get('close', 0.0))
                    
                    # 仮終値フラグを取得（デフォルトは0=正式）
                    is_temporary = safe_int(row.get('is_temporary_close', 0))
                    
                    # 既存データを確認（仮終値フラグも含む）
                    cursor.execute('''
                        SELECT id, is_temporary_close FROM ohlcv_data
                        WHERE symbol = ? AND datetime = ? AND timeframe = ? AND source = ?
                    ''', (
                        symbol,
                        dt.isoformat() if isinstance(dt, pd.Timestamp) else dt,
                        timeframe,
                        source
                    ))
                    
                    existing = cursor.fetchone()
                    
                    if existing:
                        existing_id, existing_is_temporary = existing
                        
                        # 日付をdatetimeに変換（比較用）
                        dt_datetime = pd.to_datetime(dt) if not isinstance(dt, pd.Timestamp) else dt
                        
                        # 最新日より古いデータは更新しない（過去データは確定済み）
                        if latest_date is not None and dt_datetime < latest_date:
                            skipped_count += 1
                            continue
                        
                        # 最新日と同じ日付の場合のみ更新を検討
                        # 最新日より新しいデータは新規保存として扱う（下のelse節で処理）
                        is_latest_date = (latest_date is not None and dt_datetime == latest_date) or latest_date is None
                        
                        # 既存データが正式で、新データが仮の場合、上書きしない
                        if existing_is_temporary == 0 and is_temporary == 1:
                            skipped_count += 1
                            continue
                        
                        # 既存データが仮で、新データが正式の場合、上書き（最新日のみ）
                        if is_latest_date and existing_is_temporary == 1 and is_temporary == 0:
                            cursor.execute('''
                                UPDATE ohlcv_data
                                SET open = ?, high = ?, low = ?, close = ?, volume = ?,
                                    is_temporary_close = 0, updated_at = ?
                                WHERE id = ?
                            ''', (
                                open_val,
                                high_val,
                                low_val,
                                close_val,
                                volume_val,
                                datetime.now().isoformat(),
                                existing_id
                            ))
                            updated_count += 1
                            continue
                        
                        # 既存データが仮で、新データも仮の場合、最新の仮データで更新（最新日のみ）
                        if is_latest_date and existing_is_temporary == 1 and is_temporary == 1:
                            cursor.execute('''
                                UPDATE ohlcv_data
                                SET open = ?, high = ?, low = ?, close = ?, volume = ?,
                                    updated_at = ?
                                WHERE id = ?
                            ''', (
                                open_val,
                                high_val,
                                low_val,
                                close_val,
                                volume_val,
                                datetime.now().isoformat(),
                                existing_id
                            ))
                            updated_count += 1
                            continue
                        
                        # 既存データが正式で、新データも正式の場合、通常の上書き処理（最新日のみ）
                        # ただし、overwriteフラグがTrueの場合のみ
                        if is_latest_date and overwrite:
                            cursor.execute('''
                                UPDATE ohlcv_data
                                SET open = ?, high = ?, low = ?, close = ?, volume = ?,
                                    updated_at = ?
                                WHERE id = ?
                            ''', (
                                open_val,
                                high_val,
                                low_val,
                                close_val,
                                volume_val,
                                datetime.now().isoformat(),
                                existing_id
                            ))
                            updated_count += 1
                        else:
                            skipped_count += 1
                    
                    else:
                        # 新規データを挿入
                        cursor.execute('''
                            INSERT INTO ohlcv_data
                            (symbol, datetime, timeframe, open, high, low, close, volume, source, is_temporary_close, created_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            symbol,
                            dt.isoformat() if isinstance(dt, pd.Timestamp) else dt,
                            timeframe,
                            open_val,
                            high_val,
                            low_val,
                            close_val,
                            volume_val,
                            source,
                            is_temporary,
                            datetime.now().isoformat()
                        ))
                        saved_count += 1
                
                except Exception as e:
                    # エラーの詳細を表示（デバッグ用）
                    error_msg = str(e)
                    symbol_info = f"symbol={symbol}, datetime={dt}"
                    print(f"[OHLCVDataManager] データ保存エラー: {error_msg} ({symbol_info})")
                    skipped_count += 1
            
            conn.commit()
        
        return {
            'saved_count': saved_count,
            'skipped_count': skipped_count,
            'updated_count': updated_count,
            'total_count': len(df)
        }
    
    def get_ohlcv_data_with_temporary_flag(
        self,
        symbol: str,
        timeframe: str = "1d",
        start_datetime: Optional[datetime] = None,
        end_datetime: Optional[datetime] = None,
        source: Optional[str] = None,
        include_temporary: bool = True
    ) -> pd.DataFrame:
        """
        OHLCVデータを取得（仮終値フラグを含む）
        
        Args:
            symbol: 銘柄コード
            timeframe: 時間足
            start_datetime: 開始日時
            end_datetime: 終了日時
            source: データソース
            include_temporary: 仮終値データを含めるか（デフォルト: True）
            
        Returns:
            pd.DataFrame: OHLCVデータ（is_temporary_close列を含む）
        """
        with sqlite3.connect(self.db_path) as conn:
            query = '''
                SELECT datetime, open, high, low, close, volume, is_temporary_close
                FROM ohlcv_data
                WHERE symbol = ? AND timeframe = ?
            '''
            params = [symbol, timeframe]
            
            if source:
                query += ' AND source = ?'
                params.append(source)
            
            if not include_temporary:
                query += ' AND (is_temporary_close = 0 OR is_temporary_close IS NULL)'
            
            if start_datetime:
                query += ' AND datetime >= ?'
                params.append(start_datetime.isoformat())
            
            if end_datetime:
                query += ' AND datetime <= ?'
                params.append(end_datetime.isoformat())
            
            query += ' ORDER BY datetime'
            
            df = pd.read_sql_query(query, conn, params=params)
            
            if df.empty:
                return pd.DataFrame()
            
            # datetimeをインデックスに設定
            df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce', utc=True)
            df = df.dropna(subset=['datetime'])
            
            if df.empty:
                return pd.DataFrame()
            
            # タイムゾーン情報を削除
            if df['datetime'].dt.tz is not None:
                df['datetime'] = df['datetime'].dt.tz_localize(None)
            
            df = df.set_index('datetime').sort_index()
            
            # 重複インデックスを削除
            df = df[~df.index.duplicated(keep='first')]
            
            # 欠損値を削除
            df = df.dropna(subset=['open', 'high', 'low', 'close', 'volume'])
            
            # is_temporary_close列の欠損値を0（正式）で埋める
            if 'is_temporary_close' in df.columns:
                df['is_temporary_close'] = df['is_temporary_close'].fillna(0).astype(int)
            else:
                df['is_temporary_close'] = 0
            
            return df
    
    def update_ohlcv_1s(
        self,
        conn: sqlite3.Connection,
        symbol: str,
        tick_time: datetime,
        price: float,
        volume: int
    ):
        """
        1秒足OHLCVデータを更新（リアルタイム生成用）
        
        Args:
            conn: データベース接続（既に開かれているもの）
            symbol: 銘柄コード
            tick_time: 時刻
            price: 価格
            volume: 出来高
        """
        cursor = conn.cursor()
        
        try:
            # 1秒の開始時刻（秒単位で切り捨て）
            second_start = tick_time.replace(microsecond=0)
            second_start_str = second_start.isoformat()
            
            # 既存のOHLCVデータを取得
            cursor.execute('''
                SELECT open, high, low, close, volume
                FROM ohlcv_data
                WHERE symbol = ? AND datetime = ? AND timeframe = ? AND source = ?
            ''', (symbol, second_start_str, "1s", "kabu"))
            
            existing = cursor.fetchone()
            
            if existing:
                # 既存データを更新
                existing_open, existing_high, existing_low, existing_close, existing_volume = existing
                
                # High/Low/Close/Volumeを更新
                new_high = max(existing_high, price)
                new_low = min(existing_low, price)
                new_close = price  # 最新の価格がClose
                new_volume = existing_volume + volume
                
                cursor.execute('''
                    UPDATE ohlcv_data
                    SET high = ?, low = ?, close = ?, volume = ?, created_at = ?
                    WHERE symbol = ? AND datetime = ? AND timeframe = ? AND source = ?
                ''', (
                    new_high,
                    new_low,
                    new_close,
                    new_volume,
                    datetime.now().isoformat(),
                    symbol,
                    second_start_str,
                    "1s",
                    "kabu"
                ))
            else:
                # 新規データを挿入
                cursor.execute('''
                    INSERT INTO ohlcv_data
                    (symbol, datetime, timeframe, open, high, low, close, volume, source, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    symbol,
                    second_start_str,
                    "1s",
                    price,  # Open
                    price,  # High
                    price,  # Low
                    price,  # Close
                    volume,  # Volume
                    "kabu",
                    datetime.now().isoformat()
                ))
        except Exception as e:
            # OHLCV更新エラーは警告のみ（歩み値データは保存済み）
            print(f"[OHLCVDataManager] OHLCV更新エラー: {e}")
    
    def get_data_stats(
        self,
        symbol: str,
        timeframe: str = "1s",
        source: Optional[str] = None
    ) -> dict:
        """
        データベース内のデータ統計を取得
        
        Args:
            symbol: 銘柄コード
            timeframe: 時間足
            source: データソース（Noneの場合は全ソース）
        
        Returns:
            dict: 統計情報（件数、期間など）
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # サブクエリ用の条件を構築
            subquery_where = 'symbol = ? AND timeframe = ?'
            subquery_params = [symbol, timeframe]
            if source:
                subquery_where += ' AND source = ?'
                subquery_params.append(source)
            
            query = f'''
                SELECT 
                    COUNT(*) as total_count,
                    MIN(datetime) as start_date,
                    MAX(datetime) as end_date,
                    (SELECT updated_at FROM ohlcv_data 
                     WHERE {subquery_where}
                     ORDER BY datetime DESC, updated_at DESC LIMIT 1) as last_updated_at
                FROM ohlcv_data
                WHERE symbol = ? AND timeframe = ?
            '''
            params = subquery_params + [symbol, timeframe]
            
            if source:
                query += ' AND source = ?'
                params.append(source)
            
            cursor.execute(query, params)
            row = cursor.fetchone()
            
            return {
                'total_count': row[0] if row else 0,
                'start_date': row[1] if row else None,
                'end_date': row[2] if row else None,
                'last_updated_at': row[3] if row and len(row) > 3 else None
            }
    
    def get_all_symbols(
        self,
        timeframe: Optional[str] = None,
        source: Optional[str] = None
    ) -> List[dict]:
        """
        DBに保存されている全銘柄の一覧を取得
        
        Args:
            timeframe: 時間足（Noneの場合は全時間足）
            source: データソース（Noneの場合は全ソース）
            
        Returns:
            List[dict]: 銘柄情報のリスト
                - symbol: 銘柄コード
                - timeframe: 時間足
                - source: データソース
                - data_count: データ件数
                - first_date: 最初のデータ日時
                - last_date: 最後のデータ日時
        """
        with sqlite3.connect(self.db_path) as conn:
            # サブクエリで最新の更新時刻を取得
            query = '''
                SELECT 
                    symbol,
                    timeframe,
                    source,
                    COUNT(*) as data_count,
                    MIN(datetime) as first_date,
                    MAX(datetime) as last_date,
                    (SELECT updated_at FROM ohlcv_data o2
                     WHERE o2.symbol = ohlcv_data.symbol 
                     AND o2.timeframe = ohlcv_data.timeframe 
                     AND o2.source = ohlcv_data.source
                     ORDER BY o2.datetime DESC, o2.updated_at DESC LIMIT 1) as last_updated_at
                FROM ohlcv_data
                WHERE 1=1
            '''
            params = []
            
            if timeframe:
                query += ' AND timeframe = ?'
                params.append(timeframe)
            
            if source:
                query += ' AND source = ?'
                params.append(source)
            
            query += ' GROUP BY symbol, timeframe, source ORDER BY symbol, timeframe, source'
            
            cursor = conn.cursor()
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            result = []
            for row in rows:
                result.append({
                    'symbol': row[0],
                    'timeframe': row[1],
                    'source': row[2],
                    'data_count': row[3],
                    'first_date': row[4],
                    'last_date': row[5],
                    'last_updated_at': row[6] if len(row) > 6 else None
                })
            
            return result
    
    def get_symbol_list(
        self,
        timeframe: Optional[str] = None,
        source: Optional[str] = None
    ) -> List[str]:
        """
        DBに保存されている銘柄コードの一覧を取得（重複なし）
        
        Args:
            timeframe: 時間足（Noneの場合は全時間足）
            source: データソース（Noneの場合は全ソース）
            
        Returns:
            List[str]: 銘柄コードのリスト（ソート済み）
        """
        with sqlite3.connect(self.db_path) as conn:
            query = 'SELECT DISTINCT symbol FROM ohlcv_data WHERE 1=1'
            params = []
            
            if timeframe:
                query += ' AND timeframe = ?'
                params.append(timeframe)
            
            if source:
                query += ' AND source = ?'
                params.append(source)
            
            query += ' ORDER BY symbol'
            
            cursor = conn.cursor()
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            return [row[0] for row in rows]
    
    # ================= 銘柄名管理（後方互換性のため委譲） =================
    def save_symbol_name(self, symbol: str, name: str, sector: Optional[str] = None):
        """銘柄名を保存（SymbolNameManagerに委譲）"""
        return self._symbol_name_manager.save_symbol_name(symbol, name, sector)
    
    def get_symbol_name(self, symbol: str) -> Optional[str]:
        """銘柄名を取得（SymbolNameManagerに委譲）"""
        return self._symbol_name_manager.get_symbol_name(symbol)
    
    def get_symbol_names(self, symbols: List[str]) -> Dict[str, str]:
        """複数の銘柄名を一括取得（SymbolNameManagerに委譲）"""
        return self._symbol_name_manager.get_symbol_names(symbols)
    
    def get_symbol_sector(self, symbol: str) -> Optional[str]:
        """セクター情報を取得（SymbolNameManagerに委譲）"""
        return self._symbol_name_manager.get_symbol_sector(symbol)
    
    def get_symbol_sectors(self, symbols: List[str]) -> Dict[str, str]:
        """複数のセクター情報を一括取得（SymbolNameManagerに委譲）"""
        return self._symbol_name_manager.get_symbol_sectors(symbols)
    
    def get_symbol_industry(self, symbol: str) -> Optional[str]:
        """業種情報を取得（SymbolNameManagerに委譲）"""
        return self._symbol_name_manager.get_symbol_industry(symbol)
    
    def get_symbol_industries(self, symbols: List[str]) -> Dict[str, str]:
        """複数の業種情報を一括取得（SymbolNameManagerに委譲）"""
        return self._symbol_name_manager.get_symbol_industries(symbols)
    
    def convert_sectors_to_japanese(self, symbols: Optional[List[str]] = None) -> Dict[str, int]:
        """既存のセクター情報を英語から日本語に変換（SymbolNameManagerに委譲）"""
        return self._symbol_name_manager.convert_sectors_to_japanese(symbols)
    
    def fetch_and_save_symbol_names(
        self,
        symbols: List[str],
        progress_callback: Optional[Callable] = None,
        max_retries: int = 3,
        retry_delay: float = 1.0
    ) -> Dict[str, Dict]:
        """複数の銘柄名をYahoo Financeから取得してDBに保存（SymbolNameManagerに委譲）"""
        return self._symbol_name_manager.fetch_and_save_symbol_names(
            symbols, progress_callback, max_retries, retry_delay
        )
    
    def fetch_and_save_sectors(
        self,
        symbols: List[str],
        progress_callback: Optional[Callable] = None,
        max_retries: int = 3,
        retry_delay: float = 1.0
    ) -> Dict[str, Dict]:
        """複数の銘柄のセクター情報をYahoo Financeから取得してDBに保存（SymbolNameManagerに委譲）"""
        return self._symbol_name_manager.fetch_and_save_sectors(
            symbols, progress_callback, max_retries, retry_delay
        )
    
    def _extract_japanese_name(self, info: dict, symbol: str) -> Optional[str]:
        """Yahoo Financeから日本語名（和名）を取得（SymbolNameManagerに委譲）"""
        return self._symbol_name_manager._extract_japanese_name(info, symbol)

