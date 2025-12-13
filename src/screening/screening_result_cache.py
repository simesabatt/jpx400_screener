"""
スクリーニング結果キャッシュ管理モジュール

スクリーニング結果を保存し、データが更新されていない場合は再利用します。
"""
import sqlite3
import json
from datetime import datetime
from typing import List, Dict, Optional, Tuple
import os


class ScreeningResultCache:
    """スクリーニング結果キャッシュ管理クラス"""
    
    def __init__(self, db_path: str = "data/tick_data.db"):
        """
        初期化
        
        Args:
            db_path: データベースパス
        """
        self.db_path = db_path
        self._ensure_table()
    
    def _ensure_table(self):
        """キャッシュテーブルを作成"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # スクリーニング結果キャッシュテーブル
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS screening_result_cache (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    condition1 BOOLEAN NOT NULL,
                    condition2 BOOLEAN NOT NULL,
                    data_updated_at TEXT NOT NULL,
                    screening_executed_at TEXT NOT NULL,
                    results TEXT NOT NULL,
                    symbol_count INTEGER NOT NULL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # インデックスを作成
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_screening_cache_conditions 
                ON screening_result_cache(condition1, condition2, data_updated_at)
            """)
            
            conn.commit()
    
    def get_latest_data_updated_at(self) -> Optional[str]:
        """
        データの最終更新日時を取得
        
        Returns:
            str: 最終更新日時（ISO形式）、データがない場合はNone
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # ohlcv_dataテーブルの最新のupdated_atを取得
            cursor.execute("""
                SELECT MAX(updated_at) 
                FROM ohlcv_data 
                WHERE updated_at IS NOT NULL AND updated_at != ''
            """)
            
            result = cursor.fetchone()
            if result and result[0]:
                return result[0]
            
            # updated_atがない場合は、最新のdatetimeを取得
            cursor.execute("""
                SELECT MAX(datetime) 
                FROM ohlcv_data
            """)
            
            result = cursor.fetchone()
            if result and result[0]:
                return result[0]
            
            return None
    
    def get_cached_result(
        self, 
        check_condition1: bool, 
        check_condition2: bool
    ) -> Optional[Dict]:
        """
        キャッシュされた結果を取得
        
        Args:
            check_condition1: 条件1の有効/無効
            check_condition2: 条件2の有効/無効
            
        Returns:
            dict: キャッシュされた結果（条件が一致し、データが更新されていない場合）、
                  またはNone
        """
        current_data_updated_at = self.get_latest_data_updated_at()
        
        if not current_data_updated_at:
            # データがない場合はキャッシュも無効
            return None
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # 条件が一致し、データ更新日時も一致する最新の結果を取得
            cursor.execute("""
                SELECT 
                    id,
                    condition1,
                    condition2,
                    data_updated_at,
                    screening_executed_at,
                    results,
                    symbol_count
                FROM screening_result_cache
                WHERE condition1 = ? 
                  AND condition2 = ?
                  AND data_updated_at = ?
                ORDER BY screening_executed_at DESC
                LIMIT 1
            """, (check_condition1, check_condition2, current_data_updated_at))
            
            row = cursor.fetchone()
            
            if row:
                return {
                    'id': row[0],
                    'condition1': bool(row[1]),
                    'condition2': bool(row[2]),
                    'data_updated_at': row[3],
                    'screening_executed_at': row[4],
                    'results': json.loads(row[5]),
                    'symbol_count': row[6]
                }
            
            return None
    
    def save_result(
        self,
        check_condition1: bool,
        check_condition2: bool,
        results: List[Dict]
    ) -> bool:
        """
        スクリーニング結果を保存
        
        Args:
            check_condition1: 条件1の有効/無効
            check_condition2: 条件2の有効/無効
            results: スクリーニング結果のリスト
            
        Returns:
            bool: 保存成功かどうか
        """
        try:
            data_updated_at = self.get_latest_data_updated_at()
            
            if not data_updated_at:
                print("[キャッシュ] データの更新日時が取得できませんでした。キャッシュを保存しません。")
                return False
            
            screening_executed_at = datetime.now().isoformat()
            results_json = json.dumps(results, ensure_ascii=False, default=str)
            symbol_count = len(results)
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # 同じ条件・同じデータ更新日時の古いキャッシュを削除
                cursor.execute("""
                    DELETE FROM screening_result_cache
                    WHERE condition1 = ? 
                      AND condition2 = ?
                      AND data_updated_at = ?
                """, (check_condition1, check_condition2, data_updated_at))
                
                # 新しい結果を保存
                cursor.execute("""
                    INSERT INTO screening_result_cache (
                        condition1,
                        condition2,
                        data_updated_at,
                        screening_executed_at,
                        results,
                        symbol_count
                    ) VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    check_condition1,
                    check_condition2,
                    data_updated_at,
                    screening_executed_at,
                    results_json,
                    symbol_count
                ))
                
                conn.commit()
                
                print(f"[キャッシュ] スクリーニング結果を保存しました（条件1: {check_condition1}, 条件2: {check_condition2}, 結果数: {symbol_count}件）")
                return True
                
        except Exception as e:
            print(f"[ERROR] キャッシュ保存エラー: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def clear_cache(self):
        """キャッシュをすべて削除"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM screening_result_cache")
                conn.commit()
                print("[キャッシュ] すべてのキャッシュを削除しました")
        except Exception as e:
            print(f"[ERROR] キャッシュ削除エラー: {e}")

