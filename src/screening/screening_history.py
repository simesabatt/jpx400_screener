"""
スクリーニング履歴管理モジュール

スクリーニング結果を履歴として保存し、後から検証できるようにします。

Copyright (c) 2025 JPX400スクリーニングシステム

This software is licensed under the MIT License.
See LICENSE file for details.
"""
import sqlite3
import json
from datetime import datetime
from typing import List, Dict, Optional
import os


class ScreeningHistory:
    """スクリーニング履歴管理クラス"""
    
    def __init__(self, db_path: str = "data/tick_data.db"):
        """
        初期化
        
        Args:
            db_path: データベースパス
        """
        self.db_path = db_path
        self._ensure_tables()
    
    def _ensure_tables(self):
        """履歴テーブルを作成"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # スクリーニング履歴テーブル
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS screening_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    executed_at TEXT NOT NULL,
                    conditions_json TEXT NOT NULL,
                    symbol_count INTEGER NOT NULL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # スクリーニング結果銘柄テーブル
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS screening_history_symbols (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    history_id INTEGER NOT NULL,
                    symbol TEXT NOT NULL,
                    symbol_name TEXT,
                    sector TEXT,
                    industry TEXT,
                    price_at_screening REAL NOT NULL,
                    volume_at_screening INTEGER,
                    volume_sigma REAL,
                    ma5 REAL,
                    ma25 REAL,
                    ma75 REAL,
                    ma200 REAL,
                    is_temporary_close INTEGER DEFAULT 0,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (history_id) REFERENCES screening_history(id) ON DELETE CASCADE
                )
            """)
            
            # マイグレーション: industry列を追加（既存テーブル用）
            try:
                cursor.execute('ALTER TABLE screening_history_symbols ADD COLUMN industry TEXT')
                print("[ScreeningHistory] screening_history_symbolsテーブルにindustry列を追加しました")
            except sqlite3.OperationalError:
                # 既に存在する場合はスキップ
                pass
            
            # インデックスを作成
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_screening_history_executed_at 
                ON screening_history(executed_at DESC)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_screening_history_symbols_history_id 
                ON screening_history_symbols(history_id)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_screening_history_symbols_symbol 
                ON screening_history_symbols(symbol)
            """)
            
            conn.commit()
    
    def save_history(
        self,
        results: List[Dict],
        conditions: Dict
    ) -> Optional[int]:
        """
        スクリーニング履歴を保存
        
        Args:
            results: スクリーニング結果のリスト
            conditions: スクリーニング条件の辞書
            
        Returns:
            int: 保存された履歴ID、失敗時はNone
        """
        try:
            executed_at = datetime.now().isoformat()
            conditions_json = json.dumps(conditions, ensure_ascii=False, default=str)
            symbol_count = len(results)
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # 履歴を保存
                cursor.execute("""
                    INSERT INTO screening_history (
                        executed_at,
                        conditions_json,
                        symbol_count
                    ) VALUES (?, ?, ?)
                """, (executed_at, conditions_json, symbol_count))
                
                history_id = cursor.lastrowid
                
                # 銘柄データを保存
                for result in results:
                    # テーブル構造を確認
                    cursor.execute("PRAGMA table_info(screening_history_symbols)")
                    columns = [row[1] for row in cursor.fetchall()]
                    has_industry = 'industry' in columns
                    
                    if has_industry:
                        cursor.execute("""
                            INSERT INTO screening_history_symbols (
                                history_id,
                                symbol,
                                symbol_name,
                                sector,
                                industry,
                                price_at_screening,
                                volume_at_screening,
                                volume_sigma,
                                ma5,
                                ma25,
                                ma75,
                                ma200,
                                is_temporary_close
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            history_id,
                            result.get('symbol', ''),
                            result.get('symbol_name', ''),
                            result.get('sector', ''),
                            result.get('industry', ''),
                            result.get('current_price', 0.0),
                            result.get('latest_volume', None),
                            result.get('volume_sigma', None),
                            result.get('ma5', None),
                            result.get('ma25', None),
                            result.get('ma75', None),
                            result.get('ma200', None),
                            result.get('is_temporary_close', 0)
                        ))
                    else:
                        # industry列がない場合（後方互換性）
                        cursor.execute("""
                            INSERT INTO screening_history_symbols (
                                history_id,
                                symbol,
                                symbol_name,
                                sector,
                                price_at_screening,
                                volume_at_screening,
                                volume_sigma,
                                ma5,
                                ma25,
                                ma75,
                                ma200,
                                is_temporary_close
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            history_id,
                            result.get('symbol', ''),
                            result.get('symbol_name', ''),
                            result.get('sector', ''),
                            result.get('current_price', 0.0),
                            result.get('latest_volume', None),
                            result.get('volume_sigma', None),
                            result.get('ma5', None),
                            result.get('ma25', None),
                            result.get('ma75', None),
                            result.get('ma200', None),
                            result.get('is_temporary_close', 0)
                        ))
                
                conn.commit()
                
                print(f"[スクリーニング履歴] 履歴を保存しました（ID: {history_id}, 銘柄数: {symbol_count}件）")
                return history_id
                
        except Exception as e:
            print(f"[ERROR] 履歴保存エラー: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def get_history_list(self, limit: int = 100) -> List[Dict]:
        """
        スクリーニング履歴一覧を取得
        
        Args:
            limit: 取得件数の上限
            
        Returns:
            List[Dict]: 履歴一覧
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                cursor.execute("""
                    SELECT 
                        id,
                        executed_at,
                        conditions_json,
                        symbol_count,
                        created_at
                    FROM screening_history
                    ORDER BY executed_at DESC
                    LIMIT ?
                """, (limit,))
                
                rows = cursor.fetchall()
                history_list = []
                
                for row in rows:
                    history_list.append({
                        'id': row['id'],
                        'executed_at': row['executed_at'],
                        'conditions': json.loads(row['conditions_json']),
                        'symbol_count': row['symbol_count'],
                        'created_at': row['created_at']
                    })
                
                return history_list
                
        except Exception as e:
            print(f"[ERROR] 履歴取得エラー: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def get_history_detail(self, history_id: int) -> Optional[Dict]:
        """
        スクリーニング履歴の詳細を取得
        
        Args:
            history_id: 履歴ID
            
        Returns:
            Dict: 履歴詳細（履歴情報と銘柄リスト）、存在しない場合はNone
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                # 履歴情報を取得
                cursor.execute("""
                    SELECT 
                        id,
                        executed_at,
                        conditions_json,
                        symbol_count,
                        created_at
                    FROM screening_history
                    WHERE id = ?
                """, (history_id,))
                
                row = cursor.fetchone()
                if not row:
                    return None
                
                history = {
                    'id': row['id'],
                    'executed_at': row['executed_at'],
                    'conditions': json.loads(row['conditions_json']),
                    'symbol_count': row['symbol_count'],
                    'created_at': row['created_at']
                }
                
                # テーブル構造を確認
                cursor.execute("PRAGMA table_info(screening_history_symbols)")
                pragma_rows = cursor.fetchall()
                columns = [row[1] for row in pragma_rows]
                has_industry = 'industry' in columns
                
                # 銘柄リストを取得
                if has_industry:
                    cursor.execute("""
                        SELECT 
                            id,
                            symbol,
                            symbol_name,
                            sector,
                            industry,
                            price_at_screening,
                            volume_at_screening,
                            volume_sigma,
                            ma5,
                            ma25,
                            ma75,
                            ma200,
                            is_temporary_close
                        FROM screening_history_symbols
                        WHERE history_id = ?
                        ORDER BY symbol
                    """, (history_id,))
                else:
                    cursor.execute("""
                        SELECT 
                            id,
                            symbol,
                            symbol_name,
                            sector,
                            price_at_screening,
                            volume_at_screening,
                            volume_sigma,
                            ma5,
                            ma25,
                            ma75,
                            ma200,
                            is_temporary_close
                        FROM screening_history_symbols
                        WHERE history_id = ?
                        ORDER BY symbol
                    """, (history_id,))
                
                symbol_rows = cursor.fetchall()
                symbols = []
                
                for sym_row in symbol_rows:
                    symbol_data = {
                        'symbol': sym_row['symbol'],
                        'symbol_name': sym_row['symbol_name'],
                        'sector': sym_row['sector'],
                        'current_price': sym_row['price_at_screening'],
                        'latest_volume': sym_row['volume_at_screening'],
                        'volume_sigma': sym_row['volume_sigma'],
                        'ma5': sym_row['ma5'],
                        'ma25': sym_row['ma25'],
                        'ma75': sym_row['ma75'],
                        'ma200': sym_row['ma200'],
                        'is_temporary_close': sym_row['is_temporary_close']
                    }
                    if has_industry:
                        # industry列がある場合のみ取得
                        try:
                            # sqlite3.Rowオブジェクトは辞書のようにアクセス可能
                            if 'industry' in sym_row.keys():
                                industry_value = sym_row['industry']
                                symbol_data['industry'] = industry_value if industry_value else ''
                            else:
                                symbol_data['industry'] = ''
                        except (KeyError, IndexError, TypeError):
                            # industry列が存在しない、または取得できない場合
                            symbol_data['industry'] = ''
                    else:
                        # industry列がない場合は空文字
                        symbol_data['industry'] = ''
                    symbols.append(symbol_data)
                
                history['symbols'] = symbols
                return history
                
        except Exception as e:
            print(f"[ERROR] 履歴詳細取得エラー: {e}")
            import traceback
            error_detail = traceback.format_exc()
            print(f"[ERROR] 詳細: {error_detail}")
            # エラーが発生してもNoneを返す（呼び出し側でエラーメッセージを表示）
            return None
    
    def delete_history(self, history_id: int) -> bool:
        """
        スクリーニング履歴を削除
        
        Args:
            history_id: 履歴ID
            
        Returns:
            bool: 削除成功かどうか
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM screening_history WHERE id = ?", (history_id,))
                conn.commit()
                print(f"[スクリーニング履歴] 履歴を削除しました（ID: {history_id}）")
                return True
        except Exception as e:
            print(f"[ERROR] 履歴削除エラー: {e}")
            return False

