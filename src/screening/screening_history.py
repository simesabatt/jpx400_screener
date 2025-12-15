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
from typing import List, Dict, Optional, Tuple
import os

from src.data_collector.ohlcv_data_manager import OHLCVDataManager


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
        self._ohlcv_manager = OHLCVDataManager(db_path)
    
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
                        'created_at': row['created_at'],
                        'performance_summary': self._calculate_future_performance_for_history(
                            row['id'],
                            row['executed_at']
                        )
                    })
                
                return history_list
                
        except Exception as e:
            print(f"[ERROR] 履歴取得エラー: {e}")
            import traceback
            traceback.print_exc()
            return []

    def _calculate_future_performance_for_history(
        self,
        history_id: int,
        executed_at: str,
        horizons: Tuple[int, int, int] = (1, 2, 3)
    ) -> Dict:
        """
        履歴単位で翌営業日以降の勝率のみを集計（一覧表示用）
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute(
                    """
                    SELECT symbol, price_at_screening
                    FROM screening_history_symbols
                    WHERE history_id = ?
                    """,
                    (history_id,)
                )
                rows = cursor.fetchall()
                symbols = []
                for r in rows:
                    symbols.append({
                        "symbol": r["symbol"],
                        "price_at_screening": r["price_at_screening"]
                    })
                return self._calculate_future_performance(symbols, executed_at, horizons)
        except Exception:
            return {"win_rates": {}}
    
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
                
                # パフォーマンスを計算して付与
                perf_summary = self._calculate_future_performance(
                    symbols,
                    executed_at=history['executed_at']
                )
                history['symbols'] = symbols
                history['performance_summary'] = perf_summary
                return history
                
        except Exception as e:
            print(f"[ERROR] 履歴詳細取得エラー: {e}")
            import traceback
            error_detail = traceback.format_exc()
            print(f"[ERROR] 詳細: {error_detail}")
            # エラーが発生してもNoneを返す（呼び出し側でエラーメッセージを表示）
            return None

    def _calculate_future_performance(
        self,
        symbols: List[Dict],
        executed_at: str,
        horizons: Tuple[int, int, int] = (1, 2, 3)
    ) -> Dict:
        """
        翌営業日以降のパフォーマンスを計算し、各銘柄に付与する

        Args:
            symbols: 履歴に保存された銘柄データ
            executed_at: スクリーニング実行日時（ISO文字列）
            horizons: 計算する営業日オフセット

        Returns:
            dict: 勝率集計とサマリー
        """
        try:
            exec_dt = datetime.fromisoformat(executed_at)
            exec_date = exec_dt.date()
        except Exception:
            exec_date = None
        
        win_counts = {h: 0 for h in horizons}
        valid_counts = {h: 0 for h in horizons}
        change_lists = {h: [] for h in horizons}
        
        for symbol in symbols:
            price_at_sc = symbol.get("price_at_screening") or symbol.get("current_price")
            try:
                price_at_sc = float(price_at_sc) if price_at_sc is not None else None
            except Exception:
                price_at_sc = None
            
            if price_at_sc is None or price_at_sc == 0:
                for h in horizons:
                    symbol[f"perf_day{h}_label"] = "N/A"
                    symbol[f"perf_day{h}_pct"] = None
                continue
            
            df = self._ohlcv_manager.get_ohlcv_data_with_temporary_flag(
                symbol=symbol.get("symbol"),
                timeframe="1d",
                source="yahoo",
                include_temporary=True
            )
            if df is None or df.empty:
                for h in horizons:
                    symbol[f"perf_day{h}_label"] = "N/A"
                    symbol[f"perf_day{h}_pct"] = None
                continue
            
            future_df = df[df.index.date > exec_date] if exec_date else df
            future_dates = list(future_df.index.date)
            
            for h in horizons:
                if len(future_dates) >= h:
                    target_date = future_dates[h - 1]
                    try:
                        close_val = float(future_df[future_df.index.date == target_date].iloc[-1]["close"])
                        change_pct = (close_val / price_at_sc - 1.0) * 100
                        symbol[f"perf_day{h}_pct"] = change_pct
                        is_up = change_pct > 0
                        arrow = "↑" if is_up else "↓"
                        symbol[f"perf_day{h}_label"] = f"{arrow}{change_pct:+.2f}%"
                        valid_counts[h] += 1
                        if is_up:
                            win_counts[h] += 1
                        change_lists[h].append(change_pct)
                    except Exception:
                        symbol[f"perf_day{h}_label"] = "N/A"
                        symbol[f"perf_day{h}_pct"] = None
                else:
                    symbol[f"perf_day{h}_label"] = "N/A"
                    symbol[f"perf_day{h}_pct"] = None
        
        summary = {}
        for h in horizons:
            if valid_counts[h] > 0:
                rate = win_counts[h] / valid_counts[h] * 100
                changes = change_lists[h]
                avg = sum(changes) / len(changes) if changes else None
                med = None
                if changes:
                    sorted_chg = sorted(changes)
                    mid = len(sorted_chg) // 2
                    if len(sorted_chg) % 2 == 0:
                        med = (sorted_chg[mid - 1] + sorted_chg[mid]) / 2
                    else:
                        med = sorted_chg[mid]
                summary[h] = {
                    "win": win_counts[h],
                    "total": valid_counts[h],
                    "rate": rate,
                    "avg": avg,
                    "median": med
                }
            else:
                summary[h] = {"win": 0, "total": 0, "rate": None, "avg": None, "median": None}
        
        return {
            "win_counts": win_counts,
            "valid_counts": valid_counts,
            "win_rates": summary
        }
    
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

