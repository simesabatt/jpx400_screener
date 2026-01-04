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
from src.screening.jpx400_manager import JPX400Manager
from datetime import date


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
        self._jpx400_manager = JPX400Manager()
    
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
            
            # 全銘柄パフォーマンステーブル
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS all_symbols_performance (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT NOT NULL UNIQUE,
                    performance_json TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
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
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_all_symbols_performance_date 
                ON all_symbols_performance(date)
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
    
    def get_history_list_basic(self, limit: int = 100) -> List[Dict]:
        """
        スクリーニング履歴一覧を取得（基本情報のみ、パフォーマンス計算なし）
        
        Args:
            limit: 取得件数の上限
            
        Returns:
            List[Dict]: 履歴一覧（performance_summaryは空）
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
                        'performance_summary': {}  # パフォーマンス計算は後で行う
                    })
                
                return history_list
                
        except Exception as e:
            print(f"[ERROR] 履歴取得エラー: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def get_history_list(self, limit: int = 100) -> List[Dict]:
        """
        スクリーニング履歴一覧を取得（パフォーマンス計算込み）
        
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
            
            # 正式データのみを使用（仮終値は除外）
            df = self._ohlcv_manager.get_ohlcv_data_with_temporary_flag(
                symbol=symbol.get("symbol"),
                timeframe="1d",
                source="yahoo",
                include_temporary=False
            )
            if df is None or df.empty:
                for h in horizons:
                    symbol[f"perf_day{h}_label"] = "N/A"
                    symbol[f"perf_day{h}_pct"] = None
                continue
            
            # 実行日より後の正式データのみを取得
            # exec_dateがNoneの場合は全データを使用
            if exec_date:
                future_df = df[df.index.date > exec_date].copy()
            else:
                future_df = df.copy()
            
            # 取引日のみを抽出（実際にDBにデータが存在する日のみ）
            # インデックスの日付を取得し、重複を除去してソート
            if future_df.empty:
                # 未来データが存在しない場合
                for h in horizons:
                    symbol[f"perf_day{h}_label"] = "N/A"
                    symbol[f"perf_day{h}_pct"] = None
                continue
            
            # 実際にDBに存在する日付のみを取得（重複を除去してソート）
            trading_dates = sorted(list(set(future_df.index.date)))
            
            for h in horizons:
                if len(trading_dates) >= h:
                    # h番目の取引日を取得（実際にDBに存在する日付）
                    target_date = trading_dates[h - 1]
                    
                    # 該当日付のデータが実際に存在することを再確認
                    target_df = future_df[future_df.index.date == target_date]
                    if target_df.empty or len(target_df) == 0:
                        # データが存在しない場合はスキップ（valid_countsにカウントしない）
                        symbol[f"perf_day{h}_label"] = "N/A"
                        symbol[f"perf_day{h}_pct"] = None
                        continue
                    
                    try:
                        # データが存在することを確認してから取得
                        if 'close' not in target_df.columns:
                            symbol[f"perf_day{h}_label"] = "N/A"
                            symbol[f"perf_day{h}_pct"] = None
                            continue
                        
                        close_val = float(target_df.iloc[-1]["close"])
                        if close_val <= 0:
                            symbol[f"perf_day{h}_label"] = "N/A"
                            symbol[f"perf_day{h}_pct"] = None
                            continue
                        
                        change_pct = (close_val / price_at_sc - 1.0) * 100
                        symbol[f"perf_day{h}_pct"] = change_pct
                        is_up = change_pct > 0
                        arrow = "↑" if is_up else "↓"
                        symbol[f"perf_day{h}_label"] = f"{arrow}{change_pct:+.2f}%"
                        valid_counts[h] += 1
                        if is_up:
                            win_counts[h] += 1
                        change_lists[h].append(change_pct)
                    except Exception as e:
                        # エラーが発生した場合はスキップ（valid_countsにカウントしない）
                        symbol[f"perf_day{h}_label"] = "N/A"
                        symbol[f"perf_day{h}_pct"] = None
                else:
                    # 取引日が不足している場合
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
    
    def _calculate_all_symbols_performance(
        self,
        executed_date: date,
        horizons: Tuple[int, int, int] = (1, 2, 3)
    ) -> Dict:
        """
        全銘柄（JPX400全体）のパフォーマンスを計算
        
        まずデータベースから取得を試み、存在しない場合のみ計算して保存します。
        
        Args:
            executed_date: スクリーニング実行日（date型）
            horizons: 計算する営業日オフセット
            
        Returns:
            dict: 勝率集計とサマリー（_calculate_future_performanceと同じ形式）
        """
        date_str = executed_date.isoformat()
        
        # データベースから取得を試みる
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT performance_json
                    FROM all_symbols_performance
                    WHERE date = ?
                """, (date_str,))
                row = cursor.fetchone()
                if row:
                    # データベースから取得できた場合
                    try:
                        performance_data = json.loads(row['performance_json'])
                        # JSONから読み込んだ場合、win_ratesのキーが文字列になっている可能性があるため、整数キーに変換
                        if 'win_rates' in performance_data:
                            win_rates_original = performance_data['win_rates']
                            win_rates_normalized = {}
                            for key, value in win_rates_original.items():
                                # 文字列キーを整数に変換
                                try:
                                    int_key = int(key)
                                    win_rates_normalized[int_key] = value
                                except (ValueError, TypeError):
                                    # 変換できない場合はそのまま
                                    win_rates_normalized[key] = value
                            performance_data['win_rates'] = win_rates_normalized
                        
                        # データが有効か確認（win_ratesが存在するか）
                        if 'win_rates' in performance_data:
                            # win_ratesに有効なデータがあるか確認
                            # JSONから読み込んだ場合、キーが文字列になっている可能性があるため、両方チェック
                            has_valid_data = False
                            win_rates = performance_data.get('win_rates', {})
                            
                            # デバッグ用：win_ratesの内容を出力
                            print(f"[DEBUG] データベースから取得したwin_rates keys: {list(win_rates.keys())}, 型: {[type(k) for k in win_rates.keys()]}")
                            
                            for h in horizons:
                                # 整数キーと文字列キーの両方をチェック
                                h_key = h
                                h_str_key = str(h)
                                win_rate_info = None
                                if h_key in win_rates:
                                    win_rate_info = win_rates[h_key]
                                    print(f"[DEBUG] 整数キー {h_key} で取得: {win_rate_info}")
                                elif h_str_key in win_rates:
                                    win_rate_info = win_rates[h_str_key]
                                    print(f"[DEBUG] 文字列キー {h_str_key} で取得: {win_rate_info}")
                                
                                if win_rate_info:
                                    total = win_rate_info.get('total', 0)
                                    print(f"[DEBUG] horizon {h}: total={total}, win={win_rate_info.get('win', 0)}, rate={win_rate_info.get('rate')}")
                                    if total > 0:
                                        has_valid_data = True
                                        break
                            
                            if has_valid_data:
                                print(f"[全銘柄パフォーマンス] データベースから取得: {date_str}")
                                return performance_data
                            else:
                                print(f"[WARN] 全銘柄パフォーマンスデータが空: {date_str} (有効なデータがありません)")
                                # デバッグ用：win_ratesの内容を詳細に出力
                                for h in horizons:
                                    h_str_key = str(h)
                                    if h_str_key in win_rates:
                                        info = win_rates[h_str_key]
                                        print(f"[DEBUG] horizon {h} (key='{h_str_key}'): {info}")
                        else:
                            print(f"[WARN] 全銘柄パフォーマンスデータが不正: {date_str} (win_ratesが存在しません)")
                    except json.JSONDecodeError as e:
                        print(f"[WARN] 全銘柄パフォーマンスJSON解析エラー: {date_str}, {e}")
                        # JSON解析エラーの場合は再計算
                else:
                    print(f"[全銘柄パフォーマンス] データベースに存在しません: {date_str} (検索条件: date='{date_str}')")
        except Exception as e:
            print(f"[WARN] 全銘柄パフォーマンス取得エラー: {e}")
            import traceback
            traceback.print_exc()
            # エラーが発生した場合は計算を続行
        
        # データベースに存在しない場合は計算
        try:
            # JPX400全銘柄リストを取得
            all_symbols = self._jpx400_manager.load_symbols()
            if not all_symbols:
                print("[全銘柄パフォーマンス] JPX400銘柄リストが空です")
                return {"win_rates": {}}
            
            print(f"[全銘柄パフォーマンス] {executed_date}の全銘柄パフォーマンスを計算中... ({len(all_symbols)}銘柄)")
            
            # 各銘柄のデータを準備（price_at_screeningは実行日の終値を使用）
            symbols_data = []
            executed_at_str = executed_date.isoformat()
            
            for symbol in all_symbols:
                try:
                    # 正式データのみを使用（仮終値は除外）
                    df = self._ohlcv_manager.get_ohlcv_data_with_temporary_flag(
                        symbol=symbol,
                        timeframe="1d",
                        source="yahoo",
                        include_temporary=False
                    )
                    if df is None or df.empty:
                        continue
                    
                    # 実行日以前のデータを取得
                    past_df = df[df.index.date <= executed_date]
                    if past_df.empty:
                        continue
                    
                    # 実行日の終値を取得（なければ最新の終値）
                    latest_row = past_df.iloc[-1]
                    price_at_sc = float(latest_row['close'])
                    
                    if price_at_sc > 0:
                        symbols_data.append({
                            "symbol": symbol,
                            "price_at_screening": price_at_sc
                        })
                except Exception as e:
                    # 個別の銘柄でエラーが発生しても処理を継続
                    continue
            
            if not symbols_data:
                print(f"[全銘柄パフォーマンス] 有効な銘柄データがありません")
                result = {"win_rates": {}}
                # 空の結果はデータベースに保存しない（次回再計算されるようにする）
                print(f"[全銘柄パフォーマンス] 正式データが不足しているため、データベースへの保存をスキップします: {date_str}")
                return result
            
            print(f"[全銘柄パフォーマンス] {len(symbols_data)}銘柄のデータを取得しました")
            
            # 既存の_calculate_future_performanceメソッドを使用して計算
            result = self._calculate_future_performance(
                symbols_data,
                executed_at_str,
                horizons
            )
            
            # 正式データが十分に存在するかチェック（各horizonについて）
            # 正式データが不足している場合は保存をスキップ（次回再計算されるようにする）
            should_save = True
            min_valid_ratio = 0.5  # 全銘柄の50%以上のデータが必要
            
            for h in horizons:
                valid_count = result.get('valid_counts', {}).get(h, 0)
                if valid_count < len(symbols_data) * min_valid_ratio:
                    print(f"[全銘柄パフォーマンス] +{h}日後の正式データが不足しています (有効: {valid_count}/{len(symbols_data)}, 必要: {int(len(symbols_data) * min_valid_ratio)}以上)")
                    should_save = False
                    break
            
            if not should_save:
                print(f"[全銘柄パフォーマンス] 正式データが不足しているため、データベースへの保存をスキップします: {date_str}")
                print(f"[全銘柄パフォーマンス] 正式データが揃った後に再計算してください")
                # 保存せずに結果を返す（次回再計算される）
                return result
            
            # データベースに保存（正式データが十分に存在する場合のみ）
            try:
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.cursor()
                    performance_json = json.dumps(result, ensure_ascii=False, default=str)
                    # 計算結果のサマリーをログに出力
                    win_rates_summary = {}
                    for h in horizons:
                        if h in result.get('win_rates', {}):
                            info = result['win_rates'][h]
                            rate = info.get('rate')
                            rate_str = f"{rate:.1f}%" if rate is not None else "N/A"
                            win_rates_summary[h] = f"{info.get('win', 0)}/{info.get('total', 0)} ({rate_str})"
                    print(f"[全銘柄パフォーマンス] 保存前チェック: date={date_str}, json長={len(performance_json)}, 結果={win_rates_summary}")
                    
                    cursor.execute("""
                        INSERT OR REPLACE INTO all_symbols_performance (date, performance_json)
                        VALUES (?, ?)
                    """, (date_str, performance_json))
                    conn.commit()
                    
                    # 保存確認のため、再度取得して確認
                    # row_factoryを設定してRowオブジェクトとして取得
                    conn.row_factory = sqlite3.Row
                    cursor = conn.cursor()
                    cursor.execute("""
                        SELECT performance_json
                        FROM all_symbols_performance
                        WHERE date = ?
                    """, (date_str,))
                    saved_row = cursor.fetchone()
                    if saved_row:
                        # 保存されたデータを確認
                        saved_data = json.loads(saved_row['performance_json'])
                        saved_win_rates = saved_data.get('win_rates', {})
                        print(f"[全銘柄パフォーマンス] データベースに保存成功: {date_str} (保存確認: win_rates keys={list(saved_win_rates.keys())})")
                    else:
                        print(f"[ERROR] 全銘柄パフォーマンス保存失敗: {date_str} (保存後に取得できませんでした)")
            except Exception as e:
                print(f"[ERROR] 全銘柄パフォーマンス保存エラー: {e}")
                import traceback
                traceback.print_exc()
                # エラーが発生しても計算結果は返す（次回再計算される）
            
            print(f"[全銘柄パフォーマンス] 計算完了: {executed_date}")
            return result
            
        except Exception as e:
            print(f"[ERROR] 全銘柄パフォーマンス計算エラー: {e}")
            import traceback
            traceback.print_exc()
            return {"win_rates": {}}
    
    def delete_all_symbols_performance(self) -> bool:
        """
        全銘柄パフォーマンスデータをすべて削除
        
        Returns:
            bool: 削除成功かどうか
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM all_symbols_performance")
                deleted_count = cursor.rowcount
                conn.commit()
                print(f"[全銘柄パフォーマンス] {deleted_count}件のデータを削除しました")
                return True
        except Exception as e:
            print(f"[ERROR] 全銘柄パフォーマンス削除エラー: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def recalculate_all_symbols_performance_for_dates(self, dates: List[date]) -> Dict[date, Dict]:
        """
        指定された日付の全銘柄パフォーマンスを再計算して保存
        
        Args:
            dates: 再計算する日付のリスト
            
        Returns:
            Dict[date, Dict]: 日付ごとの計算結果
        """
        results = {}
        for target_date in dates:
            print(f"[全銘柄パフォーマンス] 再計算開始: {target_date}")
            # まず該当日付のデータを削除
            try:
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.cursor()
                    date_str = target_date.isoformat()
                    cursor.execute("DELETE FROM all_symbols_performance WHERE date = ?", (date_str,))
                    conn.commit()
                    print(f"[全銘柄パフォーマンス] {target_date}のデータを削除しました")
            except Exception as e:
                print(f"[WARN] {target_date}のデータ削除エラー: {e}")
            
            # 再計算
            result = self._calculate_all_symbols_performance(target_date)
            results[target_date] = result
        
        return results
    
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

