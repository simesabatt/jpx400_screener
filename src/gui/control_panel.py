"""
JPX400スクリーニング コントロールパネル

JPX400銘柄のスクリーニング機能を提供するメインウィンドウ

Copyright (c) 2025 JPX400スクリーニングシステム

This software is licensed under the MIT License.
See LICENSE file for details.
"""

import threading
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
from datetime import datetime
from typing import List
import os
import json
import sqlite3


class ControlPanel(tk.Tk):
    """JPX400スクリーニング コントロールパネル"""

    def __init__(self, db_path: str):
        super().__init__()

        self.db_path = db_path  # DBパスを保持

        self.title("JPX400スクリーニング コントロールパネル")
        self.geometry("1000x700")  # サイズを大きく

        # 実行中フラグ（DataManagementTabで管理されるものは除く）
        self._backtesting = False  # バックテスト実行中フラグ
        self.jpx_collect_status_var = tk.StringVar(value="最終収集: 未実行")

        # 自動タスクマネージャー
        self.auto_task_manager = None

        # UIを構築（status_varを作成）
        self._build_ui()
        
        # スクリーニングUIマネージャーを初期化（UIボタンとstatus_varが作成された後）
        from src.gui.screening_ui import ScreeningUI
        self.screening_ui = ScreeningUI(
            db_path=self.db_path,
            status_var=self.status_var,
            on_button_state_change=self._on_screening_button_state_change,
            on_chart_display=self._show_chart
        )
        self._ensure_jpx_log_table()
        self._load_jpx_status()
        
        # 自動実行スケジューラーを起動
        self._start_auto_tasks()
        
        # アプリ終了時の処理を登録
        self.protocol("WM_DELETE_WINDOW", self._on_closing)
        
        # ウィンドウをクリックしたときに前面に表示されるようにする
        self.bind('<FocusIn>', lambda e: self.lift())
        self.bind('<Button-1>', lambda e: self.lift())

    # ================= UI 構築 =================
    def _build_ui(self):
        pad = 8

        # メインフレーム
        main_frame = ttk.Frame(self)
        main_frame.pack(fill="both", expand=True, padx=pad, pady=pad)

        # タイトル
        title_label = ttk.Label(main_frame, text="JPX400スクリーニング", font=("", 16, "bold"))
        title_label.pack(pady=(0, pad))

        # 状態表示（共通）を先に作成（DataManagementTabで使用するため）
        status_frame = ttk.Frame(main_frame)
        status_frame.pack(fill="x", pady=pad)

        self.status_var = tk.StringVar(value="状態: 待機中")
        ttk.Label(status_frame, textvariable=self.status_var, font=("", 10)).pack(side="left", padx=pad)

        # 情報表示
        info_frame = ttk.Frame(main_frame)
        info_frame.pack(fill="x")

        self.info_var = tk.StringVar(value=f"データベース: {self.db_path}")
        ttk.Label(info_frame, textvariable=self.info_var, font=("", 9), foreground="gray").pack(side="left", padx=pad)
    
        # タブコントロール
        self.notebook = ttk.Notebook(main_frame)
        self.notebook.pack(fill="both", expand=True, pady=pad)

        # タブ1: データ管理
        self.data_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.data_tab, text="データ管理")
        from src.gui.data_management_tab import DataManagementTab
        self.data_management = DataManagementTab(
            self.data_tab,
            self.db_path,
            self.status_var,
            self.jpx_collect_status_var,
            on_screening_requested=self._run_screening_from_window,
            on_show_chart_requested=self._show_chart,
            on_record_jpx_collection=self._record_jpx_collection,
            on_load_jpx_status=self._load_jpx_status
        )
        # DataManagementTab内の主要ボタンを参照（既存コード互換のため）
        self.jpx400_collect_button = self.data_management.jpx400_collect_button
        self.jpx400_update_list_button = self.data_management.jpx400_update_list_button
        self.show_symbols_button = self.data_management.show_symbols_button
        self.fetch_names_button = self.data_management.fetch_names_button
        
        # 地合いスコアタブを追加
        from src.gui.sentiment_tab import SentimentTab
        self.sentiment_tab = SentimentTab(
            self.data_tab,
            self.db_path,
            self.status_var
        )
        # ステータス変数を参照（自動実行で使用）
        self.sentiment_calc_status_var = self.sentiment_tab.sentiment_calc_status_var
        self.sentiment_eval_status_var = self.sentiment_tab.sentiment_eval_status_var
        
        # タブ2: 市況
        self.market_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.market_tab, text="市況")
        from src.gui.market_conditions_tab import MarketConditionsTab
        self.market_conditions = MarketConditionsTab(
            self.market_tab,
            self.db_path,
            self.status_var
        )
    
    def _start_auto_tasks(self):
        """自動タスクのスケジューリングを開始"""
        try:
            from src.gui.auto_tasks import AutoTaskManager
            
            self.auto_task_manager = AutoTaskManager()
            
            # JPXデータ収集の自動実行コールバック
            jpx_callback = lambda run_type: self.data_management.auto_collect_jpx400(run_type)
            
            # センチメントスコアの自動実行コールバック
            sentiment_calc_callback = lambda: self.sentiment_tab.auto_calculate_sentiment_score()
            sentiment_eval_callback = lambda: self.sentiment_tab.auto_record_and_evaluate_sentiment()
            
            self.auto_task_manager.start(
                jpx_collect_callback=jpx_callback,
                sentiment_calc_callback=sentiment_calc_callback,
                sentiment_eval_callback=sentiment_eval_callback
            )
        except Exception as e:
            print(f"[警告] 自動タスクの起動に失敗: {e}")
            import traceback
            traceback.print_exc()
    
    def _on_closing(self):
        """アプリ終了時の処理"""
        # 実行中の処理がある場合は確認
        jpx_collecting = self.data_management.get_collecting_state() if hasattr(self, 'data_management') else False
        fetching_names = self.data_management.get_fetching_state() if hasattr(self, 'data_management') else False
        screening_running = self.screening_ui.is_running() if hasattr(self, 'screening_ui') else False
        sentiment_running = self.sentiment_tab.is_running() if hasattr(self, 'sentiment_tab') else False
        market_analyzing = self.market_conditions.is_running() if hasattr(self, 'market_conditions') else False
        
        if jpx_collecting or screening_running or fetching_names or sentiment_running or market_analyzing or self._backtesting:
            if not messagebox.askokcancel("確認", "処理が実行中です。終了しますか？"):
                return  # キャンセルした場合は終了しない
        
        # 自動タスクを停止
        if self.auto_task_manager:
            self.auto_task_manager.stop()
        
        # アプリを終了
        self.destroy()
    
    # ================= スクリーニング機能（ScreeningUIへの委譲） =================
    def _on_screening_button_state_change(self, enable: bool):
        """スクリーニング実行時のボタン状態変更"""
        state = "normal" if enable else "disabled"
        self.jpx400_collect_button.config(state=state)
        self.jpx400_update_list_button.config(state=state)
        self.show_symbols_button.config(state=state)
        self.fetch_names_button.config(state=state)
    
    def _run_screening_from_window(
        self, 
        parent_window, 
        symbols: List[str],
        check_condition1: bool = True,
        check_condition2: bool = True,
        check_condition3: bool = False,
        check_condition4: bool = False,
        check_condition5: bool = False,
        check_condition6: bool = False,
        check_golden_cross_5_25: bool = False,
        check_golden_cross_25_75: bool = False,
        golden_cross_mode: str = 'just_crossed',
        use_macd_kd_filter: bool = True,
        macd_kd_window: int = 3,
        is_history: bool = False,
        executed_at: str = None
    ):
        """選択した銘柄をスクリーニング（ScreeningUIに委譲）"""
        if is_history:
            # 履歴表示の場合は直接結果を表示
            self.screening_ui.show_results(
                parent_window, symbols, check_condition1, check_condition2,
                            check_condition3, check_condition4, check_condition5, check_condition6,
                check_golden_cross_5_25, check_golden_cross_25_75, golden_cross_mode,
                use_macd_kd_filter, macd_kd_window, is_history, executed_at
            )
        else:
            # 通常のスクリーニング実行
            self.screening_ui.run_screening(
                parent_window, symbols, check_condition1, check_condition2,
                check_condition3, check_condition4, check_condition5, check_condition6,
                check_golden_cross_5_25, check_golden_cross_25_75, golden_cross_mode,
                use_macd_kd_filter, macd_kd_window
            )
    
    def _show_chart(self, parent_window, symbol: str, symbol_name: str):
        """ローソクグラフを表示"""
        try:
            from src.gui.chart_window import ChartWindow
            from src.data_collector.ohlcv_data_manager import OHLCVDataManager
            
            ohlcv_manager = OHLCVDataManager(self.db_path)
            ChartWindow(parent_window, symbol, symbol_name, ohlcv_manager)
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            print(f"[ERROR] チャート表示エラー: {e}")
            print(f"[ERROR] 詳細: {error_detail}")
            messagebox.showerror("エラー", f"チャート表示でエラーが発生しました:\n{e}\n\n詳細はコンソールを確認してください。", parent=parent_window)
    
    # ================= JPX収集ログ関連 =================
    def _ensure_jpx_log_table(self):
        """JPX収集ログテーブルの作成"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS jpx_collection_log (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        run_type TEXT,
                        executed_at TEXT NOT NULL,
                        created_at TEXT NOT NULL
                    )
                    """
                )
        except Exception as e:
            print(f"[WARN] jpx_collection_logテーブル作成に失敗: {e}")

    def _record_jpx_collection(self, run_type: str):
        """JPX収集をログに記録"""
        try:
            now = datetime.now().isoformat()
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    """
                    INSERT INTO jpx_collection_log (run_type, executed_at, created_at)
                    VALUES (?, ?, ?)
                    """,
                    (run_type, now, now),
                )
            self._load_jpx_status()
        except Exception as e:
            print(f"[WARN] jpx_collection_logへの記録に失敗: {e}")

    def _load_jpx_status(self):
        """DBから最新のJPX収集時刻を読み込み、ラベルに表示"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                row = conn.execute(
                    """
                    SELECT executed_at, run_type
                    FROM jpx_collection_log
                    ORDER BY executed_at DESC
                    LIMIT 1
                    """
                ).fetchone()
            if row and row["executed_at"]:
                dt_str = row["executed_at"][:19]
                run_type_label = {"manual": "手動", "15": "15時", "20": "20時"}.get(row["run_type"], row["run_type"])
                self.jpx_collect_status_var.set(f"最終収集: {dt_str} ({run_type_label})")
            else:
                self.jpx_collect_status_var.set("最終収集: 未実行")
        except Exception as e:
            print(f"[WARN] JPXステータス読み込みに失敗: {e}")
            self.jpx_collect_status_var.set("最終収集: 不明")


def main():
    """メイン関数"""
    import sys
    from pathlib import Path
    
    # データベースパス
    db_path = "data/tick_data.db"
    
    # データディレクトリを作成
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    
    # GUIを起動
    app = ControlPanel(db_path)
    app.mainloop()


if __name__ == '__main__':
    main()
