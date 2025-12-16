"""
地合いスコアタブモジュール

地合いスコア算出、評価、表示の機能を提供します。

Copyright (c) 2025 JPX400スクリーニングシステム

This software is licensed under the MIT License.
See LICENSE file for details.
"""
import threading
import tkinter as tk
from tkinter import ttk, messagebox
from datetime import datetime, timedelta
from typing import Optional, Callable
import sqlite3


class SentimentTab:
    """地合いスコアタブのUIとハンドラを管理するクラス"""
    
    def __init__(
        self,
        parent: ttk.Frame,
        db_path: str,
        status_var: tk.StringVar
    ):
        """
        初期化
        
        Args:
            parent: 親フレーム（地合いスコアタブ）
            db_path: データベースパス
            status_var: ステータス表示用のStringVar
        """
        self.parent = parent
        self.db_path = db_path
        self.status_var = status_var
        
        # 実行中フラグ
        self._sentiment_running = False
        
        # ステータス変数
        self.sentiment_calc_status_var = tk.StringVar(value="最終実行: 未実行")
        self.sentiment_eval_status_var = tk.StringVar(value="最終実行: 未実行")
        
        # UI構築
        self._build_ui()
        
        # 初期ステータスをロード
        self._load_sentiment_status()
    
    def _build_ui(self):
        """地合いスコアタブのUI構築"""
        pad = 8
        
        sentiment_frame = ttk.LabelFrame(self.parent, text="地合いスコア（手動）", padding=pad)
        sentiment_frame.pack(fill="x", pady=pad, padx=pad)

        button_row = ttk.Frame(sentiment_frame)
        button_row.pack(fill="x", pady=(0, pad))

        # 地合いスコア算出・保存ボタンとそのステータス
        calc_frame = ttk.Frame(button_row)
        calc_frame.pack(side="left", padx=pad, pady=pad)
        self.sentiment_calc_button = ttk.Button(
            calc_frame,
            text="地合いスコア算出・保存（推奨 8:45-8:55）",
            command=self.on_calculate_sentiment_score,
            width=35
        )
        self.sentiment_calc_button.pack()
        ttk.Label(calc_frame, textvariable=self.sentiment_calc_status_var, font=("", 8), foreground="gray").pack()

        # 市場動向記録・評価ボタンとそのステータス
        eval_frame = ttk.Frame(button_row)
        eval_frame.pack(side="left", padx=pad, pady=pad)
        self.sentiment_eval_button = ttk.Button(
            eval_frame,
            text="市場動向記録・評価（推奨 15:35-15:45）",
            command=self.on_record_and_evaluate_sentiment,
            width=35
        )
        self.sentiment_eval_button.pack()
        ttk.Label(eval_frame, textvariable=self.sentiment_eval_status_var, font=("", 8), foreground="gray").pack()

        # 保存済みスコア表示ボタン
        ttk.Button(
            button_row,
            text="保存済みスコア表示",
            command=self.on_view_sentiment_scores,
            width=30
        ).pack(side="left", padx=pad, pady=pad)

        sentiment_info = (
            "• 朝（8:45-8:55）: 地合いスコア算出・保存\n"
            "• 夕方（15:35-15:45）: 市場実績の記録と前日スコアの評価\n"
            "• 保存済みスコア表示: 過去のスコアと評価結果を確認\n"
            "  ※アプリ常時起動不要。必要時に手動実行してください。"
        )
        ttk.Label(
            sentiment_frame,
            text=sentiment_info,
            font=("", 9),
            justify="left",
            foreground="gray"
        ).pack(anchor="w", padx=pad, pady=(0, pad))
    
    def is_running(self) -> bool:
        """地合いスコア処理が実行中かどうかを返す"""
        return self._sentiment_running
    
    def auto_calculate_sentiment_score(self):
        """自動実行：地合いスコアを算出・保存（確認なし）"""
        if self._sentiment_running:
            print("[自動実行] 地合いスコア処理は既に実行中です")
            return
        
        def task():
            try:
                self._sentiment_running = True
                self.status_var.set("状態: 地合いスコア算出中（自動）...")
                print(f"[自動実行] 地合いスコア算出を開始します（{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}）")

                from src.sentiment.yahoo_finance_scorer import YahooFinanceSentimentScorer
                from src.sentiment.sentiment_evaluator import SentimentEvaluator

                evaluator = SentimentEvaluator(self.db_path)
                latest_weights = evaluator.get_latest_weights()
                if latest_weights:
                    scorer = YahooFinanceSentimentScorer(
                        weights=latest_weights["weights"],
                        weights_version=latest_weights["version"],
                    )
                else:
                    scorer = YahooFinanceSentimentScorer()

                today = datetime.now().strftime("%Y-%m-%d")
                sentiment = scorer.calculate_score()
                evaluator.save_score(today, sentiment)
                self._load_sentiment_status()

                print(f"[自動実行完了] 地合いスコア算出: スコア={sentiment['score']:.1f}点, レベル={sentiment['level']}")
            except Exception as e:
                print(f"[自動実行エラー] 地合いスコア算出で例外: {e}")
                import traceback
                traceback.print_exc()
            finally:
                self._sentiment_running = False
                self.parent.after(0, lambda: self.status_var.set("状態: 待機中"))
        
        import threading
        thread = threading.Thread(target=task, daemon=True)
        thread.start()
    
    def auto_record_and_evaluate_sentiment(self):
        """自動実行：市場動向記録・評価（確認なし）"""
        if self._sentiment_running:
            print("[自動実行] 地合いスコア処理は既に実行中です")
            return
        
        def task():
            try:
                self._sentiment_running = True
                self.status_var.set("状態: 市場動向記録・評価中（自動）...")
                print(f"[自動実行] 市場動向記録・評価を開始します（{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}）")

                from src.sentiment.sentiment_evaluator import SentimentEvaluator

                evaluator = SentimentEvaluator(self.db_path)
                today = datetime.now().strftime("%Y-%m-%d")
                yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

                # 当日の市場動向を記録
                evaluator.record_market_outcome(today)
                
                # 前日のスコアを当日の市場動向と比較して評価
                evaluator.evaluate_scores(yesterday, today)
                
                report = evaluator.generate_evaluation_report(days=30)
                self._load_sentiment_status()

                print(f"[自動実行完了] 市場動向記録・評価: 方向性一致率(30日)={report['direction_accuracy']:.1f}%, 平均予測誤差={report['avg_error']:.1f}点, サンプル数={report['sample_size']}")
            except Exception as e:
                print(f"[自動実行エラー] 市場動向記録・評価で例外: {e}")
                import traceback
                traceback.print_exc()
            finally:
                self._sentiment_running = False
                self.parent.after(0, lambda: self.status_var.set("状態: 待機中"))
        
        import threading
        thread = threading.Thread(target=task, daemon=True)
        thread.start()
    
    def on_calculate_sentiment_score(self):
        """地合いスコアを算出・保存（推奨 8:45-8:55）"""
        if self._sentiment_running:
            messagebox.showwarning("実行中", "地合いスコア関連の処理が実行中です。完了をお待ちください。")
            return

        def task():
            try:
                self._sentiment_running = True
                self._set_sentiment_buttons_state("disabled")
                self.status_var.set("状態: 地合いスコア算出中...")

                from src.sentiment.yahoo_finance_scorer import YahooFinanceSentimentScorer
                from src.sentiment.sentiment_evaluator import SentimentEvaluator

                evaluator = SentimentEvaluator(self.db_path)
                latest_weights = evaluator.get_latest_weights()
                if latest_weights:
                    scorer = YahooFinanceSentimentScorer(
                        weights=latest_weights["weights"],
                        weights_version=latest_weights["version"],
                    )
                else:
                    scorer = YahooFinanceSentimentScorer()

                today = datetime.now().strftime("%Y-%m-%d")
                sentiment = scorer.calculate_score()
                evaluator.save_score(today, sentiment)
                self._load_sentiment_status()

                msg = (
                    "地合いスコアを計算し、保存しました。\n\n"
                    f"スコア: {sentiment['score']:.1f} 点\n"
                    f"レベル: {sentiment['level']}\n"
                    f"計算時刻: {sentiment['calculated_at']}\n"
                    f"weights_version: {sentiment.get('weights_version', 1)}"
                )
                self.parent.after(0, lambda: messagebox.showinfo("完了", msg))
            except ImportError as e:
                self.parent.after(0, lambda: messagebox.showerror("エラー", f"必要なライブラリが不足しています:\n{e}"))
            except Exception as e:
                self.parent.after(0, lambda: messagebox.showerror("エラー", f"地合いスコア算出でエラーが発生しました:\n{e}"))
            finally:
                self._sentiment_running = False
                self.parent.after(0, lambda: self._set_sentiment_buttons_state("normal"))
                self.parent.after(0, lambda: self.status_var.set("状態: 待機中"))

        threading.Thread(target=task, daemon=True).start()

    def on_record_and_evaluate_sentiment(self):
        """市場動向の記録と前日スコアの評価（推奨 15:35-15:45）"""
        if self._sentiment_running:
            messagebox.showwarning("実行中", "地合いスコア関連の処理が実行中です。完了をお待ちください。")
            return

        def task():
            try:
                self._sentiment_running = True
                self._set_sentiment_buttons_state("disabled")
                self.status_var.set("状態: 市場動向記録・評価実行中...")

                from src.sentiment.sentiment_evaluator import SentimentEvaluator

                evaluator = SentimentEvaluator(self.db_path)
                today = datetime.now().strftime("%Y-%m-%d")
                yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

                # 当日の市場動向を記録
                evaluator.record_market_outcome(today)
                
                # 前日のスコアを当日の市場動向と比較して評価
                evaluator.evaluate_scores(yesterday, today)
                
                report = evaluator.generate_evaluation_report(days=30)
                self._load_sentiment_status()

                msg = (
                    "市場動向の記録と評価が完了しました。\n\n"
                    f"方向性一致率(30日): {report['direction_accuracy']:.1f}%\n"
                    f"平均予測誤差: {report['avg_error']:.1f} 点\n"
                    f"Precision: {report['precision']:.2f}\n"
                    f"Recall: {report['recall']:.2f}\n"
                    f"サンプル数: {report['sample_size']}"
                )
                self.parent.after(0, lambda: messagebox.showinfo("完了", msg))
            except ImportError as e:
                self.parent.after(0, lambda: messagebox.showerror("エラー", f"必要なライブラリが不足しています:\n{e}"))
            except Exception as e:
                import traceback
                error_detail = traceback.format_exc()
                print(f"[ERROR] 市場動向記録・評価エラー: {e}")
                print(f"[ERROR] 詳細: {error_detail}")
                self.parent.after(0, lambda: messagebox.showerror("エラー", f"市場動向の記録・評価でエラーが発生しました:\n{e}\n\n詳細はコンソールを確認してください。"))
            finally:
                self._sentiment_running = False
                self.parent.after(0, lambda: self._set_sentiment_buttons_state("normal"))
                self.parent.after(0, lambda: self.status_var.set("状態: 待機中"))

        threading.Thread(target=task, daemon=True).start()

    def on_view_sentiment_scores(self):
        """保存済みスコア表示ウィンドウを開く"""
        try:
            from src.gui.sentiment_view_window import SentimentViewWindow
            # self.parentはFrameなので、その親ウィンドウ（Toplevelまたはルートウィンドウ）を取得
            root_window = self.parent.winfo_toplevel()
            view_window = SentimentViewWindow(root_window, self.db_path)
            view_window.transient(root_window)
            view_window.grab_set()
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            print(f"[ERROR] スコア表示ウィンドウエラー: {e}")
            print(f"[ERROR] 詳細: {error_detail}")
            messagebox.showerror("エラー", f"スコア表示ウィンドウの表示でエラーが発生しました:\n{e}")

    def _set_sentiment_buttons_state(self, state: str):
        """地合いスコア関連ボタンの状態をまとめて変更"""
        for btn in (self.sentiment_calc_button, self.sentiment_eval_button):
            if btn:
                btn.config(state=state)

    def _load_sentiment_status(self):
        """DBから最新の実行時刻を読み込み、ラベルに表示"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                calc_row = conn.execute(
                    """
                    SELECT calculated_at FROM sentiment_scores
                    ORDER BY calculated_at DESC
                    LIMIT 1
                    """
                ).fetchone()
                eval_row = conn.execute(
                    """
                    SELECT created_at FROM score_evaluations
                    ORDER BY created_at DESC
                    LIMIT 1
                    """
                ).fetchone()

            if calc_row and calc_row["calculated_at"]:
                self.sentiment_calc_status_var.set(f"最終実行: {calc_row['calculated_at'][:19]}")
            else:
                self.sentiment_calc_status_var.set("最終実行: 未実行")

            if eval_row and eval_row["created_at"]:
                self.sentiment_eval_status_var.set(f"最終実行: {eval_row['created_at'][:19]}")
            else:
                self.sentiment_eval_status_var.set("最終実行: 未実行")
        except Exception as e:
            print(f"[WARN] ステータス読み込みに失敗: {e}")

