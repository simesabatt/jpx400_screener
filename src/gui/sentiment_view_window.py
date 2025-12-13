"""
地合いスコア表示ウィンドウ

保存済みのスコアと評価結果を表示します。
"""

import tkinter as tk
from tkinter import ttk
from datetime import datetime, timedelta
import sqlite3
import json
from typing import List, Dict, Optional
from tkinter import messagebox

from src.sentiment.sentiment_evaluator import SentimentEvaluator


class SentimentViewWindow(tk.Toplevel):
    """地合いスコア表示ウィンドウ"""

    def __init__(self, parent, db_path: str):
        super().__init__(parent)
        
        self.db_path = db_path
        self.title("地合いスコア表示")
        self.geometry("1200x700")
        
        # テーブルが存在することを確認（作成）
        evaluator = SentimentEvaluator(db_path)
        evaluator._ensure_tables()
        
        self._build_ui()
        self._load_data()
        
    def _build_ui(self):
        """UI構築"""
        pad = 8
        
        # 上部コントロール
        control_frame = ttk.Frame(self)
        control_frame.pack(fill="x", padx=pad, pady=pad)
        
        ttk.Label(control_frame, text="表示期間:").pack(side="left", padx=pad)
        
        self.days_var = tk.IntVar(value=30)
        days_frame = ttk.Frame(control_frame)
        days_frame.pack(side="left", padx=pad)
        for days in [7, 30, 90, 365]:
            ttk.Radiobutton(
                days_frame,
                text=f"{days}日",
                variable=self.days_var,
                value=days,
                command=self._load_data
            ).pack(side="left", padx=2)
        
        ttk.Button(
            control_frame,
            text="更新",
            command=self._load_data
        ).pack(side="left", padx=pad)
        
        # タブ
        notebook = ttk.Notebook(self)
        notebook.pack(fill="both", expand=True, padx=pad, pady=pad)
        
        # タブ1: スコア一覧
        scores_frame = ttk.Frame(notebook)
        notebook.add(scores_frame, text="スコア一覧")
        self._build_scores_tab(scores_frame)
        
        # タブ2: 評価結果
        evaluations_frame = ttk.Frame(notebook)
        notebook.add(evaluations_frame, text="評価結果")
        self._build_evaluations_tab(evaluations_frame)
        
        # タブ3: 統計サマリー
        summary_frame = ttk.Frame(notebook)
        notebook.add(summary_frame, text="統計サマリー")
        self._build_summary_tab(summary_frame)
        
        # タブ4: パラメータ一覧
        params_frame = ttk.Frame(notebook)
        notebook.add(params_frame, text="パラメータ一覧")
        self._build_params_tab(params_frame)
    
    def _build_scores_tab(self, parent):
        """スコア一覧タブ"""
        pad = 8
        
        # ツリービュー
        tree_frame = ttk.Frame(parent)
        tree_frame.pack(fill="both", expand=True, padx=pad, pady=pad)
        
        scrollbar_y = ttk.Scrollbar(tree_frame, orient="vertical")
        scrollbar_x = ttk.Scrollbar(tree_frame, orient="horizontal")
        
        self.scores_tree = ttk.Treeview(
            tree_frame,
            columns=("date", "score", "level", "calculated_at", "weights_version"),
            show="headings",
            yscrollcommand=scrollbar_y.set,
            xscrollcommand=scrollbar_x.set
        )
        
        scrollbar_y.config(command=self.scores_tree.yview)
        scrollbar_x.config(command=self.scores_tree.xview)
        
        # 列設定
        self.scores_tree.heading("date", text="日付")
        self.scores_tree.heading("score", text="スコア")
        self.scores_tree.heading("level", text="レベル")
        self.scores_tree.heading("calculated_at", text="計算時刻")
        self.scores_tree.heading("weights_version", text="重み付けバージョン")
        
        self.scores_tree.column("date", width=100, anchor="center")
        self.scores_tree.column("score", width=80, anchor="e")
        self.scores_tree.column("level", width=100, anchor="center")
        self.scores_tree.column("calculated_at", width=180, anchor="center")
        self.scores_tree.column("weights_version", width=120, anchor="center")
        
        self.scores_tree.pack(side="left", fill="both", expand=True)
        scrollbar_y.pack(side="right", fill="y")
        scrollbar_x.pack(side="bottom", fill="x")
        
        # ダブルクリックで詳細表示
        self.scores_tree.bind("<Double-1>", self._on_score_double_click)
    
    def _build_evaluations_tab(self, parent):
        """評価結果タブ"""
        pad = 8
        
        # ツリービュー
        tree_frame = ttk.Frame(parent)
        tree_frame.pack(fill="both", expand=True, padx=pad, pady=pad)
        
        scrollbar_y = ttk.Scrollbar(tree_frame, orient="vertical")
        scrollbar_x = ttk.Scrollbar(tree_frame, orient="horizontal")
        
        self.eval_tree = ttk.Treeview(
            tree_frame,
            columns=("date", "predicted_score", "actual_strength", "prediction_error", 
                    "direction_match", "is_correct_positive", "is_false_positive"),
            show="headings",
            yscrollcommand=scrollbar_y.set,
            xscrollcommand=scrollbar_x.set
        )
        
        scrollbar_y.config(command=self.eval_tree.yview)
        scrollbar_x.config(command=self.eval_tree.xview)
        
        # 列設定
        self.eval_tree.heading("date", text="日付")
        self.eval_tree.heading("predicted_score", text="予測スコア")
        self.eval_tree.heading("actual_strength", text="実際の強さ")
        self.eval_tree.heading("prediction_error", text="予測誤差")
        self.eval_tree.heading("direction_match", text="方向一致")
        self.eval_tree.heading("is_correct_positive", text="正しく上昇予測")
        self.eval_tree.heading("is_false_positive", text="誤って上昇予測")
        
        self.eval_tree.column("date", width=100, anchor="center")
        self.eval_tree.column("predicted_score", width=100, anchor="e")
        self.eval_tree.column("actual_strength", width=100, anchor="e")
        self.eval_tree.column("prediction_error", width=100, anchor="e")
        self.eval_tree.column("direction_match", width=80, anchor="center")
        self.eval_tree.column("is_correct_positive", width=120, anchor="center")
        self.eval_tree.column("is_false_positive", width=120, anchor="center")
        
        self.eval_tree.pack(side="left", fill="both", expand=True)
        scrollbar_y.pack(side="right", fill="y")
        scrollbar_x.pack(side="bottom", fill="x")
    
    def _build_summary_tab(self, parent):
        """統計サマリータブ"""
        pad = 8
        
        # サマリー表示エリア
        summary_text = tk.Text(parent, wrap="word", font=("", 10))
        summary_text.pack(fill="both", expand=True, padx=pad, pady=pad)
        
        self.summary_text = summary_text
    
    def _load_data(self):
        """データを読み込み"""
        days = self.days_var.get()
        
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            
            # スコア一覧
            scores = conn.execute(
                """
                SELECT * FROM sentiment_scores
                WHERE date >= date('now', '-' || ? || ' days')
                ORDER BY date DESC, calculated_at DESC
                """,
                (days,)
            ).fetchall()
            
            # 評価結果
            evaluations = conn.execute(
                """
                SELECT * FROM score_evaluations
                WHERE date >= date('now', '-' || ? || ' days')
                ORDER BY date DESC
                """,
                (days,)
            ).fetchall()
            
            # 統計
            stats = conn.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    SUM(direction_match) AS matches,
                    AVG(prediction_error) AS avg_error,
                    SUM(is_correct_positive) AS tp,
                    SUM(is_false_positive) AS fp,
                    SUM(is_false_negative) AS fn,
                    SUM(is_correct_negative) AS tn
                FROM score_evaluations
                WHERE date >= date('now', '-' || ? || ' days')
                """,
                (days,)
            ).fetchone()
        
        # スコア一覧を更新
        for item in self.scores_tree.get_children():
            self.scores_tree.delete(item)
        
        for score in scores:
            tag = self._get_score_tag(score["total_score"])
            self.scores_tree.insert(
                "",
                "end",
                values=(
                    score["date"],
                    f"{score['total_score']:.1f}",
                    score["sentiment_level"],
                    score["calculated_at"][:19] if score["calculated_at"] else "",
                    score["weights_version"]
                ),
                tags=(tag,)
            )
        
        # タグで色設定
        self.scores_tree.tag_configure("very_good", foreground="blue")
        self.scores_tree.tag_configure("good", foreground="green")
        self.scores_tree.tag_configure("neutral", foreground="gray")
        self.scores_tree.tag_configure("bad", foreground="orange")
        self.scores_tree.tag_configure("very_bad", foreground="red")
        
        # 評価結果を更新
        for item in self.eval_tree.get_children():
            self.eval_tree.delete(item)
        
        for eval_row in evaluations:
            tag = "correct" if eval_row["direction_match"] else "incorrect"
            self.eval_tree.insert(
                "",
                "end",
                values=(
                    eval_row["date"],
                    f"{eval_row['predicted_score']:.1f}",
                    f"{eval_row['actual_market_strength']:.1f}",
                    f"{eval_row['prediction_error']:.1f}",
                    "○" if eval_row["direction_match"] else "×",
                    "○" if eval_row["is_correct_positive"] else "",
                    "○" if eval_row["is_false_positive"] else ""
                ),
                tags=(tag,)
            )
        
        self.eval_tree.tag_configure("correct", foreground="blue")
        self.eval_tree.tag_configure("incorrect", foreground="red")
        
        # 統計サマリーを更新
        if stats and stats["total"]:
            total = stats["total"]
            matches = stats["matches"] or 0
            avg_error = stats["avg_error"] or 0.0
            tp = stats["tp"] or 0
            fp = stats["fp"] or 0
            fn = stats["fn"] or 0
            tn = stats["tn"] or 0
            
            direction_accuracy = (matches / total * 100) if total > 0 else 0.0
            precision = (tp / (tp + fp) * 100) if (tp + fp) > 0 else 0.0
            recall = (tp / (tp + fn) * 100) if (tp + fn) > 0 else 0.0
            
            summary = f"""
【統計サマリー（過去{days}日間）】

■ 基本統計
  サンプル数: {total}件
  方向性一致率: {direction_accuracy:.1f}%
  平均予測誤差: {avg_error:.1f}点

■ Precision/Recall
  Precision（正しく上昇を予測した割合）: {precision:.1f}%
  Recall（実際に上昇したうち正しく予測した割合）: {recall:.1f}%

■ 混同行列
  正しく上昇を予測（TP）: {tp}件
  誤って上昇を予測（FP）: {fp}件
  誤って下落を予測（FN）: {fn}件
  正しく下落を予測（TN）: {tn}件

■ 評価
  {'✅ 良好' if direction_accuracy >= 60 else '⚠️ 要改善' if direction_accuracy >= 50 else '❌ 改善必要'}
"""
        else:
            summary = f"過去{days}日間の評価データがありません。\n\n市場動向記録・評価を実行してください。"
        
        self.summary_text.delete("1.0", "end")
        self.summary_text.insert("1.0", summary)
    
    def _build_params_tab(self, parent):
        """パラメータ一覧タブ"""
        pad = 8
        
        # 重み付けバージョン選択
        version_frame = ttk.Frame(parent)
        version_frame.pack(fill="x", padx=pad, pady=pad)
        
        ttk.Label(version_frame, text="重み付けバージョン:").pack(side="left", padx=pad)
        
        self.weights_version_var = tk.IntVar(value=1)
        version_combo = ttk.Combobox(version_frame, textvariable=self.weights_version_var, state="readonly", width=10)
        version_combo.pack(side="left", padx=pad)
        version_combo.bind("<<ComboboxSelected>>", lambda e: self._load_params())
        self.version_combo = version_combo
        
        ttk.Button(version_frame, text="更新", command=self._load_params).pack(side="left", padx=pad)
        ttk.Button(version_frame, text="自動調整(30日)", command=self._on_optimize_weights).pack(side="left", padx=pad)
        
        # パラメータ表示エリア
        params_text = tk.Text(parent, wrap="word", font=("", 10))
        params_text.pack(fill="both", expand=True, padx=pad, pady=pad)
        
        self.params_text = params_text
        
        # 初期読み込み
        self._load_params()

    def _on_optimize_weights(self):
        """評価結果に基づいて重みを自動調整"""
        try:
            evaluator = SentimentEvaluator(self.db_path)
            result = evaluator.optimize_weights(days=30, step=0.2)
            if not result:
                messagebox.showinfo("情報", "評価データが不足しているため、自動調整をスキップしました。")
                return

            new_version = result["new_version"]
            report = result.get("report", {})
            direction_accuracy = report.get("direction_accuracy", 0.0)
            avg_error = report.get("avg_error", 0.0)

            # 新バージョンを選択状態にして再読み込み
            self.weights_version_var.set(new_version)
            self._load_params()

            messagebox.showinfo(
                "完了",
                f"重みを自動調整しました。\n"
                f"新バージョン: {new_version}\n"
                f"方向性一致率(過去30日): {direction_accuracy:.1f}%\n"
                f"平均予測誤差: {avg_error:.1f}点"
            )
        except Exception as e:
            messagebox.showerror("エラー", f"重みの自動調整でエラーが発生しました:\n{e}")
    
    def _load_params(self):
        """パラメータを読み込み"""
        version = self.weights_version_var.get()
        
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            
            # 重み付けパラメータを取得
            weights_row = conn.execute(
                "SELECT * FROM sentiment_weights WHERE version = ?",
                (version,)
            ).fetchone()
            
            # 利用可能なバージョン一覧を取得
            versions = conn.execute(
                "SELECT DISTINCT version FROM sentiment_weights ORDER BY version DESC"
            ).fetchall()
            
            # バージョン一覧を更新
            if versions:
                version_list = [str(v[0]) for v in versions]
                self.version_combo['values'] = version_list
                if str(version) not in version_list and version_list:
                    self.weights_version_var.set(int(version_list[0]))
                    version = int(version_list[0])
                    weights_row = conn.execute(
                        "SELECT * FROM sentiment_weights WHERE version = ?",
                        (version,)
                    ).fetchone()
        
        # パラメータを表示
        if weights_row:
            try:
                weights = json.loads(weights_row['weights_json'])
                description = weights_row['description'] if 'description' in weights_row.keys() else ''
                created_at = weights_row['created_at'] if 'created_at' in weights_row.keys() else ''
                updated_at = weights_row['updated_at'] if 'updated_at' in weights_row.keys() else ''
                
                params_lines = [f"【重み付けパラメータ バージョン {version}】\n"]
                if description:
                    params_lines.append(f"説明: {description}\n")
                params_lines.append(f"作成日時: {created_at[:19] if created_at else 'N/A'}\n")
                if updated_at:
                    params_lines.append(f"更新日時: {updated_at[:19]}\n")
                params_lines.append("\n")
                
                # カテゴリ別に分類
                categories = {
                    "マクロ経済指標": ["nikkei225", "nikkei225_futures", "sp500", "sox", "usdjpy"],
                    "市場全体指標": ["advance_decline", "vix", "volume"],
                    "セクター指標": ["sector"],
                    "テクニカル指標": ["rsi", "moving_average", "bollinger_bands"],
                }
                
                for category, keys in categories.items():
                    params_lines.append(f"\n【{category}】\n")
                    for key in keys:
                        if key in weights:
                            weight = weights[key]
                            params_lines.append(f"  {key:25s}: {weight:5.2f}\n")
                
                # 未分類のパラメータ
                all_keys = set(weights.keys())
                categorized_keys = set()
                for keys in categories.values():
                    categorized_keys.update(keys)
                uncategorized = all_keys - categorized_keys
                
                if uncategorized:
                    params_lines.append(f"\n【その他】\n")
                    for key in sorted(uncategorized):
                        weight = weights[key]
                        params_lines.append(f"  {key:25s}: {weight:5.2f}\n")
                
                params_lines.append(f"\n【合計】\n")
                total_weight = sum(weights.values())
                params_lines.append(f"  合計重み: {total_weight:.2f}\n")
                
                self.params_text.delete("1.0", "end")
                self.params_text.insert("1.0", "".join(params_lines))
            except Exception as e:
                self.params_text.delete("1.0", "end")
                self.params_text.insert("1.0", f"パラメータの読み込みに失敗しました: {e}")
        else:
            # デフォルトの重み付けを表示
            default_weights = {
                "nikkei225": 1.0,
                "nikkei225_futures": 0.0,
                "sp500": 1.2,
                "sox": 1.0,
                "usdjpy": 0.8,
                "advance_decline": 1.0,
                "vix": 1.0,
                "volume": 0.6,
                "sector": 0.8,
                "rsi": 0.6,
                "moving_average": 0.8,
                "bollinger_bands": 0.5,
            }
            
            params_lines = [f"【重み付けパラメータ バージョン {version}（デフォルト）】\n"]
            params_lines.append("※DBに保存されていないため、デフォルト値を表示しています\n\n")
            
            categories = {
                "マクロ経済指標": ["nikkei225", "nikkei225_futures", "sp500", "sox", "usdjpy"],
                "市場全体指標": ["advance_decline", "vix", "volume"],
                "セクター指標": ["sector"],
                "テクニカル指標": ["rsi", "moving_average", "bollinger_bands"],
            }
            
            for category, keys in categories.items():
                params_lines.append(f"\n【{category}】\n")
                for key in keys:
                    if key in default_weights:
                        weight = default_weights[key]
                        params_lines.append(f"  {key:25s}: {weight:5.2f}\n")
            
            params_lines.append(f"\n【合計】\n")
            total_weight = sum(default_weights.values())
            params_lines.append(f"  合計重み: {total_weight:.2f}\n")
            
            self.params_text.delete("1.0", "end")
            self.params_text.insert("1.0", "".join(params_lines))
    
    def _get_score_tag(self, score: float) -> str:
        """スコアからタグを取得"""
        if score >= 80:
            return "very_good"
        if score >= 60:
            return "good"
        if score >= 40:
            return "neutral"
        if score >= 20:
            return "bad"
        return "very_bad"
    
    def _on_score_double_click(self, event):
        """スコアのダブルクリックで詳細表示"""
        selection = self.scores_tree.selection()
        if not selection:
            return
        
        item = self.scores_tree.item(selection[0])
        date_str = item["values"][0]
        
        # 詳細ウィンドウを開く
        self._show_score_detail(date_str)
    
    def _show_score_detail(self, date_str: str):
        """スコア詳細を表示"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            score = conn.execute(
                """
                SELECT * FROM sentiment_scores
                WHERE date = ?
                ORDER BY calculated_at DESC
                LIMIT 1
                """,
                (date_str,)
            ).fetchone()
        
        if not score:
            return
        
        # 詳細ウィンドウ
        detail_window = tk.Toplevel(self)
        detail_window.title(f"スコア詳細 - {date_str}")
        detail_window.geometry("800x600")
        
        # 基本情報
        info_frame = ttk.LabelFrame(detail_window, text="基本情報", padding=8)
        info_frame.pack(fill="x", padx=8, pady=8)
        
        info_text = f"""
日付: {score['date']}
総合スコア: {score['total_score']:.1f}点
地合いレベル: {score['sentiment_level']}
計算時刻: {score['calculated_at']}
重み付けバージョン: {score['weights_version']}
"""
        ttk.Label(info_frame, text=info_text.strip(), font=("", 10), justify="left").pack(anchor="w")
        
        # 指標詳細
        indicators_frame = ttk.LabelFrame(detail_window, text="指標詳細", padding=8)
        indicators_frame.pack(fill="both", expand=True, padx=8, pady=8)
        
        scrollbar = ttk.Scrollbar(indicators_frame)
        scrollbar.pack(side="right", fill="y")
        
        indicators_text = tk.Text(indicators_frame, wrap="word", font=("", 9), yscrollcommand=scrollbar.set)
        scrollbar.config(command=indicators_text.yview)
        indicators_text.pack(side="left", fill="both", expand=True)
        
        try:
            indicators = json.loads(score['indicators_json'])
            detail_lines = []
            for key, data in indicators.items():
                value = data.get("value")
                change_pct = data.get("change_pct")
                score_val = data.get("score", 0.0)
                weight = data.get("weight", 1.0)
                weighted_score = data.get("weighted_score", 0.0)
                
                value_str = f"{value:.2f}" if value is not None else "N/A"
                change_str = f"{change_pct:+.2f}%" if change_pct is not None else "N/A"
                
                detail_lines.append(
                    f"{key:20s}: 値={value_str:10s} 変化={change_str:8s} "
                    f"スコア={score_val:6.1f} 重み={weight:.1f} 重み付きスコア={weighted_score:6.1f}"
                )
            
            indicators_text.insert("1.0", "\n".join(detail_lines))
        except Exception as e:
            indicators_text.insert("1.0", f"指標データの解析に失敗しました: {e}")

