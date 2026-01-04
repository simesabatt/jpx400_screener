"""
市況タブモジュール

セクター資金流動分析の表示機能を提供します。

Copyright (c) 2025 JPX400スクリーニングシステム

This software is licensed under the MIT License.
See LICENSE file for details.
"""

import threading
import tkinter as tk
from tkinter import ttk, messagebox
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Tuple
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk
from matplotlib.figure import Figure
import matplotlib.dates as mdates
import numpy as np
import warnings
import webbrowser

# 日本語フォント設定（モジュール読み込み時に設定）
plt.rcParams['font.sans-serif'] = ['MS Gothic', 'Yu Gothic', 'Meiryo', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False
# フォント警告を抑制
warnings.filterwarnings('ignore', category=UserWarning, module='matplotlib')


class MarketConditionsTab:
    """市況タブのUIとハンドラを管理するクラス"""
    
    def __init__(
        self,
        parent: ttk.Frame,
        db_path: str,
        status_var: tk.StringVar
    ):
        """
        初期化
        
        Args:
            parent: 親フレーム（市況タブ）
            db_path: データベースパス
            status_var: ステータス表示用のStringVar
        """
        self.parent = parent
        self.db_path = db_path
        self.status_var = status_var
        
        # 実行中フラグ
        self._analyzing = False
        
        # データ保持用
        self.current_flow_df: Optional[pd.DataFrame] = None
        self.current_share_df: Optional[pd.DataFrame] = None
        self.current_change_df: Optional[pd.DataFrame] = None
        self.current_flow_per_stock_df: Optional[pd.DataFrame] = None
        
        # グラフウィンドウの参照
        self.chart_window = None
        
        # UI構築
        self._build_ui()
    
    def _build_ui(self):
        """市況タブのUI構築"""
        pad = 8
        
        # コントロールフレーム
        control_frame = ttk.LabelFrame(self.parent, text="セクター資金流動分析", padding=pad)
        control_frame.pack(fill="x", pady=pad, padx=pad)
        
        # 期間選択
        period_frame = ttk.Frame(control_frame)
        period_frame.pack(fill="x", pady=(0, pad))
        
        ttk.Label(period_frame, text="表示期間:").pack(side="left", padx=pad)
        
        self.days_var = tk.StringVar(value="all")  # "all"で全期間を表す
        ttk.Radiobutton(
            period_frame,
            text="全期間",
            variable=self.days_var,
            value="all",
            command=self._on_period_changed
        ).pack(side="left", padx=2)
        for days in [7, 30, 60, 90, 180]:
            ttk.Radiobutton(
                period_frame,
                text=f"{days}日",
                variable=self.days_var,
                value=str(days),
                command=self._on_period_changed
            ).pack(side="left", padx=2)
        
        # グラフタイプ選択（新しい行に配置）
        chart_type_frame = ttk.Frame(control_frame)
        chart_type_frame.pack(fill="x", pady=(0, pad))
        
        ttk.Label(chart_type_frame, text="グラフタイプ:").pack(side="left", padx=pad)
        
        self.chart_type_var = tk.StringVar(value="flow")
        ttk.Radiobutton(
            chart_type_frame,
            text="売買代金",
            variable=self.chart_type_var,
            value="flow",
            command=self._on_chart_type_changed
        ).pack(side="left", padx=2)
        
        ttk.Radiobutton(
            chart_type_frame,
            text="前日比",
            variable=self.chart_type_var,
            value="change",
            command=self._on_chart_type_changed
        ).pack(side="left", padx=2)
        
        ttk.Radiobutton(
            chart_type_frame,
            text="移動平均",
            variable=self.chart_type_var,
            value="moving_average",
            command=self._on_chart_type_changed
        ).pack(side="left", padx=2)
        
        ttk.Radiobutton(
            chart_type_frame,
            text="積み上げ棒グラフ",
            variable=self.chart_type_var,
            value="stacked_bar",
            command=self._on_chart_type_changed
        ).pack(side="left", padx=2)
        
        ttk.Radiobutton(
            chart_type_frame,
            text="シェア（積み上げ）",
            variable=self.chart_type_var,
            value="share",
            command=self._on_chart_type_changed
        ).pack(side="left", padx=2)
        
        ttk.Radiobutton(
            chart_type_frame,
            text="1銘柄あたり売買代金",
            variable=self.chart_type_var,
            value="flow_per_stock",
            command=self._on_chart_type_changed
        ).pack(side="left", padx=2)
        
        ttk.Radiobutton(
            chart_type_frame,
            text="1銘柄あたり売買代金（移動平均）",
            variable=self.chart_type_var,
            value="flow_per_stock_ma",
            command=self._on_chart_type_changed
        ).pack(side="left", padx=2)
        
        # 移動平均期間選択（新しい行に配置）
        ma_frame = ttk.Frame(control_frame)
        ma_frame.pack(fill="x", pady=(0, pad))
        
        ttk.Label(ma_frame, text="移動平均期間:").pack(side="left", padx=pad)
        self.ma_period_var = tk.IntVar(value=20)
        for period in [5, 10, 20, 30, 60]:
            ttk.Radiobutton(
                ma_frame,
                text=f"{period}日",
                variable=self.ma_period_var,
                value=period,
                command=self._on_period_changed
            ).pack(side="left", padx=2)
        
        # ボタンフレーム
        button_frame = ttk.Frame(control_frame)
        button_frame.pack(fill="x", pady=(0, pad))
        
        self.analyze_button = ttk.Button(
            button_frame,
            text="分析実行",
            command=self.on_analyze,
            width=20
        )
        self.analyze_button.pack(side="left", padx=pad)
        
        ttk.Label(
            button_frame,
            text="※ 初回実行時は時間がかかります（全銘柄のデータを取得します）",
            font=("", 9),
            foreground="gray"
        ).pack(side="left", padx=pad)
        
        # セクター別銘柄数表示フレーム
        sector_count_frame = ttk.LabelFrame(control_frame, text="セクター別登録銘柄数", padding=pad)
        sector_count_frame.pack(fill="x", pady=pad)
        
        # 財務指標取得ボタンと説明文のフレーム
        info_frame = ttk.Frame(sector_count_frame)
        info_frame.pack(fill="x", pady=(0, pad))
        
        # 財務指標取得ボタン
        fetch_metrics_button = ttk.Button(
            info_frame,
            text="財務指標を取得",
            command=self._on_fetch_financial_metrics
        )
        fetch_metrics_button.pack(side="left", padx=(0, pad))
        
        # 最新実施日時を表示するラベル
        self.last_fetch_time_label = ttk.Label(
            info_frame,
            text="最新実施: 未実行",
            font=("", 9),
            foreground="gray"
        )
        self.last_fetch_time_label.pack(side="left", padx=(0, pad))
        
        # 最新実施日時を読み込んで表示
        self._update_last_fetch_time_display()
        
        # 説明文を追加
        info_label = ttk.Label(
            info_frame,
            text="※ セクター行をダブルクリックすると、そのセクターの銘柄一覧が表示されます\n※ 業種行をダブルクリックすると、そのセクター・業種の銘柄一覧が表示されます\n※ セクター行をクリックすると、業種が展開/折りたたみされます",
            font=("", 9),
            foreground="gray"
        )
        info_label.pack(side="left", fill="x", expand=True)
        
        # セクター別銘柄数のTreeview（アコーディオン表示用）
        count_tree_frame = ttk.Frame(sector_count_frame)
        count_tree_frame.pack(fill="both", expand=True)
        
        count_v_scrollbar = ttk.Scrollbar(count_tree_frame, orient="vertical")
        count_v_scrollbar.pack(side="right", fill="y")
        
        count_columns = ("セクター", "業種", "銘柄数", "平均PER", "平均PBR", "平均利回り", "平均ROA", "平均ROE", "平均NC比率")
        self.sector_count_tree = ttk.Treeview(
            count_tree_frame,
            columns=count_columns,
            show="tree headings",
            yscrollcommand=count_v_scrollbar.set,
            height=15,  # セクター全体が表示できるように高さを増やす
            selectmode="browse"
        )
        self.sector_count_tree.pack(side="left", fill="both", expand=True)
        
        count_v_scrollbar.config(command=self.sector_count_tree.yview)
        
        # 水平スクロールバーを追加
        count_h_scrollbar = ttk.Scrollbar(count_tree_frame, orient="horizontal")
        count_h_scrollbar.pack(side="bottom", fill="x")
        self.sector_count_tree.config(xscrollcommand=count_h_scrollbar.set)
        count_h_scrollbar.config(command=self.sector_count_tree.xview)
        
        # 列の設定
        self.sector_count_tree.column("#0", width=20, stretch=False)  # ツリーアイコン用
        self.sector_count_tree.column("セクター", width=150, anchor="w")
        self.sector_count_tree.column("業種", width=120, anchor="w")
        self.sector_count_tree.column("銘柄数", width=100, anchor="e")
        self.sector_count_tree.column("平均PER", width=80, anchor="e")
        self.sector_count_tree.column("平均PBR", width=80, anchor="e")
        self.sector_count_tree.column("平均利回り", width=90, anchor="e")
        self.sector_count_tree.column("平均ROA", width=80, anchor="e")
        self.sector_count_tree.column("平均ROE", width=80, anchor="e")
        self.sector_count_tree.column("平均NC比率", width=100, anchor="e")
        
        for col in count_columns:
            self.sector_count_tree.heading(col, text=col)
        
        # 初期状態では空のメッセージを表示
        self.sector_count_tree.insert("", "end", values=("分析実行後に表示されます", "", "", "", "", "", "", "", ""))
        
        # 展開/折りたたみイベントを追加（セクター行の展開時に業種を表示）
        self.sector_count_tree.bind("<<TreeviewOpen>>", self._on_sector_count_open)
        
        # ダブルクリックイベントを追加（別ウィンドウ表示用）
        self.sector_count_tree.bind("<Double-1>", self._on_sector_count_double_click)
        
        # グラフ表示フレーム
        self.chart_frame = ttk.Frame(self.parent)
        self.chart_frame.pack(fill="both", expand=True, padx=pad, pady=pad)
        
        # 初期状態ではグラフを表示しない
        self._show_placeholder()
        
        # セクター別銘柄数を自動読み込み
        self._load_sector_counts_on_init()
    
    def _load_sector_counts_on_init(self):
        """タブ初期化時にセクター別銘柄数を読み込む"""
        def load_in_thread():
            try:
                from src.sentiment.sector_flow_analyzer import SectorFlowAnalyzer
                
                analyzer = SectorFlowAnalyzer(self.db_path)
                sector_count_df = analyzer.get_sector_stock_counts()
                
                # セクター・業種別銘柄数も読み込む
                sector_industry_count_df = analyzer.get_sector_industry_stock_counts()
                
                # セクター別銘柄数をアコーディオン表示
                if not sector_count_df.empty and not sector_industry_count_df.empty:
                    self.parent.after(0, lambda: self._display_sector_counts_with_industries(
                        sector_count_df, sector_industry_count_df
                    ))
                elif not sector_count_df.empty:
                    self.parent.after(0, lambda: self._display_sector_counts(sector_count_df))
            except Exception as e:
                print(f"[ERROR] セクター別銘柄数読み込みエラー: {e}")
                import traceback
                traceback.print_exc()
        
        thread = threading.Thread(target=load_in_thread, daemon=True)
        thread.start()
    
    def _show_placeholder(self):
        """プレースホルダーを表示"""
        for widget in self.chart_frame.winfo_children():
            widget.destroy()
        
        placeholder = ttk.Label(
            self.chart_frame,
            text="「分析実行」ボタンをクリックしてセクター資金流動を分析してください\n（グラフは別ウィンドウで表示されます）",
            font=("", 12),
            foreground="gray",
            justify="center"
        )
        placeholder.pack(expand=True)
    
    def _on_period_changed(self):
        """期間が変更されたときの処理"""
        if self.current_flow_df is not None:
            # 既にデータがある場合は再表示
            self._display_chart()
    
    def _on_chart_type_changed(self):
        """グラフタイプが変更されたときの処理"""
        if self.current_flow_df is not None:
            # 既にデータがある場合は再表示
            self._display_chart()
        
        # 移動平均選択時のみ移動平均期間選択を有効化
        chart_type = self.chart_type_var.get()
        if chart_type == "moving_average" or chart_type == "flow_per_stock_ma":
            for widget in self.parent.winfo_children():
                if isinstance(widget, ttk.Frame):
                    for child in widget.winfo_children():
                        if isinstance(child, ttk.LabelFrame):
                            for grandchild in child.winfo_children():
                                if isinstance(grandchild, ttk.Frame):
                                    # 移動平均期間選択のラジオボタンを有効化
                                    for rb in grandchild.winfo_children():
                                        if isinstance(rb, ttk.Radiobutton):
                                            rb.config(state="normal")
    
    def is_running(self) -> bool:
        """分析処理が実行中かどうかを返す"""
        return self._analyzing
    
    def on_analyze(self):
        """分析実行ボタンのハンドラ"""
        if self._analyzing:
            messagebox.showwarning("警告", "分析が実行中です。", parent=self.parent)
            return
        
        def task():
            try:
                self._analyzing = True
                self.analyze_button.config(state="disabled")
                self.status_var.set("状態: セクター資金流動分析中...")
                
                from src.sentiment.sector_flow_analyzer import SectorFlowAnalyzer
                
                analyzer = SectorFlowAnalyzer(self.db_path)
                days_value = self.days_var.get()
                
                # セクター別銘柄数を取得
                self.status_var.set("状態: セクター別銘柄数を取得中...")
                sector_count_df = analyzer.get_sector_stock_counts()
                
                # セクター・業種別銘柄数も取得
                sector_industry_count_df = analyzer.get_sector_industry_stock_counts()
                
                # セクター別銘柄数をアコーディオン表示
                if not sector_count_df.empty and not sector_industry_count_df.empty:
                    self.parent.after(0, lambda: self._display_sector_counts_with_industries(
                        sector_count_df, sector_industry_count_df
                    ))
                elif not sector_count_df.empty:
                    self.parent.after(0, lambda: self._display_sector_counts(sector_count_df))
                
                # データを取得
                if days_value == "all":
                    self.status_var.set("状態: データ取得中...（全期間）")
                    flow_df, change_df = analyzer.calculate_sector_flow_with_change(days=None)
                    share_df = analyzer.calculate_sector_share(days=None)
                    flow_per_stock_df = analyzer.calculate_sector_flow_per_stock(days=None)
                else:
                    days = int(days_value)
                    self.status_var.set(f"状態: データ取得中...（{days}日分）")
                    flow_df, change_df = analyzer.calculate_sector_flow_with_change(days=days)
                    share_df = analyzer.calculate_sector_share(days=days)
                    flow_per_stock_df = analyzer.calculate_sector_flow_per_stock(days=days)
                
                if flow_df.empty:
                    self.parent.after(0, lambda: messagebox.showwarning(
                        "警告",
                        "データが取得できませんでした。\n日足データが不足している可能性があります。",
                        parent=self.parent
                    ))
                    return
                
                # データを保持
                self.current_flow_df = flow_df
                self.current_share_df = share_df
                self.current_change_df = change_df
                self.current_flow_per_stock_df = flow_per_stock_df
                
                # グラフを表示
                self.parent.after(0, self._display_chart)
                
                self.status_var.set("状態: 分析完了")
                
            except Exception as e:
                import traceback
                error_detail = traceback.format_exc()
                print(f"[ERROR] セクター資金流動分析エラー: {e}")
                print(f"[ERROR] 詳細: {error_detail}")
                self.parent.after(0, lambda: messagebox.showerror(
                    "エラー",
                    f"分析でエラーが発生しました:\n{e}\n\n詳細はコンソールを確認してください。",
                    parent=self.parent
                ))
            finally:
                self._analyzing = False
                self.parent.after(0, lambda: self.analyze_button.config(state="normal"))
                self.parent.after(0, lambda: self.status_var.set("状態: 待機中"))
        
        thread = threading.Thread(target=task, daemon=True)
        thread.start()
    
    def _display_sector_counts(self, sector_count_df: pd.DataFrame):
        """セクター別銘柄数をTreeviewに表示（業種情報なしの場合、財務指標付き）"""
        # 既存のデータをクリア
        for item in self.sector_count_tree.get_children():
            self.sector_count_tree.delete(item)
        
        # 財務指標を取得
        try:
            from src.sentiment.sector_flow_analyzer import SectorFlowAnalyzer
            analyzer = SectorFlowAnalyzer(self.db_path)
            sector_metrics_df = analyzer.get_sector_financial_metrics()
            
            # デバッグ情報
            print(f"[DEBUG] セクター別財務指標: {len(sector_metrics_df)}件")
            if not sector_metrics_df.empty:
                print(f"[DEBUG] セクター別財務指標の列: {sector_metrics_df.columns.tolist()}")
                print(f"[DEBUG] サンプルデータ（最初の3行）:")
                print(sector_metrics_df.head(3))
            
            # セクター別財務指標を辞書に変換
            sector_metrics_dict = {}
            if not sector_metrics_df.empty:
                for _, row in sector_metrics_df.iterrows():
                    sector_metrics_dict[row['sector']] = {
                        'avg_per': row.get('avg_per'),
                        'avg_pbr': row.get('avg_pbr'),
                        'avg_dividend_yield': row.get('avg_dividend_yield'),
                        'avg_roa': row.get('avg_roa'),
                        'avg_roe': row.get('avg_roe'),
                        'avg_net_cash_ratio': row.get('avg_net_cash_ratio')
                    }
                    # NC比率のデバッグ情報
                    if row.get('avg_net_cash_ratio') is not None:
                        print(f"[DEBUG] {row['sector']}の平均NC比率: {row.get('avg_net_cash_ratio')}")
                print(f"[DEBUG] セクター別財務指標辞書: {len(sector_metrics_dict)}件")
            else:
                print("[DEBUG] セクター別財務指標データが空です")
        except Exception as e:
            print(f"[ERROR] 財務指標取得エラー: {e}")
            import traceback
            traceback.print_exc()
            sector_metrics_dict = {}
        
        # 財務指標をフォーマットする関数
        def format_metric(value, is_percent=False, decimals=2):
            if value is None or pd.isna(value):
                return "-"
            if is_percent:
                return f"{value:.{decimals}f}%"
            else:
                return f"{value:.{decimals}f}"
        
        # データを挿入
        for _, row in sector_count_df.iterrows():
            sector = row['sector']
            count = int(row['count'])
            
            # セクターの財務指標を取得
            sector_metrics = sector_metrics_dict.get(sector, {})
            
            self.sector_count_tree.insert(
                "",
                "end",
                values=(
                    sector,
                    "",
                    f"{count:,}",
                    format_metric(sector_metrics.get('avg_per'), decimals=1),
                    format_metric(sector_metrics.get('avg_pbr'), decimals=2),
                    format_metric(sector_metrics.get('avg_dividend_yield'), is_percent=True, decimals=2),
                    format_metric(sector_metrics.get('avg_roa'), is_percent=True, decimals=2),
                    format_metric(sector_metrics.get('avg_roe'), is_percent=True, decimals=2),
                    format_metric(sector_metrics.get('avg_net_cash_ratio'), decimals=4)
                ),
                tags=("sector",)
            )
        
        # セクター行のスタイル設定
        self.sector_count_tree.tag_configure("sector", font=("", 9, "bold"))
    
    def _display_sector_counts_with_industries(
        self, 
        sector_count_df: pd.DataFrame, 
        sector_industry_count_df: pd.DataFrame
    ):
        """セクター別銘柄数をアコーディオン表示（業種を含む、財務指標付き）"""
        # 既存のデータをクリア
        for item in self.sector_count_tree.get_children():
            self.sector_count_tree.delete(item)
        
        # 財務指標を取得
        try:
            from src.sentiment.sector_flow_analyzer import SectorFlowAnalyzer
            analyzer = SectorFlowAnalyzer(self.db_path)
            sector_metrics_df = analyzer.get_sector_financial_metrics()
            sector_industry_metrics_df = analyzer.get_sector_industry_financial_metrics()
            
            # デバッグ情報
            print(f"[DEBUG] セクター別財務指標: {len(sector_metrics_df)}件")
            print(f"[DEBUG] セクター・業種別財務指標: {len(sector_industry_metrics_df)}件")
            
            # セクター別財務指標を辞書に変換
            sector_metrics_dict = {}
            if not sector_metrics_df.empty:
                for _, row in sector_metrics_df.iterrows():
                    sector_metrics_dict[row['sector']] = {
                        'avg_per': row.get('avg_per'),
                        'avg_pbr': row.get('avg_pbr'),
                        'avg_dividend_yield': row.get('avg_dividend_yield'),
                        'avg_roa': row.get('avg_roa'),
                        'avg_roe': row.get('avg_roe'),
                        'avg_net_cash_ratio': row.get('avg_net_cash_ratio')
                    }
                print(f"[DEBUG] セクター別財務指標辞書: {len(sector_metrics_dict)}件")
            else:
                print("[DEBUG] セクター別財務指標データが空です")
            
            # セクター・業種別財務指標を辞書に変換
            sector_industry_metrics_dict = {}
            if not sector_industry_metrics_df.empty:
                for _, row in sector_industry_metrics_df.iterrows():
                    key = (row['sector'], row['industry'])
                    sector_industry_metrics_dict[key] = {
                        'avg_per': row.get('avg_per'),
                        'avg_pbr': row.get('avg_pbr'),
                        'avg_dividend_yield': row.get('avg_dividend_yield'),
                        'avg_roe': row.get('avg_roe'),
                        'avg_net_cash_ratio': row.get('avg_net_cash_ratio')
                    }
                print(f"[DEBUG] セクター・業種別財務指標辞書: {len(sector_industry_metrics_dict)}件")
            else:
                print("[DEBUG] セクター・業種別財務指標データが空です")
        except Exception as e:
            print(f"[ERROR] 財務指標取得エラー: {e}")
            import traceback
            traceback.print_exc()
            sector_metrics_dict = {}
            sector_industry_metrics_dict = {}
        
        # 財務指標をフォーマットする関数
        def format_metric(value, is_percent=False, decimals=2):
            if value is None or pd.isna(value):
                return "-"
            if is_percent:
                return f"{value:.{decimals}f}%"
            else:
                return f"{value:.{decimals}f}"
        
        # セクター・業種別データをセクターごとにグループ化
        sector_industry_dict: Dict[str, List[Tuple[str, int]]] = {}
        for _, row in sector_industry_count_df.iterrows():
            sector = row['sector']
            industry = row['industry']
            count = int(row['count'])
            if sector not in sector_industry_dict:
                sector_industry_dict[sector] = []
            sector_industry_dict[sector].append((industry, count))
        
        # セクターごとにソート（銘柄数の降順）
        for sector in sector_industry_dict:
            sector_industry_dict[sector].sort(key=lambda x: x[1], reverse=True)
        
        # セクター別データを挿入（親アイテム）
        sector_items = {}  # セクター名 -> アイテムIDのマッピング
        for _, row in sector_count_df.iterrows():
            sector = row['sector']
            count = int(row['count'])
            
            # セクターの財務指標を取得
            sector_metrics = sector_metrics_dict.get(sector, {})
            
            # セクター行を親アイテムとして挿入
            sector_item = self.sector_count_tree.insert(
                "", "end", 
                text="",  # ツリーアイコン用
                values=(
                    sector,
                    "",
                    f"{count:,}",
                    format_metric(sector_metrics.get('avg_per'), decimals=1),
                    format_metric(sector_metrics.get('avg_pbr'), decimals=2),
                    format_metric(sector_metrics.get('avg_dividend_yield'), is_percent=True, decimals=2),
                    format_metric(sector_metrics.get('avg_roa'), is_percent=True, decimals=2),
                    format_metric(sector_metrics.get('avg_roe'), is_percent=True, decimals=2),
                    format_metric(sector_metrics.get('avg_net_cash_ratio'), decimals=4)
                ),
                tags=("sector",)
            )
            sector_items[sector] = sector_item
            
            # 業種は+アイコンをクリックしたときに表示されるため、初期表示では追加しない
            # ただし、+アイコンを表示するためにダミー子アイテムを追加
            if sector in sector_industry_dict:
                # ダミー子アイテムを追加して+アイコンを表示
                self.sector_count_tree.insert(
                    sector_item, "end",
                    text="",
                    values=("", "", "", "", "", "", "", "", ""),
                    tags=("dummy",)
                )
        
        # セクター行のスタイル設定
        self.sector_count_tree.tag_configure("sector", font=("", 9, "bold"))
        self.sector_count_tree.tag_configure("industry", font=("", 9))
        self.sector_count_tree.tag_configure("stock", font=("", 8))  # 銘柄行は少し小さめのフォント
        
        # セクター数に応じてTreeviewの高さを動的に調整（セクター全体が表示できるように）
        sector_count = len(sector_count_df)
        # セクター数 + ヘッダー行 + 余裕を持たせる
        optimal_height = min(max(sector_count + 2, 10), 20)  # 最小10行、最大20行
        self.sector_count_tree.config(height=optimal_height)
    
    def _display_chart(self):
        """グラフを別ウィンドウで表示"""
        if self.current_flow_df is None or self.current_flow_df.empty:
            return
        
        # 既存のグラフウィンドウがあれば閉じる
        if self.chart_window is not None:
            try:
                if self.chart_window.winfo_exists():
                    self.chart_window.destroy()
            except:
                pass
            self.chart_window = None
        
        # 新しいグラフウィンドウを作成
        self.chart_window = tk.Toplevel(self.parent)
        self.chart_window.title("セクター資金流動分析 - グラフ")
        self.chart_window.geometry("1400x800")
        
        # ウィンドウが閉じられたときの処理
        def on_window_close():
            try:
                if self.chart_window and self.chart_window.winfo_exists():
                    self.chart_window.destroy()
            except:
                pass
            finally:
                self.chart_window = None
        
        self.chart_window.protocol("WM_DELETE_WINDOW", on_window_close)
        
        # グラフフレーム
        chart_frame = ttk.Frame(self.chart_window)
        chart_frame.pack(fill="both", expand=True, padx=8, pady=8)
        
        # 日本語フォント設定
        plt.rcParams['font.sans-serif'] = ['MS Gothic', 'Yu Gothic', 'Meiryo', 'DejaVu Sans']
        plt.rcParams['axes.unicode_minus'] = False
        
        chart_type = self.chart_type_var.get()
        
        # グラフを作成（大きめのサイズ）
        fig = Figure(figsize=(16, 10), dpi=100)
        ax = fig.add_subplot(111)
        
        if chart_type == "flow":
            self._plot_flow_chart(ax, self.current_flow_df)
        elif chart_type == "share":
            self._plot_share_chart(ax, self.current_share_df)
        elif chart_type == "change":
            self._plot_change_chart(ax, self.current_change_df)
        elif chart_type == "moving_average":
            ma_period = self.ma_period_var.get()
            self._plot_moving_average_chart(ax, self.current_flow_df, ma_period)
        elif chart_type == "stacked_bar":
            self._plot_stacked_bar_chart(ax, self.current_flow_df)
        elif chart_type == "flow_per_stock":
            if self.current_flow_per_stock_df is not None and not self.current_flow_per_stock_df.empty:
                self._plot_flow_per_stock_chart(ax, self.current_flow_per_stock_df)
            else:
                ax.text(0.5, 0.5, "データが取得できませんでした", 
                       ha='center', va='center', transform=ax.transAxes, fontsize=14)
        elif chart_type == "flow_per_stock_ma":
            if self.current_flow_per_stock_df is not None and not self.current_flow_per_stock_df.empty:
                ma_period = self.ma_period_var.get()
                self._plot_moving_average_chart(ax, self.current_flow_per_stock_df, ma_period, is_per_stock=True)
            else:
                ax.text(0.5, 0.5, "データが取得できませんでした", 
                       ha='center', va='center', transform=ax.transAxes, fontsize=14)
        
        # Canvasに配置
        canvas = FigureCanvasTkAgg(fig, chart_frame)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="both", expand=True)
        
        # ツールバーを追加
        toolbar = NavigationToolbar2Tk(canvas, chart_frame)
        toolbar.update()
    
    def _plot_flow_chart(self, ax, df: pd.DataFrame):
        """売買代金の線グラフを描画"""
        # 主要セクターのみを表示（上位10セクター）
        if len(df.columns) > 10:
            # 最新日の売買代金でソート
            latest_values = df.iloc[-1].sort_values(ascending=False)
            top_sectors = latest_values.head(10).index.tolist()
            df_plot = df[top_sectors]
        else:
            df_plot = df
        
        # 線グラフを描画
        for sector in df_plot.columns:
            ax.plot(df_plot.index, df_plot[sector], label=sector, linewidth=2, marker='o', markersize=3)
        
        ax.set_xlabel("日付", fontsize=10)
        ax.set_ylabel("売買代金（億円）", fontsize=10)
        ax.set_title("セクター別売買代金推移", fontsize=12, fontweight="bold")
        ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=8)
        ax.grid(True, alpha=0.3)
        
        # 日付フォーマット（毎月1日）
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        ax.xaxis.set_major_locator(mdates.MonthLocator(bymonthday=1))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
        
        fig = ax.get_figure()
        if fig:
            fig.tight_layout()
    
    def _plot_share_chart(self, ax, df: pd.DataFrame):
        """シェアの積み上げエリアチャートを描画"""
        # 主要セクターのみを表示（上位10セクター）
        if len(df.columns) > 10:
            # 最新日のシェアでソート
            latest_values = df.iloc[-1].sort_values(ascending=False)
            top_sectors = latest_values.head(10).index.tolist()
            df_plot = df[top_sectors]
            # その他を追加
            df_plot['その他'] = df.drop(columns=top_sectors).sum(axis=1)
        else:
            df_plot = df
        
        # 積み上げエリアチャートを描画
        ax.stackplot(df_plot.index, *[df_plot[col] for col in df_plot.columns], labels=df_plot.columns, alpha=0.7)
        
        ax.set_xlabel("日付", fontsize=10)
        ax.set_ylabel("シェア（%）", fontsize=10)
        ax.set_title("セクター別売買代金シェア推移", fontsize=12, fontweight="bold")
        ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=8)
        ax.grid(True, alpha=0.3)
        ax.set_ylim(0, 100)
        
        # 日付フォーマット（毎月1日）
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        ax.xaxis.set_major_locator(mdates.MonthLocator(bymonthday=1))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
        
        fig = ax.get_figure()
        if fig:
            fig.tight_layout()
    
    def _plot_moving_average_chart(self, ax, df: pd.DataFrame, ma_period: int, is_per_stock: bool = False):
        """移動平均の線グラフを描画"""
        # 主要セクターのみを表示（上位10セクター）
        if len(df.columns) > 10:
            # 最新日の値でソート
            latest_values = df.iloc[-1].sort_values(ascending=False)
            top_sectors = latest_values.head(10).index.tolist()
            df_plot = df[top_sectors]
        else:
            df_plot = df
        
        # 移動平均を計算
        df_ma = df_plot.rolling(window=ma_period, min_periods=1).mean()
        
        # 元のデータを薄い線で表示（背景として）
        for sector in df_plot.columns:
            ax.plot(df_plot.index, df_plot[sector], label=None, linewidth=0.5, 
                   alpha=0.2, color='gray')
        
        # 移動平均を太い線で表示
        for sector in df_ma.columns:
            ax.plot(df_ma.index, df_ma[sector], label=sector, linewidth=2.5, 
                   marker='o', markersize=2)
        
        ax.set_xlabel("日付", fontsize=10)
        
        # Y軸ラベルとタイトルを設定
        if is_per_stock:
            ax.set_ylabel("1銘柄あたり売買代金（億円）", fontsize=10)
            ax.set_title(f"セクター別1銘柄あたり売買代金推移（{ma_period}日移動平均）", fontsize=12, fontweight="bold")
        else:
            ax.set_ylabel("売買代金（億円）", fontsize=10)
            ax.set_title(f"セクター別売買代金推移（{ma_period}日移動平均）", fontsize=12, fontweight="bold")
        
        ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=8)
        ax.grid(True, alpha=0.3)
        
        # 日付フォーマット（毎月1日）
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        ax.xaxis.set_major_locator(mdates.MonthLocator(bymonthday=1))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
        
        fig = ax.get_figure()
        if fig:
            fig.tight_layout()
    
    def _plot_change_chart(self, ax, df: pd.DataFrame):
        """前日比の棒グラフを描画"""
        # 最新日のデータのみを表示
        if df.empty:
            return
        
        latest_date = df.index[-1]
        latest_data = df.loc[latest_date].sort_values(ascending=False)
        
        # 上位10セクターのみ表示
        if len(latest_data) > 10:
            latest_data = latest_data.head(10)
        
        # 色を設定（プラスは緑、マイナスは赤）
        colors = ['green' if x >= 0 else 'red' for x in latest_data.values]
        
        # 棒グラフを描画
        ax.barh(latest_data.index, latest_data.values, color=colors, alpha=0.7)
        
        ax.set_xlabel("前日比（%）", fontsize=10)
        ax.set_ylabel("セクター", fontsize=10)
        ax.set_title(f"セクター別売買代金前日比（{latest_date.strftime('%Y-%m-%d')}）", fontsize=12, fontweight="bold")
        ax.grid(True, alpha=0.3, axis='x')
        ax.axvline(x=0, color='black', linewidth=0.8, linestyle='--')
        
        fig = ax.get_figure()
        fig.tight_layout()
    
    def _plot_stacked_bar_chart(self, ax, df: pd.DataFrame):
        """積み上げ棒グラフを描画"""
        # 主要セクターのみを表示（上位10セクター）
        if len(df.columns) > 10:
            # 最新日の売買代金でソート
            latest_values = df.iloc[-1].sort_values(ascending=False)
            top_sectors = latest_values.head(10).index.tolist()
            df_plot = df[top_sectors].copy()
            # その他を追加
            other_sectors = [col for col in df.columns if col not in top_sectors]
            if other_sectors:
                df_plot['その他'] = df[other_sectors].sum(axis=1)
        else:
            df_plot = df.copy()
        
        # データをサンプリング（日数が多い場合は間引く）
        # 最大100日分に制限
        if len(df_plot) > 100:
            step = len(df_plot) // 100
            df_plot = df_plot.iloc[::step]
        
        # 日付を数値に変換（matplotlibの日付処理用）
        dates = mdates.date2num(df_plot.index.to_pydatetime())
        
        # 各セクターの色を設定
        colors = plt.cm.tab10(range(len(df_plot.columns)))
        
        # 積み上げ棒グラフを描画
        bottom = None
        for i, sector in enumerate(df_plot.columns):
            values = df_plot[sector].values
            ax.bar(dates, values, bottom=bottom, label=sector, color=colors[i], alpha=0.8, width=1.0)
            if bottom is None:
                bottom = values.copy()
            else:
                bottom += values
        
        ax.set_xlabel("日付", fontsize=10)
        ax.set_ylabel("売買代金（億円）", fontsize=10)
        ax.set_title("セクター別売買代金推移（積み上げ棒グラフ）", fontsize=12, fontweight="bold")
        ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=8)
        ax.grid(True, alpha=0.3, axis='y')
        
        # 日付フォーマット（毎月1日）
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        ax.xaxis.set_major_locator(mdates.MonthLocator(bymonthday=1))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
        
        fig = ax.get_figure()
        if fig:
            fig.tight_layout()
    
    def _plot_flow_per_stock_chart(self, ax, df: pd.DataFrame):
        """1銘柄あたり売買代金の線グラフを描画"""
        # 主要セクターのみを表示（上位10セクター）
        if len(df.columns) > 10:
            # 最新日の1銘柄あたり売買代金でソート
            latest_values = df.iloc[-1].sort_values(ascending=False)
            top_sectors = latest_values.head(10).index.tolist()
            df_plot = df[top_sectors]
        else:
            df_plot = df
        
        # 線グラフを描画
        for sector in df_plot.columns:
            ax.plot(df_plot.index, df_plot[sector], label=sector, linewidth=2, marker='o', markersize=3)
        
        ax.set_xlabel("日付", fontsize=10)
        ax.set_ylabel("1銘柄あたり売買代金（億円）", fontsize=10)
        ax.set_title("セクター別1銘柄あたり売買代金推移", fontsize=12, fontweight="bold")
        ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=8)
        ax.grid(True, alpha=0.3)
        
        # 日付フォーマット（毎月1日）
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        ax.xaxis.set_major_locator(mdates.MonthLocator(bymonthday=1))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
        
        fig = ax.get_figure()
        if fig:
            fig.tight_layout()
    
    def _on_fetch_financial_metrics(self):
        """財務指標取得ボタンがクリックされたときの処理"""
        if self._analyzing:
            messagebox.showwarning(
                "警告",
                "既に処理が実行中です。",
                parent=self.parent
            )
            return
        
        # 確認ダイアログ
        result = messagebox.askyesno(
            "確認",
            "全銘柄の財務指標を取得しますか？\n（初回実行時は時間がかかります）",
            parent=self.parent
        )
        
        if not result:
            return
        
        def fetch_in_thread():
            try:
                self._analyzing = True
                self.status_var.set("状態: 財務指標を取得中...")
                
                from src.data_collector.financial_metrics_manager import FinancialMetricsManager
                from src.screening.jpx400_manager import JPX400Manager
                
                financial_metrics_manager = FinancialMetricsManager(self.db_path)
                jpx400_manager = JPX400Manager()
                
                # JPX400銘柄リストを取得
                symbols = jpx400_manager.load_symbols()
                if not symbols:
                    self.parent.after(0, lambda: messagebox.showwarning(
                        "警告",
                        "JPX400銘柄リストが空です。",
                        parent=self.parent
                    ))
                    return
                
                # 進捗コールバック
                def progress_callback(symbol, success, current, total):
                    # 毎回ステータスを更新（10件ごとまたは最初/最後）
                    if current % 10 == 0 or current == 1 or current == total:
                        status_text = f"状態: 財務指標取得中... ({current}/{total})"
                        self.parent.after(0, lambda: self.status_var.set(status_text))
                        print(f"[進捗] {status_text} - {symbol} {'成功' if success else 'エラー'}")
                
                # 財務指標を一括取得
                results = financial_metrics_manager.fetch_and_save_batch(
                    symbols,
                    progress_callback=progress_callback,
                    max_retries=3,
                    retry_delay=1.0
                )
                
                # 結果を表示
                success_count = results['success_count']
                error_count = results['error_count']
                
                self.parent.after(0, lambda: self.status_var.set(
                    f"状態: 財務指標取得完了（成功: {success_count}, エラー: {error_count}）"
                ))
                
                # 成功メッセージ
                self.parent.after(0, lambda: messagebox.showinfo(
                    "完了",
                    f"財務指標の取得が完了しました。\n成功: {success_count}銘柄\nエラー: {error_count}銘柄",
                    parent=self.parent
                ))
                
                # 最新実施日時を更新
                self.parent.after(0, self._update_last_fetch_time_display)
                
                # セクター別銘柄数を再読み込み（財務指標を表示）
                self.parent.after(0, self._reload_sector_counts)
                
            except Exception as e:
                error_msg = f"財務指標取得エラー: {e}"
                print(f"[ERROR] {error_msg}")
                import traceback
                traceback.print_exc()
                self.parent.after(0, lambda: messagebox.showerror(
                    "エラー",
                    error_msg,
                    parent=self.parent
                ))
                self.parent.after(0, lambda: self.status_var.set("状態: エラーが発生しました"))
            finally:
                self._analyzing = False
        
        thread = threading.Thread(target=fetch_in_thread, daemon=True)
        thread.start()
    
    def _update_last_fetch_time_display(self):
        """最新実施日時の表示を更新"""
        try:
            from src.data_collector.financial_metrics_manager import FinancialMetricsManager
            financial_metrics_manager = FinancialMetricsManager(self.db_path)
            last_fetch_time = financial_metrics_manager.get_last_fetch_time()
            
            if last_fetch_time:
                self.last_fetch_time_label.config(text=f"最新実施: {last_fetch_time}")
            else:
                self.last_fetch_time_label.config(text="最新実施: 未実行")
        except Exception as e:
            print(f"[ERROR] 最新実施日時の取得エラー: {e}")
            self.last_fetch_time_label.config(text="最新実施: 取得エラー")
    
    def auto_fetch_financial_metrics(self):
        """自動実行：財務指標取得（確認なし）"""
        if self._analyzing:
            print("[自動実行] 財務指標取得は既に実行中です")
            return
        
        def fetch_in_thread():
            try:
                self._analyzing = True
                self.status_var.set("状態: 財務指標を取得中（自動）...")
                print(f"[自動実行] 財務指標取得を開始します（{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}）")
                
                from src.data_collector.financial_metrics_manager import FinancialMetricsManager
                from src.screening.jpx400_manager import JPX400Manager
                
                financial_metrics_manager = FinancialMetricsManager(self.db_path)
                jpx400_manager = JPX400Manager()
                
                # JPX400銘柄リストを取得
                symbols = jpx400_manager.load_symbols()
                if not symbols:
                    print("[自動実行] JPX400銘柄リストが空です")
                    return
                
                # 財務指標を一括取得（進捗コールバックなし）
                results = financial_metrics_manager.fetch_and_save_batch(
                    symbols,
                    progress_callback=None,
                    max_retries=3,
                    retry_delay=1.0
                )
                
                # 結果をログに出力
                success_count = results['success_count']
                error_count = results['error_count']
                print(f"[自動実行完了] 財務指標取得: 成功 {success_count}銘柄, エラー {error_count}銘柄")
                
                # 最新実施日時を更新
                self.parent.after(0, self._update_last_fetch_time_display)
                
            except Exception as e:
                print(f"[自動実行エラー] 財務指標取得で例外: {e}")
                import traceback
                traceback.print_exc()
            finally:
                self._analyzing = False
                self.parent.after(0, lambda: self.status_var.set("状態: 待機中"))
        
        thread = threading.Thread(target=fetch_in_thread, daemon=True)
        thread.start()
    
    def _reload_sector_counts(self):
        """セクター別銘柄数を再読み込み"""
        def load_in_thread():
            try:
                from src.sentiment.sector_flow_analyzer import SectorFlowAnalyzer
                
                analyzer = SectorFlowAnalyzer(self.db_path)
                sector_count_df = analyzer.get_sector_stock_counts()
                sector_industry_count_df = analyzer.get_sector_industry_stock_counts()
                
                # セクター別銘柄数をアコーディオン表示
                if not sector_count_df.empty and not sector_industry_count_df.empty:
                    self.parent.after(0, lambda: self._display_sector_counts_with_industries(
                        sector_count_df, sector_industry_count_df
                    ))
                elif not sector_count_df.empty:
                    self.parent.after(0, lambda: self._display_sector_counts(sector_count_df))
            except Exception as e:
                print(f"[ERROR] セクター別銘柄数再読み込みエラー: {e}")
                import traceback
                traceback.print_exc()
        
        thread = threading.Thread(target=load_in_thread, daemon=True)
        thread.start()
    
    def _on_sector_count_open(self, event):
        """セクター行が展開されたときの処理（業種を表示）"""
        # 展開されたアイテムを取得（focusされているアイテム）
        focus_item = self.sector_count_tree.focus()
        if not focus_item:
            return
        
        item = self.sector_count_tree.item(focus_item)
        tags = item.get('tags', [])
        values = item['values']
        
        # セクター行の場合のみ処理
        if 'sector' in tags and values and len(values) > 0:
            sector = values[0]
            if sector and sector != "分析実行後に表示されます":
                # 子アイテムを確認
                children = self.sector_count_tree.get_children(focus_item)
                
                # ダミー子アイテムのみの場合は業種を追加
                has_real_children = False
                for child in children:
                    child_tags = self.sector_count_tree.item(child).get('tags', [])
                    if 'dummy' not in child_tags:
                        has_real_children = True
                        break
                
                if not has_real_children:
                    # ダミー子アイテムを削除して業種を追加
                    for child in list(children):
                        child_tags = self.sector_count_tree.item(child).get('tags', [])
                        if 'dummy' in child_tags:
                            self.sector_count_tree.delete(child)
                    
                    # 業種を追加
                    self._expand_sector(focus_item, sector)
    
    def _expand_sector(self, sector_item_id: str, sector: str):
        """セクター行を展開（業種を表示）"""
        # 子アイテムを確認
        children = self.sector_count_tree.get_children(sector_item_id)
        
        # ダミー子アイテムを削除
        for child in list(children):
            child_tags = self.sector_count_tree.item(child).get('tags', [])
            if 'dummy' in child_tags:
                self.sector_count_tree.delete(child)
        
        # 既に業種行がある場合は何もしない
        children = self.sector_count_tree.get_children(sector_item_id)
        if children:
            return
        
        # セクター・業種別データを取得
        try:
            from src.sentiment.sector_flow_analyzer import SectorFlowAnalyzer
            analyzer = SectorFlowAnalyzer(self.db_path)
            sector_industry_count_df = analyzer.get_sector_industry_stock_counts()
            sector_industry_metrics_df = analyzer.get_sector_industry_financial_metrics()
            
            # セクター・業種別財務指標を辞書に変換
            sector_industry_metrics_dict = {}
            if not sector_industry_metrics_df.empty:
                for _, row in sector_industry_metrics_df.iterrows():
                    key = (row['sector'], row['industry'])
                    sector_industry_metrics_dict[key] = {
                        'avg_per': row.get('avg_per'),
                        'avg_pbr': row.get('avg_pbr'),
                        'avg_dividend_yield': row.get('avg_dividend_yield'),
                        'avg_roa': row.get('avg_roa'),
                        'avg_roe': row.get('avg_roe'),
                        'avg_net_cash_ratio': row.get('avg_net_cash_ratio')
                    }
                # デバッグ情報
                print(f"[DEBUG] _expand_sector: セクター・業種別財務指標辞書: {len(sector_industry_metrics_dict)}件")
                # サンプルデータを表示
                if sector_industry_metrics_dict:
                    sample_key = list(sector_industry_metrics_dict.keys())[0]
                    sample_data = sector_industry_metrics_dict[sample_key]
                    print(f"[DEBUG] _expand_sector: サンプルデータ ({sample_key}): {sample_data}")
            else:
                print("[DEBUG] _expand_sector: セクター・業種別財務指標データが空です")
            
            # 財務指標をフォーマットする関数
            def format_metric(value, is_percent=False, decimals=2):
                if value is None or pd.isna(value):
                    return "-"
                if is_percent:
                    return f"{value:.{decimals}f}%"
                else:
                    return f"{value:.{decimals}f}"
            
            # 該当セクターの業種を取得
            sector_industries = sector_industry_count_df[
                sector_industry_count_df['sector'] == sector
            ].sort_values('count', ascending=False)
            
            # 業種行を追加
            for _, row in sector_industries.iterrows():
                industry = row['industry']
                industry_count = int(row['count'])
                industry_metrics = sector_industry_metrics_dict.get((sector, industry), {})
                
                self.sector_count_tree.insert(
                    sector_item_id, "end",
                    text="",  # ツリーアイコン用
                    values=(
                        "",
                        industry,
                        f"{industry_count:,}",
                            format_metric(industry_metrics.get('avg_per'), decimals=1),
                            format_metric(industry_metrics.get('avg_pbr'), decimals=2),
                            format_metric(industry_metrics.get('avg_dividend_yield'), is_percent=True, decimals=2),
                            format_metric(industry_metrics.get('avg_roa'), is_percent=True, decimals=2),
                            format_metric(industry_metrics.get('avg_roe'), is_percent=True, decimals=2),
                            format_metric(industry_metrics.get('avg_net_cash_ratio'), decimals=4)
                    ),
                    tags=("industry",)
                )
        except Exception as e:
            print(f"[ERROR] セクター展開エラー: {e}")
            import traceback
            traceback.print_exc()
    
    def _collapse_sector(self, sector_item_id: str):
        """セクター行を折りたたみ（業種を非表示）"""
        # 子アイテム（業種行）を削除
        children = self.sector_count_tree.get_children(sector_item_id)
        for child in children:
            self.sector_count_tree.delete(child)
        
        # ダミー子アイテムを追加して+アイコンを再表示
        self.sector_count_tree.insert(
            sector_item_id, "end",
            text="",
            values=("", "", "", "", "", "", "", ""),
            tags=("dummy",)
        )
    
    def _on_sector_count_double_click(self, event):
        """セクター別登録銘柄数の行をダブルクリックしたときの処理"""
        selection = self.sector_count_tree.selection()
        if not selection:
            return
        
        item_id = selection[0]
        item = self.sector_count_tree.item(item_id)
        tags = item.get('tags', [])
        values = item['values']
        
        if not values or len(values) == 0:
            return
        
        # 業種行の場合
        if 'industry' in tags:
            # 親アイテム（セクター行）を取得
            parent_id = self.sector_count_tree.parent(item_id)
            if not parent_id:
                return
            
            parent_item = self.sector_count_tree.item(parent_id)
            parent_values = parent_item['values']
            
            if not parent_values or len(parent_values) == 0:
                return
            
            sector = parent_values[0]  # セクター名は親アイテムの最初の列
            industry = values[1]  # 業種名は業種行の2番目の列
            
            if not sector or not industry:
                return
            
            # セクター・業種の銘柄一覧を表示
            self._load_and_show_sector_industry_symbols(sector, industry)
            return
        
        # セクター行の場合
        sector = values[0]  # セクター名は最初の列
        
        # "分析実行後に表示されます"などのメッセージの場合は無視
        if sector == "分析実行後に表示されます" or not sector:
            return
        
        # セクターの銘柄一覧を表示
        self._load_and_show_sector_symbols(sector)
    
    def _load_and_show_sector_industry_symbols(self, selected_sector: str, selected_industry: str):
        """指定されたセクター・業種の銘柄一覧を読み込んで表示"""
        def load_symbols():
            try:
                self.status_var.set(f"状態: {selected_sector} - {selected_industry}の銘柄一覧を読み込み中...")
                
                from src.data_collector.ohlcv_data_manager import OHLCVDataManager
                from src.screening.jpx400_manager import JPX400Manager
                
                ohlcv_manager = OHLCVDataManager(self.db_path)
                jpx400_manager = JPX400Manager()
                
                # JPX400銘柄リストを取得
                symbols = jpx400_manager.load_symbols()
                if not symbols:
                    self.parent.after(0, lambda: messagebox.showwarning(
                        "警告",
                        "JPX400銘柄リストが空です。",
                        parent=self.parent
                    ))
                    return
                
                # セクター情報と業種情報を一括取得
                sectors_dict = ohlcv_manager.get_symbol_sectors(symbols)
                industries_dict = ohlcv_manager.get_symbol_industries(symbols)
                
                # 選択したセクター・業種の銘柄を抽出
                sector_industry_symbols = [
                    s for s in symbols 
                    if sectors_dict.get(s) == selected_sector and industries_dict.get(s) == selected_industry
                ]
                
                if not sector_industry_symbols:
                    self.parent.after(0, lambda: messagebox.showinfo(
                        "情報",
                        f"{selected_sector} - {selected_industry}に属する銘柄が見つかりませんでした。",
                        parent=self.parent
                    ))
                    return
                
                # 銘柄名を取得
                symbol_names = ohlcv_manager.get_symbol_names(sector_industry_symbols)
                
                # データ統計を取得
                symbol_stats = {}
                for symbol in sector_industry_symbols:
                    stats = ohlcv_manager.get_data_stats(symbol, timeframe="1d", source="yahoo")
                    if stats:
                        # 統計情報を初期化
                        symbol_stats[symbol] = {
                            'data_count': stats.get('total_count', 0),
                            'first_date': stats.get('start_date'),
                            'last_date': stats.get('end_date'),
                            'last_updated_at': stats.get('last_updated_at'),
                            'latest_price': None,
                            'latest_volume': None,
                            'sigma_value': None
                        }
                
                # 最新出来高と現在株価、σ値を取得
                print(f"[セクター・業種銘柄一覧] 最新出来高と現在株価、σ値を取得中...")
                for symbol in sector_industry_symbols:
                    try:
                        df_latest = ohlcv_manager.get_ohlcv_data_with_temporary_flag(
                            symbol=symbol,
                            timeframe='1d',
                            source='yahoo',
                            include_temporary=True
                        )
                        if not df_latest.empty and symbol in symbol_stats:
                            latest_row = df_latest.iloc[-1]
                            symbol_stats[symbol]['latest_price'] = float(latest_row['close'])
                            symbol_stats[symbol]['latest_volume'] = int(latest_row['volume']) if pd.notna(latest_row['volume']) else None
                            
                            # σ値計算（過去20日の出来高から）
                            if len(df_latest) >= 5 and symbol_stats[symbol]['latest_volume'] is not None:
                                volumes = df_latest['volume'].tail(20)
                                if len(volumes) >= 5:
                                    mean_volume = volumes.mean()
                                    std_volume = volumes.std()
                                    if std_volume > 0:
                                        sigma_value = (symbol_stats[symbol]['latest_volume'] - mean_volume) / std_volume
                                        symbol_stats[symbol]['sigma_value'] = float(sigma_value)
                    except Exception as e:
                        print(f"[セクター・業種銘柄一覧] {symbol}の最新データ取得エラー: {e}")
                
                # 財務指標を取得
                from src.data_collector.financial_metrics_manager import FinancialMetricsManager
                financial_metrics_manager = FinancialMetricsManager(self.db_path)
                financial_metrics_dict = financial_metrics_manager.get_financial_metrics_batch(sector_industry_symbols)
                
                # NC比率を取得
                from src.data_collector.net_cash_ratio_manager import NetCashRatioManager
                net_cash_ratio_manager = NetCashRatioManager(self.db_path)
                net_cash_ratio_dict = net_cash_ratio_manager.get_net_cash_ratio_batch(sector_industry_symbols)
                
                # ウィンドウを表示
                self.parent.after(0, lambda: self._show_sector_symbols_window(
                    f"{selected_sector} - {selected_industry}", 
                    sector_industry_symbols, 
                    symbol_names, 
                    sectors_dict, 
                    industries_dict, 
                    symbol_stats,
                    financial_metrics_dict,
                    net_cash_ratio_dict
                ))
                
                self.status_var.set("状態: 待機中")
                
            except Exception as e:
                import traceback
                error_detail = traceback.format_exc()
                print(f"[ERROR] セクター・業種別銘柄一覧取得エラー: {e}")
                print(f"[ERROR] 詳細: {error_detail}")
                self.parent.after(0, lambda: messagebox.showerror(
                    "エラー",
                    f"銘柄一覧の取得でエラーが発生しました:\n{e}\n\n詳細はコンソールを確認してください。",
                    parent=self.parent
                ))
                self.status_var.set("状態: 待機中")
        
        thread = threading.Thread(target=load_symbols, daemon=True)
        thread.start()
    
    def _load_and_show_sector_symbols(self, selected_sector: str):
        """指定されたセクターの銘柄一覧を読み込んで表示"""
        def load_symbols():
            try:
                self.status_var.set(f"状態: {selected_sector}の銘柄一覧を読み込み中...")
                
                from src.data_collector.ohlcv_data_manager import OHLCVDataManager
                from src.screening.jpx400_manager import JPX400Manager
                
                ohlcv_manager = OHLCVDataManager(self.db_path)
                jpx400_manager = JPX400Manager()
                
                # JPX400銘柄リストを取得
                symbols = jpx400_manager.load_symbols()
                if not symbols:
                    self.parent.after(0, lambda: messagebox.showwarning(
                        "警告",
                        "JPX400銘柄リストが空です。",
                        parent=self.parent
                    ))
                    return
                
                # セクター情報を一括取得
                sectors_dict = ohlcv_manager.get_symbol_sectors(symbols)
                
                # 選択したセクターの銘柄を抽出
                sector_symbols = [s for s, sector in sectors_dict.items() if sector == selected_sector]
                
                if not sector_symbols:
                    self.parent.after(0, lambda: messagebox.showinfo(
                        "情報",
                        f"{selected_sector}に属する銘柄が見つかりませんでした。",
                        parent=self.parent
                    ))
                    return
                
                # 銘柄名を取得
                symbol_names = ohlcv_manager.get_symbol_names(sector_symbols)
                
                # 業種情報を取得
                industries_dict = ohlcv_manager.get_symbol_industries(sector_symbols)
                
                # データ統計を取得
                symbol_stats = {}
                for symbol in sector_symbols:
                    stats = ohlcv_manager.get_data_stats(symbol, timeframe="1d", source="yahoo")
                    if stats:
                        # 統計情報を初期化
                        symbol_stats[symbol] = {
                            'data_count': stats.get('total_count', 0),
                            'first_date': stats.get('start_date'),
                            'last_date': stats.get('end_date'),
                            'last_updated_at': stats.get('last_updated_at'),
                            'latest_price': None,
                            'latest_volume': None,
                            'sigma_value': None
                        }
                
                # 最新出来高と現在株価、σ値を取得
                print(f"[セクター銘柄一覧] 最新出来高と現在株価、σ値を取得中...")
                for symbol in sector_symbols:
                    try:
                        df_latest = ohlcv_manager.get_ohlcv_data_with_temporary_flag(
                            symbol=symbol,
                            timeframe='1d',
                            source='yahoo',
                            include_temporary=True
                        )
                        if not df_latest.empty and symbol in symbol_stats:
                            latest_row = df_latest.iloc[-1]
                            symbol_stats[symbol]['latest_price'] = float(latest_row['close'])
                            symbol_stats[symbol]['latest_volume'] = int(latest_row['volume']) if pd.notna(latest_row['volume']) else None
                            
                            # σ値計算（過去20日の出来高から）
                            if len(df_latest) >= 5 and symbol_stats[symbol]['latest_volume'] is not None:
                                volumes = df_latest['volume'].tail(20)
                                if len(volumes) >= 5:
                                    mean_volume = volumes.mean()
                                    std_volume = volumes.std()
                                    if std_volume > 0:
                                        sigma_value = (symbol_stats[symbol]['latest_volume'] - mean_volume) / std_volume
                                        symbol_stats[symbol]['sigma_value'] = float(sigma_value)
                    except Exception as e:
                        print(f"[セクター銘柄一覧] {symbol}の最新データ取得エラー: {e}")
                
                # 財務指標を取得
                from src.data_collector.financial_metrics_manager import FinancialMetricsManager
                financial_metrics_manager = FinancialMetricsManager(self.db_path)
                financial_metrics_dict = financial_metrics_manager.get_financial_metrics_batch(sector_symbols)
                
                # NC比率を取得
                from src.data_collector.net_cash_ratio_manager import NetCashRatioManager
                net_cash_ratio_manager = NetCashRatioManager(self.db_path)
                net_cash_ratio_dict = net_cash_ratio_manager.get_net_cash_ratio_batch(sector_symbols)
                
                # ウィンドウを表示
                self.parent.after(0, lambda: self._show_sector_symbols_window(
                    selected_sector, sector_symbols, symbol_names, sectors_dict, industries_dict, symbol_stats, financial_metrics_dict, net_cash_ratio_dict
                ))
                
                self.status_var.set("状態: 待機中")
                
            except Exception as e:
                import traceback
                error_detail = traceback.format_exc()
                print(f"[ERROR] セクター別銘柄一覧取得エラー: {e}")
                print(f"[ERROR] 詳細: {error_detail}")
                self.parent.after(0, lambda: messagebox.showerror(
                    "エラー",
                    f"銘柄一覧の取得でエラーが発生しました:\n{e}\n\n詳細はコンソールを確認してください。",
                    parent=self.parent
                ))
                self.status_var.set("状態: 待機中")
        
        thread = threading.Thread(target=load_symbols, daemon=True)
        thread.start()
    
    def _show_sector_symbols_window(
        self,
        sector: str,
        symbols: List[str],
        symbol_names: Dict[str, str],
        sectors_dict: Dict[str, str],
        industries_dict: Dict[str, str],
        symbol_stats: Dict[str, dict],
        financial_metrics_dict: Optional[Dict[str, Dict[str, Optional[float]]]] = None,
        net_cash_ratio_dict: Optional[Dict[str, Optional[float]]] = None
    ):
        """セクター別銘柄一覧を別ウィンドウで表示"""
        window = tk.Toplevel(self.parent)
        window.title(f"{sector} - 銘柄一覧")
        window.geometry("1200x700")
        
        # メインフレーム
        main_frame = ttk.Frame(window)
        main_frame.pack(fill="both", expand=True, padx=8, pady=8)
        
        # ヘッダー情報
        header_frame = ttk.Frame(main_frame)
        header_frame.pack(fill="x", pady=(0, 8))
        
        ttk.Label(
            header_frame,
            text=f"{sector}に属する銘柄: {len(symbols)}件",
            font=("", 10, "bold")
        ).pack(side="left")
        
        # Treeviewとスクロールバー
        tree_frame = ttk.Frame(main_frame)
        tree_frame.pack(fill="both", expand=True)
        
        v_scrollbar = ttk.Scrollbar(tree_frame, orient="vertical")
        v_scrollbar.pack(side="right", fill="y")
        
        h_scrollbar = ttk.Scrollbar(tree_frame, orient="horizontal")
        h_scrollbar.pack(side="bottom", fill="x")
        
        columns = ("銘柄コード", "銘柄名", "セクター", "業種", "PER", "PBR", "利回り", "ROA", "ROE", "NC比率", "データ件数", "最初の日付", "最後の日付", "現在株価", "最新出来高", "σ値")
        tree = ttk.Treeview(
            tree_frame,
            columns=columns,
            show="headings",
            yscrollcommand=v_scrollbar.set,
            xscrollcommand=h_scrollbar.set,
            selectmode="browse"
        )
        tree.pack(side="left", fill="both", expand=True)
        
        v_scrollbar.config(command=tree.yview)
        h_scrollbar.config(command=tree.xview)
        
        # 列の設定
        tree.column("銘柄コード", width=100, anchor="center")
        tree.column("銘柄名", width=200, anchor="w")
        tree.column("セクター", width=150, anchor="w")
        tree.column("業種", width=200, anchor="w")
        tree.column("PER", width=80, anchor="e")
        tree.column("PBR", width=80, anchor="e")
        tree.column("利回り", width=90, anchor="e")
        tree.column("ROA", width=80, anchor="e")
        tree.column("ROE", width=80, anchor="e")
        tree.column("NC比率", width=100, anchor="e")
        tree.column("データ件数", width=100, anchor="e")
        tree.column("最初の日付", width=120, anchor="center")
        tree.column("最後の日付", width=120, anchor="center")
        tree.column("現在株価", width=100, anchor="e")
        tree.column("最新出来高", width=120, anchor="e")
        tree.column("σ値", width=120, anchor="e")
        
        for col in columns:
            tree.heading(col, text=col)
        
        # ソート機能
        sort_state = {}
        
        def sort_treeview(column):
            reverse = sort_state.get(column, False)
            sort_state[column] = not reverse
            
            items = [(tree.set(item, column), item) for item in tree.get_children('')]
            
            if column in ["現在株価", "σ値", "データ件数", "最新出来高", "PER", "PBR", "利回り", "ROA", "ROE", "NC比率"]:
                def sort_key(x):
                    try:
                        val = x[0].replace('σ', '').replace('+', '').replace(',', '').replace('N/A', '0').strip()
                        return float(val)
                    except (ValueError, TypeError):
                        return float('inf') if reverse else float('-inf')
                items.sort(key=sort_key, reverse=reverse)
            else:
                items.sort(key=lambda x: x[0], reverse=reverse)
            
            for index, (val, item) in enumerate(items):
                tree.move(item, '', index)
            
            for col in columns:
                if col == column:
                    indicator = " ▲" if reverse else " ▼"
                    tree.heading(col, text=tree.heading(col)['text'].rstrip(" ▲▼") + indicator)
                else:
                    tree.heading(col, text=tree.heading(col)['text'].rstrip(" ▲▼"))
        
        tree.heading("銘柄コード", text="銘柄コード", command=lambda: sort_treeview("銘柄コード"))
        tree.heading("セクター", text="セクター", command=lambda: sort_treeview("セクター"))
        tree.heading("現在株価", text="現在株価", command=lambda: sort_treeview("現在株価"))
        tree.heading("σ値", text="出来高σ(20日)", command=lambda: sort_treeview("σ値"))
        
        # データを挿入
        sorted_symbols = sorted(symbols)
        for symbol in sorted_symbols:
            name = symbol_names.get(symbol, "（未取得）")
            sector = sectors_dict.get(symbol, "（未取得）")
            industry = industries_dict.get(symbol, "（未取得）")
            stats = symbol_stats.get(symbol, {})
            
            data_count = stats.get('data_count', 0)
            # first_dateとstart_dateの両方をチェック（symbol_statsにはfirst_dateとして保存されている）
            first_date = stats.get('first_date') or stats.get('start_date', '')
            
            # 最後の日付の処理（DB銘柄一覧と同じ形式）
            # last_dateとend_dateの両方をチェック（symbol_statsにはlast_dateとして保存されている）
            if stats.get('last_updated_at'):
                try:
                    dt = pd.to_datetime(stats['last_updated_at'])
                    last_date = dt.strftime('%Y-%m-%d %H:%M')
                except:
                    date_str = str(stats['last_updated_at'])
                    if 'T' in date_str:
                        date_str = date_str.replace('T', ' ')
                    last_date = date_str[:16] if len(date_str) >= 16 else date_str
            elif stats.get('last_date') or stats.get('end_date'):
                last_date_value = stats.get('last_date') or stats.get('end_date')
                try:
                    dt = pd.to_datetime(last_date_value)
                    last_date = dt.strftime('%Y-%m-%d %H:%M')
                except:
                    date_str = str(last_date_value)
                    if 'T' in date_str:
                        date_str = date_str.replace('T', ' ')
                    last_date = date_str[:16] if len(date_str) >= 16 else date_str
            else:
                last_date = "N/A"
            
            # 最初の日付のフォーマット
            if first_date:
                try:
                    first_date = pd.to_datetime(first_date).strftime('%Y-%m-%d')
                except:
                    first_date = str(first_date)[:10] if len(str(first_date)) >= 10 else str(first_date)
            else:
                first_date = "N/A"
            
            # 現在株価、最新出来高、σ値
            latest_price = f"{stats['latest_price']:.2f}" if stats.get('latest_price') is not None else "N/A"
            latest_volume = f"{stats['latest_volume']:,}" if stats.get('latest_volume') is not None else "N/A"
            sigma_str = f"{stats['sigma_value']:+.2f}σ" if stats.get('sigma_value') is not None else "N/A"
            
            # 財務指標を取得
            metrics = financial_metrics_dict.get(symbol, {}) if financial_metrics_dict else {}
            per = f"{metrics.get('per', 0):.1f}" if metrics.get('per') is not None else "-"
            pbr = f"{metrics.get('pbr', 0):.2f}" if metrics.get('pbr') is not None else "-"
            dividend_yield = f"{metrics.get('dividend_yield', 0):.2f}%" if metrics.get('dividend_yield') is not None else "-"
            roa = f"{metrics.get('roa', 0):.2f}%" if metrics.get('roa') is not None else "-"
            roe = f"{metrics.get('roe', 0):.2f}%" if metrics.get('roe') is not None else "-"
            
            # NC比率を取得
            net_cash_ratio = net_cash_ratio_dict.get(symbol) if net_cash_ratio_dict else None
            nc_ratio_str = f"{net_cash_ratio:.4f}" if net_cash_ratio is not None else "-"
            
            tree.insert("", "end", values=(
                symbol,
                name,
                sector,
                industry,
                per,
                pbr,
                dividend_yield,
                roa,
                roe,
                nc_ratio_str,
                f"{data_count:,}" if data_count > 0 else "0",
                first_date,
                last_date,
                latest_price,
                latest_volume,
                sigma_str
            ))
        
        # ダブルクリックでチャート表示
        def on_double_click(event):
            selection = tree.selection()
            if selection:
                item = tree.item(selection[0])
                symbol = item['values'][0]
                symbol_name = item['values'][1]  # 銘柄名は2番目の列（財務指標追加後も変わらず）
                
                # チャート表示
                try:
                    from src.gui.chart_window import ChartWindow
                    from src.data_collector.ohlcv_data_manager import OHLCVDataManager
                    
                    ohlcv_manager = OHLCVDataManager(self.db_path)
                    ChartWindow(window, symbol, symbol_name, ohlcv_manager)
                except Exception as e:
                    import traceback
                    error_detail = traceback.format_exc()
                    print(f"[ERROR] チャート表示エラー: {e}")
                    print(f"[ERROR] 詳細: {error_detail}")
                    messagebox.showerror("エラー", f"チャート表示でエラーが発生しました:\n{e}", parent=window)
        
        tree.bind("<Double-1>", on_double_click)
        
        # 右クリックメニュー（株探・バフェット・コードへのリンク）
        context_menu = tk.Menu(window, tearoff=0)
        
        def open_kabutan(event):
            """株探で開く"""
            selection = tree.selection()
            if selection:
                item = tree.item(selection[0])
                symbol = item['values'][0]  # 銘柄コードは最初の列
                url = f"https://kabutan.jp/stock/?code={symbol}"
                webbrowser.open(url)
        
        def open_buffett_code(event):
            """バフェット・コードで開く"""
            selection = tree.selection()
            if selection:
                item = tree.item(selection[0])
                symbol = item['values'][0]  # 銘柄コードは最初の列
                url = f"https://www.buffett-code.com/company/{symbol}/"
                webbrowser.open(url)
        
        def show_context_menu(event):
            """右クリックメニューを表示"""
            item = tree.identify_row(event.y)
            if item:
                tree.selection_set(item)
                context_menu.post(event.x_root, event.y_root)
        
        context_menu.add_command(label="株探で開く", command=lambda: open_kabutan(None))
        context_menu.add_command(label="バフェット・コードで開く", command=lambda: open_buffett_code(None))
        tree.bind("<Button-3>", show_context_menu)  # Windows/Linux
        tree.bind("<Button-2>", show_context_menu)  # Mac
        
        # 閉じるボタン
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill="x", pady=(8, 0))
        
        ttk.Button(
            button_frame,
            text="閉じる",
            command=window.destroy
        ).pack(side="right", padx=4)

