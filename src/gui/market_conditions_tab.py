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
from typing import Optional, List, Dict, List, Dict
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
        
        # セクター別銘柄表示フレーム
        sector_frame = ttk.LabelFrame(control_frame, text="セクター別銘柄一覧", padding=pad)
        sector_frame.pack(fill="x", pady=pad)
        
        sector_select_frame = ttk.Frame(sector_frame)
        sector_select_frame.pack(fill="x", pady=(0, pad))
        
        ttk.Label(sector_select_frame, text="セクター選択:").pack(side="left", padx=pad)
        
        self.sector_var = tk.StringVar()
        self.sector_combo = ttk.Combobox(
            sector_select_frame,
            textvariable=self.sector_var,
            state="readonly",
            width=30
        )
        self.sector_combo.pack(side="left", padx=pad)
        
        # セクター一覧を読み込む
        self._load_sector_list()
        
        ttk.Button(
            sector_select_frame,
            text="銘柄表示",
            command=self.on_show_sector_symbols,
            width=15
        ).pack(side="left", padx=pad)
        
        # グラフ表示フレーム
        self.chart_frame = ttk.Frame(self.parent)
        self.chart_frame.pack(fill="both", expand=True, padx=pad, pady=pad)
        
        # 初期状態ではグラフを表示しない
        self._show_placeholder()
    
    def _load_sector_list(self):
        """セクター一覧を読み込んでComboboxに設定"""
        try:
            from src.data_collector.ohlcv_data_manager import OHLCVDataManager
            from src.screening.jpx400_manager import JPX400Manager
            
            ohlcv_manager = OHLCVDataManager(self.db_path)
            jpx400_manager = JPX400Manager()
            
            # JPX400銘柄リストを取得
            symbols = jpx400_manager.load_symbols()
            if not symbols:
                return
            
            # セクター情報を一括取得
            sectors_dict = ohlcv_manager.get_symbol_sectors(symbols)
            
            # セクター一覧を取得（重複を除去してソート）
            sectors = sorted(set(sectors_dict.values()))
            
            # Comboboxに設定
            self.sector_combo['values'] = sectors
            
            # デフォルトで最初のセクターを選択
            if sectors:
                self.sector_var.set(sectors[0])
        except Exception as e:
            print(f"[ERROR] セクター一覧読み込みエラー: {e}")
            import traceback
            traceback.print_exc()
    
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
        if chart_type == "moving_average":
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
                
                # データを取得
                if days_value == "all":
                    self.status_var.set("状態: データ取得中...（全期間）")
                    flow_df, change_df = analyzer.calculate_sector_flow_with_change(days=None)
                    share_df = analyzer.calculate_sector_share(days=None)
                else:
                    days = int(days_value)
                    self.status_var.set(f"状態: データ取得中...（{days}日分）")
                    flow_df, change_df = analyzer.calculate_sector_flow_with_change(days=days)
                    share_df = analyzer.calculate_sector_share(days=days)
                
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
    
    def _plot_moving_average_chart(self, ax, df: pd.DataFrame, ma_period: int):
        """移動平均の線グラフを描画"""
        # 主要セクターのみを表示（上位10セクター）
        if len(df.columns) > 10:
            # 最新日の売買代金でソート
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
    
    def on_show_sector_symbols(self):
        """選択したセクターの銘柄一覧を表示"""
        selected_sector = self.sector_var.get()
        
        if not selected_sector:
            messagebox.showwarning("警告", "セクターを選択してください。", parent=self.parent)
            return
        
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
                
                # ウィンドウを表示
                self.parent.after(0, lambda: self._show_sector_symbols_window(
                    selected_sector, sector_symbols, symbol_names, sectors_dict, industries_dict, symbol_stats
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
        symbol_stats: Dict[str, dict]
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
        
        columns = ("銘柄コード", "銘柄名", "セクター", "業種", "データ件数", "最初の日付", "最後の日付", "現在株価", "最新出来高", "σ値")
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
            
            if column in ["現在株価", "σ値", "データ件数", "最新出来高"]:
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
            
            tree.insert("", "end", values=(
                symbol,
                name,
                sector,
                industry,
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
                symbol_name = item['values'][1]  # 銘柄名は2番目の列
                
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

