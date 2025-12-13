"""
ローソクグラフ表示ウィンドウ

日足・週足・月足のローソクグラフを表示します。

Copyright (c) 2025 JPX400スクリーニングシステム

This software is licensed under the MIT License.
See LICENSE file for details.
"""
import tkinter as tk
from tkinter import ttk
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk
from matplotlib.figure import Figure
import matplotlib.dates as mdates
import pandas as pd
import matplotlib.transforms as mtransforms
from datetime import datetime, timedelta
from typing import Optional
import numpy as np
import platform


class ChartWindow:
    """ローソクグラフ表示ウィンドウ"""
    
    def __init__(self, parent, symbol: str, symbol_name: str, ohlcv_manager):
        """
        初期化
        
        Args:
            parent: 親ウィンドウ
            symbol: 銘柄コード
            symbol_name: 銘柄名
            ohlcv_manager: OHLCVDataManagerインスタンス
        """
        self.symbol = symbol
        self.symbol_name = symbol_name
        self.ohlcv_manager = ohlcv_manager
        
        # データを保持する変数
        self.df_daily = pd.DataFrame()
        self.df_weekly = pd.DataFrame()
        self.df_monthly = pd.DataFrame()
        
        # ウィンドウ作成
        self.window = tk.Toplevel(parent)
        self.window.title(f"{symbol} {symbol_name} - ローソクグラフ")
        self.window.geometry("1400x800")
        
        # データ取得とグラフ表示
        self._load_and_display()
    
    def _load_and_display(self):
        """データを取得してグラフを表示"""
        try:
            print(f"[チャート] {self.symbol} のデータを取得中...")
            
            # 日足データを取得
            df_daily = self.ohlcv_manager.get_ohlcv_data_with_temporary_flag(
                symbol=self.symbol,
                timeframe='1d',
                source='yahoo',
                include_temporary=True
            )
            
            # 週足・月足データを取得（日足から変換）
            df_weekly = self._convert_to_weekly(df_daily) if not df_daily.empty else pd.DataFrame()
            df_monthly = self._convert_to_monthly(df_daily) if not df_daily.empty else pd.DataFrame()
            
            if df_daily.empty:
                from tkinter import messagebox
                messagebox.showwarning("警告", f"{self.symbol} の日足データがありません。", parent=self.window)
                self.window.destroy()
                return
            
            print(f"[チャート] 日足: {len(df_daily)}件, 週足: {len(df_weekly)}件, 月足: {len(df_monthly)}件")
            
            # データを保持
            self.df_daily = df_daily
            self.df_weekly = df_weekly
            self.df_monthly = df_monthly
            
            # UIを作成（ラジオボタンと表示ボタンを含む）
            self._create_ui()
            
            # デフォルトで日足を表示
            self._display_chart("daily")
            
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            print(f"[ERROR] チャート表示エラー: {e}")
            print(f"[ERROR] 詳細: {error_detail}")
            from tkinter import messagebox
            messagebox.showerror("エラー", f"チャート表示でエラーが発生しました:\n{e}\n\n詳細はコンソールを確認してください。", parent=self.window)
            self.window.destroy()
    
    def _convert_to_weekly(self, df_daily: pd.DataFrame) -> pd.DataFrame:
        """
        日足データを週足データに変換
        
        Args:
            df_daily: 日足データ
            
        Returns:
            pd.DataFrame: 週足データ
        """
        if df_daily.empty:
            return pd.DataFrame()
        
        # 週ごとにグループ化（週末を基準: 金曜終値）
        df_weekly = df_daily.resample('W-FRI').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        })
        
        # 空の行を削除
        df_weekly = df_weekly.dropna()
        
        return df_weekly
    
    def _convert_to_monthly(self, df_daily: pd.DataFrame) -> pd.DataFrame:
        """
        日足データを月足データに変換
        
        Args:
            df_daily: 日足データ
            
        Returns:
            pd.DataFrame: 月足データ
        """
        if df_daily.empty:
            return pd.DataFrame()
        
        # 月ごとにグループ化
        df_monthly = df_daily.resample('ME').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        })
        
        # 空の行を削除
        df_monthly = df_monthly.dropna()
        
        return df_monthly
    
    def _calculate_macd(self, df: pd.DataFrame, short: int = 6, long: int = 13, signal: int = 5) -> pd.DataFrame:
        """MACDを計算し、macd, macd_signal, macd_hist列を追加"""
        out = df.copy()
        ema_short = out['close'].ewm(span=short, adjust=False, min_periods=short).mean()
        ema_long = out['close'].ewm(span=long, adjust=False, min_periods=long).mean()
        out['macd'] = ema_short - ema_long
        out['macd_signal'] = out['macd'].ewm(span=signal, adjust=False, min_periods=signal).mean()
        out['macd_hist'] = out['macd'] - out['macd_signal']
        return out

    def _calculate_stochastic(self, df: pd.DataFrame, k_period: int = 9, smooth_k: int = 3, d_period: int = 3) -> pd.DataFrame:
        """Stochastic Slow (K,D) を計算し、stoch_k, stoch_d列を追加"""
        out = df.copy()
        lowest_low = out['low'].rolling(window=k_period, min_periods=k_period).min()
        highest_high = out['high'].rolling(window=k_period, min_periods=k_period).max()
        raw_k = (out['close'] - lowest_low) / (highest_high - lowest_low) * 100
        out['stoch_k'] = raw_k.rolling(window=smooth_k, min_periods=smooth_k).mean()
        out['stoch_d'] = out['stoch_k'].rolling(window=d_period, min_periods=d_period).mean()
        return out

    def _plot_price_macd_kd(self, fig: Figure, df: pd.DataFrame, title: str, show_volume: bool = True):
        """
        価格・MACD・KDを3段で表示（比率 6:2:2）
        """
        # サブプロットを分割
        gs = fig.add_gridspec(10, 1)
        ax_price = fig.add_subplot(gs[:6, 0])
        ax_macd = fig.add_subplot(gs[6:8, 0], sharex=ax_price)
        ax_kd = fig.add_subplot(gs[8:, 0], sharex=ax_price)

        # 指標計算
        df_calc = self._calculate_macd(df)
        df_calc = self._calculate_stochastic(df_calc)

        # 価格（必要に応じて出来高をオフ）
        ax_volume = None
        if show_volume:
            ax_volume = ax_price.twinx()
            self._plot_candlestick_with_volume(ax_price, ax_volume, df_calc, title)
        else:
            self._plot_candlestick(ax_price, df_calc, title)

        # MACD
        dates = mdates.date2num(df_calc.index.to_pydatetime())
        ax_macd.plot(dates, df_calc['macd'], label='MACD', color='blue', linewidth=1)
        ax_macd.plot(dates, df_calc['macd_signal'], label='Signal', color='red', linewidth=1)
        ax_macd.bar(dates, df_calc['macd_hist'], color=['#2ca02c' if v >= 0 else '#d62728' for v in df_calc['macd_hist']], alpha=0.4, width=0.8)
        ax_macd.legend(loc='upper left', fontsize=8)
        ax_macd.set_ylabel("MACD")
        ax_macd.grid(True, linestyle=':', alpha=0.5)

        # KD
        ax_kd.plot(dates, df_calc['stoch_k'], label='%K', color='green', linewidth=1)
        ax_kd.plot(dates, df_calc['stoch_d'], label='%D', color='orange', linewidth=1)
        ax_kd.axhline(80, color='gray', linestyle='--', linewidth=0.8, alpha=0.7)
        ax_kd.axhline(20, color='gray', linestyle='--', linewidth=0.8, alpha=0.7)
        ax_kd.legend(loc='upper left', fontsize=8)
        ax_kd.set_ylabel("KD")
        ax_kd.grid(True, linestyle=':', alpha=0.5)

        # X軸フォーマットと表示位置
        # 価格軸とMACD軸はラベル非表示
        ax_price.xaxis.set_tick_params(labelbottom=False)
        ax_macd.xaxis.set_tick_params(labelbottom=False)
        # KD軸の下側に日付ラベルを表示（各月1日のみ）
        ax_kd.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        ax_kd.xaxis.set_major_locator(mdates.MonthLocator(bymonthday=1))
        ax_kd.tick_params(axis='x', which='both', labelbottom=True)
        # 日付ラベルの回転を設定
        plt.setp(ax_kd.xaxis.get_majorticklabels(), rotation=45, ha='right')

        # 返却: すべてのaxesと、イベント→表示用の軸マッピング（出来高は価格軸にマップ）
        axes_all = [ax_price, ax_macd, ax_kd] + ([ax_volume] if ax_volume else [])
        axis_map = {
            ax_price: ax_price,
            ax_macd: ax_macd,
            ax_kd: ax_kd,
        }
        if ax_volume:
            axis_map[ax_volume] = ax_price  # 出来高軸でカーソルが乗った場合は価格軸にラベルを出す

        return axes_all, axis_map
    
    def _create_ui(self):
        """UIを作成（ラジオボタンと表示ボタンを含む）"""
        # メインフレーム
        self.main_frame = ttk.Frame(self.window)
        self.main_frame.pack(fill="both", expand=True, padx=8, pady=8)
        
        # ヘッダー
        header_frame = ttk.Frame(self.main_frame)
        header_frame.pack(fill="x", pady=(0, 8))
        
        ttk.Label(
            header_frame,
            text=f"{self.symbol} {self.symbol_name}",
            font=("", 12, "bold")
        ).pack(side="left")
        
        # 選択フレーム（ラジオボタンと表示ボタン）
        select_frame = ttk.LabelFrame(self.main_frame, text="表示選択", padding=8)
        select_frame.pack(fill="x", pady=(0, 8))
        
        # ラジオボタン
        self.chart_type_var = tk.StringVar(value="daily")
        ttk.Radiobutton(
            select_frame,
            text="日足",
            variable=self.chart_type_var,
            value="daily"
        ).pack(side="left", padx=8)
        
        ttk.Radiobutton(
            select_frame,
            text="週足",
            variable=self.chart_type_var,
            value="weekly"
        ).pack(side="left", padx=8)
        
        ttk.Radiobutton(
            select_frame,
            text="月足",
            variable=self.chart_type_var,
            value="monthly"
        ).pack(side="left", padx=8)

        # 出来高表示トグル
        self.show_volume_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            select_frame,
            text="出来高を表示(ｶｰｿﾙ位置の価格表示は消えます)",
            variable=self.show_volume_var
        ).pack(side="left", padx=8)
        
        # 表示ボタン
        ttk.Button(
            select_frame,
            text="表示",
            command=self._on_display_button_clicked
        ).pack(side="left", padx=8)
        
        # グラフ表示用のフレーム
        self.chart_frame = ttk.Frame(self.main_frame)
        self.chart_frame.pack(fill="both", expand=True)
        
        # 閉じるボタン
        button_frame = ttk.Frame(self.main_frame)
        button_frame.pack(fill="x", pady=(8, 0))
        
        ttk.Button(
            button_frame,
            text="閉じる",
            command=self.window.destroy
        ).pack(side="right", padx=4)
    
    def _on_display_button_clicked(self):
        """表示ボタンがクリックされたときの処理"""
        chart_type = self.chart_type_var.get()
        self._display_chart(chart_type)
    
    def _display_chart(self, chart_type: str):
        """選択されたグラフを表示"""
        # 既存のグラフとツールバーを削除
        for widget in self.chart_frame.winfo_children():
            widget.destroy()
        
        # ツールバーの参照をクリア
        if hasattr(self, 'toolbar'):
            del self.toolbar
        
        # 日本語フォント設定
        plt.rcParams['font.sans-serif'] = ['MS Gothic', 'Yu Gothic', 'Meiryo', 'DejaVu Sans']
        plt.rcParams['axes.unicode_minus'] = False
        
        # 図を作成
        fig = Figure(figsize=(13, 8), dpi=100)
        axes = []
        axis_map = None
        
        show_volume = self.show_volume_var.get() if hasattr(self, "show_volume_var") else True

        if chart_type == "daily":
            axes, axis_map = self._plot_price_macd_kd(fig, self.df_daily, "日足", show_volume=show_volume)
        elif chart_type == "weekly":
            if not self.df_weekly.empty:
                axes, axis_map = self._plot_price_macd_kd(fig, self.df_weekly, "週足", show_volume=show_volume)
            else:
                ax = fig.add_subplot(1, 1, 1)
                ax.text(0.5, 0.5, "週足データがありません", 
                        ha='center', va='center', transform=ax.transAxes)
                ax.set_title("週足", fontsize=12, fontweight='bold')
                axes = [ax]
        elif chart_type == "monthly":
            if not self.df_monthly.empty:
                axes, axis_map = self._plot_price_macd_kd(fig, self.df_monthly, "月足", show_volume=show_volume)
            else:
                ax = fig.add_subplot(1, 1, 1)
                ax.text(0.5, 0.5, "月足データがありません", 
                        ha='center', va='center', transform=ax.transAxes)
                ax.set_title("月足", fontsize=12, fontweight='bold')
                axes = [ax]
        else:
            axes = [fig.add_subplot(1, 1, 1)]
        
        fig.tight_layout()
        
        # Canvasに埋め込み
        canvas = FigureCanvasTkAgg(fig, self.chart_frame)
        canvas.draw()
        canvas_widget = canvas.get_tk_widget()
        canvas_widget.pack(fill="both", expand=True)
        
        # ナビゲーションツールバーを追加（拡大縮小、パンなどの機能）
        self.toolbar = NavigationToolbar2Tk(canvas, self.chart_frame)
        self.toolbar.update()
        self.toolbar.pack(side="bottom", fill="x")
        
        # マウスホイールでの拡大縮小を有効化
        # 出来高軸がある場合は、価格軸のみを拡大縮小（出来高軸は自動調整）
        if chart_type == "daily" or (chart_type == "weekly" and not self.df_weekly.empty) or (chart_type == "monthly" and not self.df_monthly.empty):
            self._enable_mouse_wheel_zoom(fig, axes, canvas, canvas_widget)
            self._enable_drag_pan(fig, axes, canvas, canvas_widget)

        # カーソル横棒表示
        # axis_mapがない場合（データなし時）はaxesをそのままマップ
        if axis_map is None:
            axis_map = {ax: ax for ax in axes}
        self._setup_crosshair(axes, axis_map, canvas)
    
    def _enable_mouse_wheel_zoom(self, fig, axes, canvas, canvas_widget):
        """
        マウスホイールでの拡大縮小を有効化（Tkinterレベルでイベントを処理）
        
        Args:
            fig: matplotlib Figure
            ax: matplotlib Axes
            canvas: FigureCanvasTkAgg
            canvas_widget: Tkinter Canvasウィジェット
        """
        def on_scroll(event):
            """マウスホイールイベントハンドラー（Tkinterイベント）"""
            # マウス位置を取得
            # 親ウィンドウからのイベントの場合は、Canvas座標に変換
            if hasattr(event, 'x_root') and hasattr(event, 'y_root'):
                # 親ウィンドウからのイベント
                canvas_x = canvas_widget.winfo_pointerx() - canvas_widget.winfo_rootx()
                canvas_y = canvas_widget.winfo_pointery() - canvas_widget.winfo_rooty()
            else:
                canvas_x = event.x
                canvas_y = event.y
            
            canvas_y_disp = canvas_widget.winfo_height() - canvas_y
            
            target_ax = None
            for candidate_ax in axes:
                bbox = candidate_ax.bbox
                if bbox is None:
                    continue
                if bbox.x0 <= canvas_x <= bbox.x1 and bbox.y0 <= canvas_y_disp <= bbox.y1:
                    target_ax = candidate_ax
                    break
            if target_ax is None:
                    return
                
                # データ座標に変換
            try:
                inv = target_ax.transData.inverted()
                xdata, ydata = inv.transform((canvas_x, canvas_y_disp))
            except Exception:
                xdata = (target_ax.get_xlim()[0] + target_ax.get_xlim()[1]) / 2
                ydata = (target_ax.get_ylim()[0] + target_ax.get_ylim()[1]) / 2
            
            # スクロール方向を判定（Windows/Linux/Mac対応）
            if platform.system() == 'Windows':
                delta = event.delta
            elif platform.system() == 'Darwin':  # Mac
                delta = event.delta
            else:  # Linux
                if event.num == 4:
                    delta = 120
                elif event.num == 5:
                    delta = -120
                else:
                    delta = 0
            
            # 拡大縮小の倍率
            base_scale = 1.1
            scale_factor = base_scale if delta > 0 else 1.0 / base_scale
            
            # 現在の表示範囲を取得
            cur_xlim = target_ax.get_xlim()
            cur_ylim = target_ax.get_ylim()
            
            # マウス位置を中心に拡大縮小
            new_xlim = [
                xdata - (xdata - cur_xlim[0]) * scale_factor,
                xdata + (cur_xlim[1] - xdata) * scale_factor
            ]
            new_ylim = [
                ydata - (ydata - cur_ylim[0]) * scale_factor,
                ydata + (cur_ylim[1] - ydata) * scale_factor
            ]
            
            # 新しい表示範囲を設定
            for ax_share in axes:
                ax_share.set_xlim(new_xlim)
            target_ax.set_ylim(new_ylim)
            
            # グラフを再描画
            canvas.draw_idle()
        
        # プラットフォームに応じたイベントバインディング
        # Windowsでは、Canvasと親ウィンドウの両方にバインド
        if platform.system() == 'Windows':
            canvas_widget.bind('<MouseWheel>', on_scroll)
            # 親ウィンドウにもバインド（フォーカスがなくても動作するように）
            self.window.bind('<MouseWheel>', lambda e: on_scroll(e) if canvas_widget.winfo_containing(e.x_root, e.y_root) == canvas_widget else None)
        elif platform.system() == 'Darwin':  # Mac
            canvas_widget.bind('<MouseWheel>', on_scroll)
            self.window.bind('<MouseWheel>', lambda e: on_scroll(e) if canvas_widget.winfo_containing(e.x_root, e.y_root) == canvas_widget else None)
        else:  # Linux
            canvas_widget.bind('<Button-4>', on_scroll)
            canvas_widget.bind('<Button-5>', on_scroll)
            self.window.bind('<Button-4>', lambda e: on_scroll(e) if canvas_widget.winfo_containing(e.x_root, e.y_root) == canvas_widget else None)
            self.window.bind('<Button-5>', lambda e: on_scroll(e) if canvas_widget.winfo_containing(e.x_root, e.y_root) == canvas_widget else None)
        
        # フォーカスを取得できるようにする
        canvas_widget.focus_set()
        
        # マウスがCanvas上にあるときにフォーカスを設定
        def on_enter(event):
            canvas_widget.focus_set()
        canvas_widget.bind('<Enter>', on_enter)
    
    def _enable_drag_pan(self, fig, axes, canvas, canvas_widget):
        """
        クリック＆ドラッグでグラフを移動（パン）する機能を有効化
        
        Args:
            fig: matplotlib Figure
            axes: List[matplotlib Axes]
            canvas: FigureCanvasTkAgg
            canvas_widget: Tkinter Canvasウィジェット
        """
        # ドラッグ状態を管理する変数
        drag_state = {'active': False, 'start_x': 0, 'start_y': 0, 'start_xlim': None, 'start_ylim': None, 'target_ax': None}
        
        def on_button_press(event):
            """マウスボタンを押したときの処理"""
            # 左クリックのみ処理
            if event.num != 1:
                return
            
            # マウス位置を取得
            if hasattr(event, 'x_root') and hasattr(event, 'y_root'):
                canvas_x = canvas_widget.winfo_pointerx() - canvas_widget.winfo_rootx()
                canvas_y = canvas_widget.winfo_pointery() - canvas_widget.winfo_rooty()
            else:
                canvas_x = event.x
                canvas_y = event.y
            
            canvas_y_disp = canvas_widget.winfo_height() - canvas_y

            target_ax = None
            for candidate_ax in axes:
                bbox = candidate_ax.bbox
                if bbox is None:
                    continue
                if bbox.x0 <= canvas_x <= bbox.x1 and bbox.y0 <= canvas_y_disp <= bbox.y1:
                    target_ax = candidate_ax
                    break
            if target_ax is None:
                return

            drag_state['active'] = True
            drag_state['start_x'] = canvas_x
            drag_state['start_y'] = canvas_y_disp
            drag_state['start_xlim'] = target_ax.get_xlim()
            drag_state['start_ylim'] = target_ax.get_ylim()
            drag_state['target_ax'] = target_ax
        
        def on_motion(event):
            """マウスをドラッグ中の処理"""
            if not drag_state['active'] or drag_state['target_ax'] is None:
                return
            
            # マウス位置を取得
            if hasattr(event, 'x_root') and hasattr(event, 'y_root'):
                canvas_x = canvas_widget.winfo_pointerx() - canvas_widget.winfo_rootx()
                canvas_y = canvas_widget.winfo_pointery() - canvas_widget.winfo_rooty()
            else:
                canvas_x = event.x
                canvas_y = event.y

            canvas_y_disp = canvas_widget.winfo_height() - canvas_y
            
            try:
                # 移動距離を計算（ピクセル単位）
                dx = canvas_x - drag_state['start_x']
                dy = canvas_y_disp - drag_state['start_y']
                
                # データ座標への変換（X方向は全軸同期、Y方向は対象軸のみ）
                xlim = drag_state['start_xlim']
                ylim = drag_state['start_ylim']
                bbox = drag_state['target_ax'].bbox
                if bbox is None:
                    return
                
                # X軸の移動（日付軸）
                x_range = xlim[1] - xlim[0]
                x_pixels = bbox.x1 - bbox.x0
                x_scale = x_range / x_pixels if x_pixels > 0 else 0
                x_shift = -dx * x_scale
                
                # Y軸の移動（対象軸のみ）
                y_range = ylim[1] - ylim[0]
                y_pixels = bbox.y1 - bbox.y0
                y_scale = y_range / y_pixels if y_pixels > 0 else 0
                # マウスを上へ動かしたらビューも上へ動くよう符号を反転
                y_shift = -dy * y_scale
                
                # 新しい表示範囲を設定
                new_xlim = [xlim[0] + x_shift, xlim[1] + x_shift]
                for ax_share in axes:
                    ax_share.set_xlim(new_xlim)
                if drag_state['target_ax'] is not None:
                    new_ylim = [ylim[0] + y_shift, ylim[1] + y_shift]
                    drag_state['target_ax'].set_ylim(new_ylim)
                
                # グラフを再描画
                canvas.draw_idle()
            except:
                pass
        
        def on_button_release(event):
            """マウスボタンを離したときの処理"""
            drag_state['active'] = False
        
        # イベントバインディング
        canvas_widget.bind('<Button-1>', on_button_press)
        canvas_widget.bind('<B1-Motion>', on_motion)
        canvas_widget.bind('<ButtonRelease-1>', on_button_release)
        
        # 親ウィンドウにもバインド（フォーカスがなくても動作するように）
        def create_window_handler(handler):
            def wrapper(event):
                # マウスがCanvas上にある場合のみ処理
                if canvas_widget.winfo_containing(event.x_root, event.y_root) == canvas_widget:
                    # イベントの座標をCanvas座標に変換
                    canvas_x = event.x_root - canvas_widget.winfo_rootx()
                    canvas_y = event.y_root - canvas_widget.winfo_rooty()
                    # イベントオブジェクトを作成
                    class FakeEvent:
                        def __init__(self, x, y, num):
                            self.x = x
                            self.y = y
                            self.x_root = event.x_root
                            self.y_root = event.y_root
                            self.num = num
                    fake_event = FakeEvent(canvas_x, canvas_y, event.num if hasattr(event, 'num') else 1)
                    handler(fake_event)
            return wrapper
        
        self.window.bind('<Button-1>', create_window_handler(on_button_press))
        self.window.bind('<B1-Motion>', create_window_handler(on_motion))
        self.window.bind('<ButtonRelease-1>', create_window_handler(on_button_release))
    
    def _setup_crosshair(self, axes, axis_map, canvas):
        """
        マウス位置に横棒とY値ラベルを表示（アクティブなAxesのみ）
        """
        # 既存のクロスヘアをクリア
        if hasattr(self, '_crosshair'):
            ch = self._crosshair
            if ch.get('cid'):
                canvas.mpl_disconnect(ch['cid'])
            # ラインとテキストを削除
            for line in ch.get('hlines', []):
                line.remove()
            for txt in ch.get('labels', []):
                txt.remove()

        # 軸マッピング（イベント発生軸 -> 表示用軸）
        unique_axes = []
        for ax in axis_map.values():
            if ax not in unique_axes:
                unique_axes.append(ax)

        hlines = []
        labels = []
        for ax in unique_axes:
            line = ax.axhline(np.nan, color='gray', lw=0.8, ls='--', alpha=0.6, visible=False, zorder=9, clip_on=False)
            txt = ax.annotate(
                "",
                xy=(0, np.nan),
                xycoords=mtransforms.blended_transform_factory(ax.transAxes, ax.transData),
                xytext=(6, 0),  # 軸内側にオフセット
                textcoords="offset points",
                va="center",
                ha="left",
                fontsize=8,
                bbox=dict(facecolor="white", edgecolor="gray", alpha=0.95, boxstyle="round,pad=0.2"),
                visible=False,
                zorder=10,
                clip_on=False,
                annotation_clip=False
            )
            hlines.append(line)
            labels.append(txt)

        # 共有X軸用の日付表示（KD軸下側に固定）
        x_axis_ref = axes[0] if axes else None
        vline = None
        xdate_label = None
        if x_axis_ref is not None:
            vline = x_axis_ref.axvline(np.nan, color='gray', lw=0.8, ls='--', alpha=0.4, visible=False, zorder=8, clip_on=False)
            xdate_label = x_axis_ref.annotate(
                "",
                xy=(np.nan, 0),
                xycoords=('data', 'axes fraction'),
                xytext=(0, -10),
                textcoords="offset points",
                ha="center",
                va="top",
                fontsize=8,
                bbox=dict(facecolor="white", edgecolor="gray", alpha=0.95, boxstyle="round,pad=0.2"),
                visible=False,
                zorder=9,
                clip_on=False,
                annotation_clip=False
            )

        def hide_all():
            updated = False
            for line, txt in zip(hlines, labels):
                if line.get_visible() or txt.get_visible():
                    updated = True
                line.set_visible(False)
                txt.set_visible(False)
            if vline is not None and vline.get_visible():
                vline.set_visible(False)
                updated = True
            if xdate_label is not None and xdate_label.get_visible():
                xdate_label.set_visible(False)
                updated = True
            if updated:
                canvas.draw_idle()

        def on_move(event):
            if event.inaxes is None or event.ydata is None:
                hide_all()
                return
            if event.inaxes not in axis_map:
                hide_all()
                return

            ax = axis_map[event.inaxes]
            idx = unique_axes.index(ax)
            y = event.ydata
            x = event.xdata

            # 位置更新
            hlines[idx].set_ydata([y])
            hlines[idx].set_visible(True)

            # 左軸に沿わせる（軸座標×データ座標で配置、少し左にオフセット）
            labels[idx].xy = (0, y)
            labels[idx].set_text(f"{y:.2f}")
            labels[idx].set_visible(True)

            # X軸の表示（共有）
            if x is not None and vline is not None and xdate_label is not None:
                vline.set_xdata([x])
                vline.set_visible(True)
                try:
                    xdate = mdates.num2date(x)
                    xdate_label.set_text(xdate.strftime("%Y-%m-%d"))
                except Exception:
                    xdate_label.set_text("")
                xdate_label.xy = (x, 0)
                xdate_label.set_visible(True)

            # 他の軸は隠す
            for i, (line, txt) in enumerate(zip(hlines, labels)):
                if i != idx:
                    line.set_visible(False)
                    txt.set_visible(False)

            canvas.draw_idle()

        cid = canvas.mpl_connect("motion_notify_event", on_move)

        self._crosshair = {
            "cid": cid,
            "hlines": hlines,
            "labels": labels,
        }
    
    def _plot_candlestick_with_volume(self, ax_price, ax_volume, df: pd.DataFrame, title: str):
        """
        ローソクグラフと出来高を1つのグラフに描画
        
        Args:
            ax_price: 価格用のaxes（左軸）
            ax_volume: 出来高用のaxes（右軸）
            df: OHLCVデータ
            title: グラフタイトル
        """
        if df.empty:
            return
        
        # データを準備
        dates = df.index
        opens = df['open'].values
        highs = df['high'].values
        lows = df['low'].values
        closes = df['close'].values
        volumes = df['volume'].values if 'volume' in df.columns else None
        
        # 陽線と陰線を分ける
        up = closes >= opens
        
        # ローソクの幅（日数）- 隣と重ならない程度に太く設定
        width = 0.8
        
        # 陽線（赤）と陰線（青）を描画
        for i in range(len(df)):
            date = dates[i]
            open_price = opens[i]
            high_price = highs[i]
            low_price = lows[i]
            close_price = closes[i]
            
            # ヒゲ（上下の線）
            ax_price.plot([date, date], [low_price, high_price], 
                   color='black', linewidth=0.5, alpha=0.8)
            
            # 実体（四角）
            if up[i]:
                # 陽線（赤）
                color = 'red'
                bottom = open_price
                height = close_price - open_price
            else:
                # 陰線（青）
                color = 'blue'
                bottom = close_price
                height = open_price - close_price
            
            # 実体を描画
            ax_price.bar(date, height, width=width, bottom=bottom, 
                  color=color, edgecolor='black', linewidth=0.5, alpha=0.8)
        
        # 移動平均線を追加
        if len(df) >= 5:
            df['ma5'] = df['close'].rolling(window=5, min_periods=1).mean()
            ax_price.plot(dates, df['ma5'], label='5MA', color='orange', linewidth=1, alpha=0.7)
        
        if len(df) >= 25:
            df['ma25'] = df['close'].rolling(window=25, min_periods=1).mean()
            ax_price.plot(dates, df['ma25'], label='25MA', color='green', linewidth=1, alpha=0.7)
        
        if len(df) >= 75:
            df['ma75'] = df['close'].rolling(window=75, min_periods=1).mean()
            ax_price.plot(dates, df['ma75'], label='75MA', color='purple', linewidth=1, alpha=0.7)
        
        if len(df) >= 200:
            df['ma200'] = df['close'].rolling(window=200, min_periods=1).mean()
            ax_price.plot(dates, df['ma200'], label='200MA', color='brown', linewidth=1, alpha=0.7)
        
        # 出来高を右軸に表示
        if volumes is not None:
            colors = ['red' if u else 'blue' for u in up]
            ax_volume.bar(dates, volumes, color=colors, alpha=0.3, width=0.8)
            ax_volume.set_ylabel('出来高 (株)', fontsize=10)
            ax_volume.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x/1000:,.0f}K'))
        
        # グラフの設定
        ax_price.set_title(title, fontsize=12, fontweight='bold')
        ax_price.set_ylabel('価格 (円)', fontsize=10)
        ax_price.legend(loc='best', fontsize=8)
        ax_price.grid(True, alpha=0.3)
        
        # x軸の日付フォーマット
        if title == "月足":
            # 月足の場合は各月を表示
            ax_price.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
            ax_price.xaxis.set_major_locator(mdates.MonthLocator())
        elif title == "週足":
            # 週足の場合は週単位（週末）で表示
            ax_price.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
            ax_price.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=mdates.FR))  # 金曜を基準
        else:
            # 日足の場合は各月1日を表示
            ax_price.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
            # 各月1日に目盛りを設定
            ax_price.xaxis.set_major_locator(mdates.MonthLocator(bymonthday=1))
        
        plt.setp(ax_price.xaxis.get_majorticklabels(), rotation=45, ha='right')
        
        # y軸のフォーマット
        ax_price.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:,.0f}'))
    
    def _plot_candlestick(self, ax, df: pd.DataFrame, title: str):
        """
        ローソクグラフを描画
        
        Args:
            ax: matplotlibのaxes
            df: OHLCVデータ
            title: グラフタイトル
        """
        if df.empty:
            return
        
        # データを準備
        dates = df.index
        opens = df['open'].values
        highs = df['high'].values
        lows = df['low'].values
        closes = df['close'].values
        
        # 陽線と陰線を分ける
        up = closes >= opens
        down = closes < opens
        
        # ローソクの幅（日数）- 隣と重ならない程度に太く設定
        width = 0.8
        width2 = 0.05
        
        # 陽線（赤）と陰線（青）を描画
        for i in range(len(df)):
            date = dates[i]
            open_price = opens[i]
            high_price = highs[i]
            low_price = lows[i]
            close_price = closes[i]
            
            # ヒゲ（上下の線）
            ax.plot([date, date], [low_price, high_price], 
                   color='black', linewidth=0.5, alpha=0.8)
            
            # 実体（四角）
            if up[i]:
                # 陽線（赤）
                color = 'red'
                bottom = open_price
                height = close_price - open_price
            else:
                # 陰線（青）
                color = 'blue'
                bottom = close_price
                height = open_price - close_price
            
            # 実体を描画
            ax.bar(date, height, width=width, bottom=bottom, 
                  color=color, edgecolor='black', linewidth=0.5, alpha=0.8)
        
        # 移動平均線を追加
        if len(df) >= 5:
            df['ma5'] = df['close'].rolling(window=5, min_periods=1).mean()
            ax.plot(dates, df['ma5'], label='5MA', color='orange', linewidth=1, alpha=0.7)
        
        if len(df) >= 25:
            df['ma25'] = df['close'].rolling(window=25, min_periods=1).mean()
            ax.plot(dates, df['ma25'], label='25MA', color='green', linewidth=1, alpha=0.7)
        
        if len(df) >= 75:
            df['ma75'] = df['close'].rolling(window=75, min_periods=1).mean()
            ax.plot(dates, df['ma75'], label='75MA', color='purple', linewidth=1, alpha=0.7)
        
        if len(df) >= 200:
            df['ma200'] = df['close'].rolling(window=200, min_periods=1).mean()
            ax.plot(dates, df['ma200'], label='200MA', color='brown', linewidth=1, alpha=0.7)
        
        # グラフの設定
        ax.set_title(title, fontsize=12, fontweight='bold')
        ax.set_ylabel('価格 (円)', fontsize=10)
        ax.legend(loc='best', fontsize=8)
        ax.grid(True, alpha=0.3)
        
        # x軸の日付フォーマット
        if title == "月足":
            # 月足の場合は各月を表示
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
            ax.xaxis.set_major_locator(mdates.MonthLocator())
        elif title == "週足":
            # 週足の場合は週単位（週末）で表示
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
            ax.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=mdates.FR))  # 金曜を基準
        else:
            # 日足の場合は各月1日を表示
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
            # 各月1日に目盛りを設定
            ax.xaxis.set_major_locator(mdates.MonthLocator(bymonthday=1))
        
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
        
        # y軸のフォーマット
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:,.0f}'))
    
    def _plot_volume(self, ax, df: pd.DataFrame, title: str):
        """
        出来高グラフを描画
        
        Args:
            ax: matplotlibのaxes
            df: OHLCVデータ
            title: グラフタイトル
        """
        if df.empty or 'volume' not in df.columns:
            return
        
        dates = df.index
        volumes = df['volume'].values
        closes = df['close'].values
        opens = df['open'].values
        
        # 陽線と陰線を分ける
        up = closes >= opens
        
        # 出来高を棒グラフで表示（陽線は赤、陰線は青）
        colors = ['red' if u else 'blue' for u in up]
        ax.bar(dates, volumes, color=colors, alpha=0.6, width=0.8)
        
        # グラフの設定
        ax.set_title(title, fontsize=12, fontweight='bold')
        ax.set_ylabel('出来高 (株)', fontsize=10)
        ax.grid(True, alpha=0.3)
        
        # x軸の日付フォーマット（各月1日の目盛りを表示）
        if title == "月足出来高":
            # 月足の場合は各月を表示
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
            ax.xaxis.set_major_locator(mdates.MonthLocator())
        else:
            # 日足の場合は各月1日を表示
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
            # 各月1日に目盛りを設定
            ax.xaxis.set_major_locator(mdates.MonthLocator(bymonthday=1))
        
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
        
        # y軸のフォーマット（出来高は千株単位で表示）
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x/1000:,.0f}K'))

