"""
スクリーニングUIモジュール

スクリーニング実行と結果表示の機能を提供します。

Copyright (c) 2025 JPX400スクリーニングシステム

This software is licensed under the MIT License.
See LICENSE file for details.
"""
import threading
import tkinter as tk
from tkinter import ttk, messagebox
from datetime import datetime
from typing import List, Dict, Optional, Callable
import webbrowser


class ScreeningUI:
    """スクリーニングUIを管理するクラス"""
    
    def __init__(
        self,
        db_path: str,
        status_var: tk.StringVar,
        on_button_state_change: Optional[Callable] = None,
        on_chart_display: Optional[Callable] = None
    ):
        """
        初期化
        
        Args:
            db_path: データベースパス
            status_var: ステータス表示用のStringVar
            on_button_state_change: ボタン状態変更時のコールバック (enable: bool) -> None
            on_chart_display: チャート表示要求時のコールバック (parent, symbol, name) -> None
        """
        self.db_path = db_path
        self.status_var = status_var
        self.on_button_state_change = on_button_state_change
        self.on_chart_display = on_chart_display
        self._screening_running = False
    
    def is_running(self) -> bool:
        """スクリーニング実行中かどうかを返す"""
        return self._screening_running
    
    def run_screening(
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
        check_golden_cross_5_200: bool = False,
        golden_cross_mode: str = 'just_crossed',
        use_macd_kd_filter: bool = True,
        macd_kd_window: int = 3
    ):
        """
        選択した銘柄をスクリーニング
        
        Args:
            parent_window: 親ウィンドウ
            symbols: スクリーニング対象銘柄リスト
            check_condition1: 移動平均線順序条件
            check_condition2: 陽線連続条件
            check_condition3: 5MA上向き条件
            check_condition4: 25MA上向き条件
            check_condition5: 75MA上向き条件
            check_condition6: 200MA上向き条件
            check_golden_cross_5_25: 5MA/25MAゴールデンクロス条件
            check_golden_cross_25_75: 25MA/75MAゴールデンクロス条件
            check_golden_cross_5_200: 5MA/200MAゴールデンクロス条件
            golden_cross_mode: ゴールデンクロス判定モード
            use_macd_kd_filter: MACD/KD近接フィルタを使用するか
            macd_kd_window: MACD/KD近接許容日数
        """
        if self._screening_running:
            print("[スクリーニング] 既に実行中です")
            messagebox.showwarning("警告", "スクリーニングは既に実行中です。")
            return
        
        def screen_in_thread():
            try:
                print(f"[スクリーニング] 開始: {len(symbols)}銘柄")
                print(f"[スクリーニング] 条件1（移動平均線順序）: {'有効' if check_condition1 else '無効'}")
                print(f"[スクリーニング] 条件2（陽線連続）: {'有効' if check_condition2 else '無効'}")
                print(f"[スクリーニング] 条件3（5MA上向き）: {'有効' if check_condition3 else '無効'}")
                print(f"[スクリーニング] 条件4（25MA上向き）: {'有効' if check_condition4 else '無効'}")
                print(f"[スクリーニング] 条件5（75MA上向き）: {'有効' if check_condition5 else '無効'}")
                print(f"[スクリーニング] 条件6（200MA上向き）: {'有効' if check_condition6 else '無効'}")
                print(f"[スクリーニング] 5MA/25MAゴールデンクロス: {'有効' if check_golden_cross_5_25 else '無効'}")
                print(f"[スクリーニング] 25MA/75MAゴールデンクロス: {'有効' if check_golden_cross_25_75 else '無効'}")
                print(f"[スクリーニング] 5MA/200MAゴールデンクロス: {'有効' if check_golden_cross_5_200 else '無効'}")
                print(f"[スクリーニング] ゴールデンクロス判定モード: {golden_cross_mode}")
                print(f"[スクリーニング] MACD/KD近接フィルタ: {'有効' if use_macd_kd_filter else '無効'} (±{macd_kd_window}営業日)")
                
                self._screening_running = True
                if self.on_button_state_change:
                    self.on_button_state_change(False)  # ボタンを無効化
                self.status_var.set(f"状態: スクリーニング実行中... ({len(symbols)}銘柄)")
                
                # JPX400スクリーニングを実行
                from src.screening.jpx400_screener import JPX400Screener
                screener = JPX400Screener(self.db_path)
                
                # 銘柄名、セクター、業種情報を事前に取得（効率化のため）
                from src.data_collector.ohlcv_data_manager import OHLCVDataManager
                ohlcv_manager = OHLCVDataManager(self.db_path)
                symbol_names = ohlcv_manager.get_symbol_names(symbols)
                symbol_sectors = ohlcv_manager.get_symbol_sectors(symbols)
                symbol_industries = ohlcv_manager.get_symbol_industries(symbols)
                
                results = []
                for i, symbol in enumerate(symbols, 1):
                    if i % 10 == 0 or i == 1 or i == len(symbols):
                        print(f"[スクリーニング] 進捗: {i}/{len(symbols)} ({symbol})")
                    self.status_var.set(f"状態: スクリーニング実行中... ({i}/{len(symbols)}) - {symbol}")
                    
                    result = screener.screen_symbol(
                        symbol,
                        complement_today=True,
                        check_condition1=check_condition1,
                        check_condition2=check_condition2,
                        check_condition3=check_condition3,
                        check_condition4=check_condition4,
                        check_condition5=check_condition5,
                        check_condition6=check_condition6,
                        check_golden_cross_5_25=check_golden_cross_5_25,
                        check_golden_cross_25_75=check_golden_cross_25_75,
                        check_golden_cross_5_200=check_golden_cross_5_200,
                        golden_cross_mode=golden_cross_mode,
                        use_macd_kd_filter=use_macd_kd_filter,
                        macd_kd_window=macd_kd_window
                    )
                    if result:
                        # 銘柄名、セクター、業種を追加
                        result['symbol_name'] = symbol_names.get(symbol, '')
                        result['sector'] = symbol_sectors.get(symbol, '')
                        result['industry'] = symbol_industries.get(symbol, '')
                        # 出来高σ値を計算して追加（履歴保存用）
                        try:
                            import pandas as pd
                            df_latest = ohlcv_manager.get_ohlcv_data_with_temporary_flag(
                                symbol=symbol,
                                timeframe='1d',
                                source='yahoo',
                                include_temporary=True
                            )
                            if not df_latest.empty and len(df_latest) >= 5 and result.get('latest_volume') is not None:
                                volumes = df_latest['volume'].tail(20)  # 過去20日
                                if len(volumes) >= 5:
                                    mean_volume = volumes.mean()
                                    std_volume = volumes.std()
                                    if std_volume > 0:
                                        sigma_value = (result['latest_volume'] - mean_volume) / std_volume
                                        result['volume_sigma'] = float(sigma_value)
                        except:
                            pass
                        
                        print(f"[スクリーニング] ✓ {symbol}: 条件を満たす (価格: {result['current_price']:.2f}円)")
                        results.append(result)
                
                print(f"[スクリーニング] 完了: {len(results)}銘柄が条件を満たしました")
                
                # スクリーニング結果に銘柄名、セクター、業種情報を追加
                from src.data_collector.ohlcv_data_manager import OHLCVDataManager
                ohlcv_manager = OHLCVDataManager(self.db_path)
                symbol_list = [r['symbol'] for r in results]
                symbol_names = ohlcv_manager.get_symbol_names(symbol_list)
                symbol_sectors = ohlcv_manager.get_symbol_sectors(symbol_list)
                symbol_industries = ohlcv_manager.get_symbol_industries(symbol_list)
                
                for result in results:
                    symbol = result['symbol']
                    result['symbol_name'] = symbol_names.get(symbol, '（未取得）')
                    result['sector'] = symbol_sectors.get(symbol, '（未取得）')
                    result['industry'] = symbol_industries.get(symbol, '（未取得）')
                
                # スクリーニング履歴を保存
                try:
                    from src.screening.screening_history import ScreeningHistory
                    history_manager = ScreeningHistory(self.db_path)
                    conditions = {
                        'check_condition1': check_condition1,
                        'check_condition2': check_condition2,
                        'check_condition3': check_condition3,
                        'check_condition4': check_condition4,
                        'check_condition5': check_condition5,
                        'check_condition6': check_condition6,
                        'check_golden_cross_5_25': check_golden_cross_5_25,
                        'check_golden_cross_25_75': check_golden_cross_25_75,
                        'check_golden_cross_5_200': check_golden_cross_5_200,
                        'golden_cross_mode': golden_cross_mode,
                        'use_macd_kd_filter': use_macd_kd_filter,
                        'macd_kd_window': macd_kd_window
                    }
                    history_manager.save_history(results, conditions)
                except Exception as e:
                    print(f"[WARN] スクリーニング履歴の保存に失敗: {e}")
                
                # 結果を新しいウィンドウで表示（メインスレッドで実行）
                if results:
                    parent_window.after(0, lambda: self.show_results(
                        parent_window, results, check_condition1, check_condition2,
                        check_condition3, check_condition4, check_condition5, check_condition6,
                        check_golden_cross_5_25, check_golden_cross_25_75, check_golden_cross_5_200,
                        golden_cross_mode, use_macd_kd_filter, macd_kd_window
                    ))
                else:
                    print("[スクリーニング] 条件を満たす銘柄はありませんでした")
                    parent_window.after(0, lambda: messagebox.showinfo("スクリーニング結果", "条件を満たす銘柄はありませんでした。", parent=parent_window))
            
            except Exception as e:
                import traceback
                error_detail = traceback.format_exc()
                print(f"[ERROR] スクリーニングエラー: {e}")
                print(f"[ERROR] 詳細: {error_detail}")
                parent_window.after(0, lambda: messagebox.showerror(
                    "エラー",
                    f"スクリーニング処理でエラーが発生しました:\n{e}\n\n詳細はコンソールを確認してください。",
                    parent=parent_window
                ))
            
            finally:
                self._screening_running = False
                if self.on_button_state_change:
                    self.on_button_state_change(True)  # ボタンを有効化
                self.status_var.set("状態: 待機中")
        
        # 別スレッドで実行
        thread = threading.Thread(target=screen_in_thread, daemon=True)
        thread.start()
    
    def show_results(
        self,
        parent_window,
        results: List[dict],
        check_condition1: bool,
        check_condition2: bool,
        check_condition3: bool = False,
        check_condition4: bool = False,
        check_condition5: bool = False,
        check_condition6: bool = False,
        check_golden_cross_5_25: bool = False,
        check_golden_cross_25_75: bool = False,
        check_golden_cross_5_200: bool = False,
        golden_cross_mode: str = "just_crossed",
        use_macd_kd_filter: bool = False,
        macd_kd_window: int = 1,
        is_history: bool = False,
        executed_at: str = None,
        performance_summary: dict = None
    ):
        """
        スクリーニング結果を新しいウィンドウで表示
        
        Args:
            parent_window: 親ウィンドウ
            results: スクリーニング結果リスト
            check_condition1～check_condition6: 各種条件フラグ
            check_golden_cross_5_25, check_golden_cross_25_75, check_golden_cross_5_200: ゴールデンクロス条件
            golden_cross_mode: ゴールデンクロス判定モード
            use_macd_kd_filter: MACD/KDフィルタ使用フラグ
            macd_kd_window: MACD/KD近接許容日数
            is_history: 履歴表示かどうか
            executed_at: 実施日時（履歴表示時のみ）
        """
        result_window = tk.Toplevel(parent_window)
        if is_history and executed_at:
            # 履歴表示の場合は、実施日時をタイトルに追加
            try:
                dt = datetime.fromisoformat(executed_at)
                executed_at_str = dt.strftime("%Y/%m/%d %H:%M")
                result_window.title(f"スクリーニング結果（履歴：{executed_at_str}）")
            except:
                result_window.title("スクリーニング結果（履歴）")
        elif is_history:
            result_window.title("スクリーニング結果（履歴）")
        else:
            result_window.title("スクリーニング結果")
        result_window.geometry("1800x600")
        
        # メインフレーム
        main_frame = ttk.Frame(result_window)
        main_frame.pack(fill="both", expand=True, padx=8, pady=8)
        
        # ヘッダー情報
        header_frame = ttk.Frame(main_frame)
        header_frame.pack(fill="x", pady=(0, 8))
        
        condition_text = []
        if check_condition1:
            condition_text.append("移動平均線順序")
        if check_condition2:
            condition_text.append("陽線連続")
        if check_condition3:
            condition_text.append("5MA上向き")
        if check_condition4:
            condition_text.append("25MA上向き")
        if check_condition5:
            condition_text.append("75MA上向き")
        if check_condition6:
            condition_text.append("200MA上向き")
        if check_golden_cross_5_25:
            condition_text.append(f"5/25GC({golden_cross_mode})")
        if check_golden_cross_25_75:
            condition_text.append(f"25/75GC({golden_cross_mode})")
        if check_golden_cross_5_200:
            condition_text.append(f"5/200GC({golden_cross_mode})")
        if use_macd_kd_filter:
            condition_text.append(f"MACD/KD近接(±{macd_kd_window}営業日)")
        
        ttk.Label(
            header_frame,
            text=f"条件を満たす銘柄: {len(results)}件（条件: {', '.join(condition_text)}）",
            font=("", 10, "bold")
        ).pack(side="left")
        
        # Treeviewとスクロールバー
        tree_frame = ttk.Frame(main_frame)
        tree_frame.pack(fill="both", expand=True)
        
        # スクロールバー（縦）
        v_scrollbar = ttk.Scrollbar(tree_frame, orient="vertical")
        v_scrollbar.pack(side="right", fill="y")
        
        # スクロールバー（横）
        h_scrollbar = ttk.Scrollbar(tree_frame, orient="horizontal")
        h_scrollbar.pack(side="bottom", fill="x")
        
        # Treeview（履歴表示の場合は追加の列を表示）
        if is_history:
            columns = (
                "銘柄コード", "銘柄名", "セクター", "業種",
                "SC時価格", "SC時出来高",
                "翌1日", "翌2日", "翌3日",
                "現在価格", "最新出来高",
                "出来高σ(20日)", "PER", "PBR", "利回り", "ROA", "ROE",
                "5MA乖離率", "25MA乖離率", "75MA乖離率", "200MA乖離率"
            )
        else:
            columns = (
                "銘柄コード", "銘柄名", "セクター", "業種",
                "現在価格", "最新出来高", "出来高σ(20日)", "PER", "PBR", "利回り", "ROA", "ROE", "状態",
                "GC 5/25", "GC 25/75", "GC 5/200",
                "5MA乖離率", "25MA乖離率", "75MA乖離率", "200MA乖離率"
            )
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
        self._setup_treeview_columns(tree, columns, is_history)
        
        # 銘柄名とセクター情報を取得（履歴表示の場合は既存の値を使用）
        from src.data_collector.ohlcv_data_manager import OHLCVDataManager
        ohlcv_manager = OHLCVDataManager(self.db_path)
        
        self._populate_treeview_data(tree, results, ohlcv_manager, is_history)

        # 勝率サマリーを表示（履歴のみ）
        if is_history and performance_summary:
            summary_label = ttk.Label(
                header_frame,
                foreground="gray",
                font=("", 9)
            )
            parts = []
            for h_key in [1, 2, 3]:
                info = performance_summary.get("win_rates", {}).get(h_key, {})
                total = info.get("total", 0)
                rate = info.get("rate", None)
                win = info.get("win", 0)
                if total and rate is not None:
                    parts.append(f"+{h_key}日: {win}/{total} ({rate:.1f}%)")
                else:
                    parts.append(f"+{h_key}日: データなし")
            summary_label.config(text=" | ".join(parts))
            summary_label.pack(side="left", padx=(12, 0))
        
        # ダブルクリックでチャート表示
        def on_double_click(event):
            item = tree.selection()[0] if tree.selection() else None
            if item:
                symbol = tree.item(item)['tags'][0]
                name = tree.item(item)['values'][1]
                if self.on_chart_display:
                    self.on_chart_display(result_window, symbol, name)
        
        tree.bind("<Double-1>", on_double_click)
        
        # 右クリックメニュー（株探・バフェット・コードへのリンク）
        context_menu = tk.Menu(result_window, tearoff=0)
        
        def open_kabutan(event):
            """株探で開く"""
            item = tree.selection()[0] if tree.selection() else None
            if item:
                symbol = tree.item(item)['tags'][0]
                url = f"https://kabutan.jp/stock/?code={symbol}"
                webbrowser.open(url)
        
        def open_buffett_code(event):
            """バフェット・コードで開く"""
            item = tree.selection()[0] if tree.selection() else None
            if item:
                symbol = tree.item(item)['tags'][0]
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
            command=result_window.destroy
        ).pack(side="right", padx=4)
    
    def _setup_treeview_columns(self, tree, columns, is_history):
        """Treeviewの列を設定"""
        # 列ヘッダーの設定
        tree.heading("銘柄コード", text="銘柄コード")
        tree.heading("銘柄名", text="銘柄名")
        tree.heading("セクター", text="セクター")
        tree.heading("業種", text="業種")
        tree.heading("PER", text="PER")
        tree.heading("PBR", text="PBR")
        tree.heading("利回り", text="利回り")
        tree.heading("ROA", text="ROA")
        tree.heading("ROE", text="ROE")
        
        if is_history:
            tree.heading("SC時価格", text="SC時価格")
            tree.heading("SC時出来高", text="SC時出来高")
            tree.heading("翌1日", text="翌1日騰落率")
            tree.heading("翌2日", text="翌2日騰落率")
            tree.heading("翌3日", text="翌3日騰落率")
            tree.heading("現在価格", text="現在価格")
            tree.heading("最新出来高", text="最新出来高")
            tree.heading("出来高σ(20日)", text="出来高σ(20日)")
            tree.heading("5MA乖離率", text="(当時)5MA乖離率(%)")
            tree.heading("25MA乖離率", text="(当時)25MA乖離率(%)")
            tree.heading("75MA乖離率", text="(当時)75MA乖離率(%)")
            tree.heading("200MA乖離率", text="(当時)200MA乖離率(%)")
        else:
            tree.heading("現在価格", text="現在価格")
            tree.heading("最新出来高", text="最新出来高")
            tree.heading("出来高σ(20日)", text="出来高σ(20日)")
            tree.heading("状態", text="状態")
            tree.heading("GC 5/25", text="GC 5/25")
            tree.heading("GC 25/75", text="GC 25/75")
            tree.heading("GC 5/200", text="GC 5/200")
            tree.heading("5MA乖離率", text="5MA乖離率(%)")
            tree.heading("25MA乖離率", text="25MA乖離率(%)")
            tree.heading("75MA乖離率", text="75MA乖離率(%)")
            tree.heading("200MA乖離率", text="200MA乖離率(%)")
        
        # 列幅の設定
        tree.column("銘柄コード", width=70, anchor="center")
        tree.column("銘柄名", width=170, anchor="w")
        tree.column("セクター", width=120, anchor="w")
        tree.column("業種", width=170, anchor="w")
        tree.column("PER", width=50, anchor="e")
        tree.column("PBR", width=50, anchor="e")
        tree.column("利回り", width=60, anchor="e")
        tree.column("ROA", width=50, anchor="e")
        tree.column("ROE", width=50, anchor="e")
        
        if is_history:
            tree.column("SC時価格", width=70, anchor="e")
            tree.column("SC時出来高", width=90, anchor="e")
            tree.column("翌1日", width=80, anchor="center")
            tree.column("翌2日", width=80, anchor="center")
            tree.column("翌3日", width=80, anchor="center")
            tree.column("現在価格", width=70, anchor="e")
            tree.column("最新出来高", width=90, anchor="e")
        else:
            tree.column("現在価格", width=70, anchor="e")
            tree.column("最新出来高", width=90, anchor="e")
        
        tree.column("出来高σ(20日)", width=90, anchor="e")
        if is_history:
            tree.column("5MA乖離率", width=80, anchor="e")
            tree.column("25MA乖離率", width=80, anchor="e")
            tree.column("75MA乖離率", width=80, anchor="e")
            tree.column("200MA乖離率", width=80, anchor="e")
        else:
            tree.column("状態", width=70, anchor="center")
            tree.column("GC 5/25", width=80, anchor="center")
            tree.column("GC 25/75", width=80, anchor="center")
            tree.column("GC 5/200", width=80, anchor="center")
            tree.column("5MA乖離率", width=80, anchor="e")
            tree.column("25MA乖離率", width=80, anchor="e")
            tree.column("75MA乖離率", width=80, anchor="e")
            tree.column("200MA乖離率", width=80, anchor="e")
    
    def _populate_treeview_data(self, tree, results, ohlcv_manager, is_history):
        """Treeviewにデータを追加"""
        symbol_list = [r['symbol'] for r in results]
        
        # 財務指標を取得
        from src.data_collector.financial_metrics_manager import FinancialMetricsManager
        financial_metrics_manager = FinancialMetricsManager(self.db_path)
        financial_metrics_dict = financial_metrics_manager.get_financial_metrics_batch(symbol_list)
        
        if is_history:
            # 履歴表示の場合は、resultに既に含まれている値を使用
            # 業種情報がない場合はデータベースから取得（フォールバック）
            symbol_names = {r['symbol']: r.get('symbol_name', '（未取得）') for r in results}
            symbol_sectors = {r['symbol']: r.get('sector', '（未取得）') for r in results}
            symbol_industries = {r['symbol']: r.get('industry', '（未取得）') for r in results}
            
            # 業種情報がない銘柄がある場合は、データベースから取得
            symbols_missing_industry = [s for s in symbol_list if not symbol_industries.get(s) or symbol_industries.get(s) == '（未取得）']
            if symbols_missing_industry:
                db_industries = ohlcv_manager.get_symbol_industries(symbols_missing_industry)
                for symbol, industry in db_industries.items():
                    if industry:
                        symbol_industries[symbol] = industry
        else:
            # 通常のスクリーニング結果の場合は、最新のデータベースから取得
            symbol_names = ohlcv_manager.get_symbol_names(symbol_list)
            symbol_sectors = ohlcv_manager.get_symbol_sectors(symbol_list)
            symbol_industries = ohlcv_manager.get_symbol_industries(symbol_list)
        
        # データを挿入
        for result in results:
            symbol = result['symbol']
            # 銘柄名、セクター、業種を取得
            name = symbol_names.get(symbol, "（未取得）")
            sector = symbol_sectors.get(symbol, "（未取得）")
            industry = symbol_industries.get(symbol, "（未取得）")
            
            # 財務指標を取得
            metrics = financial_metrics_dict.get(symbol, {}) if financial_metrics_dict else {}
            per = f"{metrics.get('per', 0):.1f}" if metrics.get('per') is not None else "-"
            pbr = f"{metrics.get('pbr', 0):.2f}" if metrics.get('pbr') is not None else "-"
            dividend_yield = f"{metrics.get('dividend_yield', 0):.2f}%" if metrics.get('dividend_yield') is not None else "-"
            roa = f"{metrics.get('roa', 0):.2f}%" if metrics.get('roa') is not None else "-"
            roe = f"{metrics.get('roe', 0):.2f}%" if metrics.get('roe') is not None else "-"
            
            # 価格と出来高の取得
            if is_history:
                # 履歴表示の場合は、スクリーニング実施時の値と最新の値を両方取得
                sc_price = result['current_price']  # スクリーニング実施時の価格
                sc_volume = result.get('latest_volume')  # スクリーニング実施時の出来高
                sc_volume_str = f"{sc_volume:,}" if sc_volume is not None else "N/A"
                perf1 = result.get("perf_day1_label", "N/A")
                perf2 = result.get("perf_day2_label", "N/A")
                perf3 = result.get("perf_day3_label", "N/A")
                
                # 最新の価格と出来高を取得
                try:
                    df_latest = ohlcv_manager.get_ohlcv_data_with_temporary_flag(
                        symbol=symbol,
                        timeframe='1d',
                        source='yahoo',
                        include_temporary=True
                    )
                    if not df_latest.empty:
                        latest_row = df_latest.iloc[-1]
                        current_price = float(latest_row['close'])
                        current_volume = int(latest_row['volume'])
                    else:
                        current_price = sc_price
                        current_volume = sc_volume
                except:
                    current_price = sc_price
                    current_volume = sc_volume
                
                current_volume_str = f"{current_volume:,}" if current_volume is not None else "N/A"
                price = current_price  # 乖離率計算用
                latest_volume = current_volume  # σ値計算用
            else:
                # 通常のスクリーニング結果の場合は、現在の値のみ
                price = result['current_price']
                latest_volume = result.get('latest_volume')
                sc_price = None
                sc_volume = None
                sc_volume_str = None
                current_price = price
                current_volume = latest_volume
                current_volume_str = f"{latest_volume:,}" if latest_volume is not None else "N/A"
            
            is_temporary = result['is_temporary_close']
            status = "⚠️仮終値" if is_temporary == 1 else "✅正式"
            
            # σ値を計算（履歴表示の場合は既存の値を使用、通常の場合は計算）
            if is_history and result.get('volume_sigma') is not None:
                # 履歴表示の場合は保存されているσ値を使用
                sigma_str = f"{result['volume_sigma']:+.2f}σ"
            else:
                # 通常のスクリーニング結果の場合は計算
                sigma_str = "N/A"
                if latest_volume is not None:
                    try:
                        import pandas as pd
                        df_latest = ohlcv_manager.get_ohlcv_data_with_temporary_flag(
                            symbol=symbol,
                            timeframe='1d',
                            source='yahoo',
                            include_temporary=True
                        )
                        if not df_latest.empty and len(df_latest) >= 5:
                            volumes = df_latest['volume'].tail(20)  # 過去20日
                            if len(volumes) >= 5:
                                mean_volume = volumes.mean()
                                std_volume = volumes.std()
                                if std_volume > 0:
                                    sigma_value = (latest_volume - mean_volume) / std_volume
                                    sigma_str = f"{sigma_value:+.2f}σ"
                    except Exception as e:
                        # エラーが発生した場合はN/Aのまま
                        pass
            
            # 乖離率を計算
            # 履歴表示時はスクリーニング実施時の価格と移動平均線、通常時は現在価格と移動平均線
            # 乖離率 = ((株価 - 移動平均線) / 移動平均線) * 100
            if is_history:
                # 履歴表示時はスクリーニング実施時の価格と移動平均線を使用
                calc_price = sc_price
            else:
                # 通常時は現在価格を使用
                calc_price = price
            
            def calc_deviation(ma_value):
                if ma_value and ma_value > 0:
                    return ((calc_price - ma_value) / ma_value) * 100
                return None
            
            ma5_dev = calc_deviation(result.get('ma5'))
            ma25_dev = calc_deviation(result.get('ma25'))
            ma75_dev = calc_deviation(result.get('ma75'))
            ma200_dev = calc_deviation(result.get('ma200'))
            
            ma5_str = f"{ma5_dev:+.2f}" if ma5_dev is not None else "N/A"
            ma25_str = f"{ma25_dev:+.2f}" if ma25_dev is not None else "N/A"
            ma75_str = f"{ma75_dev:+.2f}" if ma75_dev is not None else "N/A"
            ma200_str = f"{ma200_dev:+.2f}" if ma200_dev is not None else "N/A"

            gc5_25 = result.get("golden_cross_5_25", {})
            gc25_75 = result.get("golden_cross_25_75", {})
            gc5_200 = result.get("golden_cross_5_200", {})
            gc5_25_str = "直近GC" if gc5_25.get("just_crossed") else "クロス中" if gc5_25.get("has_crossed") else "-"
            gc25_75_str = "直近GC" if gc25_75.get("just_crossed") else "クロス中" if gc25_75.get("has_crossed") else "-"
            gc5_200_str = "直近GC" if gc5_200.get("just_crossed") else "クロス中" if gc5_200.get("has_crossed") else "-"
            
            # 履歴表示の場合は追加の列を含める
            if is_history:
                tree.insert(
                    "",
                    "end",
                    values=(
                        symbol,
                        name,
                        sector,
                        industry,
                        f"{sc_price:.2f}",  # SC時価格
                        sc_volume_str,  # SC時出来高
                        perf1,
                        perf2,
                        perf3,
                        f"{current_price:.2f}",  # 現在価格
                        current_volume_str,  # 最新出来高
                        sigma_str,  # 出来高σ(20日)
                        per,
                        pbr,
                        dividend_yield,
                        roa,
                        roe,
                        ma5_str,
                        ma25_str,
                        ma75_str,
                        ma200_str
                    ),
                    tags=(symbol,)
                )
            else:
                tree.insert(
                    "",
                    "end",
                    values=(
                        symbol,
                        name,
                        sector,
                        industry,
                        f"{price:.2f}",
                        current_volume_str,
                        sigma_str,  # 出来高σ(20日)
                        per,
                        pbr,
                        dividend_yield,
                        roa,
                        roe,
                        status,
                        gc5_25_str,
                        gc25_75_str,
                        gc5_200_str,
                        ma5_str,
                        ma25_str,
                        ma75_str,
                        ma200_str
                    ),
                    tags=(symbol,)
                )

