"""
データ管理タブモジュール

JPX400データ収集、銘柄一覧表示、銘柄名取得、スクリーニング履歴などの
データ管理機能を提供します。

Copyright (c) 2025 JPX400スクリーニングシステム

This software is licensed under the MIT License.
See LICENSE file for details.
"""
import threading
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
from datetime import datetime
from typing import List, Dict, Optional, Callable
import sqlite3
import pandas as pd
import webbrowser


class DataManagementTab:
    """データ管理タブのUIとハンドラを管理するクラス"""
    
    def __init__(
        self,
        parent: ttk.Frame,
        db_path: str,
        status_var: tk.StringVar,
        jpx_collect_status_var: tk.StringVar,
        on_screening_requested: Optional[Callable] = None,
        on_show_chart_requested: Optional[Callable] = None,
        on_record_jpx_collection: Optional[Callable] = None,
        on_load_jpx_status: Optional[Callable] = None
    ):
        """
        初期化
        
        Args:
            parent: 親フレーム（データ管理タブ）
            db_path: データベースパス
            status_var: ステータス表示用のStringVar
            jpx_collect_status_var: JPX収集ステータス表示用のStringVar
            on_screening_requested: スクリーニング実行要求時のコールバック
            on_show_chart_requested: チャート表示要求時のコールバック
            on_record_jpx_collection: JPX収集記録時のコールバック
            on_load_jpx_status: JPXステータス読み込み時のコールバック
        """
        self.parent = parent
        self.db_path = db_path
        self.status_var = status_var
        self.jpx_collect_status_var = jpx_collect_status_var
        self.on_screening_requested = on_screening_requested
        self.on_show_chart_requested = on_show_chart_requested
        self.on_record_jpx_collection = on_record_jpx_collection
        self.on_load_jpx_status = on_load_jpx_status
        
        # 実行中フラグ
        self._jpx400_collecting = False
        self._stop_collecting = False
        self._fetching_names = False
        
        # UI構築
        self._build_ui()
    
    def _build_ui(self):
        """データ管理タブのUI構築"""
        pad = 8

        # ボタンフレーム
        button_frame = ttk.Frame(self.parent)
        button_frame.pack(fill="x", pady=pad, padx=pad)

        # JPX400データ収集ボタン
        self.jpx400_collect_button = ttk.Button(
            button_frame, 
            text="JPX400データ収集", 
            command=self.on_collect_jpx400,
            width=20
        )
        self.jpx400_collect_button.pack(side="left", padx=pad)
        ttk.Label(button_frame, textvariable=self.jpx_collect_status_var, foreground="gray").pack(side="left", padx=pad)

        # 自動実行案内（視認性向上）
        ttk.Label(
            button_frame,
            text="自動実行: 15:00 / 20:00 に収集を実施",
            foreground="gray"
        ).pack(side="left", padx=pad)
        
        # データ収集強制終了ボタン（初期状態は非表示）
        self.stop_collect_button = ttk.Button(
            button_frame,
            text="データ収集を停止",
            command=self.on_stop_collect_jpx400,
            width=20,
            state="disabled"
        )
        self.stop_collect_button.pack(side="left", padx=pad)

        # JPX400銘柄リスト更新ボタン
        self.jpx400_update_list_button = ttk.Button(
            button_frame, 
            text="JPX400リスト更新", 
            command=self.on_update_jpx400_list,
            width=20
        )
        self.jpx400_update_list_button.pack(side="left", padx=pad)

        # 銘柄名取得ボタン（JPX400リスト更新の右隣）
        self.fetch_names_button = ttk.Button(
            button_frame, 
            text="銘柄名取得", 
            command=self.on_fetch_symbol_names,
            width=20
        )
        self.fetch_names_button.pack(side="left", padx=pad)

        # 2段目ボタンフレーム（DB銘柄一覧）
        bottom_button_frame = ttk.Frame(self.parent)
        bottom_button_frame.pack(fill="x", pady=(0, pad), padx=pad, anchor="w")

        # DB銘柄一覧表示ボタン
        self.show_symbols_button = ttk.Button(
            bottom_button_frame, 
            text="DB銘柄一覧", 
            command=self.on_show_symbols,
            width=20
        )
        self.show_symbols_button.pack(side="left", padx=pad)

        # スクリーニング履歴ボタン
        self.show_history_button = ttk.Button(
            bottom_button_frame, 
            text="スクリーニング履歴", 
            command=self.on_show_screening_history,
            width=20
        )
        self.show_history_button.pack(side="left", padx=pad)

        # 説明ラベル
        info_text = (
            "【データ管理】\n\n"
            "• JPX400データ収集: JPX400銘柄の日足データを一括収集します\n"
            "  （当日の始値・終値は1分足データから取得し、仮終値として保存します）\n"
            "• データ収集を停止: 実行中のデータ収集を安全に停止します\n"
            "• JPX400リスト更新: JPX400銘柄リストを最新に更新します\n"
            "• 銘柄名取得: 銘柄コードから銘柄名とセクター情報を取得します（英語名を日本語に翻訳）\n"
            "• DB銘柄一覧: データベースに保存されている銘柄を確認します\n"
            "• スクリーニング履歴: 過去のスクリーニング結果を確認・検証します"
        )
        ttk.Label(
            self.parent,
            text=info_text,
            font=("", 9),
            justify="left",
            foreground="gray"
        ).pack(pady=pad, padx=pad*2, anchor="w")
    
    def auto_collect_jpx400(self, run_type: str):
        """自動実行：確認なしでJPXデータ収集"""
        if self._jpx400_collecting:
            print(f"[自動実行] データ収集は既に実行中です（{run_type}時枠）")
            return
        
        def collect_in_thread():
            start_time = datetime.now()
            collect_result = None
            try:
                self._jpx400_collecting = True
                self._stop_collecting = False
                self.status_var.set(f"状態: JPX400データ収集中（自動: {run_type}時枠）...")
                print(f"[自動実行] JPXデータ収集({run_type}時枠)を開始します（{start_time.strftime('%Y-%m-%d %H:%M:%S')}）")

                from src.screening.data_collector import JPX400DataCollector
                collector = JPX400DataCollector(self.db_path)

                def check_stop():
                    return self._stop_collecting

                collect_result = collector.collect_jpx400_data(
                    complement_today=True,
                    stop_check=check_stop
                )

                if collect_result.get('stopped', False):
                    print(f"[自動実行停止] {run_type}時枠 JPX収集が停止されました。")
                elif collect_result.get('success'):
                    # キャッシュクリア
                    if collect_result.get('success_count', 0) > 0 or collect_result.get('skip_count', 0) > 0:
                        from src.screening.screening_result_cache import ScreeningResultCache
                        cache = ScreeningResultCache(self.db_path)
                        cache.clear_cache()
                        print("[キャッシュ] データ収集が完了したため、スクリーニング結果のキャッシュをクリアしました")
                    print(f"[自動実行完了] {run_type}時枠 JPX収集: 成功={collect_result.get('success_count',0)}, スキップ={collect_result.get('skip_count',0)}, エラー={collect_result.get('error_count',0)}")
                    if self.on_record_jpx_collection:
                        self.on_record_jpx_collection(run_type=run_type)
                else:
                    print(f"[自動実行エラー] {run_type}時枠 JPX収集失敗: {collect_result.get('error', '不明なエラー')}")

                if self.on_load_jpx_status:
                    self.parent.after(0, lambda: self.on_load_jpx_status())
            except Exception as e:
                import traceback
                print(f"[自動実行エラー] JPX収集で例外: {e}")
                print(traceback.format_exc())
            finally:
                end_time = datetime.now()
                elapsed = end_time - start_time
                elapsed_minutes = int(elapsed.total_seconds() // 60)
                elapsed_seconds = int(elapsed.total_seconds() % 60)
                
                if collect_result is None:
                    print(f"[自動実行終了] JPXデータ収集({run_type}時枠)が終了しました（エラーにより中断、経過時間: {elapsed_minutes}分{elapsed_seconds}秒）")
                elif collect_result.get('stopped', False):
                    print(f"[自動実行終了] JPXデータ収集({run_type}時枠)が終了しました（停止、経過時間: {elapsed_minutes}分{elapsed_seconds}秒）")
                elif collect_result.get('success'):
                    print(f"[自動実行終了] JPXデータ収集({run_type}時枠)が終了しました（正常終了、経過時間: {elapsed_minutes}分{elapsed_seconds}秒）")
                else:
                    print(f"[自動実行終了] JPXデータ収集({run_type}時枠)が終了しました（失敗、経過時間: {elapsed_minutes}分{elapsed_seconds}秒）")
                
                self._jpx400_collecting = False
                self._stop_collecting = False
                self.parent.after(0, lambda: self.status_var.set("状態: 待機中"))
        
        thread = threading.Thread(target=collect_in_thread, daemon=True)
        thread.start()
    
    def on_collect_jpx400(self):
        """JPX400銘柄のデータを一括収集"""
        if self._jpx400_collecting:
            messagebox.showwarning("警告", "データ収集は既に実行中です。")
            return
        
        # 確認ダイアログ
        result = messagebox.askyesno(
            "確認",
            "JPX400銘柄のデータを一括収集しますか？\n\n"
            "この処理には時間がかかる場合があります。"
        )
        
        if not result:
            return
        
        def collect_in_thread():
            try:
                self._jpx400_collecting = True
                self._stop_collecting = False
                self.jpx400_collect_button.config(state="disabled")
                self.jpx400_update_list_button.config(state="disabled")
                self.show_symbols_button.config(state="disabled")
                self.fetch_names_button.config(state="disabled")
                self.stop_collect_button.config(state="normal")
                self.status_var.set("状態: JPX400データ収集中...")
                
                # JPX400データ収集を実行
                from src.screening.data_collector import JPX400DataCollector
                collector = JPX400DataCollector(self.db_path)
                
                def check_stop():
                    return self._stop_collecting
                
                collect_result = collector.collect_jpx400_data(
                    complement_today=True,
                    stop_check=check_stop
                )
                
                if collect_result.get('stopped', False):
                    msg = (
                        f"データ収集が停止されました。\n\n"
                        f"処理済み: {collect_result.get('processed_count', 0)}件\n"
                        f"成功: {collect_result['success_count']}件\n"
                        f"スキップ: {collect_result.get('skip_count', 0)}件\n"
                        f"エラー: {collect_result['error_count']}件"
                    )
                    self.parent.after(0, lambda: messagebox.showwarning("停止", msg))
                elif collect_result['success']:
                    msg = (
                        f"データ収集が完了しました。\n\n"
                        f"成功: {collect_result['success_count']}件\n"
                        f"スキップ: {collect_result.get('skip_count', 0)}件\n"
                        f"エラー: {collect_result['error_count']}件"
                    )
                    
                    # キャッシュクリア
                    if collect_result['success_count'] > 0 or collect_result.get('skip_count', 0) > 0:
                        from src.screening.screening_result_cache import ScreeningResultCache
                        cache = ScreeningResultCache(self.db_path)
                        cache.clear_cache()
                        print("[キャッシュ] データ収集が完了したため、スクリーニング結果のキャッシュをクリアしました")
                    
                    self.parent.after(0, lambda: messagebox.showinfo("完了", msg))
                    if self.on_record_jpx_collection:
                        self.on_record_jpx_collection(run_type="manual")
                else:
                    self.parent.after(0, lambda: messagebox.showerror("エラー", f"データ収集に失敗しました:\n{collect_result.get('error', '不明なエラー')}"))
            
            except Exception as e:
                import traceback
                error_detail = traceback.format_exc()
                print(f"[ERROR] JPX400データ収集エラー: {e}")
                print(f"[ERROR] 詳細: {error_detail}")
                self.parent.after(0, lambda: messagebox.showerror("エラー", f"データ収集処理でエラーが発生しました:\n{e}\n\n詳細はコンソールを確認してください。"))
            
            finally:
                self._jpx400_collecting = False
                self._stop_collecting = False
                self.jpx400_collect_button.config(state="normal")
                self.jpx400_update_list_button.config(state="normal")
                self.show_symbols_button.config(state="normal")
                self.fetch_names_button.config(state="normal")
                self.stop_collect_button.config(state="disabled")
                self.status_var.set("状態: 待機中")
        
        thread = threading.Thread(target=collect_in_thread, daemon=True)
        thread.start()
    
    def on_stop_collect_jpx400(self):
        """データ収集を強制終了"""
        if not self._jpx400_collecting:
            messagebox.showwarning("警告", "データ収集は実行されていません。")
            return
        
        result = messagebox.askyesno(
            "確認",
            "データ収集を停止しますか？\n\n"
            "現在処理中の銘柄の処理が完了した後に停止します。"
        )
        
        if result:
            self._stop_collecting = True
            self.status_var.set("状態: データ収集停止中...")
            print("[データ収集] 停止要求を受け付けました。処理中の銘柄が完了次第、停止します。")
    
    def on_update_jpx400_list(self):
        """JPX400銘柄リストを更新"""
        try:
            from src.screening.jpx400_manager import JPX400Manager
            from src.screening.jpx400_fetcher import JPX400Fetcher
            
            choice = messagebox.askyesnocancel(
                "JPX400リスト更新",
                "JPX400銘柄リストの更新方法を選択してください。\n\n"
                "「はい」: JPX公式サイトから自動取得（推奨）\n"
                "「いいえ」: CSVファイルから読み込み\n"
                "「キャンセル」: キャンセル"
            )
            
            if choice is None:
                return
            
            fetcher = JPX400Fetcher()
            symbols = None
            
            if choice:
                self.status_var.set("状態: JPX400リスト取得中...")
                self.jpx400_update_list_button.config(state="disabled")
                
                try:
                    symbols = fetcher.fetch_from_jpx_website()
                    
                    if not symbols or len(symbols) < 300:
                        # エラーメッセージを詳細に表示
                        error_msg = f"JPX公式サイトからの取得に失敗しました。\n"
                        error_msg += f"（取得できた銘柄数: {len(symbols) if symbols else 0}件）\n\n"
                        
                        # PDF解析ライブラリがインストールされているか確認
                        try:
                            import pdfplumber
                        except ImportError:
                            try:
                                import PyPDF2
                            except ImportError:
                                error_msg += "【原因】PDF解析ライブラリがインストールされていません。\n"
                                error_msg += "以下のコマンドでインストールしてください:\n"
                                error_msg += "  pip install pdfplumber\n\n"
                        
                        error_msg += "CSVファイルから読み込みますか？"
                        
                        retry_choice = messagebox.askyesno(
                            "取得失敗",
                            error_msg
                        )
                        
                        if retry_choice:
                            csv_file = filedialog.askopenfilename(
                                title="JPX400銘柄リスト（CSV）を選択",
                                filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
                            )
                            
                            if csv_file:
                                symbols = fetcher.parse_csv_file(csv_file)
                finally:
                    self.jpx400_update_list_button.config(state="normal")
                    self.status_var.set("状態: 待機中")
            else:
                csv_file = filedialog.askopenfilename(
                    title="JPX400銘柄リスト（CSV）を選択",
                    filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
                )
                
                if not csv_file:
                    return
                
                symbols = fetcher.parse_csv_file(csv_file)
            
            if not symbols:
                messagebox.showwarning("警告", "銘柄リストが空です。\n取得方法を確認してください。")
                return
        
            result = messagebox.askyesno(
                "確認",
                f"{len(symbols)}銘柄を読み込みました。\n\n"
                f"JPX400銘柄リストを更新しますか？"
            )
            
            if not result:
                return
        
            manager = JPX400Manager()
            source_info = {
                'source': 'JPX公式サイト' if choice else 'CSVファイル',
                'updated_at': datetime.now().isoformat()
            }
            if not choice and csv_file:
                source_info['csv_file'] = csv_file
            
            manager.save_symbols(symbols, source_info)
            
            messagebox.showinfo("完了", f"JPX400銘柄リストを更新しました。\n\n銘柄数: {len(symbols)}件")
        
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            print(f"[ERROR] JPX400リスト更新エラー: {e}")
            print(f"[ERROR] 詳細: {error_detail}")
            messagebox.showerror("エラー", f"リスト更新処理でエラーが発生しました:\n{e}\n\n詳細はコンソールを確認してください。")
            self.jpx400_update_list_button.config(state="normal")
            self.status_var.set("状態: 待機中")
    
    def on_show_symbols(self):
        """DBに保存されている銘柄一覧を表示"""
        def load_in_thread():
            try:
                from src.data_collector.ohlcv_data_manager import OHLCVDataManager
                from collections import defaultdict
                
                print("[DB銘柄一覧] 銘柄一覧の取得を開始します...")
                self.status_var.set("状態: 銘柄一覧取得中...")
                self.show_symbols_button.config(state="disabled")
                
                ohlcv_manager = OHLCVDataManager(self.db_path)
                print(f"[DB銘柄一覧] データベース: {self.db_path}")
                
                print("[DB銘柄一覧] 日足データ（yahoo）を取得中...")
                symbols_info = ohlcv_manager.get_all_symbols(
                    timeframe='1d',
                    source='yahoo'
                )
                print(f"[DB銘柄一覧] 取得した銘柄情報: {len(symbols_info)}件")
                
                if not symbols_info:
                    print("[DB銘柄一覧] 銘柄情報がありません")
                    self.parent.after(0, lambda: messagebox.showinfo("銘柄一覧", "DBに保存されている銘柄はありません。"))
                    self.status_var.set("状態: 待機中")
                    self.show_symbols_button.config(state="normal")
                    return

                # 銘柄コードでグループ化して統計情報を集計
                print("[DB銘柄一覧] 統計情報を集計中...")
                symbol_stats = defaultdict(lambda: {
                    'data_count': 0,
                    'first_date': None,
                    'last_date': None,
                    'last_updated_at': None,
                    'latest_volume': None,
                    'latest_price': None,
                    'sigma_value': None,
                    'latest_is_temporary_close': None
                })
                
                for info in symbols_info:
                    symbol = info['symbol']
                    symbol_stats[symbol]['data_count'] += info['data_count']
                    if symbol_stats[symbol]['first_date'] is None or info['first_date'] < symbol_stats[symbol]['first_date']:
                        symbol_stats[symbol]['first_date'] = info['first_date']
                    if symbol_stats[symbol]['last_date'] is None or info['last_date'] > symbol_stats[symbol]['last_date']:
                        symbol_stats[symbol]['last_date'] = info['last_date']
                    # 最新の更新時刻を取得（last_updated_atがあれば使用）
                    if 'last_updated_at' in info and info['last_updated_at']:
                        if symbol_stats[symbol].get('last_updated_at') is None or info['last_updated_at'] > symbol_stats[symbol].get('last_updated_at'):
                            symbol_stats[symbol]['last_updated_at'] = info['last_updated_at']
                
                print(f"[DB銘柄一覧] 集計完了: {len(symbol_stats)}銘柄")
                
                print("[DB銘柄一覧] 最新出来高と現在株価、σ値を取得中...")
                # 最新データを取得して更新
                for symbol in symbol_stats.keys():
                    try:
                        df_latest = ohlcv_manager.get_ohlcv_data_with_temporary_flag(
                            symbol=symbol,
                            timeframe='1d',
                            source='yahoo',
                            include_temporary=True
                        )
                        if not df_latest.empty:
                            latest_row = df_latest.iloc[-1]
                            symbol_stats[symbol]['latest_price'] = float(latest_row['close'])
                            symbol_stats[symbol]['latest_volume'] = int(latest_row['volume']) if pd.notna(latest_row['volume']) else None
                            # 1分足から作成した仮終値かどうかを保持する
                            is_temp_flag = latest_row.get('is_temporary_close', 0)
                            try:
                                symbol_stats[symbol]['latest_is_temporary_close'] = bool(int(is_temp_flag))
                            except (TypeError, ValueError):
                                symbol_stats[symbol]['latest_is_temporary_close'] = None
                            
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
                        print(f"[DB銘柄一覧] {symbol}の最新データ取得エラー: {e}")
                
                print("[DB銘柄一覧] 銘柄名を取得中...")
                symbol_names = ohlcv_manager.get_symbol_names(list(symbol_stats.keys()))
                print(f"[DB銘柄一覧] 銘柄名取得完了: {len(symbol_names)}件")
                
                print("[DB銘柄一覧] セクター情報を取得中...")
                symbol_sectors = ohlcv_manager.get_symbol_sectors(list(symbol_stats.keys()))
                print(f"[DB銘柄一覧] セクター情報取得完了: {len(symbol_sectors)}件")
                
                print("[DB銘柄一覧] 業種情報を取得中...")
                symbol_industries = ohlcv_manager.get_symbol_industries(list(symbol_stats.keys()))
                print(f"[DB銘柄一覧] 業種情報取得完了: {len(symbol_industries)}件")
                
                print("[DB銘柄一覧] ウィンドウを表示します...")
                self.parent.after(0, lambda: self._show_symbols_window(symbol_stats, symbol_names, symbol_sectors, symbol_industries))
                print(f"[DB銘柄一覧] 完了: {len(symbol_stats)}銘柄を表示")
                
                # ステータスとボタン状態を復帰
                self.status_var.set("状態: 待機中")
                # ボタン状態は_show_symbols_windowのon_window_closeで管理
            
            except Exception as e:
                import traceback
                error_detail = traceback.format_exc()
                print(f"[ERROR] 銘柄一覧取得エラー: {e}")
                print(f"[ERROR] 詳細: {error_detail}")
                self.parent.after(0, lambda: messagebox.showerror("エラー", f"銘柄一覧取得処理でエラーが発生しました:\n{e}\n\n詳細はコンソールを確認してください。"))
                self.status_var.set("状態: 待機中")
                self.show_symbols_button.config(state="normal")
        
        thread = threading.Thread(target=load_in_thread, daemon=True)
        thread.start()
    
    def _show_symbols_window(self, symbol_stats: dict, symbol_names: dict, symbol_sectors: dict = None, symbol_industries: dict = None):
        """銘柄一覧を別ウィンドウで表示（Treeview使用）"""
        window = tk.Toplevel(self.parent)
        window.title("DB銘柄一覧")
        window.geometry("1200x820")
        
        # メインフレーム
        main_frame = ttk.Frame(window)
        main_frame.pack(fill="both", expand=True, padx=8, pady=8)
        
        # ヘッダー情報
        header_frame = ttk.Frame(main_frame)
        header_frame.pack(fill="x", pady=(0, 8))
        
        ttk.Label(
            header_frame,
            text=f"DBに保存されている銘柄: {len(symbol_stats)}件",
            font=("", 10, "bold")
        ).pack(side="left")
        
        # Treeviewとスクロールバー
        tree_frame = ttk.Frame(main_frame)
        tree_frame.pack(fill="both", expand=True)
        
        v_scrollbar = ttk.Scrollbar(tree_frame, orient="vertical")
        v_scrollbar.pack(side="right", fill="y")
        
        h_scrollbar = ttk.Scrollbar(tree_frame, orient="horizontal")
        h_scrollbar.pack(side="bottom", fill="x")
        
        columns = (
            "銘柄コード",
            "銘柄名",
            "セクター",
            "業種",
            "データ件数",
            "最初の日付",
            "最後の日付",
            "データ区分",
            "現在株価",
            "最新出来高",
            "σ値"
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
        for col in columns:
            tree.heading(col, text=col)
        
        tree.column("銘柄コード", width=100, anchor="center")
        tree.column("銘柄名", width=200, anchor="w")
        tree.column("セクター", width=150, anchor="w")
        tree.column("業種", width=200, anchor="w")
        tree.column("データ件数", width=100, anchor="e")
        tree.column("最初の日付", width=120, anchor="center")
        tree.column("最後の日付", width=120, anchor="center")
        tree.column("データ区分", width=110, anchor="center")
        tree.column("現在株価", width=100, anchor="e")
        tree.column("最新出来高", width=120, anchor="e")
        tree.column("σ値", width=120, anchor="e")
        
        # ソート機能（簡略版）
        sort_state = {}
        
        def sort_treeview(column):
            reverse = sort_state.get(column, False)
            sort_state[column] = not reverse
            
            items = [(tree.set(item, column), item) for item in tree.get_children('')]
            
            if column in ["現在株価", "σ値"]:
                def sort_key(x):
                    try:
                        return float(x[0].replace('σ', '').replace('+', '').replace(',', '').strip())
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
        if symbol_sectors is None:
            symbol_sectors = {}
        if symbol_industries is None:
            symbol_industries = {}
        
        import pandas as pd
        sorted_symbols = sorted(symbol_stats.items())
        for symbol, stats in sorted_symbols:
            name = symbol_names.get(symbol, "（未取得）")
            sector = symbol_sectors.get(symbol, "（未取得）")
            industry = symbol_industries.get(symbol, "（未取得）")
            first_date = stats['first_date'][:10] if stats['first_date'] else "N/A"
            
            if stats.get('last_updated_at'):
                try:
                    dt = pd.to_datetime(stats['last_updated_at'])
                    last_date = dt.strftime('%Y-%m-%d %H:%M')
                except:
                    date_str = str(stats['last_updated_at'])
                    if 'T' in date_str:
                        date_str = date_str.replace('T', ' ')
                    last_date = date_str[:16] if len(date_str) >= 16 else date_str
            elif stats['last_date']:
                try:
                    dt = pd.to_datetime(stats['last_date'])
                    last_date = dt.strftime('%Y-%m-%d %H:%M')
                except:
                    date_str = str(stats['last_date'])
                    if 'T' in date_str:
                        date_str = date_str.replace('T', ' ')
                    last_date = date_str[:16] if len(date_str) >= 16 else date_str
            else:
                last_date = "N/A"
            
            latest_price = f"{stats['latest_price']:.2f}" if stats['latest_price'] is not None else "N/A"
            latest_volume = f"{stats['latest_volume']:,}" if stats['latest_volume'] is not None else "N/A"
            sigma_str = f"{stats['sigma_value']:+.2f}σ" if stats.get('sigma_value') is not None else "N/A"
            data_status_label = "仮データ" if stats.get('latest_is_temporary_close') else "正式データ"
            if stats.get('latest_is_temporary_close') is None:
                data_status_label = "不明"
            
            tree.insert(
                "",
                "end",
                values=(
                    symbol,
                    name,
                    sector,
                    industry,
                    f"{stats['data_count']:,}",
                    first_date,
                    last_date,
                    data_status_label,
                    latest_price,
                    latest_volume,
                    sigma_str
                ),
                tags=(symbol,)
            )
        
        # ダブルクリックでチャート表示
        def on_double_click(event):
            item = tree.selection()[0] if tree.selection() else None
            if item:
                symbol = tree.item(item)['tags'][0]
                name = tree.item(item)['values'][1]  # 銘柄名
                if self.on_show_chart_requested:
                    self.on_show_chart_requested(window, symbol, name)
        
        tree.bind("<Double-1>", on_double_click)
        
        # 右クリックメニュー（株探・バフェット・コードへのリンク）
        context_menu = tk.Menu(window, tearoff=0)
        
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
        
        # ウィンドウを閉じた時にボタン状態を復帰
        def on_window_close():
            self.show_symbols_button.config(state="normal")
            window.destroy()
        
        window.protocol("WM_DELETE_WINDOW", on_window_close)
        
        # スクリーニング条件フレーム（簡略版）
        condition_frame = ttk.LabelFrame(main_frame, text="スクリーニング条件", padding=8)
        condition_frame.pack(fill="x", pady=(8, 0))
        
        condition_container = ttk.Frame(condition_frame)
        condition_container.pack(fill="x")
        
        col1 = ttk.Frame(condition_container)
        col1.pack(side="left", anchor="nw", padx=(0, 16))
        
        condition1_var = tk.BooleanVar(value=True)
        condition2_var = tk.BooleanVar(value=True)
        condition3_var = tk.BooleanVar(value=True)
        condition4_var = tk.BooleanVar(value=True)
        condition5_var = tk.BooleanVar(value=True)
        condition6_var = tk.BooleanVar(value=True)
        
        ttk.Checkbutton(col1, text="5MA、25MA、75MA、200MAが上から順に並んでいる", variable=condition1_var).pack(anchor="w", pady=2)
        ttk.Checkbutton(col1, text="直近2日が5MAの上で陽線", variable=condition2_var).pack(anchor="w", pady=2)
        ttk.Checkbutton(col1, text="5MAが上向き", variable=condition3_var).pack(anchor="w", pady=2)
        ttk.Checkbutton(col1, text="25MAが上向き", variable=condition4_var).pack(anchor="w", pady=2)
        ttk.Checkbutton(col1, text="75MAが上向き", variable=condition5_var).pack(anchor="w", pady=2)
        ttk.Checkbutton(col1, text="200MAが上向き", variable=condition6_var).pack(anchor="w", pady=2)
        
        col2 = ttk.Frame(condition_container)
        col2.pack(side="left", anchor="nw", padx=(0, 16))
        
        golden_cross_5_25_var = tk.BooleanVar(value=True)
        golden_cross_25_75_var = tk.BooleanVar(value=False)
        golden_cross_mode_var = tk.StringVar(value="just_crossed")
        
        ttk.Label(col2, text="ゴールデンクロス").pack(anchor="w", pady=(0, 2))
        ttk.Checkbutton(col2, text="5MA/25MAゴールデンクロス", variable=golden_cross_5_25_var).pack(anchor="w", pady=2)
        ttk.Checkbutton(col2, text="25MA/75MAゴールデンクロス", variable=golden_cross_25_75_var).pack(anchor="w", pady=2)
        
        golden_cross_mode_frame = ttk.Frame(col2)
        golden_cross_mode_frame.pack(anchor="w", pady=2, padx=10)
        ttk.Label(golden_cross_mode_frame, text="判定モード:").pack(side="left", padx=(0, 5))
        ttk.Radiobutton(golden_cross_mode_frame, text="直近でクロス", variable=golden_cross_mode_var, value="just_crossed").pack(side="left", padx=5)
        ttk.Radiobutton(golden_cross_mode_frame, text="クロス中", variable=golden_cross_mode_var, value="has_crossed").pack(side="left", padx=5)
        
        col3 = ttk.Frame(condition_container)
        col3.pack(side="left", anchor="nw", padx=(0, 16))
        
        macd_kd_filter_var = tk.BooleanVar(value=False)
        macd_kd_default_window = 1
        
        ttk.Label(col3, text="MACD/KD近接").pack(anchor="w", pady=(0, 2))
        ttk.Checkbutton(col3, text="MACDとKDの上昇サインが近接（営業日±）", variable=macd_kd_filter_var).pack(anchor="w", pady=2)
        
        macd_kd_window_frame = ttk.Frame(col3)
        macd_kd_window_frame.pack(anchor="w", pady=2, padx=10)
        ttk.Label(macd_kd_window_frame, text="近接許容日数（営業日）:").pack(side="left", padx=(0, 5))
        macd_kd_window_var = tk.StringVar(value=str(macd_kd_default_window))
        ttk.Entry(macd_kd_window_frame, width=6, textvariable=macd_kd_window_var).pack(side="left")
        
        # スクリーニングボタン
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill="x", pady=(8, 0))
        
        def screen_all():
            if (not condition1_var.get() and not condition2_var.get() and 
                not condition3_var.get() and not condition4_var.get() and
                not condition5_var.get() and not condition6_var.get() and
                not golden_cross_5_25_var.get() and not golden_cross_25_75_var.get() and
                not macd_kd_filter_var.get()):
                messagebox.showwarning("警告", "少なくとも1つのスクリーニング条件を選択してください。")
                return

            try:
                macd_kd_window = int(macd_kd_window_var.get())
                if macd_kd_window <= 0:
                    raise ValueError
            except ValueError:
                macd_kd_window = macd_kd_default_window
                macd_kd_window_var.set(str(macd_kd_window))
                messagebox.showwarning("警告", f"近接日数は1以上の整数で入力してください。{macd_kd_window}日で実行します。")
            
            all_symbols = list(symbol_stats.keys())
            
            if self.on_screening_requested:
                self.on_screening_requested(
                    window,
                    all_symbols,
                    check_condition1=condition1_var.get(),
                    check_condition2=condition2_var.get(),
                    check_condition3=condition3_var.get(),
                    check_condition4=condition4_var.get(),
                    check_condition5=condition5_var.get(),
                    check_condition6=condition6_var.get(),
                    check_golden_cross_5_25=golden_cross_5_25_var.get(),
                    check_golden_cross_25_75=golden_cross_25_75_var.get(),
                    golden_cross_mode=golden_cross_mode_var.get(),
                    use_macd_kd_filter=macd_kd_filter_var.get(),
                    macd_kd_window=macd_kd_window
                )
        
        ttk.Button(button_frame, text="スクリーニング", command=screen_all).pack(side="left", padx=4)
        ttk.Button(button_frame, text="閉じる", command=window.destroy).pack(side="right", padx=4)
    
    def on_fetch_symbol_names(self):
        """DBに保存されている銘柄の銘柄名を取得"""
        if self._fetching_names:
            messagebox.showwarning("警告", "銘柄名取得は既に実行中です。")
            return
        
        result = messagebox.askyesno(
            "確認",
            "DBに保存されている全銘柄の銘柄名を取得しますか？\n\n"
            "この処理には時間がかかる場合があります。\n"
            "既に銘柄名が保存されている銘柄はスキップされます。"
        )
        
        if not result:
            return
        
        def fetch_in_thread():
            try:
                self._fetching_names = True
                self.jpx400_collect_button.config(state="disabled")
                self.jpx400_update_list_button.config(state="disabled")
                self.show_symbols_button.config(state="disabled")
                self.fetch_names_button.config(state="disabled")
                
                from src.data_collector.ohlcv_data_manager import OHLCVDataManager
                
                ohlcv_manager = OHLCVDataManager(self.db_path)
                
                symbols = ohlcv_manager.get_symbol_list(
                    timeframe='1d',
                    source='yahoo'
                )
                
                if not symbols:
                    self.parent.after(0, lambda: messagebox.showinfo("情報", "DBに保存されている銘柄がありません。"))
                    return
                
                self.status_var.set(f"状態: 銘柄名取得中... (0/{len(symbols)})")
                
                def progress_callback(symbol, success, name, error, current, total):
                    if success:
                        if name:
                            status = f"状態: 銘柄名取得中... ({current}/{total}) - {symbol}: {name[:20]}"
                        else:
                            status = f"状態: 銘柄名取得中... ({current}/{total}) - {symbol}: スキップ"
                    else:
                        status = f"状態: 銘柄名取得中... ({current}/{total}) - {symbol}: エラー"
                    self.status_var.set(status)
                    self.parent.update()
                
                results = ohlcv_manager.fetch_and_save_symbol_names(
                    symbols,
                    progress_callback=progress_callback
                )
                
                msg = (
                    f"銘柄名取得が完了しました。\n\n"
                    f"成功: {results['success_count']}件\n"
                    f"スキップ: {results['skipped_count']}件（既に名前あり）\n"
                    f"エラー: {results['error_count']}件"
                )
                self.parent.after(0, lambda: messagebox.showinfo("完了", msg))
            
            except Exception as e:
                import traceback
                error_detail = traceback.format_exc()
                print(f"[ERROR] 銘柄名取得エラー: {e}")
                print(f"[ERROR] 詳細: {error_detail}")
                self.parent.after(0, lambda: messagebox.showerror("エラー", f"銘柄名取得処理でエラーが発生しました:\n{e}\n\n詳細はコンソールを確認してください。"))
            
            finally:
                self._fetching_names = False
                self.jpx400_collect_button.config(state="normal")
                self.jpx400_update_list_button.config(state="normal")
                self.show_symbols_button.config(state="normal")
                self.fetch_names_button.config(state="normal")
                self.status_var.set("状態: 待機中")
        
        thread = threading.Thread(target=fetch_in_thread, daemon=True)
        thread.start()
    
    def on_show_screening_history(self):
        """スクリーニング履歴一覧を表示"""
        try:
            from src.screening.screening_history import ScreeningHistory
            history_manager = ScreeningHistory(self.db_path)
            
            history_list = history_manager.get_history_list(limit=100)
            
            if not history_list:
                messagebox.showinfo("スクリーニング履歴", "履歴がありません。")
                return
            
            history_window = tk.Toplevel(self.parent)
            history_window.title("スクリーニング履歴")
            # デフォルトサイズを横長に拡大（一覧で勝率/平均/中央値を見やすくする）
            history_window.geometry("1500x650")
            
            main_frame = ttk.Frame(history_window)
            main_frame.pack(fill="both", expand=True, padx=8, pady=8)
            
            header_frame = ttk.Frame(main_frame)
            header_frame.pack(fill="x", pady=(0, 8))
            ttk.Label(
                header_frame,
                text=f"スクリーニング履歴: {len(history_list)}件",
                font=("", 10, "bold")
            ).pack(side="left")
            
            tree_frame = ttk.Frame(main_frame)
            tree_frame.pack(fill="both", expand=True)
            
            v_scrollbar = ttk.Scrollbar(tree_frame, orient="vertical")
            v_scrollbar.pack(side="right", fill="y")
            
            columns = (
                "実行日時", "銘柄数", "条件",
                "+1日勝率", "+1日平均", "+1日中央値",
                "+2日勝率", "+2日平均", "+2日中央値",
                "+3日勝率", "+3日平均", "+3日中央値"
            )
            tree = ttk.Treeview(
                tree_frame,
                columns=columns,
                show="headings",
                yscrollcommand=v_scrollbar.set
            )
            tree.pack(side="left", fill="both", expand=True)
            v_scrollbar.config(command=tree.yview)
            
            # 列の設定
            tree.heading("実行日時", text="実行日時")
            tree.heading("銘柄数", text="銘柄数")
            tree.heading("条件", text="条件")
            tree.heading("+1日勝率", text="+1日勝率")
            tree.heading("+1日平均", text="+1日平均")
            tree.heading("+1日中央値", text="+1日中央値")
            tree.heading("+2日勝率", text="+2日勝率")
            tree.heading("+2日平均", text="+2日平均")
            tree.heading("+2日中央値", text="+2日中央値")
            tree.heading("+3日勝率", text="+3日勝率")
            tree.heading("+3日平均", text="+3日平均")
            tree.heading("+3日中央値", text="+3日中央値")
            
            tree.column("実行日時", width=180, anchor="center")
            tree.column("銘柄数", width=90, anchor="e")  # 右詰め
            tree.column("条件", width=450, anchor="w")
            tree.column("+1日勝率", width=100, anchor="center")
            tree.column("+1日平均", width=100, anchor="center")
            tree.column("+1日中央値", width=100, anchor="center")
            tree.column("+2日勝率", width=100, anchor="center")
            tree.column("+2日平均", width=100, anchor="center")
            tree.column("+2日中央値", width=100, anchor="center")
            tree.column("+3日勝率", width=100, anchor="center")
            tree.column("+3日平均", width=100, anchor="center")
            tree.column("+3日中央値", width=100, anchor="center")
            
            for history in history_list:
                executed_at = history['executed_at']
                try:
                    dt = datetime.fromisoformat(executed_at)
                    executed_at_str = dt.strftime('%Y/%m/%d %H:%M')
                except:
                    executed_at_str = executed_at
                
                # 条件を日本語に変換
                conditions = history.get('conditions', {})
                condition_texts = []
                if conditions.get('check_condition1'):
                    condition_texts.append("移動平均線順序")
                if conditions.get('check_condition2'):
                    condition_texts.append("陽線連続")
                if conditions.get('check_condition3'):
                    condition_texts.append("5MA上向き")
                if conditions.get('check_condition4'):
                    condition_texts.append("25MA上向き")
                if conditions.get('check_condition5'):
                    condition_texts.append("75MA上向き")
                if conditions.get('check_condition6'):
                    condition_texts.append("200MA上向き")
                if conditions.get('check_golden_cross_5_25'):
                    mode = conditions.get('golden_cross_mode', 'just_crossed')
                    mode_text = "直近でクロス" if mode == "just_crossed" else "クロス中"
                    condition_texts.append(f"5/25MA GC({mode_text})")
                if conditions.get('check_golden_cross_25_75'):
                    mode = conditions.get('golden_cross_mode', 'just_crossed')
                    mode_text = "直近でクロス" if mode == "just_crossed" else "クロス中"
                    condition_texts.append(f"25/75MA GC({mode_text})")
                if conditions.get('use_macd_kd_filter'):
                    window = conditions.get('macd_kd_window', 1)
                    condition_texts.append(f"MACD/KD近接(±{window}営業日)")
                
                condition_str = ", ".join(condition_texts) if condition_texts else "条件なし"
                
                # 勝率の整形
                def fmt_rate(h: int) -> str:
                    info = history.get('performance_summary', {}).get('win_rates', {}).get(h, {})
                    total = info.get('total')
                    rate = info.get('rate')
                    win = info.get('win')
                    if total and rate is not None:
                        return f"{win}/{total} ({rate:.1f}%)"
                    return "N/A"

                def fmt_avg(h: int) -> str:
                    info = history.get('performance_summary', {}).get('win_rates', {}).get(h, {})
                    avg = info.get('avg')
                    if avg is None:
                        return "N/A"
                    return f"{avg:+.2f}%"

                def fmt_med(h: int) -> str:
                    info = history.get('performance_summary', {}).get('win_rates', {}).get(h, {})
                    med = info.get('median')
                    if med is None:
                        return "N/A"
                    return f"{med:+.2f}%"

                tree.insert(
                    "",
                    "end",
                    values=(
                        executed_at_str,
                        f"{history['symbol_count']}件",
                        condition_str,
                        fmt_rate(1), fmt_avg(1), fmt_med(1),
                        fmt_rate(2), fmt_avg(2), fmt_med(2),
                        fmt_rate(3), fmt_avg(3), fmt_med(3)
                    ),
                    tags=(history['id'],)
                )
            
            def on_double_click(event):
                item = tree.selection()[0] if tree.selection() else None
                if item:
                    history_id = int(tree.item(item)['tags'][0])
                    self._show_history_detail(history_id, history_window)
            
            tree.bind("<Double-1>", on_double_click)
            
            button_frame = ttk.Frame(main_frame)
            button_frame.pack(fill="x", pady=(8, 0))
            
            # 削除ボタン
            def on_delete():
                selected_item = tree.selection()[0] if tree.selection() else None
                if not selected_item:
                    messagebox.showwarning("警告", "削除する履歴を選択してください。", parent=history_window)
                    return
                
                history_id = int(tree.item(selected_item)['tags'][0])
                executed_at_str = tree.item(selected_item)['values'][0]
                
                # 確認ダイアログ
                result = messagebox.askyesno(
                    "確認", 
                    f"以下の履歴を削除しますか？\n\n実行日時: {executed_at_str}",
                    parent=history_window
                )
                
                if result:
                    if history_manager.delete_history(history_id):
                        tree.delete(selected_item)
                        # ヘッダーのカウントを更新
                        remaining_count = len(tree.get_children())
                        for widget in header_frame.winfo_children():
                            if isinstance(widget, ttk.Label):
                                widget.config(text=f"スクリーニング履歴: {remaining_count}件")
                                break
                        messagebox.showinfo("完了", "履歴を削除しました。", parent=history_window)
                    else:
                        messagebox.showerror("エラー", "履歴の削除に失敗しました。", parent=history_window)
            
            ttk.Button(button_frame, text="削除", command=on_delete).pack(side="left", padx=4)
            ttk.Button(button_frame, text="閉じる", command=history_window.destroy).pack(side="right", padx=4)
        
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            print(f"[ERROR] スクリーニング履歴表示エラー: {e}")
            print(f"[ERROR] 詳細: {error_detail}")
            messagebox.showerror("エラー", f"履歴表示でエラーが発生しました:\n{e}\n\n詳細はコンソールを確認してください。")
    
    def _show_history_detail(self, history_id: int, parent_window):
        """スクリーニング履歴の詳細を表示"""
        try:
            from src.screening.screening_history import ScreeningHistory
            history_manager = ScreeningHistory(self.db_path)
            
            print(f"[履歴詳細] 履歴ID {history_id} の詳細を取得中...")
            history = history_manager.get_history_detail(history_id)
            
            if not history:
                print(f"[履歴詳細] 履歴ID {history_id} が見つかりませんでした")
                messagebox.showerror("エラー", f"履歴が見つかりません。\n履歴ID: {history_id}", parent=parent_window)
                return
            
            print(f"[履歴詳細] 履歴ID {history_id} の詳細を取得しました（銘柄数: {len(history.get('symbols', []))}件）")
            
            # 履歴データを使って結果を表示
            conditions = history['conditions']
            results = history['symbols']
            
            # on_screening_requestedコールバックを使って履歴を表示
            if self.on_screening_requested:
                self.on_screening_requested(
                    parent_window,
                    results,  # 履歴の場合はresultsをそのまま渡す
                    check_condition1=conditions.get('check_condition1', False),
                    check_condition2=conditions.get('check_condition2', False),
                    check_condition3=conditions.get('check_condition3', False),
                    check_condition4=conditions.get('check_condition4', False),
                    check_condition5=conditions.get('check_condition5', False),
                    check_condition6=conditions.get('check_condition6', False),
                    check_golden_cross_5_25=conditions.get('check_golden_cross_5_25', False),
                    check_golden_cross_25_75=conditions.get('check_golden_cross_25_75', False),
                    golden_cross_mode=conditions.get('golden_cross_mode', 'just_crossed'),
                    use_macd_kd_filter=conditions.get('use_macd_kd_filter', False),
                    macd_kd_window=conditions.get('macd_kd_window', 1),
                    is_history=True,
                    executed_at=history['executed_at'],
                    performance_summary=history.get('performance_summary')
                )
        
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            print(f"[ERROR] 履歴詳細表示エラー: {e}")
            print(f"[ERROR] 詳細: {error_detail}")
            messagebox.showerror("エラー", f"履歴詳細表示でエラーが発生しました:\n{e}\n\n詳細はコンソールを確認してください。", parent=parent_window)
    
    def _record_jpx_collection(self, run_type: str):
        """JPX収集ログを記録"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO jpx_collection_log (run_type, executed_at)
                    VALUES (?, ?)
                """, (run_type, datetime.now().isoformat()))
                conn.commit()
        except Exception as e:
            print(f"[WARN] JPX収集ログ記録エラー: {e}")
    
    def get_collecting_state(self) -> bool:
        """データ収集中かどうかを返す"""
        return self._jpx400_collecting
    
    def get_fetching_state(self) -> bool:
        """銘柄名取得中かどうかを返す"""
        return self._fetching_names

