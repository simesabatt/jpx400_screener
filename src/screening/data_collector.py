"""
JPX400銘柄データ収集モジュール

JPX400銘柄の日足データを一括で収集します。
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from src.utils.console import setup_console_encoding
setup_console_encoding()

import yfinance as yf
import pandas as pd
from datetime import datetime, date
from typing import List, Dict, Optional
from pathlib import Path
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.data_collector.ohlcv_data_manager import OHLCVDataManager
from src.screening.jpx400_manager import JPX400Manager


class JPX400DataCollector:
    """JPX400銘柄データ収集クラス"""
    
    def __init__(self, db_path: str = "data/tick_data.db"):
        """
        初期化
        
        Args:
            db_path: データベースパス
        """
        self.db_path = db_path
        self.ohlcv_manager = OHLCVDataManager(db_path)
        self.jpx400_manager = JPX400Manager()
    
    def complement_today_data(
        self,
        symbol: str,
        df_daily: pd.DataFrame
    ) -> pd.DataFrame:
        """
        当日データを補完（19時以降は正式データを取得、それ以前は仮終値）
        
        Args:
            symbol: 銘柄コード
            df_daily: 日足データ（DBから取得）
            
        Returns:
            pd.DataFrame: 補完後の日足データ
        """
        today = date.today()
        current_hour = datetime.now().hour
        
        is_after_19 = current_hour >= 19

        # 19時以降: 正式データで上書き（もしくは正式データが既に入っていれば何もしない）
        if is_after_19:
            try:
                ticker = yf.Ticker(f"{symbol}.T")
                # auto_adjust=False, actions=Trueで調整済み価格と未調整価格の両方を取得
                df_official = ticker.history(period="2d", interval="1d", auto_adjust=False, actions=True)
                
                if not df_official.empty:
                    # 列名を小文字に統一
                    if isinstance(df_official.columns, pd.MultiIndex):
                        df_official.columns = df_official.columns.get_level_values(0)
                    df_official.columns = [col.lower() for col in df_official.columns]
                    
                    # タイムゾーン情報を削除
                    if df_official.index.tz is not None:
                        df_official.index = df_official.index.tz_localize(None)
                    
                    # 当日のデータを確認
                    latest_date = df_official.index[-1].date()
                    if latest_date == today:
                        # 正式な日足データが取得できた
                        official_row = df_official.iloc[-1]
                        
                        # データの整合性チェック：異常なデータを修正
                        open_val = float(official_row['open'])
                        high_val = float(official_row['high'])
                        low_val = float(official_row['low'])
                        close_val = float(official_row['close'])
                        
                        # closeがhighより高い、またはlowより低い場合は修正
                        if close_val > high_val:
                            high_val = close_val
                            print(f"[{symbol}] データ整合性修正: {latest_date} - close({close_val}) > high → highを{close_val}に修正")
                        elif close_val < low_val:
                            low_val = close_val
                            print(f"[{symbol}] データ整合性修正: {latest_date} - close({close_val}) < low → lowを{close_val}に修正")
                        
                        # openがhighより高い、またはlowより低い場合も修正
                        if open_val > high_val:
                            high_val = open_val
                            print(f"[{symbol}] データ整合性修正: {latest_date} - open({open_val}) > high → highを{open_val}に修正")
                        elif open_val < low_val:
                            low_val = open_val
                            print(f"[{symbol}] データ整合性修正: {latest_date} - open({open_val}) < low → lowを{open_val}に修正")
                        
                        # adjusted_closeを取得（存在する場合）
                        adj_close = float(official_row.get('adj close', close_val)) if 'adj close' in official_row else close_val
                        
                        # 調整係数を計算（Adj Close / Close）
                        adjustment_factor = adj_close / close_val if close_val != 0 else 1.0
                        
                        # Open, High, Lowに調整係数を適用
                        today_data = {
                            'open': open_val * adjustment_factor,
                            'high': high_val * adjustment_factor,
                            'low': low_val * adjustment_factor,
                            'close': adj_close,  # 調整済み価格を使用
                            'volume': int(official_row['volume']),
                            'adjusted_close': adj_close,
                            'is_temporary_close': 0  # 正式データ
                        }
                        
                        today_index = pd.Timestamp(today)
                        if not df_daily.empty and df_daily.index[-1].date() == today:
                            # 当日データがある場合、正式データで更新
                            df_daily.loc[today_index] = today_data
                        else:
                            # 当日データがない場合、追加
                            new_row = pd.DataFrame([today_data], index=[today_index])
                            df_daily = pd.concat([df_daily, new_row])
                        
                        print(f"[{symbol}] 19時以降: 正式な日足データを取得しました")
                        return df_daily
                    else:
                        # 当日のデータがまだ更新されていない（前営業日まで）
                        print(f"[{symbol}] 19時以降: 当日の正式データがまだ更新されていません（最新: {latest_date}）")
                        return df_daily
                else:
                    # データが取得できなかった
                    print(f"[{symbol}] 19時以降: 日足データが取得できませんでした")
                    return df_daily
            except Exception as e:
                # 正式データ取得に失敗
                print(f"[{symbol}] 19時以降: 日足データ取得エラー: {e}")
                return df_daily

        # 19時以前: 必ず1分足で当日データを再計算し、仮データとして保存する
        max_retries = 2  # 補完処理は軽量なので、リトライ回数は少なめ
        for attempt in range(max_retries):
            try:
                ticker = yf.Ticker(f"{symbol}.T")
                df_1m = ticker.history(period="1d", interval="1m")
                
                if df_1m.empty:
                    print(f"[{symbol}] 19時以前: 1分足データが空です（補完スキップ）")
                    return df_daily
                
                # 列名を小文字に統一
                df_1m.columns = [col.lower() for col in df_1m.columns]
                
                # 当日の1分足データのみ抽出
                df_1m_today = df_1m[df_1m.index.date == today]
                
                if df_1m_today.empty:
                    print(f"[{symbol}] 19時以前: 当日の1分足データが空です（補完スキップ）。取得データ期間: {df_1m.index[0].date() if not df_1m.empty else 'N/A'} ～ {df_1m.index[-1].date() if not df_1m.empty else 'N/A'}, 今日: {today}")
                    return df_daily
                
                # 当日のOHLCVを作成（仮終値）
                # 調整済み価格は、最新の調整係数を使用して計算
                # 過去データから最新の調整係数を取得
                latest_adjustment_factor = 1.0
                if not df_daily.empty and 'adjusted_close' in df_daily.columns:
                    # 最新のデータで調整係数を計算
                    latest_row = df_daily.iloc[-1]
                    latest_close = latest_row.get('close', 0)
                    latest_adj_close = latest_row.get('adjusted_close', latest_close)
                    if latest_close != 0:
                        latest_adjustment_factor = latest_adj_close / latest_close
                
                current_close = float(df_1m_today.iloc[-1]['close'])
                today_data = {
                    'open': float(df_1m_today.iloc[0]['open']),
                    'high': float(df_1m_today['high'].max()),
                    'low': float(df_1m_today['low'].min()),
                    'close': current_close,  # 現在価格（仮の終値）
                    'volume': int(df_1m_today['volume'].sum()),
                    'adjusted_close': current_close * latest_adjustment_factor,  # 調整済み価格
                    'is_temporary_close': 1  # 仮終値フラグ
                }
                
                # 日足データに追加/更新（仮データとして必ず上書き）
                today_index = pd.Timestamp(today)
                if not df_daily.empty and df_daily.index[-1].date() == today:
                    df_daily.loc[today_index] = today_data
                    print(f"[{symbol}] 19時以前: 当日データを再計算し更新しました（仮終値、時刻: {current_hour}時）")
                else:
                    new_row = pd.DataFrame([today_data], index=[today_index])
                    df_daily = pd.concat([df_daily, new_row])
                    print(f"[{symbol}] 19時以前: 当日データを追加しました（仮終値、時刻: {current_hour}時、終値: {today_data['close']:.2f}）")
                
                return df_daily
            
            except Exception as e:
                error_str = str(e)
                # 404エラーやdelistedエラーの場合は補完をスキップ（リトライ不要）
                if '404' in error_str or 'Not Found' in error_str or 'delisted' in error_str.lower() or 'no data found' in error_str.lower():
                    # 上場廃止銘柄の場合は補完をスキップ
                    return df_daily
                # 接続エラーの場合はリトライ
                elif ('10061' in error_str or 'Connection refused' in error_str or 'urlopen error' in error_str) and attempt < max_retries - 1:
                    time.sleep(0.5 * (attempt + 1))  # 0.5秒、1秒と待機
                    continue
                else:
                    # その他のエラーまたはリトライ上限に達した場合はスキップ
                    if attempt == 0:  # 最初の試行でのみエラーメッセージを表示
                        print(f"[{symbol}] 当日データの補完エラー: {e}")
                    return df_daily
    
    def _check_existing_data(self, symbol: str) -> Dict:
        """
        DB内の既存データを確認
        
        Args:
            symbol: 銘柄コード
            
        Returns:
            dict: 既存データの情報
                - has_data: データが存在するか
                - latest_date: 最新の日付
                - is_temporary: 最新データが仮終値か
                - data_count: データ件数
                - needs_update: 更新が必要か
        """
        today = date.today()
        
        # DBから既存データを取得
        df_existing = self.ohlcv_manager.get_ohlcv_data_with_temporary_flag(
            symbol=symbol,
            timeframe='1d',
            source='yahoo',
            include_temporary=True
        )
        
        if df_existing.empty:
            return {
                'has_data': False,
                'latest_date': None,
                'is_temporary': False,
                'data_count': 0,
                'needs_update': True,
                'needs_full_fetch': True  # 全データ取得が必要
            }
        
        latest_date = df_existing.index[-1].date()
        latest_row = df_existing.iloc[-1]
        is_temporary = latest_row.get('is_temporary_close', 0) == 1
        data_count = len(df_existing)
        
        # 更新が必要か判断
        needs_update = False
        needs_full_fetch = False
        
        if latest_date < today:
            # 最新データが今日より前の場合、更新が必要
            needs_update = True
            # 過去1年分のデータが不足している場合は全取得
            if data_count < 200:  # 約1年分の営業日数は約250日
                needs_full_fetch = True
            else:
                needs_full_fetch = False  # 最新データのみ取得
        elif latest_date == today:
            # 今日のデータがある場合
            if is_temporary:
                # 仮終値の場合は更新が必要（正式データに更新）
                needs_update = True
                needs_full_fetch = False  # 当日データのみ更新
            else:
                # 正式データがある場合でも、調整済み価格が正しく設定されているか確認
                # adjusted_closeがNULL、または0、またはcloseと同じ値の場合は更新が必要
                adjusted_close = latest_row.get('adjusted_close')
                close = latest_row.get('close', 0)
                
                if adjusted_close is None or adjusted_close == 0 or (close != 0 and adjusted_close == close):
                    # 調整済み価格が正しく設定されていない場合は更新が必要
                    needs_update = True
                    needs_full_fetch = False  # 当日データのみ更新
                else:
                    # 調整済み価格が正しく設定されている場合は更新不要
                    needs_update = False
                    needs_full_fetch = False
        else:
            # 未来の日付（ありえないが念のため）
            needs_update = False
            needs_full_fetch = False
        
        return {
            'has_data': True,
            'latest_date': latest_date,
            'is_temporary': is_temporary,
            'data_count': data_count,
            'needs_update': needs_update,
            'needs_full_fetch': needs_full_fetch
        }
    
    def collect_symbol_data(
        self,
        symbol: str,
        complement_today: bool = True,
        max_retries: int = 3,
        retry_delay: float = 1.0
    ) -> Dict:
        """
        1銘柄のデータを収集（既存データ確認・リトライ機能付き）
        
        Args:
            symbol: 銘柄コード
            complement_today: 当日データを補完するか
            max_retries: 最大リトライ回数
            retry_delay: リトライ時の待機時間（秒）
            
        Returns:
            dict: 収集結果
        """
        # 既存データを確認
        existing_info = self._check_existing_data(symbol)
        
        # 更新が不要な場合でも、最新日のデータの整合性チェックと調整済み価格の再計算を行う
        if not existing_info['needs_update']:
            # 最新日のデータを再取得して整合性チェックと調整済み価格の計算を行う
            try:
                ticker = yf.Ticker(f"{symbol}.T")
                # 最新日のデータを取得
                df_latest = ticker.history(period="2d", interval="1d", auto_adjust=False, actions=True)
                
                if not df_latest.empty:
                    # 列名を小文字に統一
                    df_latest.columns = [col.lower() for col in df_latest.columns]
                    
                    # タイムゾーン情報を削除
                    if df_latest.index.tz is not None:
                        df_latest.index = df_latest.index.tz_localize(None)
                    
                    # 'adj close'を'adjusted_close'にリネーム
                    if 'adj close' in df_latest.columns:
                        df_latest = df_latest.rename(columns={'adj close': 'adjusted_close'})
                    
                    # 既存データを取得
                    df_existing = self.ohlcv_manager.get_ohlcv_data_with_temporary_flag(
                        symbol=symbol,
                        timeframe='1d',
                        source='yahoo',
                        include_temporary=True
                    )
                    
                    if not df_existing.empty:
                        # 最新日のデータを整合性チェックと調整済み価格の計算を行う
                        today = date.today()
                        latest_date = df_latest.index[-1].date()
                        
                        if latest_date == today or latest_date == existing_info['latest_date']:
                            # 最新日のデータを処理
                            for idx in df_latest.index:
                                row = df_latest.loc[idx]
                                open_val = float(row.get('open', 0))
                                high_val = float(row.get('high', 0))
                                low_val = float(row.get('low', 0))
                                close_val = float(row.get('close', 0))
                                
                                # 整合性チェックと修正
                                if close_val > high_val:
                                    high_val = close_val
                                    print(f"[{symbol}] データ整合性修正: {idx.date()} - close({close_val}) > high → highを{close_val}に修正")
                                elif close_val < low_val:
                                    low_val = close_val
                                    print(f"[{symbol}] データ整合性修正: {idx.date()} - close({close_val}) < low → lowを{close_val}に修正")
                                
                                if open_val > high_val:
                                    high_val = open_val
                                    print(f"[{symbol}] データ整合性修正: {idx.date()} - open({open_val}) > high → highを{open_val}に修正")
                                elif open_val < low_val:
                                    low_val = open_val
                                    print(f"[{symbol}] データ整合性修正: {idx.date()} - open({open_val}) < low → lowを{open_val}に修正")
                                
                                # 調整済み価格を計算
                                if 'adjusted_close' in df_latest.columns:
                                    adj_close = float(row.get('adjusted_close', close_val))
                                    if close_val != 0:
                                        adjustment_factor = adj_close / close_val
                                    else:
                                        adjustment_factor = 1.0
                                    
                                    # Open, High, Lowに調整係数を適用
                                    adjusted_open = open_val * adjustment_factor
                                    adjusted_high = high_val * adjustment_factor
                                    adjusted_low = low_val * adjustment_factor
                                    
                                    # 既存データと比較して、変更がある場合は更新
                                    if idx in df_existing.index:
                                        existing_row = df_existing.loc[idx]
                                        if (abs(existing_row.get('open', 0) - adjusted_open) > 0.01 or
                                            abs(existing_row.get('high', 0) - adjusted_high) > 0.01 or
                                            abs(existing_row.get('low', 0) - adjusted_low) > 0.01 or
                                            abs(existing_row.get('close', 0) - adj_close) > 0.01 or
                                            abs(existing_row.get('adjusted_close', 0) - adj_close) > 0.01):
                                            
                                            # データを更新
                                            df_existing.loc[idx, 'open'] = adjusted_open
                                            df_existing.loc[idx, 'high'] = adjusted_high
                                            df_existing.loc[idx, 'low'] = adjusted_low
                                            df_existing.loc[idx, 'close'] = adj_close
                                            df_existing.loc[idx, 'adjusted_close'] = adj_close
                                            
                                            # DBに保存
                                            result = self.ohlcv_manager.save_ohlcv_data_with_temporary_flag(
                                                symbol=symbol,
                                                df=df_existing,
                                                timeframe='1d',
                                                source='yahoo',
                                                overwrite=True,
                                                allow_temporary_overwrite_latest=True
                                            )
                                            
                                            return {
                                                'symbol': symbol,
                                                'success': True,
                                                'saved_count': result['saved_count'],
                                                'updated_count': result['updated_count'],
                                                'skipped_count': result['skipped_count'],
                                                'total_count': result['total_count'],
                                                'retry_count': 0,
                                                'skipped': False,
                                                'fetch_type': 'integrity_check'
                                            }
                                    
                                    # 最新日のデータを補完
                                    if complement_today:
                                        df_existing = self.complement_today_data(symbol, df_existing)
                                        result = self.ohlcv_manager.save_ohlcv_data_with_temporary_flag(
                                            symbol=symbol,
                                            df=df_existing,
                                            timeframe='1d',
                                            source='yahoo',
                                            overwrite=True,
                                            allow_temporary_overwrite_latest=True
                                        )
                                        
                                        if result['updated_count'] > 0:
                                            return {
                                                'symbol': symbol,
                                                'success': True,
                                                'saved_count': result['saved_count'],
                                                'updated_count': result['updated_count'],
                                                'skipped_count': result['skipped_count'],
                                                'total_count': result['total_count'],
                                                'retry_count': 0,
                                                'skipped': False,
                                                'fetch_type': 'complement'
                                            }
            except Exception as e:
                # エラーが発生してもスキップ
                print(f"[{symbol}] 最新データの整合性チェックでエラー: {e}")
            
            # 補完が不要または補完しても変更がない場合はスキップ
            return {
                'symbol': symbol,
                'success': True,
                'saved_count': 0,
                'updated_count': 0,
                'skipped_count': 0,
                'total_count': existing_info['data_count'],
                'retry_count': 0,
                'skipped': True,
                'reason': f"最新データあり（{existing_info['latest_date']}）"
            }
        
        last_error = None
        
        for attempt in range(max_retries):
            try:
                # 必要な期間のデータを取得
                if existing_info['needs_full_fetch']:
                    # 過去1年分を取得
                    period = "1y"
                    interval = "1d"
                else:
                    # 最新データのみ取得（過去30日分を取得して、不足分を補完）
                    period = "1mo"
                    interval = "1d"
                
                ticker = yf.Ticker(f"{symbol}.T")
                
                # 銘柄情報を確認（存在しない銘柄や上場廃止銘柄を検出）
                # ※銘柄名の更新は別ボタンで行うため、ここでは保存しない
                try:
                    info = ticker.info
                    if not info or len(info) <= 1:  # 空の辞書または最小限の情報のみ
                        return {
                            'symbol': symbol,
                            'success': False,
                            'error': '銘柄が見つかりません（上場廃止の可能性）'
                        }
                except Exception:
                    # 情報取得エラーは無視して続行（データ取得を試みる）
                    pass
                
                # 日足データを取得（調整済み価格と未調整価格の両方を取得）
                try:
                    df_daily = ticker.history(period=period, interval=interval, auto_adjust=False, actions=True)
                except Exception as history_error:
                    error_str = str(history_error)
                    # 404エラーやdelistedエラーを検出
                    if '404' in error_str or 'Not Found' in error_str or 'delisted' in error_str.lower() or 'no data found' in error_str.lower():
                        return {
                            'symbol': symbol,
                            'success': False,
                            'error': '銘柄が見つかりません（上場廃止または存在しない銘柄）'
                        }
                    # その他のエラーは再スロー
                    raise
                
                if df_daily.empty:
                    return {
                        'symbol': symbol,
                        'success': False,
                        'error': 'データが見つかりませんでした（上場廃止の可能性）'
                    }
                
                # 列名を小文字に統一
                df_daily.columns = [col.lower() for col in df_daily.columns]
                
                # 必要な列を抽出（adjusted closeが存在する場合は含める）
                required_cols = ['open', 'high', 'low', 'close', 'volume']
                if 'adj close' in df_daily.columns:
                    required_cols.append('adj close')
                # 存在する列のみ抽出
                available_cols = [col for col in required_cols if col in df_daily.columns]
                df_daily = df_daily[available_cols]
                
                # 'adj close'を'adjusted_close'にリネーム（統一性のため）
                if 'adj close' in df_daily.columns:
                    df_daily = df_daily.rename(columns={'adj close': 'adjusted_close'})
                
                # タイムゾーン情報を削除
                if df_daily.index.tz is not None:
                    df_daily.index = df_daily.index.tz_localize(None)
                
                # データの整合性チェック：異常なデータ（close > high や close < low）を修正
                # yfinanceのデータに異常がある場合があるため、整合性を確保
                # まず整合性を確保してから、調整済み価格を計算する
                for idx in df_daily.index:
                    row = df_daily.loc[idx]
                    open_val = float(row.get('open', 0))
                    high_val = float(row.get('high', 0))
                    low_val = float(row.get('low', 0))
                    close_val = float(row.get('close', 0))
                    
                    # 整合性チェックと修正
                    # closeがhighより高い、またはlowより低い場合は修正
                    if close_val > high_val:
                        # closeがhighより高い場合は、highをcloseに合わせる
                        high_val = close_val
                        print(f"[{symbol}] データ整合性修正: {idx.date()} - close({close_val}) > high → highを{close_val}に修正")
                    elif close_val < low_val:
                        # closeがlowより低い場合は、lowをcloseに合わせる
                        low_val = close_val
                        print(f"[{symbol}] データ整合性修正: {idx.date()} - close({close_val}) < low → lowを{close_val}に修正")
                    
                    # openがhighより高い、またはlowより低い場合も修正
                    if open_val > high_val:
                        high_val = open_val
                        print(f"[{symbol}] データ整合性修正: {idx.date()} - open({open_val}) > high → highを{open_val}に修正")
                    elif open_val < low_val:
                        low_val = open_val
                        print(f"[{symbol}] データ整合性修正: {idx.date()} - open({open_val}) < low → lowを{open_val}に修正")
                    
                    # 整合性を確保した後、調整済み価格を計算
                    if 'adjusted_close' in df_daily.columns:
                        adj_close = float(row.get('adjusted_close', close_val))
                        if close_val != 0:
                            adjustment_factor = adj_close / close_val
                        else:
                            adjustment_factor = 1.0
                        
                        # Open, High, Lowに調整係数を適用（整合性を確保した値を使用）
                        df_daily.loc[idx, 'open'] = open_val * adjustment_factor
                        df_daily.loc[idx, 'high'] = high_val * adjustment_factor
                        df_daily.loc[idx, 'low'] = low_val * adjustment_factor
                        df_daily.loc[idx, 'close'] = adj_close  # 調整済み価格を使用
                        df_daily.loc[idx, 'adjusted_close'] = adj_close
                    else:
                        # adjusted_closeがない場合は、整合性を確保した値をそのまま使用
                        df_daily.loc[idx, 'open'] = open_val
                        df_daily.loc[idx, 'high'] = high_val
                        df_daily.loc[idx, 'low'] = low_val
                        df_daily.loc[idx, 'close'] = close_val
                
                # 部分取得の場合は既存データとマージ
                if not existing_info['needs_full_fetch'] and existing_info['has_data']:
                    # 既存データを取得
                    df_existing = self.ohlcv_manager.get_ohlcv_data_with_temporary_flag(
                        symbol=symbol,
                        timeframe='1d',
                        source='yahoo',
                        include_temporary=True
                    )
                    
                    if not df_existing.empty:
                        # 既存データと新しいデータをマージ
                        # 新しいデータで既存データを上書き（重複する日付は新しいデータを優先）
                        df_combined = pd.concat([df_existing, df_daily])
                        # 重複を削除（新しいデータを優先）
                        df_combined = df_combined[~df_combined.index.duplicated(keep='last')]
                        df_combined = df_combined.sort_index()
                        df_daily = df_combined
                
                # 当日データを補完（仮終値フラグ付き）
                if complement_today:
                    df_daily = self.complement_today_data(symbol, df_daily)
                
                # 仮終値フラグがない場合は0（正式）を設定
                if 'is_temporary_close' not in df_daily.columns:
                    df_daily['is_temporary_close'] = 0
                
                # 調整済み価格が取得できた場合、既存データの調整済み価格を自動更新
                if 'adjusted_close' in df_daily.columns and not df_daily.empty:
                    # 最新データで調整係数を計算
                    latest_row = df_daily.iloc[-1]
                    latest_adj_close = latest_row.get('adjusted_close')
                    latest_close = latest_row.get('close')
                    
                    if latest_adj_close is not None and latest_close is not None and latest_close != 0:
                        adjustment_factor = float(latest_adj_close) / float(latest_close)
                        
                        # 既存データの調整済み価格を更新（調整係数が異なる場合のみ）
                        # 既存データの最新の調整済み価格と比較して、異なる場合は更新
                        try:
                            df_existing_check = self.ohlcv_manager.get_ohlcv_data_with_temporary_flag(
                                symbol=symbol,
                                timeframe='1d',
                                source='yahoo',
                                include_temporary=False  # 正式データのみ
                            )
                            
                            should_update = False
                            
                            if not df_existing_check.empty and 'adjusted_close' in df_existing_check.columns:
                                # 既存データの最新の調整済み価格を確認
                                latest_existing = df_existing_check.iloc[-1]
                                existing_adj_close = latest_existing.get('adjusted_close')
                                existing_close = latest_existing.get('close')
                                
                                # 調整係数が異なる場合（1%以上の差がある場合）は更新
                                if existing_close is not None and existing_close != 0:
                                    if existing_adj_close is not None:
                                        existing_factor = float(existing_adj_close) / float(existing_close)
                                        factor_diff = abs(adjustment_factor - existing_factor) / existing_factor if existing_factor != 0 else 1.0
                                        
                                        if factor_diff > 0.01:  # 1%以上の差がある場合
                                            should_update = True
                                            print(f"[{symbol}] 調整係数の変化を検出（既存: {existing_factor:.6f}, 新規: {adjustment_factor:.6f}）。既存データを更新します。")
                                    else:
                                        # adjusted_closeがNULLの場合は更新
                                        should_update = True
                                        print(f"[{symbol}] 調整済み価格が未設定のため、既存データを更新します。")
                                else:
                                    # adjusted_closeがcloseと同じ値の場合は更新
                                    if existing_adj_close is None or existing_adj_close == existing_close:
                                        should_update = True
                                        print(f"[{symbol}] 調整済み価格が未設定のため、既存データを更新します。")
                            else:
                                # 既存データがない場合はスキップ（新規データのみ保存）
                                pass
                            
                            if should_update:
                                # 既存データの調整済み価格を一括更新（open, high, low, close, adjusted_closeすべてを更新）
                                # 株式分割等が発生した場合、過去データも正しく調整する必要がある
                                import sqlite3
                                with sqlite3.connect(self.ohlcv_manager.db_path) as conn:
                                    cursor = conn.cursor()
                                    # 更新対象のデータを取得
                                    cursor.execute('''
                                        SELECT datetime, open, high, low, close, adjusted_close
                                        FROM ohlcv_data
                                        WHERE symbol = ? AND timeframe = '1d' AND source = 'yahoo'
                                          AND (
                                              adjusted_close IS NULL 
                                              OR adjusted_close = close
                                              OR ABS(adjusted_close / NULLIF(close, 0) - ?) > 0.01
                                          )
                                    ''', (symbol, adjustment_factor))
                                    rows_to_update = cursor.fetchall()
                                    
                                    rows_updated = 0
                                    for row in rows_to_update:
                                        dt_str, open_val, high_val, low_val, close_val, adj_close_val = row
                                        if close_val == 0:
                                            continue
                                        
                                        # 調整係数を適用してopen, high, low, close, adjusted_closeすべてを更新
                                        adjusted_open = open_val * adjustment_factor
                                        adjusted_high = high_val * adjustment_factor
                                        adjusted_low = low_val * adjustment_factor
                                        adjusted_close = close_val * adjustment_factor
                                        
                                        cursor.execute('''
                                            UPDATE ohlcv_data
                                            SET open = ?, high = ?, low = ?, close = ?, adjusted_close = ?
                                            WHERE symbol = ? AND datetime = ? AND timeframe = '1d' AND source = 'yahoo'
                                        ''', (adjusted_open, adjusted_high, adjusted_low, adjusted_close, adjusted_close, symbol, dt_str))
                                        
                                        if cursor.rowcount > 0:
                                            rows_updated += 1
                                    
                                    conn.commit()
                                    if rows_updated > 0:
                                        print(f"[{symbol}] 既存データの調整済み価格を{rows_updated}件更新しました（open, high, low, close, adjusted_closeすべて）")
                        except Exception as e:
                            # エラーが発生してもデータ収集は続行
                            print(f"[{symbol}] 既存データの調整済み価格更新でエラー: {e}")
                
                # DBに保存（仮終値フラグを考慮）
                result = self.ohlcv_manager.save_ohlcv_data_with_temporary_flag(
                    symbol=symbol,
                    df=df_daily,
                    timeframe='1d',
                    source='yahoo',
                    overwrite=True,
                    allow_temporary_overwrite_latest=True
                )
                
                return {
                    'symbol': symbol,
                    'success': True,
                    'saved_count': result['saved_count'],
                    'updated_count': result['updated_count'],
                    'skipped_count': result['skipped_count'],
                    'total_count': result['total_count'],
                    'retry_count': attempt,
                    'skipped': False,
                    'fetch_type': 'full' if existing_info['needs_full_fetch'] else 'partial'
                }
            
            except Exception as e:
                last_error = e
                error_str = str(e)
                
                # 接続エラー（WinError 10061など）の場合はリトライ
                if '10061' in error_str or 'Connection refused' in error_str or 'urlopen error' in error_str:
                    if attempt < max_retries - 1:
                        # 指数バックオフで待機（1秒、2秒、4秒...）
                        wait_time = retry_delay * (2 ** attempt) + random.uniform(0, 0.5)
                        time.sleep(wait_time)
                        continue
                    else:
                        return {
                            'symbol': symbol,
                            'success': False,
                            'error': f'接続エラー（リトライ{max_retries}回失敗）: {error_str}',
                            'retry_count': attempt + 1
                        }
                else:
                    # その他のエラーは即座に返す
                    return {
                        'symbol': symbol,
                        'success': False,
                        'error': error_str,
                        'retry_count': attempt
                    }
        
        # すべてのリトライが失敗した場合
        return {
            'symbol': symbol,
            'success': False,
            'error': f'リトライ{max_retries}回失敗: {str(last_error)}',
            'retry_count': max_retries
        }
    
    def collect_jpx400_data(
        self,
        complement_today: bool = True,
        progress_callback: Optional[callable] = None,
        stop_check: Optional[callable] = None,
        max_workers: int = 4
    ) -> Dict:
        """
        JPX400銘柄のデータを一括収集（並列処理対応）
        
        Args:
            complement_today: 当日データを補完するか
            progress_callback: 進捗コールバック関数（symbol, current, total, result）
            stop_check: 停止チェック関数（Trueを返すと停止）
            max_workers: 並列処理数（デフォルト: 4）
            
        Returns:
            dict: 収集結果のサマリー
        """
        # JPX400銘柄リストを読み込み
        symbols = self.jpx400_manager.load_symbols()
        
        if not symbols:
            return {
                'success': False,
                'error': 'JPX400銘柄リストが空です'
            }
        
        print(f"\n{'='*80}")
        print(f"JPX400銘柄データ収集を開始します")
        print(f"{'='*80}")
        print(f"対象銘柄数: {len(symbols)}")
        print(f"当日データ補完: {'有効' if complement_today else '無効'}")
        print(f"並列処理数: {max_workers}")
        print(f"{'='*80}\n")
        
        results = []
        success_count = 0
        error_count = 0
        skip_count = 0
        processed_count = 0
        
        # 並列処理を使用するかどうか（max_workers > 1の場合）
        use_parallel = max_workers > 1
        
        if use_parallel:
            # 並列処理モード
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 各銘柄の処理を並列実行
                future_to_symbol = {
                    executor.submit(self.collect_symbol_data, symbol, complement_today): symbol
                    for symbol in symbols
                }
                
                # 完了したタスクから順に処理
                for future in as_completed(future_to_symbol):
                    # 停止チェック
                    if stop_check and stop_check():
                        print(f"\n[データ収集] 停止要求を検出しました。処理を中断します。")
                        # 実行中のタスクをキャンセル
                        for f in future_to_symbol:
                            f.cancel()
                        return {
                            'success': True,
                            'stopped': True,
                            'total_count': len(symbols),
                            'processed_count': processed_count,
                            'success_count': success_count,
                            'skip_count': skip_count,
                            'error_count': error_count,
                            'results': results
                        }
                    
                    symbol = future_to_symbol[future]
                    processed_count += 1
                    
                    try:
                        result = future.result()
                        results.append(result)
                        
                        if result['success']:
                            if result.get('skipped', False):
                                # スキップされた場合
                                skip_count += 1
                                reason = result.get('reason', '最新データあり')
                                print(f"[{processed_count}/{len(symbols)}] {symbol}: ⊘ スキップ: {reason}")
                            else:
                                # データ取得・保存が完了した場合
                                success_count += 1
                                retry_info = f" (リトライ: {result.get('retry_count', 0)}回)" if result.get('retry_count', 0) > 0 else ""
                                fetch_type = result.get('fetch_type', 'unknown')
                                fetch_info = f" [{fetch_type}]" if fetch_type != 'unknown' else ""
                                print(f"[{processed_count}/{len(symbols)}] {symbol}: ✓ 完了 (保存: {result['saved_count']}, 更新: {result['updated_count']}){fetch_info}{retry_info}")
                        else:
                            error_count += 1
                            error_msg = result.get('error', '不明なエラー')
                            # エラーメッセージが長い場合は短縮
                            if len(error_msg) > 60:
                                error_msg = error_msg[:57] + "..."
                            print(f"[{processed_count}/{len(symbols)}] {symbol}: ✗ エラー: {error_msg}")
                        
                        # 進捗コールバック
                        if progress_callback:
                            progress_callback(symbol, processed_count, len(symbols), result)
                    
                    except Exception as e:
                        # タスク実行時の例外をキャッチ
                        error_count += 1
                        error_msg = str(e)
                        if len(error_msg) > 60:
                            error_msg = error_msg[:57] + "..."
                        print(f"[{processed_count}/{len(symbols)}] {symbol}: ✗ 例外: {error_msg}")
                        
                        result = {
                            'symbol': symbol,
                            'success': False,
                            'error': f'タスク実行エラー: {error_msg}',
                            'retry_count': 0
                        }
                        results.append(result)
                        
                        if progress_callback:
                            progress_callback(symbol, processed_count, len(symbols), result)
                    
                    # 並列処理時の待機時間（短縮版: 0.1〜0.3秒）
                    # ただし、最後のタスク以外のみ
                    if processed_count < len(symbols):
                        wait_time = random.uniform(0.1, 0.3)
                        time.sleep(wait_time)
        else:
            # 順次処理モード（従来の実装）
            for i, symbol in enumerate(symbols, 1):
                # 停止チェック
                if stop_check and stop_check():
                    print(f"\n[データ収集] 停止要求を検出しました。処理を中断します。")
                    return {
                        'success': True,
                        'stopped': True,
                        'total_count': len(symbols),
                        'processed_count': i - 1,
                        'success_count': success_count,
                        'skip_count': skip_count,
                        'error_count': error_count,
                        'results': results
                    }
                
                print(f"[{i}/{len(symbols)}] {symbol} のデータ収集中...", end=" ")
                
                result = self.collect_symbol_data(symbol, complement_today)
                results.append(result)
                
                if result['success']:
                    if result.get('skipped', False):
                        # スキップされた場合
                        skip_count += 1
                        reason = result.get('reason', '最新データあり')
                        print(f"⊘ スキップ: {reason}")
                    else:
                        # データ取得・保存が完了した場合
                        success_count += 1
                        retry_info = f" (リトライ: {result.get('retry_count', 0)}回)" if result.get('retry_count', 0) > 0 else ""
                        fetch_type = result.get('fetch_type', 'unknown')
                        fetch_info = f" [{fetch_type}]" if fetch_type != 'unknown' else ""
                        print(f"✓ 完了 (保存: {result['saved_count']}, 更新: {result['updated_count']}){fetch_info}{retry_info}")
                else:
                    error_count += 1
                    error_msg = result.get('error', '不明なエラー')
                    # エラーメッセージが長い場合は短縮
                    if len(error_msg) > 60:
                        error_msg = error_msg[:57] + "..."
                    print(f"✗ エラー: {error_msg}")
                
                # 進捗コールバック
                if progress_callback:
                    progress_callback(symbol, i, len(symbols), result)
                
                # レート制限対策: リクエスト間に待機時間を追加
                # 0.5秒〜1.0秒のランダムな待機時間（順次処理時）
                if i < len(symbols):  # 最後の銘柄以外は待機
                    wait_time = random.uniform(0.5, 1.0)
                    # 待機中も停止チェック（0.1秒ごとにチェック）
                    elapsed = 0.0
                    while elapsed < wait_time:
                        if stop_check and stop_check():
                            print(f"\n[データ収集] 停止要求を検出しました。処理を中断します。")
                            return {
                                'success': True,
                                'stopped': True,
                                'total_count': len(symbols),
                                'processed_count': i,
                                'success_count': success_count,
                                'skip_count': skip_count,
                                'error_count': error_count,
                                'results': results
                            }
                        sleep_interval = min(0.1, wait_time - elapsed)
                        time.sleep(sleep_interval)
                        elapsed += sleep_interval
        
        print(f"\n{'='*80}")
        print(f"データ収集完了")
        print(f"{'='*80}")
        print(f"成功: {success_count}件")
        print(f"スキップ: {skip_count}件（最新データあり）")
        print(f"エラー: {error_count}件")
        print(f"{'='*80}\n")
        
        return {
            'success': True,
            'stopped': False,
            'total_count': len(symbols),
            'success_count': success_count,
            'skip_count': skip_count,
            'error_count': error_count,
            'results': results
        }


if __name__ == '__main__':
    # テスト実行
    collector = JPX400DataCollector()
    result = collector.collect_jpx400_data(complement_today=True)
    print(f"\n収集結果: {result}")

