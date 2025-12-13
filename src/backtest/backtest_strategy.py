"""
バックテスト用戦略基底クラス

スクリーニング結果のバックテストで使用する戦略の基底クラスと、
基本的な戦略実装を提供します。

Copyright (c) 2025 JPX400スクリーニングシステム

This software is licensed under the MIT License.
See LICENSE file for details.
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pandas as pd
import numpy as np
from typing import Dict, Optional, Tuple
from datetime import datetime, timedelta
from abc import ABC, abstractmethod


class BacktestStrategy(ABC):
    """バックテスト戦略の基底クラス"""
    
    def __init__(self, name: str = "Base Strategy"):
        """
        初期化
        
        Args:
            name: 戦略名
        """
        self.name = name
    
    @abstractmethod
    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        売買シグナルを生成
        
        Args:
            df: OHLCVデータ
            
        Returns:
            シグナル列を追加したDataFrame
            - 'signal': 1=買い, -1=売り, 0=ホールド
            - 'position': 現在のポジション（1=保有中, 0=未保有）
        """
        pass
    
    def calculate_returns(
        self,
        df: pd.DataFrame,
        commission_rate: float = 0.0,
        slippage_rate: float = 0.001
    ) -> pd.DataFrame:
        """
        リターンを計算
        
        Args:
            df: シグナル付きOHLCVデータ
            commission_rate: 手数料率（片道）
            slippage_rate: スリッページ率
            
        Returns:
            リターン列を追加したDataFrame
        """
        df = df.copy()
        
        # 取引コストの合計
        total_cost_rate = commission_rate + slippage_rate
        
        # リターン計算
        df['returns'] = 0.0
        df['cumulative_returns'] = 1.0
        
        position = 0
        entry_price = 0.0
        
        for i in range(1, len(df)):
            # 前日のシグナルを確認
            signal = df.iloc[i-1]['signal']
            
            if signal == 1 and position == 0:
                # 買いエントリー（翌日の始値）
                entry_price = df.iloc[i]['open']
                # 取引コスト（エントリー時）
                entry_cost = entry_price * total_cost_rate
                position = 1
                df.loc[df.index[i], 'returns'] = -total_cost_rate
                
            elif signal == -1 and position == 1:
                # 売りエグジット（翌日の始値）
                exit_price = df.iloc[i]['open']
                # 取引コスト（エグジット時）
                exit_cost = exit_price * total_cost_rate
                
                # リターン計算
                trade_return = (exit_price - entry_price) / entry_price
                # 取引コストを差し引く
                net_return = trade_return - total_cost_rate * 2
                
                df.loc[df.index[i], 'returns'] = net_return
                position = 0
                entry_price = 0.0
            
            elif position == 1:
                # ポジション保有中（日次リターン）
                daily_return = (df.iloc[i]['close'] - df.iloc[i-1]['close']) / df.iloc[i-1]['close']
                df.loc[df.index[i], 'returns'] = daily_return
        
        # 累積リターン
        df['cumulative_returns'] = (1 + df['returns']).cumprod()
        
        return df
    
    def calculate_metrics(self, df: pd.DataFrame) -> Dict:
        """
        パフォーマンス指標を計算
        
        Args:
            df: リターン計算済みDataFrame
            
        Returns:
            パフォーマンス指標の辞書
        """
        if 'returns' not in df.columns:
            return {}
        
        returns = df['returns'].values
        cumulative_returns = df['cumulative_returns'].values
        
        # 基本統計
        total_return = cumulative_returns[-1] - 1.0
        num_trades = int(df['signal'].abs().sum() / 2)  # 売買ペアの数
        
        # 勝率計算
        trade_returns = []
        if 'signal' in df.columns:
            entry_indices = df[df['signal'] == 1].index
            exit_indices = df[df['signal'] == -1].index
            
            for entry_idx, exit_idx in zip(entry_indices, exit_indices):
                entry_pos = df.index.get_loc(entry_idx)
                exit_pos = df.index.get_loc(exit_idx)
                
                if exit_pos < len(df):
                    entry_price = df.iloc[entry_pos + 1]['open'] if entry_pos + 1 < len(df) else df.iloc[entry_pos]['close']
                    exit_price = df.iloc[exit_pos + 1]['open'] if exit_pos + 1 < len(df) else df.iloc[exit_pos]['close']
                    trade_return = (exit_price - entry_price) / entry_price
                    trade_returns.append(trade_return)
        
        win_rate = 0.0
        avg_win = 0.0
        avg_loss = 0.0
        
        if trade_returns:
            wins = [r for r in trade_returns if r > 0]
            losses = [r for r in trade_returns if r <= 0]
            win_rate = len(wins) / len(trade_returns) if trade_returns else 0
            avg_win = np.mean(wins) if wins else 0
            avg_loss = np.mean(losses) if losses else 0
        
        # 最大ドローダウン
        cummax = pd.Series(cumulative_returns).cummax()
        drawdown = (pd.Series(cumulative_returns) - cummax) / cummax
        max_drawdown = drawdown.min()
        
        # シャープレシオ（年率換算）
        if returns.std() != 0:
            sharpe_ratio = (returns.mean() / returns.std()) * np.sqrt(252)
        else:
            sharpe_ratio = 0.0
        
        return {
            'total_return': total_return,
            'num_trades': num_trades,
            'win_rate': win_rate,
            'avg_win': avg_win,
            'avg_loss': avg_loss,
            'max_drawdown': max_drawdown,
            'sharpe_ratio': sharpe_ratio,
            'final_equity': cumulative_returns[-1]
        }


class HoldNDaysStrategy(BacktestStrategy):
    """N日保有戦略"""
    
    def __init__(self, holding_days: int = 5):
        """
        初期化
        
        Args:
            holding_days: 保有日数
        """
        super().__init__(name=f"{holding_days}日保有戦略")
        self.holding_days = holding_days
    
    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        シグナル生成：初日に買い、N日後に売り
        
        Args:
            df: OHLCVデータ
            
        Returns:
            シグナル付きDataFrame
        """
        df = df.copy()
        df['signal'] = 0
        df['position'] = 0
        
        if len(df) < self.holding_days + 1:
            return df
        
        # 初日に買いシグナル
        df.loc[df.index[0], 'signal'] = 1
        
        # N日後に売りシグナル
        if len(df) > self.holding_days:
            df.loc[df.index[self.holding_days], 'signal'] = -1
        
        # ポジション状態
        position = 0
        for i in range(len(df)):
            if df.iloc[i]['signal'] == 1:
                position = 1
            elif df.iloc[i]['signal'] == -1:
                position = 0
            df.loc[df.index[i], 'position'] = position
        
        return df


class GoldenCrossStrategy(BacktestStrategy):
    """ゴールデンクロス戦略（5MA/25MA）"""
    
    def __init__(self, short_window: int = 5, long_window: int = 25, holding_days: int = 5):
        """
        初期化
        
        Args:
            short_window: 短期移動平均期間
            long_window: 長期移動平均期間
            holding_days: 最低保有日数
        """
        super().__init__(name=f"ゴールデンクロス({short_window}MA/{long_window}MA)")
        self.short_window = short_window
        self.long_window = long_window
        self.holding_days = holding_days
    
    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        シグナル生成：ゴールデンクロスで買い、最低N日保有
        
        Args:
            df: OHLCVデータ
            
        Returns:
            シグナル付きDataFrame
        """
        df = df.copy()
        df['signal'] = 0
        df['position'] = 0
        
        # 移動平均線を計算
        df['ma_short'] = df['close'].rolling(window=self.short_window, min_periods=1).mean()
        df['ma_long'] = df['close'].rolling(window=self.long_window, min_periods=1).mean()
        
        if len(df) < self.long_window + self.holding_days:
            return df
        
        position = 0
        entry_index = None
        
        for i in range(self.long_window, len(df)):
            # ゴールデンクロス検出
            if (position == 0 and 
                df.iloc[i-1]['ma_short'] <= df.iloc[i-1]['ma_long'] and
                df.iloc[i]['ma_short'] > df.iloc[i]['ma_long']):
                # 買いシグナル
                df.loc[df.index[i], 'signal'] = 1
                position = 1
                entry_index = i
            
            # デッドクロスまたは最低保有日数経過
            elif position == 1:
                days_held = i - entry_index
                
                # デッドクロス検出
                is_dead_cross = (df.iloc[i-1]['ma_short'] >= df.iloc[i-1]['ma_long'] and
                                df.iloc[i]['ma_short'] < df.iloc[i]['ma_long'])
                
                if is_dead_cross and days_held >= self.holding_days:
                    # 売りシグナル
                    df.loc[df.index[i], 'signal'] = -1
                    position = 0
                    entry_index = None
            
            df.loc[df.index[i], 'position'] = position
        
        return df


class ConsecutiveCandlesStrategy(BacktestStrategy):
    """三連続陽線戦略"""
    
    def __init__(self, consecutive_days: int = 3, holding_days: int = 3):
        """
        初期化
        
        Args:
            consecutive_days: 連続陽線の日数
            holding_days: 保有日数
        """
        super().__init__(name=f"{consecutive_days}連続陽線({holding_days}日保有)")
        self.consecutive_days = consecutive_days
        self.holding_days = holding_days
    
    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        シグナル生成：N連続陽線で買い、M日保有
        
        Args:
            df: OHLCVデータ
            
        Returns:
            シグナル付きDataFrame
        """
        df = df.copy()
        df['signal'] = 0
        df['position'] = 0
        
        # 陽線判定
        df['is_green'] = (df['close'] > df['open']).astype(int)
        
        if len(df) < self.consecutive_days + self.holding_days:
            return df
        
        position = 0
        entry_index = None
        
        for i in range(self.consecutive_days, len(df)):
            if position == 0:
                # N連続陽線チェック
                recent_greens = df.iloc[i-self.consecutive_days:i]['is_green'].sum()
                
                if recent_greens == self.consecutive_days:
                    # 買いシグナル
                    df.loc[df.index[i], 'signal'] = 1
                    position = 1
                    entry_index = i
            
            elif position == 1:
                # 保有日数経過で売り
                days_held = i - entry_index
                
                if days_held >= self.holding_days:
                    df.loc[df.index[i], 'signal'] = -1
                    position = 0
                    entry_index = None
            
            df.loc[df.index[i], 'position'] = position
        
        return df

