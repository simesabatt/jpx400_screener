"""
戦略比較バックテスト

実データで以下の3つの戦略を比較：
1. 現在の戦略（制限なし、1ティック）
2. 100万円ストップ戦略
3. ハイブリッド戦略（100万円まで1ティック、以降3ティック）
"""
# 文字化け対策（プロジェクト共通モジュール使用）
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from src.utils.console import setup_console_encoding
setup_console_encoding()

import pandas as pd
import numpy as np
from datetime import datetime

class StrategyComparison:
    """戦略比較バックテストクラス"""
    
    def __init__(self, position_size=100):
        self.position_size = position_size
        self.daily_trade_limit = 1_000_000  # 手数料無料枠
        self.commission_rate = 0.00099
        self.commission_fixed_100_200 = 2200  # 100-200万円の固定手数料
        
    def calculate_commission(self, cumulative_value):
        """
        累計約定代金から手数料を計算
        
        Args:
            cumulative_value: その日の累計約定代金
        
        Returns:
            手数料
        """
        if cumulative_value <= self.daily_trade_limit:
            return 0
        elif cumulative_value <= 2_000_000:
            return self.commission_fixed_100_200
        elif cumulative_value <= 3_000_000:
            return 3300
        else:
            # 100万円ごとに1,100円追加
            over = (cumulative_value - 3_000_000) // 1_000_000
            return 3300 + (over * 1100)
    
    def strategy_unlimited(self, tick_data):
        """
        戦略1: 制限なし（現在の戦略）
        1ティックで常に取引
        """
        print("\n" + "=" * 70)
        print("【戦略1】制限なし・1ティック戦略")
        print("=" * 70)
        
        trades = []
        position = 0
        entry_price = 0
        target_price = 0
        tick_size = 0.1
        
        # 日ごとの累計約定代金
        daily_cumulative = {}
        
        for i in range(len(tick_data)):
            current_price = tick_data['price'].iloc[i]
            timestamp = tick_data['timestamp'].iloc[i]
            date_key = timestamp.strftime('%Y-%m-%d')
            
            if date_key not in daily_cumulative:
                daily_cumulative[date_key] = 0
            
            prev_price = tick_data['price'].iloc[i-1] if i > 0 else current_price
            
            # エントリーロジック
            if position == 0 and current_price < prev_price:
                position = 1
                entry_price = current_price
                target_price = entry_price + tick_size
                trade_value = current_price * self.position_size
                daily_cumulative[date_key] += trade_value
                
                trades.append({
                    'timestamp': timestamp,
                    'action': 'BUY',
                    'price': entry_price,
                    'cumulative': daily_cumulative[date_key]
                })
            
            # 買いポジション利確
            elif position == 1 and current_price >= target_price:
                exit_price = current_price
                trade_value = exit_price * self.position_size
                daily_cumulative[date_key] += trade_value
                commission = self.calculate_commission(daily_cumulative[date_key])
                prev_commission = self.calculate_commission(daily_cumulative[date_key] - trade_value)
                trade_commission = commission - prev_commission
                
                profit = (exit_price - entry_price) * self.position_size - trade_commission
                
                trades.append({
                    'timestamp': timestamp,
                    'action': 'SELL',
                    'price': exit_price,
                    'profit': profit,
                    'commission': trade_commission,
                    'cumulative': daily_cumulative[date_key]
                })
                
                # 信用売りへ
                position = -1
                entry_price = exit_price
                target_price = entry_price - tick_size
                trade_value = entry_price * self.position_size
                daily_cumulative[date_key] += trade_value
                
                trades.append({
                    'timestamp': timestamp,
                    'action': 'SHORT',
                    'price': entry_price,
                    'cumulative': daily_cumulative[date_key]
                })
            
            # 売りポジション利確
            elif position == -1 and current_price <= target_price:
                exit_price = current_price
                trade_value = exit_price * self.position_size
                daily_cumulative[date_key] += trade_value
                commission = self.calculate_commission(daily_cumulative[date_key])
                prev_commission = self.calculate_commission(daily_cumulative[date_key] - trade_value)
                trade_commission = commission - prev_commission
                
                profit = (entry_price - exit_price) * self.position_size - trade_commission
                
                trades.append({
                    'timestamp': timestamp,
                    'action': 'COVER',
                    'price': exit_price,
                    'profit': profit,
                    'commission': trade_commission,
                    'cumulative': daily_cumulative[date_key]
                })
                
                # 買いへ
                position = 1
                entry_price = exit_price
                target_price = entry_price + tick_size
                trade_value = entry_price * self.position_size
                daily_cumulative[date_key] += trade_value
                
                trades.append({
                    'timestamp': timestamp,
                    'action': 'BUY',
                    'price': entry_price,
                    'cumulative': daily_cumulative[date_key]
                })
        
        return self.analyze_results(trades, "制限なし")
    
    def strategy_stop_at_1m(self, tick_data):
        """
        戦略2: 100万円で停止
        累計約定代金が100万円に達したら取引停止
        """
        print("\n" + "=" * 70)
        print("【戦略2】100万円ストップ戦略")
        print("=" * 70)
        
        trades = []
        position = 0
        entry_price = 0
        target_price = 0
        tick_size = 0.1
        
        daily_cumulative = {}
        daily_stopped = {}
        
        for i in range(len(tick_data)):
            current_price = tick_data['price'].iloc[i]
            timestamp = tick_data['timestamp'].iloc[i]
            date_key = timestamp.strftime('%Y-%m-%d')
            
            if date_key not in daily_cumulative:
                daily_cumulative[date_key] = 0
                daily_stopped[date_key] = False
            
            # 100万円到達でその日は停止
            if daily_stopped[date_key]:
                continue
            
            prev_price = tick_data['price'].iloc[i-1] if i > 0 else current_price
            
            # エントリーロジック
            if position == 0 and current_price < prev_price:
                trade_value = current_price * self.position_size
                
                # 100万円を超えないかチェック
                if daily_cumulative[date_key] + trade_value > self.daily_trade_limit:
                    daily_stopped[date_key] = True
                    continue
                
                position = 1
                entry_price = current_price
                target_price = entry_price + tick_size
                daily_cumulative[date_key] += trade_value
                
                trades.append({
                    'timestamp': timestamp,
                    'action': 'BUY',
                    'price': entry_price,
                    'cumulative': daily_cumulative[date_key]
                })
            
            # 買いポジション利確
            elif position == 1 and current_price >= target_price:
                exit_price = current_price
                trade_value = exit_price * self.position_size
                
                if daily_cumulative[date_key] + trade_value > self.daily_trade_limit:
                    daily_stopped[date_key] = True
                    position = 0  # ポジション解消
                    continue
                
                daily_cumulative[date_key] += trade_value
                profit = (exit_price - entry_price) * self.position_size  # 手数料0円
                
                trades.append({
                    'timestamp': timestamp,
                    'action': 'SELL',
                    'price': exit_price,
                    'profit': profit,
                    'commission': 0,
                    'cumulative': daily_cumulative[date_key]
                })
                
                # 信用売りへ
                trade_value = exit_price * self.position_size
                if daily_cumulative[date_key] + trade_value > self.daily_trade_limit:
                    daily_stopped[date_key] = True
                    position = 0
                    continue
                
                position = -1
                entry_price = exit_price
                target_price = entry_price - tick_size
                daily_cumulative[date_key] += trade_value
                
                trades.append({
                    'timestamp': timestamp,
                    'action': 'SHORT',
                    'price': entry_price,
                    'cumulative': daily_cumulative[date_key]
                })
            
            # 売りポジション利確
            elif position == -1 and current_price <= target_price:
                exit_price = current_price
                trade_value = exit_price * self.position_size
                
                if daily_cumulative[date_key] + trade_value > self.daily_trade_limit:
                    daily_stopped[date_key] = True
                    position = 0
                    continue
                
                daily_cumulative[date_key] += trade_value
                profit = (entry_price - exit_price) * self.position_size  # 手数料0円
                
                trades.append({
                    'timestamp': timestamp,
                    'action': 'COVER',
                    'price': exit_price,
                    'profit': profit,
                    'commission': 0,
                    'cumulative': daily_cumulative[date_key]
                })
                
                # 買いへ
                trade_value = exit_price * self.position_size
                if daily_cumulative[date_key] + trade_value > self.daily_trade_limit:
                    daily_stopped[date_key] = True
                    position = 0
                    continue
                
                position = 1
                entry_price = exit_price
                target_price = entry_price + tick_size
                daily_cumulative[date_key] += trade_value
                
                trades.append({
                    'timestamp': timestamp,
                    'action': 'BUY',
                    'price': entry_price,
                    'cumulative': daily_cumulative[date_key]
                })
        
        return self.analyze_results(trades, "100万円ストップ")
    
    def strategy_hybrid(self, tick_data):
        """
        戦略3: ハイブリッド戦略
        100万円まで: 1ティック
        100万円超: 3ティック
        """
        print("\n" + "=" * 70)
        print("【戦略3】ハイブリッド戦略（100万円まで1ティック、以降3ティック）")
        print("=" * 70)
        
        trades = []
        position = 0
        entry_price = 0
        target_price = 0
        
        daily_cumulative = {}
        
        for i in range(len(tick_data)):
            current_price = tick_data['price'].iloc[i]
            timestamp = tick_data['timestamp'].iloc[i]
            date_key = timestamp.strftime('%Y-%m-%d')
            
            if date_key not in daily_cumulative:
                daily_cumulative[date_key] = 0
            
            # 累計に応じてティックサイズを決定
            if daily_cumulative[date_key] < self.daily_trade_limit:
                tick_size = 0.1  # 1ティック
            else:
                tick_size = 0.3  # 3ティック
            
            prev_price = tick_data['price'].iloc[i-1] if i > 0 else current_price
            
            # エントリーロジック
            if position == 0 and current_price < prev_price:
                position = 1
                entry_price = current_price
                target_price = entry_price + tick_size
                trade_value = current_price * self.position_size
                daily_cumulative[date_key] += trade_value
                
                trades.append({
                    'timestamp': timestamp,
                    'action': 'BUY',
                    'price': entry_price,
                    'tick_size': tick_size,
                    'cumulative': daily_cumulative[date_key]
                })
            
            # 買いポジション利確
            elif position == 1 and current_price >= target_price:
                exit_price = current_price
                trade_value = exit_price * self.position_size
                daily_cumulative[date_key] += trade_value
                commission = self.calculate_commission(daily_cumulative[date_key])
                prev_commission = self.calculate_commission(daily_cumulative[date_key] - trade_value)
                trade_commission = commission - prev_commission
                
                profit = (exit_price - entry_price) * self.position_size - trade_commission
                
                trades.append({
                    'timestamp': timestamp,
                    'action': 'SELL',
                    'price': exit_price,
                    'profit': profit,
                    'commission': trade_commission,
                    'tick_size': tick_size,
                    'cumulative': daily_cumulative[date_key]
                })
                
                # 次のティックサイズを決定
                if daily_cumulative[date_key] < self.daily_trade_limit:
                    next_tick_size = 0.1
                else:
                    next_tick_size = 0.3
                
                # 信用売りへ
                position = -1
                entry_price = exit_price
                target_price = entry_price - next_tick_size
                trade_value = entry_price * self.position_size
                daily_cumulative[date_key] += trade_value
                
                trades.append({
                    'timestamp': timestamp,
                    'action': 'SHORT',
                    'price': entry_price,
                    'tick_size': next_tick_size,
                    'cumulative': daily_cumulative[date_key]
                })
            
            # 売りポジション利確
            elif position == -1 and current_price <= target_price:
                exit_price = current_price
                trade_value = exit_price * self.position_size
                daily_cumulative[date_key] += trade_value
                commission = self.calculate_commission(daily_cumulative[date_key])
                prev_commission = self.calculate_commission(daily_cumulative[date_key] - trade_value)
                trade_commission = commission - prev_commission
                
                profit = (entry_price - exit_price) * self.position_size - trade_commission
                
                trades.append({
                    'timestamp': timestamp,
                    'action': 'COVER',
                    'price': exit_price,
                    'profit': profit,
                    'commission': trade_commission,
                    'tick_size': tick_size,
                    'cumulative': daily_cumulative[date_key]
                })
                
                # 次のティックサイズを決定
                if daily_cumulative[date_key] < self.daily_trade_limit:
                    next_tick_size = 0.1
                else:
                    next_tick_size = 0.3
                
                # 買いへ
                position = 1
                entry_price = exit_price
                target_price = entry_price + next_tick_size
                trade_value = entry_price * self.position_size
                daily_cumulative[date_key] += trade_value
                
                trades.append({
                    'timestamp': timestamp,
                    'action': 'BUY',
                    'price': entry_price,
                    'tick_size': next_tick_size,
                    'cumulative': daily_cumulative[date_key]
                })
        
        return self.analyze_results(trades, "ハイブリッド")
    
    def analyze_results(self, trades, strategy_name):
        """結果を分析"""
        df = pd.DataFrame(trades)
        
        if len(df) == 0:
            return {
                'strategy': strategy_name,
                'total_trades': 0,
                'total_profit': 0,
                'total_commission': 0,
                'win_rate': 0,
                'trades_df': df
            }
        
        profit_trades = df[df['profit'].notna()]
        
        if len(profit_trades) == 0:
            total_profit = 0
            win_count = 0
            loss_count = 0
            win_rate = 0
        else:
            total_profit = profit_trades['profit'].sum()
            win_count = len(profit_trades[profit_trades['profit'] > 0])
            loss_count = len(profit_trades[profit_trades['profit'] <= 0])
            win_rate = (win_count / len(profit_trades) * 100) if len(profit_trades) > 0 else 0
        
        total_commission = df['commission'].sum() if 'commission' in df.columns else 0
        
        print(f"\n総取引回数: {len(df)}回")
        print(f"往復取引数: {len(profit_trades)}回")
        print(f"勝率: {win_rate:.1f}%")
        print(f"総利益: {total_profit:,.0f}円")
        print(f"総手数料: {total_commission:,.0f}円")
        
        if len(df) > 0:
            print(f"取引期間: {df['timestamp'].min()} 〜 {df['timestamp'].max()}")
            if 'cumulative' in df.columns:
                max_cumulative = df['cumulative'].max()
                print(f"最大累計約定代金: {max_cumulative:,.0f}円")
        
        return {
            'strategy': strategy_name,
            'total_trades': len(df),
            'round_trips': len(profit_trades),
            'total_profit': total_profit,
            'total_commission': total_commission,
            'win_rate': win_rate,
            'win_count': win_count,
            'loss_count': loss_count,
            'trades_df': df
        }

def main():
    """メイン実行"""
    print("=" * 70)
    print("戦略比較バックテスト")
    print("=" * 70)
    print("実データで3つの戦略を比較検証します\n")
    
    # データ読み込み
    print("データ読み込み中...")
    tick_data = pd.read_csv('data/無題のスプレッドシート - シート1.csv',
                            header=None, names=['timestamp', 'price', 'volume'])
    tick_data['timestamp'] = pd.to_datetime(tick_data['timestamp'])
    tick_data = tick_data.sort_values('timestamp').reset_index(drop=True)
    
    print(f"データ件数: {len(tick_data):,}件")
    print(f"期間: {tick_data['timestamp'].min()} 〜 {tick_data['timestamp'].max()}")
    
    # 戦略比較
    comparison = StrategyComparison()
    
    # 戦略1: 制限なし
    result1 = comparison.strategy_unlimited(tick_data)
    
    # 戦略2: 100万円ストップ
    result2 = comparison.strategy_stop_at_1m(tick_data)
    
    # 戦略3: ハイブリッド
    result3 = comparison.strategy_hybrid(tick_data)
    
    # 比較表示
    print("\n" + "=" * 70)
    print("【総合比較】")
    print("=" * 70)
    
    results = [result1, result2, result3]
    
    print(f"\n{'戦略名':<20} {'取引回数':<10} {'往復':<8} {'勝率':<10} {'総利益':<12} {'手数料':<12}")
    print("-" * 80)
    for r in results:
        print(f"{r['strategy']:<18} "
              f"{r['total_trades']:>8}回 "
              f"{r['round_trips']:>6}回 "
              f"{r['win_rate']:>7.1f}% "
              f"{r['total_profit']:>10,.0f}円 "
              f"{r['total_commission']:>10,.0f}円")
    
    # 推奨戦略
    print("\n" + "=" * 70)
    print("【推奨戦略】")
    print("=" * 70)
    
    best = max(results, key=lambda x: x['total_profit'])
    print(f"\n最も利益が大きい戦略: {best['strategy']}")
    print(f"総利益: {best['total_profit']:,.0f}円")
    print(f"手数料: {best['total_commission']:,.0f}円")
    
    # 月間換算
    if best['total_profit'] > 0:
        # 実データは約2日分なので、20営業日に換算
        monthly_profit = best['total_profit'] * 10  # 2日 → 20日
        print(f"\n月間換算（20営業日）: {monthly_profit:,.0f}円")
    
    print("\n" + "=" * 70)

if __name__ == '__main__':
    main()

