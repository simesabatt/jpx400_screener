"""
スクリーニング結果バックテストモジュール

JPX400スクリーニング結果に対してバックテストを実行し、
各銘柄のパフォーマンスを評価します。
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from src.utils.console import setup_console_encoding
setup_console_encoding()

import pandas as pd
import numpy as np
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta, date
import logging

from src.data_collector.ohlcv_data_manager import OHLCVDataManager
from src.data_collector.symbol_name_manager import SymbolNameManager
from src.backtest.backtest_strategy import (
    BacktestStrategy,
    HoldNDaysStrategy,
    GoldenCrossStrategy,
    ConsecutiveCandlesStrategy
)

# ロギング設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ScreeningBacktest:
    """スクリーニング結果のバックテストクラス"""
    
    def __init__(self, db_path: str = "data/tick_data.db"):
        """
        初期化
        
        Args:
            db_path: データベースパス
        """
        self.db_path = db_path
        self.ohlcv_manager = OHLCVDataManager(db_path)
        self.symbol_name_manager = SymbolNameManager(db_path)
        
        # 利用可能な戦略
        self.strategies = {
            'hold_5days': HoldNDaysStrategy(holding_days=5),
            'hold_10days': HoldNDaysStrategy(holding_days=10),
            'hold_20days': HoldNDaysStrategy(holding_days=20),
            'golden_cross': GoldenCrossStrategy(short_window=5, long_window=25, holding_days=5),
            'consecutive_candles': ConsecutiveCandlesStrategy(consecutive_days=3, holding_days=3)
        }
    
    def run_batch_backtest(
        self,
        symbols: List[str],
        strategy_name: str = 'hold_5days',
        period_days: int = 90,
        commission_rate: float = 0.0,
        slippage_rate: float = 0.001,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> pd.DataFrame:
        """
        複数銘柄の一括バックテスト
        
        Args:
            symbols: 銘柄コードリスト
            strategy_name: 戦略名（'hold_5days', 'hold_10days', 'hold_20days', 
                          'trend_following', 'momentum'）
            period_days: バックテスト期間（日数）※start_dateが指定されていない場合のみ有効
            commission_rate: 手数料率（片道）
            slippage_rate: スリッページ率
            start_date: 開始日（指定された場合はperiod_daysより優先）
            end_date: 終了日（指定されない場合は最新日）
            
        Returns:
            バックテスト結果のDataFrame
        """
        if strategy_name not in self.strategies:
            raise ValueError(f"Unknown strategy: {strategy_name}. Available: {list(self.strategies.keys())}")
        
        strategy = self.strategies[strategy_name]
        
        logger.info(f"バックテスト開始: {len(symbols)}銘柄, 戦略={strategy.name}")
        
        results = []
        total = len(symbols)
        
        for idx, symbol in enumerate(symbols, 1):
            try:
                logger.info(f"処理中 [{idx}/{total}]: {symbol}")
                
                result = self._backtest_single(
                    symbol=symbol,
                    strategy=strategy,
                    period_days=period_days,
                    commission_rate=commission_rate,
                    slippage_rate=slippage_rate,
                    start_date=start_date,
                    end_date=end_date
                )
                
                if result is not None:
                    results.append(result)
                    
            except Exception as e:
                logger.warning(f"銘柄 {symbol} のバックテスト失敗: {e}")
                continue
        
        if not results:
            logger.warning("バックテスト結果が0件です")
            return pd.DataFrame()
        
        df_results = pd.DataFrame(results)
        
        # ソート（リターン降順）
        df_results = df_results.sort_values('total_return', ascending=False).reset_index(drop=True)
        
        logger.info(f"バックテスト完了: {len(df_results)}銘柄")
        
        return df_results
    
    def _backtest_single(
        self,
        symbol: str,
        strategy: BacktestStrategy,
        period_days: int,
        commission_rate: float,
        slippage_rate: float,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> Optional[Dict]:
        """
        単一銘柄のバックテスト
        
        Args:
            symbol: 銘柄コード
            strategy: バックテスト戦略
            period_days: バックテスト期間（日数）
            commission_rate: 手数料率
            slippage_rate: スリッページ率
            start_date: 開始日
            end_date: 終了日
            
        Returns:
            バックテスト結果の辞書（失敗時はNone）
        """
        # OHLCVデータ取得
        if start_date and end_date:
            # 日付範囲を指定
            start_datetime = datetime.combine(start_date, datetime.min.time())
            end_datetime = datetime.combine(end_date, datetime.max.time())
            df = self.ohlcv_manager.get_ohlcv_data(
                symbol=symbol,
                timeframe="1d",
                start_datetime=start_datetime,
                end_datetime=end_datetime,
                source="yahoo"
            )
        else:
            # 過去N日分を取得
            end_datetime = datetime.now()
            start_datetime = end_datetime - timedelta(days=period_days)
            df = self.ohlcv_manager.get_ohlcv_data(
                symbol=symbol,
                timeframe="1d",
                start_datetime=start_datetime,
                end_datetime=end_datetime,
                source="yahoo"
            )
        
        if df is None or len(df) < 2:
            logger.debug(f"銘柄 {symbol}: データ不足")
            return None
        
        # 銘柄名取得
        symbol_name = self.symbol_name_manager.get_symbol_name(symbol)
        if symbol_name is None:
            symbol_name = symbol
        
        # 戦略実行
        df_signals = strategy.generate_signals(df)
        df_returns = strategy.calculate_returns(
            df_signals,
            commission_rate=commission_rate,
            slippage_rate=slippage_rate
        )
        
        # パフォーマンス指標計算
        metrics = strategy.calculate_metrics(df_returns)
        
        # バイアンドホールドのリターン（比較用）
        buy_hold_return = (df_returns.iloc[-1]['close'] - df_returns.iloc[0]['open']) / df_returns.iloc[0]['open']
        
        # エントリー価格とエグジット価格
        entry_price = None
        exit_price = None
        entry_date = None
        exit_date = None
        
        # 最初のエントリーを探す
        entry_signals = df_returns[df_returns['signal'] == 1]
        if len(entry_signals) > 0:
            entry_idx = df_returns.index.get_loc(entry_signals.index[0])
            if entry_idx + 1 < len(df_returns):
                entry_price = df_returns.iloc[entry_idx + 1]['open']
                entry_date = df_returns.index[entry_idx + 1]
            else:
                entry_price = df_returns.iloc[entry_idx]['close']
                entry_date = df_returns.index[entry_idx]
        
        # 最初のエグジットを探す
        exit_signals = df_returns[df_returns['signal'] == -1]
        if len(exit_signals) > 0:
            exit_idx = df_returns.index.get_loc(exit_signals.index[0])
            if exit_idx + 1 < len(df_returns):
                exit_price = df_returns.iloc[exit_idx + 1]['open']
                exit_date = df_returns.index[exit_idx + 1]
            else:
                exit_price = df_returns.iloc[exit_idx]['close']
                exit_date = df_returns.index[exit_idx]
        
        result = {
            'symbol': symbol,
            'name': symbol_name,
            'strategy': strategy.name,
            'start_date': df_returns.index[0].strftime('%Y-%m-%d'),
            'end_date': df_returns.index[-1].strftime('%Y-%m-%d'),
            'days': len(df_returns),
            'entry_price': entry_price,
            'exit_price': exit_price,
            'entry_date': entry_date.strftime('%Y-%m-%d') if entry_date else None,
            'exit_date': exit_date.strftime('%Y-%m-%d') if exit_date else None,
            'total_return': metrics.get('total_return', 0.0),
            'buy_hold_return': buy_hold_return,
            'num_trades': metrics.get('num_trades', 0),
            'win_rate': metrics.get('win_rate', 0.0),
            'avg_win': metrics.get('avg_win', 0.0),
            'avg_loss': metrics.get('avg_loss', 0.0),
            'max_drawdown': metrics.get('max_drawdown', 0.0),
            'sharpe_ratio': metrics.get('sharpe_ratio', 0.0),
            'final_equity': metrics.get('final_equity', 1.0)
        }
        
        return result
    
    def run_strategy_comparison(
        self,
        symbols: List[str],
        period_days: int = 90,
        commission_rate: float = 0.0,
        slippage_rate: float = 0.001
    ) -> Dict[str, pd.DataFrame]:
        """
        複数戦略の比較バックテスト
        
        Args:
            symbols: 銘柄コードリスト
            period_days: バックテスト期間（日数）
            commission_rate: 手数料率
            slippage_rate: スリッページ率
            
        Returns:
            戦略名をキーとするDataFrameの辞書
        """
        results = {}
        
        for strategy_name in self.strategies.keys():
            logger.info(f"戦略比較: {strategy_name}")
            
            df_result = self.run_batch_backtest(
                symbols=symbols,
                strategy_name=strategy_name,
                period_days=period_days,
                commission_rate=commission_rate,
                slippage_rate=slippage_rate
            )
            
            results[strategy_name] = df_result
        
        return results
    
    def get_strategy_summary(self, results: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        戦略比較サマリーを作成
        
        Args:
            results: run_strategy_comparisonの結果
            
        Returns:
            戦略ごとの統計サマリー
        """
        summary = []
        
        for strategy_name, df in results.items():
            if len(df) == 0:
                continue
            
            summary.append({
                'strategy': strategy_name,
                'num_symbols': len(df),
                'avg_return': df['total_return'].mean(),
                'median_return': df['total_return'].median(),
                'std_return': df['total_return'].std(),
                'min_return': df['total_return'].min(),
                'max_return': df['total_return'].max(),
                'positive_count': (df['total_return'] > 0).sum(),
                'positive_rate': (df['total_return'] > 0).mean(),
                'avg_sharpe': df['sharpe_ratio'].mean(),
                'avg_max_drawdown': df['max_drawdown'].mean()
            })
        
        return pd.DataFrame(summary)
    
    def export_detailed_results(
        self,
        df_results: pd.DataFrame,
        output_path: str = "backtest_results.csv"
    ):
        """
        詳細結果をCSVエクスポート
        
        Args:
            df_results: バックテスト結果
            output_path: 出力パス
        """
        df_results.to_csv(output_path, index=False, encoding='utf-8-sig')
        logger.info(f"結果をエクスポートしました: {output_path}")
    
    def print_summary(self, df_results: pd.DataFrame):
        """
        結果サマリーを表示
        
        Args:
            df_results: バックテスト結果
        """
        if len(df_results) == 0:
            print("結果がありません")
            return
        
        print("="*80)
        print("バックテスト結果サマリー")
        print("="*80)
        print(f"銘柄数: {len(df_results)}")
        print(f"戦略: {df_results.iloc[0]['strategy']}")
        print(f"期間: {df_results.iloc[0]['start_date']} ～ {df_results.iloc[0]['end_date']}")
        print()
        
        print("【リターン統計】")
        print(f"  平均リターン: {df_results['total_return'].mean()*100:.2f}%")
        print(f"  中央値: {df_results['total_return'].median()*100:.2f}%")
        print(f"  標準偏差: {df_results['total_return'].std()*100:.2f}%")
        print(f"  最小: {df_results['total_return'].min()*100:.2f}%")
        print(f"  最大: {df_results['total_return'].max()*100:.2f}%")
        print()
        
        print("【勝率】")
        positive_count = (df_results['total_return'] > 0).sum()
        positive_rate = positive_count / len(df_results) * 100
        print(f"  プラスリターン: {positive_count}/{len(df_results)} ({positive_rate:.1f}%)")
        print()
        
        print("【トップ10銘柄】")
        top10 = df_results.head(10)[['symbol', 'name', 'total_return', 'entry_price', 'exit_price', 'num_trades']]
        for idx, row in top10.iterrows():
            print(f"  {idx+1}. {row['symbol']} ({row['name']}): "
                  f"{row['total_return']*100:+.2f}% "
                  f"(¥{row['entry_price']:.0f} → ¥{row['exit_price']:.0f}, "
                  f"{row['num_trades']}取引)")
        print()
        
        print("【ワースト10銘柄】")
        worst10 = df_results.tail(10)[['symbol', 'name', 'total_return', 'entry_price', 'exit_price', 'num_trades']]
        for idx, row in worst10.iterrows():
            print(f"  {len(df_results)-idx}. {row['symbol']} ({row['name']}): "
                  f"{row['total_return']*100:+.2f}% "
                  f"(¥{row['entry_price']:.0f} → ¥{row['exit_price']:.0f}, "
                  f"{row['num_trades']}取引)")
        print("="*80)

