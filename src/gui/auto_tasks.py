"""
自動タスク管理モジュール

JPXデータ収集とセンチメントスコア計算の自動実行を管理
"""

from datetime import datetime
from typing import Callable, Optional
import threading


class AutoTaskManager:
    """自動タスクのスケジューリングを管理"""
    
    def __init__(self):
        self.scheduler = None
        self._scheduler_thread = None
        self._running = False
        
    def start(
        self,
        jpx_collect_callback: Optional[Callable] = None,
        sentiment_calc_callback: Optional[Callable] = None,
        sentiment_eval_callback: Optional[Callable] = None,
        financial_metrics_callback: Optional[Callable] = None,
        net_cash_ratio_update_callback: Optional[Callable] = None
    ):
        """自動タスクのスケジューリングを開始"""
        try:
            from apscheduler.schedulers.background import BackgroundScheduler
            from apscheduler.triggers.cron import CronTrigger
            
            if self._running:
                print("[自動実行] 既にスケジューラーが起動しています")
                return
            
            self.scheduler = BackgroundScheduler(timezone='Asia/Tokyo')
            
            # JPXデータ収集: 15:00と20:00に実行
            if jpx_collect_callback:
                self.scheduler.add_job(
                    lambda: jpx_collect_callback("15"),
                    CronTrigger(hour=15, minute=0),
                    id='jpx_collect_15',
                    name='JPXデータ収集(15時)',
                    replace_existing=True
                )
                self.scheduler.add_job(
                    lambda: jpx_collect_callback("20"),
                    CronTrigger(hour=20, minute=0),
                    id='jpx_collect_20',
                    name='JPXデータ収集(20時)',
                    replace_existing=True
                )
                print("[自動実行] JPXデータ収集を15:00と20:00に設定しました")
            
            # 地合いスコア算出: 8:50に実行
            if sentiment_calc_callback:
                self.scheduler.add_job(
                    sentiment_calc_callback,
                    CronTrigger(hour=8, minute=50),
                    id='sentiment_calc',
                    name='地合いスコア算出(8:50)',
                    replace_existing=True
                )
                print("[自動実行] 地合いスコア算出を8:50に設定しました")
            
            # 市場動向記録・評価: 15:40に実行
            if sentiment_eval_callback:
                self.scheduler.add_job(
                    sentiment_eval_callback,
                    CronTrigger(hour=15, minute=40),
                    id='sentiment_eval',
                    name='市場動向記録・評価(15:40)',
                    replace_existing=True
                )
                print("[自動実行] 市場動向記録・評価を15:40に設定しました")
            
            # 財務指標取得: 0:00に実行
            if financial_metrics_callback:
                self.scheduler.add_job(
                    financial_metrics_callback,
                    CronTrigger(hour=0, minute=0),
                    id='financial_metrics_fetch',
                    name='財務指標取得(0:00)',
                    replace_existing=True
                )
                print("[自動実行] 財務指標取得を0:00に設定しました")
            
            # NC比率データ更新: 1:00に実行
            if net_cash_ratio_update_callback:
                self.scheduler.add_job(
                    net_cash_ratio_update_callback,
                    CronTrigger(hour=1, minute=0),
                    id='net_cash_ratio_update',
                    name='NC比率データ更新(1:00)',
                    replace_existing=True
                )
                print("[自動実行] NC比率データ更新を1:00に設定しました")
            
            self.scheduler.start()
            self._running = True
            print("[自動実行] スケジューラーを起動しました")
            
            # 次回実行時刻を表示
            self._print_next_runs()
            
        except ImportError:
            print("[警告] APSchedulerがインストールされていません")
            print("       自動実行を使用するには: pip install apscheduler")
        except Exception as e:
            print(f"[エラー] 自動実行の開始に失敗: {e}")
            import traceback
            traceback.print_exc()
    
    def stop(self):
        """自動タスクのスケジューリングを停止"""
        if self.scheduler and self._running:
            try:
                self.scheduler.shutdown(wait=False)
                self._running = False
                print("[自動実行] スケジューラーを停止しました")
            except Exception as e:
                print(f"[エラー] スケジューラーの停止に失敗: {e}")
    
    def _print_next_runs(self):
        """次回実行時刻を表示"""
        if not self.scheduler:
            return
        
        jobs = self.scheduler.get_jobs()
        if jobs:
            print("\n[自動実行] 次回実行予定:")
            for job in jobs:
                next_run = job.next_run_time
                if next_run:
                    print(f"  - {job.name}: {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
            print()
    
    def is_running(self) -> bool:
        """スケジューラーが動作中かどうか"""
        return self._running
    
    def get_job_status(self) -> dict:
        """ジョブのステータスを取得"""
        if not self.scheduler or not self._running:
            return {}
        
        status = {}
        jobs = self.scheduler.get_jobs()
        for job in jobs:
            status[job.id] = {
                'name': job.name,
                'next_run': job.next_run_time.strftime('%Y-%m-%d %H:%M:%S') if job.next_run_time else None
            }
        return status

