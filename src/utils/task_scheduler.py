"""
Windowsタスクスケジューラの操作ユーティリティ
"""
import subprocess
import json
from typing import Optional, Dict, List
from datetime import datetime


class TaskScheduler:
    """Windowsタスクスケジューラの操作クラス"""
    
    TASK_NAME = "JPX400スクリーニング 日次データインポート"
    
    @staticmethod
    def get_task_info() -> Optional[Dict]:
        """
        タスクの情報を取得
        
        Returns:
            タスク情報の辞書、またはNone（タスクが存在しない場合）
        """
        try:
            # PowerShellコマンドでタスク情報を取得
            ps_command = f'''
            $task = Get-ScheduledTask -TaskName "{TaskScheduler.TASK_NAME}" -ErrorAction SilentlyContinue
            if ($task) {{
                $info = Get-ScheduledTaskInfo -TaskName "{TaskScheduler.TASK_NAME}"
                $trigger = (Get-ScheduledTask -TaskName "{TaskScheduler.TASK_NAME}").Triggers[0]
                
                $result = @{{
                    "exists" = $true
                    "state" = $task.State.ToString()
                    "last_run_time" = if ($info.LastRunTime) {{ $info.LastRunTime.ToString("yyyy-MM-dd HH:mm:ss") }} else {{ $null }}
                    "next_run_time" = if ($info.NextRunTime) {{ $info.NextRunTime.ToString("yyyy-MM-dd HH:mm:ss") }} else {{ $null }}
                    "last_task_result" = $info.LastTaskResult
                    "number_of_missed_runs" = $info.NumberOfMissedRuns
                    "trigger_time" = if ($trigger.StartBoundary) {{ $trigger.StartBoundary.ToString("HH:mm") }} else {{ $null }}
                }}
                $result | ConvertTo-Json
            }} else {{
                @{{"exists" = $false}} | ConvertTo-Json
            }}
            '''
            
            result = subprocess.run(
                ["powershell", "-Command", ps_command],
                capture_output=True,
                text=True,
                encoding='utf-8'
            )
            
            if result.returncode == 0 and result.stdout:
                import json
                return json.loads(result.stdout)
            else:
                return {"exists": False, "error": result.stderr}
                
        except Exception as e:
            return {"exists": False, "error": str(e)}
    
    @staticmethod
    def run_task() -> Dict:
        """
        タスクを手動実行
        
        Returns:
            実行結果の辞書
        """
        try:
            ps_command = f'Start-ScheduledTask -TaskName "{TaskScheduler.TASK_NAME}"'
            
            result = subprocess.run(
                ["powershell", "-Command", ps_command],
                capture_output=True,
                text=True,
                encoding='utf-8'
            )
            
            if result.returncode == 0:
                return {"success": True, "message": "タスクの実行を開始しました"}
            else:
                return {"success": False, "error": result.stderr}
                
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    @staticmethod
    def enable_task() -> Dict:
        """
        タスクを有効化
        
        Returns:
            実行結果の辞書
        """
        try:
            ps_command = f'Enable-ScheduledTask -TaskName "{TaskScheduler.TASK_NAME}"'
            
            result = subprocess.run(
                ["powershell", "-Command", ps_command],
                capture_output=True,
                text=True,
                encoding='utf-8'
            )
            
            if result.returncode == 0:
                return {"success": True, "message": "タスクを有効化しました"}
            else:
                return {"success": False, "error": result.stderr}
                
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    @staticmethod
    def disable_task() -> Dict:
        """
        タスクを無効化
        
        Returns:
            実行結果の辞書
        """
        try:
            ps_command = f'Disable-ScheduledTask -TaskName "{TaskScheduler.TASK_NAME}"'
            
            result = subprocess.run(
                ["powershell", "-Command", ps_command],
                capture_output=True,
                text=True,
                encoding='utf-8'
            )
            
            if result.returncode == 0:
                return {"success": True, "message": "タスクを無効化しました"}
            else:
                return {"success": False, "error": result.stderr}
                
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    @staticmethod
    def get_task_history(limit: int = 10) -> List[Dict]:
        """
        タスクの実行履歴を取得
        
        Args:
            limit: 取得する履歴の件数
            
        Returns:
            実行履歴のリスト
        """
        try:
            ps_command = f'''
            $events = Get-WinEvent -FilterHashtable @{{
                LogName = "Microsoft-Windows-TaskScheduler/Operational"
                ID = 200, 201, 202, 203
            }} -MaxEvents {limit} -ErrorAction SilentlyContinue | Where-Object {{
                $_.Message -like "*{TaskScheduler.TASK_NAME}*"
            }} | Select-Object -First {limit}
            
            $history = @()
            foreach ($event in $events) {{
                $history += @{{
                    "time" = $event.TimeCreated.ToString("yyyy-MM-dd HH:mm:ss")
                    "id" = $event.Id
                    "level" = $event.LevelDisplayName
                    "message" = $event.Message
                }}
            }}
            $history | ConvertTo-Json
            '''
            
            result = subprocess.run(
                ["powershell", "-Command", ps_command],
                capture_output=True,
                text=True,
                encoding='utf-8'
            )
            
            if result.returncode == 0 and result.stdout:
                import json
                return json.loads(result.stdout)
            else:
                return []
                
        except Exception as e:
            return []

