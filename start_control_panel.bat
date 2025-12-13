@echo off
REM JPX400スクリーニング コントロールパネル起動スクリプト

REM プロジェクトルートに移動
cd /d "%~dp0"

REM venv環境をアクティベート
call venv\Scripts\activate.bat

REM コントロールパネルを起動
python run_control_panel.py

REM エラーが発生した場合は一時停止
if %ERRORLEVEL% NEQ 0 (
    echo.
    echo エラーが発生しました: %ERRORLEVEL%
    pause
)

