@echo off
REM venv環境でコントロールパネルを起動するバッチファイル

cd /d %~dp0

REM venv環境をアクティベート
call venv\Scripts\activate.bat

REM コントロールパネルを起動
python run_control_panel.py

REM 終了時にvenvをデアクティベート
deactivate

