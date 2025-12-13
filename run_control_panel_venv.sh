#!/bin/bash
# venv環境でコントロールパネルを起動するシェルスクリプト（Git Bash用）

cd "$(dirname "$0")"

# venv環境をアクティベート
source venv/Scripts/activate

# コントロールパネルを起動
python run_control_panel.py

# 終了時にvenvをデアクティベート
deactivate

