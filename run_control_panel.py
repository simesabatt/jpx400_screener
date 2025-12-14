"""
JPX400スクリーニング コントロールパネル

起動コマンド:

    python run_control_panel.py

機能:
- JPX400銘柄のデータ収集
- JPX400銘柄のスクリーニング
- JPX400銘柄リストの更新

前提:
- 収集データは SQLite: data/tick_data.db に保存

Copyright (c) 2025 JPX400スクリーニングシステム

This software is licensed under the MIT License.
See LICENSE file for details.
"""

import os
import sys
from pathlib import Path

# プロジェクトルートをパスに追加
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_ROOT)

from src.gui import ControlPanel  # type: ignore  # noqa: E402

DB_PATH = os.environ.get("TICK_DB_PATH", "data/tick_data.db")


def main():
    try:
        # データディレクトリを作成（データベースファイルの自動作成を確実にするため）
        # exist_ok=Trueにより、既に存在する場合は何もしない（既存データを保護）
        Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
        
        # ControlPanelの初期化時に、既存のDBファイルがあればそれを使用し、
        # なければ新規作成される（_ensure_database()メソッド内で処理）
        app = ControlPanel(db_path=DB_PATH)
        app.mainloop()
    except Exception as e:
        import traceback
        print("=" * 60)
        print("エラーが発生しました")
        print("=" * 60)
        print(f"エラー内容: {e}")
        print()
        print("詳細:")
        traceback.print_exc()
        print("=" * 60)
        input("Enterキーを押して終了してください...")


if __name__ == "__main__":
    main()
