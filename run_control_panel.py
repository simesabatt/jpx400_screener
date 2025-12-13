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

# プロジェクトルートをパスに追加
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_ROOT)

from src.gui import ControlPanel  # type: ignore  # noqa: E402

DB_PATH = os.environ.get("TICK_DB_PATH", "data/tick_data.db")


def main():
    app = ControlPanel(db_path=DB_PATH)
    app.mainloop()


if __name__ == "__main__":
    main()
