# venv環境での実行方法

このプロジェクトを **conda環境ではなく、venv仮想環境で実行** する方法です。

## 環境情報

- **Python**: 3.13.1（最新安定版）
- **仮想環境**: `venv/` ディレクトリ
- **Anacondaの問題を回避**: pyodbcのバージョン情報不正などの問題が発生しません

## セットアップ（初回のみ）

既に完了しています。以下のコマンドで確認できます：

```bash
venv\Scripts\python.exe --version
# Python 3.13.1 と表示されればOK
```

## 実行方法

### 方法1: 直接実行（最も確実・推奨）

**Git Bash / PowerShell / コマンドプロンプト共通:**

```bash
venv\Scripts\python.exe run_control_panel.py
```

または（Windowsの場合）:

```bash
./venv/Scripts/python.exe run_control_panel.py
```

### 方法2: シェルスクリプトで起動（Git Bash用）

```bash
./run_control_panel_venv.sh
```

### 方法3: バッチファイルで起動（コマンドプロンプト用）

```cmd
run_control_panel_venv.bat
```

### 方法4: 手動でvenvをアクティベート

**Git Bashの場合:**
```bash
source venv/Scripts/activate
python run_control_panel.py
deactivate
```

**PowerShellの場合:**
```powershell
venv\Scripts\Activate.ps1
python run_control_panel.py
deactivate
```

**コマンドプロンプトの場合:**
```cmd
venv\Scripts\activate.bat
python run_control_panel.py
deactivate
```

## インストール済みパッケージ

- python-dotenv
- requests
- websocket-client
- pandas
- numpy
- matplotlib
- flask

## トラブルシューティング

### venv環境が壊れた場合

```bash
# venvを削除
rmdir /s venv  # Windows
rm -rf venv    # Git Bash

# 再作成
py -3.13 -m venv venv

# パッケージを再インストール
venv\Scripts\pip.exe install python-dotenv requests websocket-client pandas numpy matplotlib flask
```

### Python 3.13がインストールされていない場合

1. [Python公式サイト](https://www.python.org/downloads/)からPython 3.13.xをダウンロード
2. インストール時に「Add Python to PATH」にチェック
3. 上記のセットアップ手順を実行

### Git Bashで `sed` や `uname` エラーが出る場合

これは警告のみで、venvの動作には影響しません。無視して問題ありません。

**最も確実な方法は、直接実行することです：**

```bash
./venv/Scripts/python.exe run_control_panel.py
```
