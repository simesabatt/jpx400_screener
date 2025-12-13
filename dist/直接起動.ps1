# JPX400スクリーニング コントロールパネル起動スクリプト（PowerShell版）
# 別PCに移動しても動作するように設計されています
# 文字化けが発生する場合は、このPowerShellスクリプトを使用してください

# プロジェクトルートに移動
$scriptPath = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = Join-Path $scriptPath ".."
Set-Location $projectRoot

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "JPX400スクリーニング コントロールパネル 起動" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# 仮想環境のPythonを直接実行（最も確実な方法）
$pythonCmd = "python"

if (Test-Path "venv\Scripts\python.exe") {
    $pythonCmd = "venv\Scripts\python.exe"
    Write-Host "[情報] 仮想環境（venv）を使用します" -ForegroundColor Green
} elseif (Test-Path ".venv\Scripts\python.exe") {
    $pythonCmd = ".venv\Scripts\python.exe"
    Write-Host "[情報] 仮想環境（.venv）を使用します" -ForegroundColor Green
} else {
    Write-Host "[警告] 仮想環境が見つかりません" -ForegroundColor Yellow
    Write-Host "[警告] システムのPythonを使用します" -ForegroundColor Yellow
    Write-Host "[注意] 必要なパッケージがインストールされていることを確認してください" -ForegroundColor Yellow
    Write-Host ""
    
    # Pythonがインストールされているか確認
    try {
        $pythonVersion = & python --version 2>&1
        Write-Host "[確認] $pythonVersion" -ForegroundColor Gray
    } catch {
        Write-Host "[エラー] Pythonが見つかりません" -ForegroundColor Red
        Write-Host "Python 3.8以上をインストールしてください" -ForegroundColor Red
        Read-Host "Enterキーを押して終了"
        exit 1
    }
}

Write-Host ""

# コントロールパネルを起動
try {
    & $pythonCmd run_control_panel.py
    
    if ($LASTEXITCODE -ne 0) {
        Write-Host ""
        Write-Host "[エラー] アプリケーションの起動に失敗しました: $LASTEXITCODE" -ForegroundColor Red
        Write-Host ""
        Write-Host "トラブルシューティング:" -ForegroundColor Yellow
        Write-Host "1. Pythonが正しくインストールされているか確認"
        Write-Host "2. 必要なパッケージがインストールされているか確認（pip install -r requirements.txt）"
        Write-Host "3. 仮想環境を使用する場合は、venvまたは.venvディレクトリが存在するか確認"
        Write-Host ""
        Read-Host "Enterキーを押して終了"
    }
} catch {
    Write-Host ""
    Write-Host "[エラー] 予期しないエラーが発生しました: $_" -ForegroundColor Red
    Read-Host "Enterキーを押して終了"
    exit 1
}

