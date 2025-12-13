@echo off
setlocal enabledelayedexpansion
REM JPX400 Screening Control Panel Launcher
REM Portable - works on different PCs

REM Set UTF-8 encoding (prevents character encoding issues)
chcp 65001 >nul 2>&1

echo ========================================
echo JPX400 Screening Control Panel
echo ========================================
echo.

REM Move to project root
cd /d "%~dp0\.."

REM Use virtual environment Python directly (most reliable method)
REM Use venv if exists, otherwise use system Python
set PYTHON_CMD=python
set VENV_FOUND=0

REM Check for venv (relative path from current directory)
REM Priority: venv > .venv
if exist "venv\Scripts\python.exe" (
    set "PYTHON_CMD=venv\Scripts\python.exe"
    set VENV_FOUND=1
    goto :venv_found
)

if exist ".venv\Scripts\python.exe" (
    set "PYTHON_CMD=.venv\Scripts\python.exe"
    set VENV_FOUND=1
    goto :venv_found
)

REM No virtual environment found
echo [WARNING] Virtual environment not found
echo [WARNING] Using system Python
echo [NOTE] Make sure required packages are installed
echo.
REM Check if Python is installed
python --version >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Python not found
    echo Please install Python 3.8 or higher
    pause
    exit /b 1
)

REM Check if pandas is installed
python -c "import pandas" >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo [WARNING] Required packages are not installed
    echo.
    echo Would you like to install required packages now? (Y/N)
    set /p INSTALL_CHOICE=
    if /i "!INSTALL_CHOICE!"=="Y" (
        echo.
        echo Installing required packages...
        python -m pip install -r requirements.txt
        if %ERRORLEVEL% NEQ 0 (
            echo [ERROR] Failed to install packages
            pause
            exit /b 1
        )
        echo [INFO] Packages installed successfully
        echo.
    ) else (
        echo [INFO] Skipping package installation
        echo [NOTE] You can install packages later with: pip install -r requirements.txt
        echo.
    )
)
goto :continue

:venv_found
echo [INFO] Using virtual environment
goto :continue

:continue

echo.

REM Launch control panel
%PYTHON_CMD% run_control_panel.py

REM Pause on error
if %ERRORLEVEL% NEQ 0 (
    echo.
    echo [ERROR] Failed to launch application: %ERRORLEVEL%
    echo.
    echo Troubleshooting:
    echo 1. Check if Python is installed correctly
    echo 2. Install required packages: pip install -r requirements.txt
    echo 3. If using virtual environment, check if venv or .venv exists
    echo.
    pause
)

