@echo off
setlocal enabledelayedexpansion
REM JPX400 Screening Control Panel Launcher
REM Portable - works on different PCs

REM Set UTF-8 encoding (prevents character encoding issues)
chcp 65001 >nul 2>&1

REM Set environment variables for UTF-8 encoding
set PYTHONIOENCODING=utf-8
set PYTHONUTF8=1

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

REM Check if required packages are installed
REM Check pandas (main package)
python -c "import pandas" >nul 2>&1
set PANDAS_INSTALLED=%ERRORLEVEL%

REM Check pdfplumber (for JPX400 list fetching)
python -c "import pdfplumber" >nul 2>&1
set PDFPLUMBER_INSTALLED=%ERRORLEVEL%

REM If any required package is missing, offer to install
if !PANDAS_INSTALLED! NEQ 0 (
    echo [WARNING] Required packages are not installed
    echo.
    echo Would you like to install required packages now? (Y/N)
    set /p INSTALL_CHOICE=
    if /i "!INSTALL_CHOICE!"=="Y" (
        echo.
        echo Installing required packages...
        set PYTHONIOENCODING=utf-8
        python -m pip install -r requirements.txt --no-cache-dir
        if !ERRORLEVEL! NEQ 0 (
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
) else (
    REM pandas is installed, check pdfplumber
    echo [DEBUG] pandas is installed, checking pdfplumber...
    if !PDFPLUMBER_INSTALLED! NEQ 0 (
        echo [WARNING] Some required packages are missing (pdfplumber)
        echo.
        echo Would you like to install missing packages now? (Y/N)
        set /p INSTALL_CHOICE=
        if /i "!INSTALL_CHOICE!"=="Y" (
            echo.
            echo Installing missing packages...
            set PYTHONIOENCODING=utf-8
            python -m pip install -r requirements.txt --no-cache-dir
            if !ERRORLEVEL! NEQ 0 (
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
    ) else (
        REM All required packages are installed
        echo [INFO] All required packages are installed
        echo.
    )
)
goto :continue

:venv_found
echo [INFO] Using virtual environment
echo [DEBUG] Python command: %PYTHON_CMD%
echo [DEBUG] Current directory: %CD%
echo.

REM Verify Python command works
echo [DEBUG] Verifying Python command...
%PYTHON_CMD% --version >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Python command failed: %PYTHON_CMD%
    echo [ERROR] Please check if virtual environment is properly set up
    pause
    exit /b 1
)
echo [DEBUG] Python command verified successfully
echo.

REM Check if required packages are installed in venv
REM Check pandas (main package)
echo [DEBUG] Checking pandas...
%PYTHON_CMD% -c "import pandas" >nul 2>&1
set PANDAS_INSTALLED=%ERRORLEVEL%
echo [DEBUG] pandas check result: !PANDAS_INSTALLED!

REM Check pdfplumber (for JPX400 list fetching)
echo [DEBUG] Checking pdfplumber...
%PYTHON_CMD% -c "import pdfplumber" >nul 2>&1
set PDFPLUMBER_INSTALLED=%ERRORLEVEL%
echo [DEBUG] pdfplumber check result: !PDFPLUMBER_INSTALLED!
echo.
echo [DEBUG] Evaluating conditions: PANDAS_INSTALLED=!PANDAS_INSTALLED!, PDFPLUMBER_INSTALLED=!PDFPLUMBER_INSTALLED!
echo.
echo [DEBUG] About to check if PANDAS_INSTALLED NEQ 0...
echo [DEBUG] PANDAS_INSTALLED value as string: "!PANDAS_INSTALLED!"
echo [DEBUG] Testing condition...
set TEST_VAR=!PANDAS_INSTALLED!
echo [DEBUG] TEST_VAR=!TEST_VAR!
if "!TEST_VAR!"=="0" (
    echo [DEBUG] Condition matched: PANDAS_INSTALLED is 0
    echo [DEBUG] PANDAS_INSTALLED is 0, pandas is installed
    echo [DEBUG] Checking pdfplumber: PDFPLUMBER_INSTALLED=!PDFPLUMBER_INSTALLED!
    if "!PDFPLUMBER_INSTALLED!"=="0" (
        echo [DEBUG] All packages are installed
        echo [INFO] All required packages are installed
        echo [DEBUG] Proceeding to launch...
        echo.
    ) else (
        echo [DEBUG] Entering pdfplumber missing branch
        echo [WARNING] Some required packages are missing in virtual environment (pdfplumber)
        echo.
        echo Would you like to install missing packages now? (Y/N)
        set /p INSTALL_CHOICE=
        if /i "!INSTALL_CHOICE!"=="Y" (
            echo.
            echo Installing missing packages...
            set PYTHONIOENCODING=utf-8
            %PYTHON_CMD% -m pip install -r requirements.txt --no-cache-dir
            if !ERRORLEVEL! NEQ 0 (
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
) else (
    echo [DEBUG] PANDAS_INSTALLED is not 0, entering pandas missing branch
    echo [WARNING] Required packages are not installed in virtual environment
    echo.
    echo Would you like to install required packages now? (Y/N)
    set /p INSTALL_CHOICE=
    if /i "!INSTALL_CHOICE!"=="Y" (
        echo.
        echo Installing required packages...
        set PYTHONIOENCODING=utf-8
        %PYTHON_CMD% -m pip install -r requirements.txt --no-cache-dir
        if !ERRORLEVEL! NEQ 0 (
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

echo [DEBUG] Reaching goto :continue
goto :continue

:continue

echo [INFO] Launching control panel...
echo [DEBUG] Current directory: %CD%
echo [DEBUG] Python command: %PYTHON_CMD%
echo [DEBUG] Checking if run_control_panel.py exists...
if exist "run_control_panel.py" (
    echo [DEBUG] run_control_panel.py found
) else (
    echo [ERROR] run_control_panel.py not found in current directory
    pause
    exit /b 1
)
echo.

REM Launch control panel
echo [DEBUG] Executing: %PYTHON_CMD% run_control_panel.py
%PYTHON_CMD% run_control_panel.py 2>&1
set LAUNCH_ERROR=%ERRORLEVEL%
if %LAUNCH_ERROR% NEQ 0 (
    echo [DEBUG] Application exited with code: %LAUNCH_ERROR%
)

REM Pause on error
if %LAUNCH_ERROR% NEQ 0 (
    echo.
    echo [ERROR] Failed to launch application: %LAUNCH_ERROR%
    echo.
    echo Troubleshooting:
    echo 1. Check if Python is installed correctly
    echo 2. Install required packages: pip install -r requirements.txt
    echo 3. If using virtual environment, check if venv or .venv exists
    echo 4. Try running manually: %PYTHON_CMD% run_control_panel.py
    echo.
    pause
)

