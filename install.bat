@echo off
setlocal EnableExtensions EnableDelayedExpansion

echo.
echo === Fuckdl setup (Windows) ===
echo Telegram: @barbiedrm
echo.

:: Check for Python
where python >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERR] Python not found. Please install Python 3.10 or later.
    echo Download from: https://www.python.org/downloads/
    pause
    exit /b 1
)

:: Check Python version
for /f "tokens=2" %%I in ('python --version 2^>^&1') do set PYTHON_VER=%%I
for /f "tokens=1,2 delims=." %%a in ("%PYTHON_VER%") do (
    set PYTHON_MAJOR=%%a
    set PYTHON_MINOR=%%b
)
if %PYTHON_MAJOR% lss 3 (
    echo [ERR] Python 3.10 or later required. Found Python %PYTHON_VER%
    pause
    exit /b 1
)
if %PYTHON_MAJOR% equ 3 if %PYTHON_MINOR% lss 10 (
    echo [ERR] Python 3.10 or later required. Found Python %PYTHON_VER%
    pause
    exit /b 1
)
echo [OK] Python %PYTHON_VER% found.

:: Check for pip
python -m pip --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERR] pip not available. Try: python -m ensurepip
    pause
    exit /b 1
)
echo [OK] pip is available.

echo.
echo First step: installing necessary dependencies to Python:
echo.

:: Check if Poetry is already installed and up to date
poetry --version >nul 2>&1
if %errorlevel% equ 0 (
    for /f "tokens=2" %%V in ('poetry --version 2^>^&1') do set POETRY_VER=%%V
    echo [OK] Poetry %POETRY_VER% is already installed.
    
    :: Check if update is available (simple version check)
    python -c "import pkg_resources; v=pkg_resources.get_distribution('poetry').version; exit(0 if v=='2.3.2' else 1)" >nul 2>&1
    if %errorlevel% neq 0 (
        echo [..] Updating Poetry to latest version...
        python -m pip install --upgrade poetry >nul 2>&1
        if %errorlevel% equ 0 (
            echo [OK] Poetry updated successfully.
        ) else (
            echo [WARN] Could not update Poetry, using existing version.
        )
    )
) else (
    echo [..] Installing Poetry...
    python -m pip install --upgrade poetry
    if %errorlevel% neq 0 (
        echo [ERR] Failed to install Poetry.
        pause
        exit /b 1
    )
    echo [OK] Poetry installed successfully.
)

echo.
echo Second step: Configuring Poetry and Fuckdl dependencies:
echo.

:: Configure Poetry if needed
echo [..] Configuring Poetry to create virtual environment in project...
poetry config virtualenvs.in-project true >nul 2>&1
if %errorlevel% neq 0 (
    echo [WARN] Failed to set Poetry config. Virtual env will be created in default location.
) else (
    echo [OK] Poetry configured.
)

:: Check if dependencies are already installed
echo.
echo [..] Checking existing installation...

:: Check if .venv exists and poetry.lock is present
if exist ".venv\Scripts\python.exe" if exist "poetry.lock" (
    :: Quick check if dependencies are installed by trying to import a key package
    .venv\Scripts\python -c "import fuckdl" >nul 2>&1
    if !errorlevel! equ 0 (
        echo [OK] Dependencies appear to be already installed.
        echo.
        echo [..] To force reinstallation, delete .venv folder and run this script again.
        goto :success
    )
)

:: Install dependencies
echo [..] Installing dependencies (this may take a few minutes)...
echo.
poetry install
if %errorlevel% neq 0 (
    echo [ERR] Dependency installation failed. See errors above.
    pause
    exit /b 1
)

:success
echo.
echo ============================================
echo Installation completed successfully!
echo ============================================
echo.
echo To run Fuckdl:
echo   poetry run fuckdl --help
echo.
echo Or activate the virtual environment:
echo   .venv\Scripts\activate
echo   fuckdl --help
echo.
echo Telegram: @barbiedrm
echo.
echo Press any key to continue . . .
pause >nul
endlocal
