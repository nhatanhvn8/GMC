@echo off
chcp 65001 >nul
title Gmail Tool - Setup
echo.
echo ========================================
echo   GMAIL TOOL - SETUP MOI TRUONG
echo ========================================
echo.

:: Kiem tra quyen Admin
net session >nul 2>&1
if %errorLevel% neq 0 (
    echo [!] Can quyen Admin. Click phai setup.bat - Run as administrator
    pause
    exit /b 1
)

:: Hien version
for /f "tokens=*" %%v in ('python -c "from pathlib import Path; print(Path(\"VERSION\").read_text().strip())" 2^>nul') do set CUR_VER=%%v
if not "%CUR_VER%"=="" echo   Phien ban hien tai: v%CUR_VER%
echo.

:: Kiem tra Python
echo [1/4] Kiem tra Python...
python --version >nul 2>&1
if %errorLevel% neq 0 (
    echo     Python chua cai. Dang tai tu internet...
    powershell -Command "Invoke-WebRequest -Uri 'https://www.python.org/ftp/python/3.9.9/python-3.9.9-amd64.exe' -OutFile '%TEMP%\python-setup.exe'"
    echo     Dang cai dat Python (nho tick "Add Python to PATH")...
    start /wait "%TEMP%\python-setup.exe"
    call "%~f0"
    exit /b
)
for /f "tokens=*" %%v in ('python --version 2^>^&1') do set PYVER=%%v
echo     OK - %PYVER%

:: Nang cap pip
echo [2/4] Nang cap pip...
python -m pip install --upgrade pip --quiet
echo     OK

:: Cai thu vien
echo [3/4] Cai thu vien Python...
python -m pip install -r requirements.txt
if %errorLevel% neq 0 (
    echo     [LOI] Cai thu vien that bai.
    pause
    exit /b 1
)
python -m pip install 2captcha-python pywin32 colorama ttkthemes scikit-learn numpy --quiet
echo     OK

:: Tao thu muc
echo [4/4] Tao cau truc thu muc...
if not exist "data"       mkdir data
if not exist "config"     mkdir config
if not exist "logs"       mkdir logs
if not exist "export"     mkdir export
if not exist "profiles"   mkdir profiles
if not exist "training_data" mkdir training_data
if not exist "data\list_proxy.txt"   echo # Moi dong 1 proxy: host:port:user:pass > data\list_proxy.txt
if not exist "data\list_pass.txt"    type nul > data\list_pass.txt
if not exist "data\list_mail_kp.txt" type nul > data\list_mail_kp.txt
if not exist "data\accounts_db.json" echo [] > data\accounts_db.json
echo     OK

echo.
echo ========================================
echo   SETUP HOAN THANH!
echo ========================================
echo.
if not exist "browser" (
    echo [!] Nho copy thu muc browser/ tu may chinh vao day
    echo.
)
echo Cac buoc tiep theo:
echo   1. Copy browser/ tu may chinh (neu chua co)
echo   2. Copy data/accounts_db.json (danh sach Gmail)
echo   3. Mo tool: stop_and_run.bat
echo.
pause
