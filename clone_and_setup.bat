@echo off
chcp 65001 >nul
title Gmail Tool - Clone & Setup
echo.
echo ========================================
echo   GMAIL TOOL - CLONE CODE TU GITHUB
echo ========================================
echo.

:: ── Kiem tra Python ──────────────────────────────────────────────────────────
echo [1/3] Kiem tra Python...
python --version >nul 2>&1
if %errorLevel% neq 0 (
    echo     Python chua cai. Dang tai tu internet...
    powershell -Command "Invoke-WebRequest -Uri 'https://www.python.org/ftp/python/3.9.9/python-3.9.9-amd64.exe' -OutFile '%TEMP%\python-setup.exe'"
    echo     Dang cai dat Python (nho tick "Add Python to PATH")...
    start /wait "%TEMP%\python-setup.exe"
    echo     Cai Python xong! Dang chay lai script...
    call "%~f0"
    exit /b
)
for /f "tokens=*" %%v in ('python --version 2^>^&1') do set PYVER=%%v
echo     OK - %PYVER%

:: ── Tai code bang Python (khong can git) ─────────────────────────────────────
echo [2/3] Tai code tu GitHub...
python -c "import urllib.request,zipfile,io,shutil,os; z=urllib.request.urlopen('https://github.com/nhatanhvn8/GMC/archive/refs/heads/main.zip',timeout=60).read(); zf=zipfile.ZipFile(io.BytesIO(z)); prefix=[n for n in zf.namelist() if n.endswith('/') and n.count('/')==1][0]; [zf.extract(n,'.') for n in zf.namelist()]; src=prefix.rstrip('/'); [shutil.move(os.path.join(src,f),'.' if f=='' else f) for f in os.listdir(src)]; shutil.rmtree(src); print('Tai xong!')"
if %errorLevel% neq 0 (
    echo     [LOI] Tai that bai. Kiem tra ket noi mang.
    pause
    exit /b 1
)

:: ── Cai thu vien ─────────────────────────────────────────────────────────────
echo [3/3] Cai thu vien Python...
python -m pip install --upgrade pip --quiet
python -m pip install -r requirements.txt --quiet
python -m pip install 2captcha-python pywin32 colorama ttkthemes scikit-learn numpy --quiet

:: Tao thu muc can thiet
if not exist "data"     mkdir data
if not exist "config"   mkdir config
if not exist "logs"     mkdir logs
if not exist "export"   mkdir export
if not exist "profiles" mkdir profiles
if not exist "data\list_proxy.txt"   echo # Moi dong 1 proxy: host:port:user:pass > data\list_proxy.txt
if not exist "data\list_pass.txt"    type nul > data\list_pass.txt
if not exist "data\list_mail_kp.txt" type nul > data\list_mail_kp.txt
if not exist "data\accounts_db.json" echo [] > data\accounts_db.json

echo.
echo ========================================
echo   HOAN THANH!
echo ========================================
echo.
if not exist "browser" (
    echo [!] Nho copy thu muc browser/ tu may chinh vao day
)
echo Chay: python run.py
echo.
pause
