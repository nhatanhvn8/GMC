@echo off
chcp 65001 >nul
cd /d "%~dp0"

echo === PUSH CODE LEN GITHUB ===
echo.

:: Kiem tra git
where git >nul 2>&1
if %errorlevel% neq 0 (
    echo [LOI] Git chua duoc cai dat!
    echo Tai git tai: https://git-scm.com/download/win
    pause
    exit /b 1
)

:: Khoi tao repo neu chua co
if not exist ".git" (
    echo [SETUP] Khoi tao git repo lan dau...
    git init -b main
    git remote add gmc https://github.com/nhatanhvn8/GMC.git
    git fetch gmc
    git reset --hard gmc/main
    echo.
) else (
    git remote get-url gmc >nul 2>&1
    if %errorlevel% neq 0 (
        git remote add gmc https://github.com/nhatanhvn8/GMC.git
    )
)

:: Dong bo voi remote truoc khi push
echo Dong bo voi GitHub...
git fetch gmc >nul 2>&1

:: Hien thi version
set VER=?
for /f "delims=" %%v in (VERSION) do set VER=%%v
echo Version hien tai: %VER%
echo.

:: Hoi version moi
set NEWVER=
set /p NEWVER=Nhap version moi (Enter de giu nguyen %VER%): 
if not "%NEWVER%"=="" (
    echo %NEWVER%> VERSION
    echo Da cap nhat VERSION: %NEWVER%
    echo.
)

:: Hoi mo ta thay doi
set MSG=
set /p MSG=Nhap mo ta thay doi (Enter de dung 'update'): 
if "%MSG%"=="" set MSG=update

:: Commit
git add -A
git commit -m "%MSG%"
if %errorlevel% neq 0 (
    echo Khong co thay doi gi de commit.
    pause
    exit /b 0
)

:: Push
echo.
echo Dang push len GitHub...
git push gmc main
if %errorlevel% neq 0 (
    echo Thu rebase va push lai...
    git pull --rebase gmc main
    git push gmc main
    if %errorlevel% neq 0 (
        echo.
        echo [LOI] Push that bai!
        pause
        exit /b 1
    )
)

echo.
echo === XONG! Da push thanh cong ===
echo May khac chay pull.bat hoac bam Update trong tool de cap nhat.
pause
