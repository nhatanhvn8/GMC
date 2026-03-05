@echo off
chcp 65001 >nul
cd /d "%~dp0"

:: Set git identity
git config user.email "nhatanhvn8@gmail.com" >nul 2>&1
git config user.name "nhatanhvn8" >nul 2>&1
git config credential.helper wincred >nul 2>&1

:: Khoi tao repo neu chua co
if not exist ".git" (
    git init -b main >nul 2>&1
    git remote add gmc git@github.com:nhatanhvn8/GMC.git >nul 2>&1
) else (
    git remote get-url gmc >nul 2>&1
    if %errorlevel% neq 0 (
        git remote add gmc git@github.com:nhatanhvn8/GMC.git >nul 2>&1
    )
)

:: Hien thi version hien tai
set VER=?
for /f "delims=" %%v in (VERSION) do set VER=%%v

:: Hoi version moi
set /p NEWVER=Version hien tai: %VER%  -  Nhap version moi (Enter de giu nguyen): 
if not "%NEWVER%"=="" (
    echo %NEWVER%> VERSION
)

:: Add, commit, push
git add -A
git commit -m "update" >nul 2>&1
git push gmc HEAD:main --force

if %errorlevel% neq 0 (
    echo [LOI] Push that bai! Kiem tra lai token.
    pause & exit /b 1
)

echo.
echo === Da push thanh cong! ===
pause
