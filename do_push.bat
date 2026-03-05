@echo off
chcp 65001 >nul
cd /d "%~dp0"

echo ===================================================
echo         PUSH CODE LEN GITHUB - GMC TOOL
echo ===================================================
echo.

:: Kiem tra git
where git >nul 2>&1
if %errorlevel% neq 0 (
    echo [LOI] Git chua duoc cai dat!
    pause & exit /b 1
)

:: Set git identity
git config user.email "nhatanhvn8@gmail.com" >nul 2>&1
git config user.name "nhatanhvn8" >nul 2>&1

:: Dung credential store de luu token
git config credential.helper store >nul 2>&1

:: Khoi tao repo neu chua co
if not exist ".git" (
    echo [SETUP] Khoi tao git repo...
    git init -b main >nul 2>&1
)

:: Gan remote neu chua co
git remote get-url gmc >nul 2>&1
if %errorlevel% neq 0 (
    git remote add gmc https://github.com/nhatanhvn8/GMC.git
)

:: Hien thi version hien tai
set VER=?
for /f "delims=" %%v in (VERSION) do set VER=%%v
echo Version hien tai: %VER%
echo.

:: Hoi version moi
set NEWVER=
set /p NEWVER=Nhap version moi (Enter de giu nguyen): 
if not "%NEWVER%"=="" (
    echo %NEWVER%> VERSION
    echo Da cap nhat: %NEWVER%
    echo.
)

:: Hoi mo ta commit
set MSG=
set /p MSG=Nhap mo ta thay doi (Enter de dung 'update'): 
if "%MSG%"=="" set MSG=update

:: Add va commit
git add -A
git commit -m "%MSG%" >nul 2>&1

:: Push - se hoi Username va Password (dan token vao o Password)
echo.
echo Dang push len GitHub...
echo.
echo >>> Khi duoc hoi:
echo     Username: nhatanhvn8
echo     Password: dan GITHUB TOKEN vao (github_pat_...)
echo.

git push gmc HEAD:main --force

if %errorlevel% neq 0 (
    echo.
    echo [LOI] Push that bai! Kiem tra lai token.
    pause & exit /b 1
)

echo.
echo === XONG! Da push thanh cong len GitHub ===
echo May khac chay pull.bat de cap nhat.
pause
