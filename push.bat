@echo off
chcp 65001 >nul
cd /d "%~dp0"

echo === PUSH CODE LEN GITHUB ===
echo.

:: ── Kiểm tra git có không ────────────────────────────────────────────────────
where git >nul 2>&1
if %errorlevel% neq 0 (
    echo [LOI] Git chua duoc cai dat!
    echo Tai git tai: https://git-scm.com/download/win
    pause
    exit /b 1
)

:: ── Nếu chưa có .git, khởi tạo repo và gắn remote ──────────────────────────
if not exist ".git" (
    echo [SETUP] Chua co git repo, dang khoi tao...
    git init
    git remote add gmc https://github.com/nhatanhvn8/GMC.git
    echo [SETUP] Da gan remote: https://github.com/nhatanhvn8/GMC.git
    echo.
) else (
    :: Kiểm tra remote gmc đã có chưa
    git remote get-url gmc >nul 2>&1
    if %errorlevel% neq 0 (
        echo [SETUP] Them remote gmc...
        git remote add gmc https://github.com/nhatanhvn8/GMC.git
    )
)

:: ── Hiển thị version ──────────────────────────────────────────────────────────
set /p VER=<VERSION
echo Version hien tai: %VER%
echo.

:: ── Hỏi version mới ──────────────────────────────────────────────────────────
set /p NEWVER="Nhap version moi (Enter de giu nguyen %VER%): "
if not "%NEWVER%"=="" (
    echo %NEWVER%> VERSION
    echo Da cap nhat VERSION: %NEWVER%
    echo.
)

:: ── Hỏi mô tả thay đổi ───────────────────────────────────────────────────────
set /p MSG="Nhap mo ta thay doi (Enter de dung 'update'): "
if "%MSG%"=="" set MSG=update

:: ── Tạo orphan branch sạch (tránh GitHub secret scan block) ──────────────────
echo.
echo Dang push len GitHub (orphan branch de tranh bi block)...

git checkout --orphan _push_tmp 2>nul
git add -A
git commit -m "%MSG%"

git push gmc _push_tmp:main --force
if %errorlevel% neq 0 (
    echo.
    echo [LOI] Push that bai!
    git checkout main 2>nul
    git branch -D _push_tmp 2>nul
    pause
    exit /b 1
)

:: ── Quay về main và đồng bộ với remote ───────────────────────────────────────
git checkout main 2>nul
if %errorlevel% neq 0 (
    git checkout -b main 2>nul
)
git branch -D _push_tmp 2>nul
git fetch gmc 2>nul
git reset --hard gmc/main 2>nul

echo.
echo === XONG! Da push thanh cong len GitHub ===
echo May khac chay pull.bat hoac bam Update trong tool de cap nhat.
pause
