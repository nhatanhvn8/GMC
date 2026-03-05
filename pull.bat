@echo off
chcp 65001 >nul
cd /d "%~dp0"

echo === CAP NHAT TOOL TU GITHUB ===
echo.

:: Thử dùng git pull trước (nhanh hơn, giữ nguyên config)
where git >nul 2>&1
if %errorlevel% equ 0 (
    if exist ".git" (
        echo Dang pull bang git...
        git fetch gmc 2>nul
        git reset --hard gmc/main
        if %errorlevel% equ 0 (
            echo.
            echo Cap nhat thanh cong!
            set /p VER=<VERSION
            echo Phien ban hien tai: %VER%
            echo.
            pause
            exit /b 0
        )
    )
)

:: Fallback: dùng python update.py (zip download)
echo Dung python de cap nhat...
python update.py --force

echo.
pause
