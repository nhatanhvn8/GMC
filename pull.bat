@echo off
chcp 65001 >nul
cd /d "%~dp0"
echo === CAP NHAT TOOL TU GITHUB ===
echo.
python update.py --force
echo.
pause
