@echo off
chcp 65001 >nul
set "ROOT=%~dp0"
set "ROOT=%ROOT:~0,-1%"
cd /d "%~dp0"

echo ========================================
echo   GMAIL TOOL
echo ========================================

:: Hien version hien tai
for /f "tokens=*" %%v in ('python -c "from pathlib import Path; print(Path(\"VERSION\").read_text().strip())" 2^>nul') do set CUR_VER=%%v
if "%CUR_VER%"=="" set CUR_VER=unknown
echo   Phien ban: v%CUR_VER%
echo.

:: Auto-update tu GitHub
echo [1/3] Kiem tra cap nhat...
python update.py
echo.

:: Dung process cu
echo [2/3] Dung process cu...
powershell -NoProfile -Command ^
  "$root = '%ROOT:\=\\%'; " ^
  "Get-CimInstance Win32_Process -EA 0 | Where-Object { " ^
  "  $n = $_.Name; $c = $_.CommandLine ?? ''; " ^
  "  ($n -match '^(chrome|gpmdriver|chromedriver)\.exe$' -and $c.IndexOf($root) -ge 0) -or " ^
  "  ($n -eq 'python.exe' -and $c -match 'run\.py' -and $c.IndexOf($root) -ge 0) " ^
  "} | ForEach-Object { try { Stop-Process -Id $_.ProcessId -Force -EA Stop } catch {} }" >nul 2>&1
timeout /t 2 /nobreak >nul

:: Chay tool
echo [3/3] Khoi dong tool...
echo.
python "%~dp0run.py"
if errorlevel 1 pause
