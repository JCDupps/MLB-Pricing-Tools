@echo off
cd /d "%~dp0"

if not exist ".venv\Scripts\python.exe" (
  echo Could not find .venv\Scripts\python.exe
  pause
  exit /b 1
)

".venv\Scripts\python.exe" seed_lineup_cache.py
echo.
pause
