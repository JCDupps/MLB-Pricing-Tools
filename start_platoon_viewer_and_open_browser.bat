@echo off
setlocal

cd /d "%~dp0"

if not exist ".venv\Scripts\python.exe" (
  echo.
  echo Virtual environment not found.
  echo Expected file: .venv\Scripts\python.exe
  echo.
  pause
  exit /b 1
)

set "APP_URL=http://127.0.0.1:8010/"

echo Starting MLB Pricing Tools on %APP_URL%
echo Your browser will open automatically in a few seconds.
echo Leave this window open while you use the app.
echo Press Ctrl+C in this window to stop the server.
echo.

start "" cmd /c "timeout /t 3 /nobreak >nul && start "" "%APP_URL%""
".venv\Scripts\python.exe" -m uvicorn platoon_viewer:app --reload --host 127.0.0.1 --port 8010

echo.
echo The server has stopped.
pause
