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

echo Starting MLB Pricing Tools on http://127.0.0.1:8010/
echo Leave this window open while you use the app.
echo Press Ctrl+C in this window to stop the server.
echo.

".venv\Scripts\python.exe" -m uvicorn platoon_viewer:app --reload --host 127.0.0.1 --port 8010

echo.
echo The server has stopped.
pause
