@echo off
set "CHROME=C:\Program Files\Google\Chrome\Application\chrome.exe"
if not exist "%CHROME%" set "CHROME=C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"
if not exist "%CHROME%" set "CHROME=%LOCALAPPDATA%\Google\Chrome\Application\chrome.exe"

if not exist "%CHROME%" (
  echo Could not find Chrome.
  pause
  exit /b 1
)

set "PROFILE_DIR=%~dp0chrome_fangraphs_profile"
if not exist "%PROFILE_DIR%" mkdir "%PROFILE_DIR%"

start "" "%CHROME%" --user-data-dir="%PROFILE_DIR%" "https://www.fangraphs.com/roster-resource/platoon-lineups/braves"
