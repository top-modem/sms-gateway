@echo off
setlocal EnableExtensions
chcp 65001 >nul

set "ROOT=%~dp0"
if "%ROOT:~-1%"=="\" set "ROOT=%ROOT:~0,-1%"
set "FRONTEND=%ROOT%\frontend"
set "DRY_RUN=0"
if /I "%~1"=="--dry-run" set "DRY_RUN=1"

where pnpm >nul 2>nul || (
  echo pnpm not found in PATH.
  exit /b 1
)

where cargo >nul 2>nul || (
  echo cargo not found in PATH.
  exit /b 1
)

where powershell >nul 2>nul || (
  echo powershell not found in PATH.
  exit /b 1
)

rem Build app name from Unicode code points to avoid mojibake in non-UTF8 editors.
for /f "delims=" %%I in ('powershell -NoProfile -Command "[char]0x5C0F+[char]0x725B+[char]0x667A+[char]0x5361"') do set "APP_NAME=%%I"

set "NEWDIR=%ROOT%\release\%APP_NAME%-windows-portable-v5"
set "ZIPPATH=%ROOT%\release\%APP_NAME%-windows-portable-v5.zip"

if "%DRY_RUN%"=="1" (
  echo [DRY RUN]
  echo ROOT=%ROOT%
  echo FRONTEND=%FRONTEND%
  echo APP_NAME=%APP_NAME%
  echo NEWDIR=%NEWDIR%
  echo ZIPPATH=%ZIPPATH%
  exit /b 0
)

echo [1/6] Go to project root
cd /d "%ROOT%" || (
  echo Failed to enter project root: %ROOT%
  exit /b 1
)

echo [2/6] Build frontend
cd /d "%FRONTEND%" || (
  echo Failed to enter frontend folder: %FRONTEND%
  exit /b 1
)
call pnpm run build
if errorlevel 1 (
  echo Frontend build failed.
  exit /b 1
)

echo [3/6] Build backend release exe
cd /d "%ROOT%"
call cargo build --release
if errorlevel 1 (
  echo Backend release build failed.
  exit /b 1
)

echo [4/6] Recreate portable folder
rem Preserve existing data\ and logs\ (they hold the live sqlite database and
rem log history) across rebuilds instead of wiping them along with the folder.
rem Backup dir must live on the same drive as NEWDIR: "move" across drives fails.
set "DATA_BACKUP=%ROOT%\release\_data_backup_%RANDOM%"
if exist "%NEWDIR%\data" (
  mkdir "%DATA_BACKUP%" >nul 2>nul
  move /y "%NEWDIR%\data" "%DATA_BACKUP%\data" >nul
)
if exist "%NEWDIR%\logs" (
  if not exist "%DATA_BACKUP%" mkdir "%DATA_BACKUP%" >nul 2>nul
  move /y "%NEWDIR%\logs" "%DATA_BACKUP%\logs" >nul
)
if exist "%NEWDIR%" rmdir /s /q "%NEWDIR%"
mkdir "%NEWDIR%"
mkdir "%NEWDIR%\icon"
if exist "%DATA_BACKUP%\data" move /y "%DATA_BACKUP%\data" "%NEWDIR%\data" >nul
if exist "%DATA_BACKUP%\logs" move /y "%DATA_BACKUP%\logs" "%NEWDIR%\logs" >nul
if exist "%DATA_BACKUP%" rmdir /s /q "%DATA_BACKUP%"

copy /y "%ROOT%\target\release\sms-gateway.exe" "%NEWDIR%\%APP_NAME%.exe" >nul || (
  echo Failed to copy release exe.
  exit /b 1
)
copy /y "%ROOT%\assets\icons\tray.ico" "%NEWDIR%\icon\tray.ico" >nul || (
  echo Failed to copy tray icon.
  exit /b 1
)
copy /y "%ROOT%\assets\icons\xiaoniu-zhika.ico" "%NEWDIR%\icon\xiaoniu-zhika.ico" >nul || (
  echo Failed to copy main icon.
  exit /b 1
)

rem sound/ (SMS notification wav) and lpac/ (eSIM LPA tool) are read at
rem runtime via relative paths, so they must ship alongside the exe.
if exist "%ROOT%\sound" (
  xcopy /y /i /e /q "%ROOT%\sound" "%NEWDIR%\sound" >nul || (
    echo Failed to copy sound folder.
    exit /b 1
  )
)
if exist "%ROOT%\lpac" (
  xcopy /y /i /e /q "%ROOT%\lpac" "%NEWDIR%\lpac" >nul || (
    echo Failed to copy lpac folder.
    exit /b 1
  )
)

for %%F in (
  config.toml
  config.toml.example
  kill.bat
  README.md
  restart_and_open_browser.bat
  refresh_icon_cache.bat
  test_cnum.ps1
) do (
  if exist "%ROOT%\%%F" copy /y "%ROOT%\%%F" "%NEWDIR%\" >nul
)

echo [5/6] Create zip package
if exist "%ZIPPATH%" del /f /q "%ZIPPATH%"
powershell -NoProfile -Command "Compress-Archive -Path '%NEWDIR%\*' -DestinationPath '%ZIPPATH%'"
if errorlevel 1 (
  echo Failed to create zip package.
  exit /b 1
)

echo [6/6] Done

echo Portable folder:
echo %NEWDIR%
echo Zip package:
echo %ZIPPATH%

dir "%NEWDIR%"
dir "%ZIPPATH%"

echo.
echo Build package completed successfully.
exit /b 0
