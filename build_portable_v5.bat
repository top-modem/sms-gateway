@echo off
setlocal EnableExtensions EnableDelayedExpansion
chcp 65001 >nul

set "ROOT=%~dp0"
if "%ROOT:~-1%"=="\" set "ROOT=%ROOT:~0,-1%"
set "FRONTEND=%ROOT%\frontend"
set "DRY_RUN=0"
set "FULL_OPT=0"
set "NO_ZIP=0"
set "PROFILE=fast-release"
set "PROFILE_DIR=fast-release"

:parse_args
if "%~1"=="" goto args_done
if /I "%~1"=="--dry-run" set "DRY_RUN=1"
if /I "%~1"=="--full" (
  set "FULL_OPT=1"
  set "PROFILE=release"
  set "PROFILE_DIR=release"
)
if /I "%~1"=="--no-zip" set "NO_ZIP=1"
shift
goto parse_args
:args_done

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

rem Use every logical CPU for rustc codegen / LLVM work inside crates.
if not defined CARGO_BUILD_JOBS (
  if defined NUMBER_OF_PROCESSORS (
    set "CARGO_BUILD_JOBS=%NUMBER_OF_PROCESSORS%"
  ) else (
    set "CARGO_BUILD_JOBS=8"
  )
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
  echo PROFILE=%PROFILE%
  echo CARGO_BUILD_JOBS=%CARGO_BUILD_JOBS%
  echo FULL_OPT=%FULL_OPT%
  echo NO_ZIP=%NO_ZIP%
  exit /b 0
)

echo [1/5] Go to project root
cd /d "%ROOT%" || (
  echo Failed to enter project root: %ROOT%
  exit /b 1
)
if not exist "%ROOT%\target" mkdir "%ROOT%\target"

echo [2/5] Build frontend + backend in parallel ^(profile=%PROFILE%, jobs=%CARGO_BUILD_JOBS%^)
if "%FULL_OPT%"=="1" (
  echo Using full release LTO. For everyday rebuilds omit --full ^(uses fast-release^).
) else (
  echo Using fast-release ^(no fat LTO, incremental^). Pass --full for maximum optimisation.
)

rem Frontend and Rust share almost no work, so run them together.
powershell -NoProfile -ExecutionPolicy Bypass -File "%ROOT%\scripts\parallel_build.ps1" -Root "%ROOT%" -Frontend "%FRONTEND%" -Profile "%PROFILE%" -Jobs "%CARGO_BUILD_JOBS%"
if errorlevel 1 (
  echo Build failed. Logs: %ROOT%\target\build_frontend*.log and %ROOT%\target\build_backend*.log
  exit /b 1
)

set "EXE_SRC=%ROOT%\target\%PROFILE_DIR%\sms-gateway.exe"
if not exist "%EXE_SRC%" (
  echo Missing built exe: %EXE_SRC%
  exit /b 1
)

echo [3/5] Recreate portable folder
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

copy /y "%EXE_SRC%" "%NEWDIR%\%APP_NAME%.exe" >nul || (
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

rem tools\ffmpeg converts call recordings for Aliyun voice OTP; tools\lpac is the eSIM LPA.
rem Both are found automatically next to the exe, so ship them in the package.
if not exist "%ROOT%\tools\ffmpeg\ffmpeg.exe" (
  echo Missing tools\ffmpeg\ffmpeg.exe - voice OTP needs it. Put ffmpeg.exe there and rebuild.
  exit /b 1
)
xcopy /y /i /q "%ROOT%\tools\ffmpeg\ffmpeg.exe" "%NEWDIR%\tools\ffmpeg\" >nul || (
  echo Failed to copy ffmpeg.
  exit /b 1
)
if exist "%ROOT%\tools\lpac" (
  xcopy /y /i /e /q "%ROOT%\tools\lpac" "%NEWDIR%\tools\lpac" >nul || (
    echo Failed to copy tools\lpac folder.
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

if "%NO_ZIP%"=="1" (
  echo [4/5] Skip zip ^(--no-zip^)
) else (
  echo [4/5] Create zip package
  if exist "%ZIPPATH%" del /f /q "%ZIPPATH%"
  rem .NET ZipFile is much faster than Compress-Archive on large trees.
  powershell -NoProfile -Command "$ErrorActionPreference='Stop'; Add-Type -AssemblyName System.IO.Compression.FileSystem; if (Test-Path -LiteralPath '%ZIPPATH%') { Remove-Item -LiteralPath '%ZIPPATH%' -Force }; [System.IO.Compression.ZipFile]::CreateFromDirectory('%NEWDIR%', '%ZIPPATH%', [System.IO.Compression.CompressionLevel]::Optimal, $false)"
  if errorlevel 1 (
    echo Failed to create zip package.
    exit /b 1
  )
)

echo [5/5] Done

echo Portable folder:
echo %NEWDIR%
if "%NO_ZIP%"=="0" (
  echo Zip package:
  echo %ZIPPATH%
)

dir "%NEWDIR%"
if "%NO_ZIP%"=="0" dir "%ZIPPATH%"

echo.
echo Build package completed successfully. profile=%PROFILE% jobs=%CARGO_BUILD_JOBS%
exit /b 0
