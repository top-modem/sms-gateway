@echo off
setlocal EnableExtensions
cd /d "%~1" || exit /b 1
if not "%~4"=="" set "CARGO_BUILD_JOBS=%~4"
call cargo build --profile "%~5" -j %CARGO_BUILD_JOBS% > "%~2" 2>&1
call echo %ERRORLEVEL%> "%~3"
exit /b 0
