@echo off
setlocal EnableExtensions
cd /d "%~1" || exit /b 1
call pnpm run build > "%~2" 2>&1
call echo %ERRORLEVEL%> "%~3"
exit /b 0
