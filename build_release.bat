@echo off
setlocal EnableExtensions
call "%~dp0build_portable_v5.bat" %*
exit /b %errorlevel%
