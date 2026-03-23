@echo off
cd /d "%~dp0"
start "IMS Radar Server" powershell -NoExit -ExecutionPolicy Bypass -File ".\server.ps1"
