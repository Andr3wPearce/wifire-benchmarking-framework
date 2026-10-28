@echo off
rem TEMP(local-dev): Windows launcher
title Benchmarking framework (Dagster)
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\start_dagster.ps1"
pause
