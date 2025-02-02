@echo off
cd /d %~dp0
PowerShell -NoProfile -ExecutionPolicy Unrestricted .\run_powerautomate.ps1 "RSSAutoRunner"
