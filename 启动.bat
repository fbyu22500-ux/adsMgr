@echo off
chcp 65001 >nul
title FB 广告账户管理系统
cd /d "%~dp0"

rem —— 启动前自检：没有 Python 就自动调用「安装环境.bat」（装完它会直接把系统拉起来）——
set "PY="
where python >nul 2>nul && set "PY=python"
if not defined PY where py >nul 2>nul && set "PY=py -3"
if not defined PY (
  echo 未检测到 Python，正在进入一键安装 ...
  call "%~dp0安装环境.bat"
  exit /b
)

set PORT=%~1
if "%PORT%"=="" set PORT=8765
echo 正在启动 FB 广告账户管理系统（端口 %PORT%）...
echo 浏览器将自动打开 http://localhost:%PORT%  默认账号 admin / admin123456（登录后请立即改密）
start "" "http://localhost:%PORT%"
%PY% server.py %PORT%
pause
