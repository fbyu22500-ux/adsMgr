@echo off
chcp 65001 >nul
title FB 广告账户管理系统 · 一键安装环境（Windows）
cd /d "%~dp0"

echo ==========================================================
echo    FB 广告账户管理系统 · 一键安装（Windows）
echo    本脚本只做三件事：装 Python → 装 Excel 组件 → 启动系统
echo ==========================================================
echo.

set "PY="

echo [检测] 正在检查是否已安装 Python ...
where python >nul 2>nul && set "PY=python"
if not defined PY where py >nul 2>nul && set "PY=py -3"
if defined PY goto HAVE_PY

echo.
echo [1/3] 未检测到 Python，开始自动安装（约 1~3 分钟，请勿关闭本窗口）...
where winget >nul 2>nul
if %errorlevel%==0 (
    echo   · 检测到 winget，使用微软官方源安装 Python 3.12（仅当前用户，自动加入 PATH）...
    winget install -e --id Python.Python.3.12 --scope user --silent --accept-package-agreements --accept-source-agreements
) else (
    echo   · 未检测到 winget，改用 Python 官网安装包 ...
    powershell -NoProfile -Command "[Net.ServicePointManager]::SecurityProtocol=[Net.SecurityProtocolType]::Tls12; Invoke-WebRequest -Uri 'https://www.python.org/ftp/python/3.12.8/python-3.12.8-amd64.exe' -OutFile '%TEMP%\python-installer.exe'" >nul 2>nul
    if not exist "%TEMP%\python-installer.exe" (
        echo   [!] 自动下载失败（可能网络受限）。请手动安装：
        echo       1. 已为你打开 Python 官网下载页
        echo       2. 下载并安装，安装第一屏务必勾选 [Add python.exe to PATH]
        echo       3. 装完后重新双击本文件即可
        start "" "https://www.python.org/downloads/"
        pause
        exit /b 1
    )
    echo   · 静默安装中（已自动勾选 Add to PATH）...
    "%TEMP%\python-installer.exe" /quiet InstallAllUsers=0 PrependPath=1 Include_test=0
    del /q "%TEMP%\python-installer.exe" >nul 2>nul
)

rem —— 安装后当前窗口的 PATH 不会自动刷新，主动探测常见安装位置 ——
set "PYD=%LOCALAPPDATA%\Programs\Python\Python312"
if exist "%PYD%\python.exe" set "PY=%PYD%\python.exe" & goto HAVE_PY
set "PYD=%LOCALAPPDATA%\Programs\Python\Python311"
if exist "%PYD%\python.exe" set "PY=%PYD%\python.exe" & goto HAVE_PY
set "PYD=%ProgramFiles%\Python312"
if exist "%PYD%\python.exe" set "PY=%PYD%\python.exe" & goto HAVE_PY
where python >nul 2>nul && set "PY=python"
if not defined PY (
    echo.
    echo [!] 安装完成但仍未找到 Python。最常见原因：安装时没勾选 Add to PATH。
    echo     请关闭本窗口，重新双击「安装环境.bat」再试一次；
    echo     或手动安装时务必勾选 [Add python.exe to PATH]。
    pause
    exit /b 1
)

:HAVE_PY
echo.
echo [2/3] 安装 Excel 导入组件 openpyxl（可选，失败不影响其他功能）...
%PY% -m pip install --quiet --disable-pip-version-check openpyxl 2>nul
if errorlevel 1 (
    echo   · 默认源安装失败，改用清华镜像重试 ...
    %PY% -m pip install --quiet --disable-pip-version-check -i https://pypi.tuna.tsinghua.edu.cn/simple openpyxl 2>nul
    if errorlevel 1 echo   [!] openpyxl 未装上：仅「Excel 批量导入」不可用，其余功能全部正常。
)

echo.
echo [3/3] 启动 FB 广告账户管理系统 ...
echo     浏览器将自动打开 http://localhost:8765
echo     默认账号 admin / admin123456（请登录后立即修改密码）
echo.
start "" "http://localhost:8765"
%PY% server.py 8765
echo.
echo 系统已退出。再次使用时双击「启动.bat」即可（无需再装环境）。
pause
