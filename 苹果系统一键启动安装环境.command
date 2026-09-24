#!/bin/bash
# ==========================================================
#  FB 广告账户管理系统 · 一键安装环境（macOS）
#  ▸ 双击若弹出“Apple无法验证…”：这是 macOS 对下载文件的一刀切拦截，不是病毒。
#    放行方法（二选一）：
#    ① 终端执行：xattr -cr ~/Downloads/FB广告账户管理系统   （或把文件夹拖进终端）
#    ② 系统设置 → 隐私与安全性 → 安全性区域 → 【仍要打开】
#  详细图文见「!先看这里-安装说明.txt」
#  只做三件事：装 Python3 → 装 Excel 组件 → 启动系统
# ==========================================================
cd "$(dirname "$0")" || exit 1

echo "=========================================================="
echo "   FB 广告账户管理系统 · 一键安装（macOS）"
echo "=========================================================="

PY=""
have_py() {
  command -v "$1" >/dev/null 2>&1 || return 1
  "$1" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 8) else 1)' >/dev/null 2>&1
}

echo "[检测] 正在检查是否已安装 Python3 ..."
for c in python3 python; do
  if have_py "$c"; then PY="$c"; break; fi
done

if [ -z "$PY" ]; then
  echo ""
  echo "[1/3] 未检测到 Python3，开始自动安装 ..."
  if command -v brew >/dev/null 2>&1; then
    echo "  · 检测到 Homebrew，安装 python@3.12 ..."
    brew install python@3.12 || true
  else
    echo "  · 触发苹果官方「命令行开发者工具」安装（内含 python3）："
    echo "    弹出的窗口点【安装】即可，完成后请【重新双击本文件】。"
    xcode-select --install 2>/dev/null
    osascript -e 'display dialog "Python3 安装窗口已弹出（命令行开发者工具）。\n\n安装完成后，请重新双击「安装环境.command」。若未弹出窗口，请在终端执行：xcode-select --install" buttons {"好"} with icon note' >/dev/null 2>&1
    exit 1
  fi
  for c in python3 python /usr/local/bin/python3 /opt/homebrew/bin/python3; do
    if have_py "$c"; then PY="$c"; break; fi
  done
fi

if [ -z "$PY" ]; then
  echo ""
  echo "[!] 未能自动安装 Python3。请手动安装："
  echo "    已为你打开官网下载页 → 下载 macOS 安装包 → 一路下一步"
  open "https://www.python.org/downloads/macos/" 2>/dev/null
  exit 1
fi

echo ""
echo "[2/3] 安装 Excel 导入组件 openpyxl（可选，失败不影响其他功能）..."
"$PY" -m pip install --quiet --disable-pip-version-check --user openpyxl 2>/dev/null \
  || "$PY" -m pip install --quiet --disable-pip-version-check --user -i https://pypi.tuna.tsinghua.edu.cn/simple openpyxl 2>/dev/null \
  || echo "  [!] openpyxl 未装上：仅「Excel 批量导入」不可用，其余功能全部正常。"

echo ""
echo "[3/3] 启动 FB 广告账户管理系统 ..."
echo "    浏览器将自动打开 http://localhost:8765"
echo "    默认账号 admin / admin123456（请登录后立即修改密码）"
echo ""
open "http://localhost:8765" 2>/dev/null
"$PY" server.py 8765
echo ""
echo "系统已退出。再次使用时双击「start.command」即可（无需再装环境）。"
