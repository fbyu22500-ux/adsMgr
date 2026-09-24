#!/bin/bash
# FB 广告账户管理系统 · 启动（macOS）
# 若提示“无法打开”：右键 → 打开 → 再点“打开”
# 没有 Python3 时会自动进入「安装环境.command」完成安装并拉起系统
cd "$(dirname "$0")" || exit 1

PY=""
for c in python3 python; do
  if command -v "$c" >/dev/null 2>&1 && "$c" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 8) else 1)' 2>/dev/null; then
    PY="$c"; break
  fi
done

if [ -z "$PY" ]; then
  echo "未检测到 Python3，正在进入一键安装 ..."
  bash "$(dirname "$0")/安装环境.command"
  exit 0
fi

PORT="${1:-8765}"
echo "正在启动 FB 广告账户管理系统（端口 $PORT）..."
echo "浏览器将自动打开 http://localhost:$PORT  默认账号 admin / admin123456（登录后请立即改密）"
open "http://localhost:$PORT" 2>/dev/null
"$PY" server.py "$PORT"
