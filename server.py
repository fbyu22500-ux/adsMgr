#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FB 广告账户管理系统 v4 · 单机完整版
零依赖，Python 标准库即可运行
启动: python3 server.py
默认管理员: admin / admin123456
数据存储: data/ 目录
"""

import json
import os
import sys
import time
import re
import unicodedata
import csv
import io
import uuid
import urllib.parse
import urllib.request
import urllib.error
import threading
import gzip
from collections import OrderedDict
import smtplib
import zipfile
import socket
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
import base64
import shutil
import hashlib
import hmac
import secrets
import struct
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from datetime import datetime, timedelta

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data')

# ========== 2026-09-11 新增：部署环境变量（适配 Railway / Render / 宝塔 / 自有服务器等）==========
# API_BASE_PATH：子路径部署前缀，如 "/myapp"，留空=根路径（默认）
API_BASE_PATH = os.environ.get('API_BASE_PATH', '').rstrip('/')
# FORCE_SECURE_COOKIE：HTTPS 部署时设为 "1"，Set-Cookie 会自动加 Secure 标志
FORCE_SECURE_COOKIE = os.environ.get('FORCE_SECURE_COOKIE', '') in ('1', 'true', 'TRUE', 'yes')
# CORS_ORIGIN：允许跨域的来源，多个用逗号分隔；留空=同源（默认）；"*"=全部
CORS_ORIGIN = os.environ.get('CORS_ORIGIN', '').strip()
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(os.path.join(DATA_DIR, 'uploads', 'payment'), exist_ok=True)
os.makedirs(os.path.join(DATA_DIR, 'uploads', 'refund'), exist_ok=True)
IMPORT_ERROR_DIR = os.path.join(DATA_DIR, 'import_errors')
os.makedirs(IMPORT_ERROR_DIR, exist_ok=True)

MAPPING_FILE    = os.path.join(DATA_DIR, 'mapping.json')
RECHARGE_FILE   = os.path.join(DATA_DIR, 'recharge.json')
PAYMENT_FILE    = os.path.join(DATA_DIR, 'payment.json')
REFUND_FILE     = os.path.join(DATA_DIR, 'refund.json')
SERVICE_FEE_FILE = os.path.join(DATA_DIR, 'service_fee.json')
CLIENT_STATUS_FILE = os.path.join(DATA_DIR, 'client_status.json')
CONSUME_FILE    = os.path.join(DATA_DIR, 'consume.json')
IMPORT_LOG_FILE = os.path.join(DATA_DIR, 'import_log.json')
SERVER_LOG_FILE = os.path.join(DATA_DIR, 'server.log')   # 运行时日志（含 print 输出）
USERS_FILE      = os.path.join(DATA_DIR, 'users.json')
SESSIONS_FILE   = os.path.join(DATA_DIR, 'sessions.json')
SESSIONS_LOCK = threading.Lock()
META_TOKENS_FILE   = os.path.join(DATA_DIR, 'meta_tokens.json')
META_API_VERSION = 'v19.0'
META_HTTP_TIMEOUT = 30

# === 2026-09-24 TikTok/Google 新增 ===
TIKTOK_TOKENS_FILE = os.path.join(DATA_DIR, 'tiktok_tokens.json')
TIKTOK_API_BASE = 'https://business-api.tiktok.com/open_api/v1.3'
TIKTOK_HTTP_TIMEOUT = 30
_TIKTOK_SECRET = os.environ.get('TIKTOK_SECRET', 'fb-ads-manager-v4-tiktok-secret-2026')

GOOGLE_TOKENS_FILE = os.path.join(DATA_DIR, 'google_tokens.json')
GOOGLE_API_BASE = 'https://googleads.googleapis.com/v21'
GOOGLE_HTTP_TIMEOUT = 30
_GOOGLE_SECRET = os.environ.get('GOOGLE_SECRET', 'fb-ads-manager-v4-google-secret-2026')

# --- 服务端日志（把 print 同时写入文件，方便前端查看）---
SERVER_LOG_FILE = os.path.join(DATA_DIR, 'server.log')
MAX_LOG_SIZE    = 10 * 1024 * 1024   # 10 MB，超过则轮转

class DualLogger:
    """把 sys.stdout / sys.stderr 同时输出到终端和滚动日志文件。
    注意：flush 每行末尾自动触发，避免 print 在崩溃前没写入文件。"""
    def __init__(self, filepath: str, original):
        self.filepath = filepath
        self._orig    = original
        self._lock    = threading.Lock()

    def write(self, text: str) -> int:
        # 1) 始终先写终端
        n = self._orig.write(text)
        # 2) 追加到日志文件（线程安全）
        with self._lock:
            try:
                # 日志轮转：超过上限时截半
                if os.path.exists(self.filepath) and os.path.getsize(self.filepath) > MAX_LOG_SIZE:
                    with open(self.filepath, 'r', encoding='utf-8', errors='replace') as f:
                        lines = f.readlines()
                    with open(self.filepath, 'w', encoding='utf-8') as f:
                        f.writelines(lines[len(lines)//2:])
                with open(self.filepath, 'a', encoding='utf-8', errors='replace') as f:
                    f.write(text)
            except Exception:
                pass
        return n

    def flush(self) -> None:
        self._orig.flush()

    def isatty(self) -> bool:
        return getattr(self._orig, 'isatty', lambda: False)()

# --- Meta 速率保护（几千账号必须保守，参数全部可通过环境变量覆盖）---
META_MIN_INTERVAL = float(os.environ.get('META_MIN_INTERVAL', '0.55'))    # 全局请求最小间隔（秒）。Meta per-app 默认 ≈ 3-5 QPS，几千账号用0.55+更安全
META_ACC_BATCH = int(os.environ.get('META_ACC_BATCH', '30'))               # 每处理 N 个账号批一次节流（避免单 BM 被限）
META_ACC_BATCH_SLEEP = float(os.environ.get('META_ACC_BATCH_SLEEP', '2.5'))
META_BM_SWITCH_SLEEP = float(os.environ.get('META_BM_SWITCH_SLEEP', '1.0'))
META_MAX_429_RETRY = 3     # 单次请求遇到 429 最多重试几次（指数退避）
META_429_CIRCUIT_BREAK = 5 # 同一 BM Token 连续 N 次 429 后熔断跳过

_META_SECRET = os.environ.get('META_SECRET', 'fb-ads-manager-v4-internal-secret-key-2026')

# --- Meta 自动刷新配置 ---
META_AUTO_CFG_FILE = os.path.join(DATA_DIR, 'meta_auto_sync_cfg.json')
_META_SYNC_LOCK = threading.Lock()      # 互斥锁：一键同步 和 自动刷新 不能同时跑
_META_SYNC_STATE = {                     # 全局同步状态
    'running': False,                    # 是否正在同步中
    'mode': '',                          # 'manual' 或 'auto'
    'startedAt': '',                     # 本次开始时间
    'lastAutoSync': '',                  # 上次自动刷新完成时间
    'lastAutoSyncError': '',             # 上次自动刷新错误信息
}
_META_SYNC_STATE_LOCK = threading.Lock()

# === 2026-09-24 三平台自动刷新 ===
_PLATFORM_AUTO_CFG_FILE = os.path.join(DATA_DIR, 'platform_auto_sync_cfg.json')
_PLATFORM_AUTO_LOCKS = {'meta': threading.Lock(), 'tiktok': threading.Lock(), 'google': threading.Lock()}
_PLATFORM_AUTO_STATE = {
    'meta':    {'running': False, 'startedAt': '', 'lastError': ''},
    'tiktok':  {'running': False, 'startedAt': '', 'lastError': ''},
    'google':  {'running': False, 'startedAt': '', 'lastError': ''},
}
_PLATFORM_AUTO_STATE_LOCK = threading.Lock()


# ============================================================
# 原始凭证（转账截图）附件：打款 / 退款逐笔挂图，服务端落盘 + 鉴权读取
#   · 存储：data/uploads/<kind>/<记录id>/<attId><ext>（只走 /attachments 鉴权路由，不静态直读）
#   · 校验：魔数嗅探（png/jpg/gif/webp）+ 单文件 8MB + 单笔 12 张，不信客户端 MIME
#   · 权限：上传=能改该模块的人；查看=该笔账对其可见；删除=管理员/财务（与凭证删除同权限）
# ============================================================
ATTACH_DIR   = os.path.join(DATA_DIR, 'uploads')
ATTACH_KIND_FILE  = {'payment': PAYMENT_FILE, 'refund': REFUND_FILE}
ATTACH_KIND_LABEL = {'payment': '打款管理', 'refund': '退款管理'}
ATTACH_KIND_PERM  = {'payment': 'can_edit_payment', 'refund': 'can_refund'}
ATTACH_MAX_BYTES   = 8 * 1024 * 1024
ATTACH_MAX_PER_DOC = 12

def _att_sniff(data):
    # 按文件头判断真实类型（拒绝改扩展名的非图片）
    if not data or len(data) < 12: return None
    if data[:8] == b'\x89PNG\r\n\x1a\n': return ('.png', 'image/png')
    if data[:3] == b'\xff\xd8\xff':      return ('.jpg', 'image/jpeg')
    if data[:6] in (b'GIF87a', b'GIF89a'): return ('.gif', 'image/gif')
    if data[:4] == b'RIFF' and data[8:12] == b'WEBP': return ('.webp', 'image/webp')
    return None

def _att_say_part(rec_id):
    return re.sub(r'[^0-9A-Za-z_.-]', '', str(rec_id))[:64] or '_'

def _att_dir(kind, rec_id):
    return os.path.join(ATTACH_DIR, kind, _att_say_part(rec_id))

def _att_find(kind, rec_id):
    for r in load_json(ATTACH_KIND_FILE[kind]):
        if str(r.get('id')) == str(rec_id): return r
    return None

def _att_purge_doc(kind, rec_id):
    # 记录删除 → 凭证文件一并清掉，不留孤儿文件
    try:
        d = _att_dir(kind, rec_id)
        if os.path.isdir(d): shutil.rmtree(d)
    except Exception: pass

def _att_purge_kind(kind):
    # 「清空打款」等批量删除 → 整目录重建
    try:
        d = os.path.join(ATTACH_DIR, kind)
        if os.path.isdir(d): shutil.rmtree(d)
        os.makedirs(d, exist_ok=True)
    except Exception: pass


_lock = threading.Lock()

# ============================================================
# 角色权限
# ============================================================
ROLES = {
    'admin': {
        'label': '管理员',
        'can_see': True, 'can_upload_consume': True,
        'can_edit_mapping': True, 'can_edit_recharge': True,
        'can_edit_payment': True, 'can_refund': True,
        'can_delete_money': True,        # 2026-09-05 删除打款/退款凭证（资金凭证可删，仅管理员+财务）
        'can_export': True, 'can_manage_users': True,
    },
    'company': {
        'label': '公司人员',
        'can_see': True, 'can_upload_consume': False,
        'can_edit_mapping': False, 'can_edit_recharge': False,
        'can_export': True, 'can_manage_users': False,
    },
    'customer_service': {
        'label': '客服',
        'can_see': True, 'can_upload_consume': True,
        'can_edit_mapping': True, 'can_edit_recharge': False,
        'can_export': True, 'can_manage_users': False,
    },
    'finance': {
        'label': '财务',
        'can_see': True, 'can_upload_consume': False,
        'can_edit_mapping': False, 'can_edit_recharge': True,
        'can_edit_payment': True,
        'can_delete_money': True,        # 2026-09-05 财务可删除打款/退款凭证
        'can_export': True, 'can_manage_users': False,
    },
    'client': {
        'label': '客户',
        'can_see': 'own', 'can_upload_consume': False,
        'can_edit_mapping': False, 'can_edit_recharge': False,
        'can_export': False, 'can_manage_users': False,
    },
}
DEFAULT_ROLE   = 'company'
SESSION_MAX_AGE = 86400 * 7


# ============================================================
# 数据读写
# ============================================================
MAPPING_STATUS_VALID = ('正常', '挂户', '清零', '清零不回收')   # 2026-09-07 新增「清零不回收」：账户已清零但余额不回收
MAPPING_STATUS_DEFAULT = '正常'


def normalize_mapping_status(item):
    """兼容老格式 active boolean → 新 status 枚举；非法值全部兜底成 '正常'"""
    if not isinstance(item, dict):
        return item
    if 'status' in item and isinstance(item['status'], str) and item['status'] in MAPPING_STATUS_VALID:
        return item
    # 新格式但没值 → 给默认
    new_status = None
    if 'status' in item and isinstance(item['status'], str):
        s = item['status'].strip()
        if s in MAPPING_STATUS_VALID:
            new_status = s
        else:
            new_status = MAPPING_STATUS_DEFAULT  # 非法文字，修正
    if new_status is None:
        # 走老 active 兼容
        a = item.get('active')
        if a is True or a == 'true' or a == 1:
            new_status = '正常'
        elif a is False or a == 'false' or a == 0:
            new_status = '清零'
        else:
            new_status = MAPPING_STATUS_DEFAULT
    item['status'] = new_status
    return item


def load_json(path):
    # 缓存命中：版本 + 磁盘 stat 一致 → 直接用缓存原文解析（每次新对象，调用方可安全局部修改）
    hit = _RAW_CACHE.get(path)
    if hit is not None and hit[0] == _DATA_VERSION:
        try:
            st = os.stat(path)
            stk = (st.st_mtime_ns, st.st_size)
        except OSError:
            stk = None
        if stk is not None and stk == hit[1]:
            try:
                return json.loads(hit[2])
            except Exception:
                _RAW_CACHE.pop(path, None)
    if not os.path.exists(path):
        return [] if path != SESSIONS_FILE else {}
    try:
        with open(path, 'r', encoding='utf-8') as f:
            raw = f.read()
    except Exception:
        return [] if path != SESSIONS_FILE else {}
    try:
        data = json.loads(raw)
    except Exception as exc:
        # 损坏隔离留证：防静默清零——事故根因
        safe = os.path.basename(path)
        ts = time.strftime('%Y%m%d_%H%M%S')
        quarantine_dir = os.path.join(DATA_DIR, '_quarantine')
        os.makedirs(quarantine_dir, exist_ok=True)
        quarantine_path = os.path.join(quarantine_dir, f'{ts}_{safe}.corrupt')
        try:
            shutil.copy2(path, quarantine_path)
            print(f'[WARN] {safe} JSON损坏已隔离至 {quarantine_path}：{exc}', flush=True)
        except Exception as e2:
            print(f'[WARN] {safe} JSON损坏（隔离失败: {e2}）：{exc}', flush=True)
        return [] if path != SESSIONS_FILE else {}
    try:
        st = os.stat(path)
        _RAW_CACHE[path] = (_DATA_VERSION, (st.st_mtime_ns, st.st_size), raw)
    except OSError:
        pass
    # === 一次性兼容迁移：mapping.json boolean active → status enum ===
    if path == MAPPING_FILE and isinstance(data, list):
        dirty = False
        for it in data:
            if not isinstance(it, dict): continue
            before_status = it.get('status')
            before_active = it.get('active', None)
            normalize_mapping_status(it)
            if it.get('status') != before_status or before_status is None:
                # 有任何一项被改过（或者之前没有 status 字段）→ 需要写回
                dirty = True
            # 老 active 字段：保留下（前端老代码不会再读，留着防回滚）但不再作为主判断
        if dirty:
            try:
                save_json(MAPPING_FILE, data)
            except Exception:
                pass
    return data

# ============================================================
# 性能层（2026-09-04 数据库化改造第一步）：
#   · _RAW_CACHE：文件原文缓存（全局版本号 + 磁盘 stat 双校验）→ 免重复磁盘 IO
#   · _STATS_CACHE：统计结果缓存 → 免每次全量聚合
#   · 写一次即失效（save_json 自动 bump 版本）
# ============================================================
_DATA_VERSION = 0
_RAW_CACHE = {}      # path -> (version, (mtime_ns,size), raw_text)
_STATS_CACHE = OrderedDict()   # (version, uid, from, to) -> stats dict

# 2026-09-12 静态文件内存缓存：index.html / xlsx.full.min.js 等启动时一次性读入 + 预 gzip
# 穿透场景下避免每次请求重读磁盘 + 重压缩，响应提速 5-10x
# 结构: { abs_path: {'raw': bytes, 'gz': bytes|None, 'etag': str, 'mtime': int, 'size': int} }
_STATIC_CACHE = {}
_STATIC_CACHE_LOCK = threading.Lock()

# ============ 2026-09-12 公网穿透安全加固 ============
# per-IP 速率限制：每个 IP 的请求时间戳窗口
# 结构: { ip: [timestamp1, timestamp2, ...] } —— 滑动窗口计数
_RATE_BUCKET = {}
_RATE_BUCKET_LOCK = threading.Lock()

# 速率限制配置（穿透场景下保守值）
RATE_GENERAL_WINDOW  = 60      # 通用窗口：60 秒
RATE_GENERAL_MAX     = 200     # 通用上限：每 IP 每分钟最多 200 次请求
RATE_LOGIN_WINDOW    = 60      # 登录窗口：60 秒
RATE_LOGIN_MAX       = 10      # 登录上限：每 IP 每分钟最多 10 次登录尝试
RATE_UPLOAD_WINDOW   = 60      # 上传窗口：60 秒
RATE_UPLOAD_MAX      = 120     # 上传上限：每 IP 每分钟最多 120 次（导入导出批量操作友好）

# 允许从 X-Forwarded-For 取真实 IP（HP-PRO / Termux 穿透场景）
TRUST_PROXY = True

def _extract_real_ip(handler):
    """从请求中提取真实客户端 IP。
    - TRUST_PROXY=True 时，优先读 X-Forwarded-For 的第一个有效 IP
    - 否则直接用 handler.client_address[0]
    """
    raw = handler.headers.get('X-Forwarded-For', '').strip() if TRUST_PROXY else ''
    if raw:
        # X-Forwarded-For 可能是 "client, proxy1, proxy2" —— 第一个是真实客户端
        first = raw.split(',')[0].strip()
        # 简单格式校验
        if first and len(first) <= 45:   # IPv6 最长 45 字符
            return first
    return handler.client_address[0] if handler.client_address else '-'

def _rate_check(ip, window=60, limit=1):
    """滑动窗口速率检查。返回 (allowed: bool, retry_after: int, current_count: int)。"""
    now = time.time()
    with _RATE_BUCKET_LOCK:
        arr = _RATE_BUCKET.get(ip, [])
        arr = [t for t in arr if now - t < window]
        if len(arr) >= limit:
            retry = int(window - (now - arr[0])) + 1
            _RATE_BUCKET[ip] = arr
            return False, max(retry, 1), len(arr)
        arr.append(now)
        _RATE_BUCKET[ip] = arr
        return True, 0, len(arr)

def _rate_deduct(ip):
    """导入出错时退还本次计数，避免用户因一次失败就被限流锁死。"""
    now = time.time()
    with _RATE_BUCKET_LOCK:
        arr = _RATE_BUCKET.get(ip, [])
        arr = [t for t in arr if now - t < 120]
        if arr:
            arr.pop()   # 去掉最近一次
            _RATE_BUCKET[ip] = arr

def _rate_cleanup():
    """后台清理：每分钟清理过期 IP，防止 _RATE_BUCKET 无限增长。"""
    while True:
        time.sleep(60)
        now = time.time()
        try:
            with _RATE_BUCKET_LOCK:
                for ip in list(_RATE_BUCKET.keys()):
                    _RATE_BUCKET[ip] = [t for t in _RATE_BUCKET[ip] if now - t < 120]
                    if not _RATE_BUCKET[ip]:
                        del _RATE_BUCKET[ip]
        except Exception:
            pass

# ============ 2026-09-12 输入长度限制：防超长字段注入 / 缓存污染 ============
STR_LIMIT_USERNAME  = 64   # 用户名最大长度
STR_LIMIT_ACCOUNTID = 64   # 账号 ID 最大长度
STR_LIMIT_NAME      = 200  # 账户名称 / 用户姓名
STR_LIMIT_NOTE      = 1000 # 备注/说明
STR_LIMIT_BMID      = 64   # BM ID

def _limit_str(s, max_len):
    """截断字符串至 max_len，同时过滤控制字符（防注入）。"""
    if not isinstance(s, str):
        s = str(s or '')
    s = s.strip()
    # 移除控制字符（保留换行/制表符用于备注）
    s = ''.join(c for c in s if c >= ' ' or c in '\t')
    return s[:max_len]

def _preload_static():
    """启动时预加载所有静态文件到内存，同时预 gzip 压缩。"""
    static_files = ['index.html']
    # 扫描 BASE_DIR 下所有 .js/.css/.svg/.ico/.png 静态文件
    try:
        for fname in os.listdir(BASE_DIR):
            if fname.endswith(('.js', '.css', '.svg', '.ico', '.png')):
                static_files.append(fname)
    except Exception:
        pass
    loaded = 0
    total_raw = 0
    total_gz = 0
    for fname in static_files:
        fpath = os.path.join(BASE_DIR, fname)
        try:
            st = os.stat(fpath)
            with open(fpath, 'rb') as f:
                raw = f.read()
            gz = None
            if len(raw) >= 1024:
                gz = gzip.compress(raw, compresslevel=6)
            etag = '"F%d-%d"' % (st.st_mtime_ns & 0xffffffff, len(raw))
            _STATIC_CACHE[fpath] = {
                'raw': raw,
                'gz': gz,
                'etag': etag,
                'mtime': st.st_mtime_ns,
                'size': len(raw),
            }
            loaded += 1
            total_raw += len(raw)
            total_gz += len(gz) if gz else len(raw)
        except Exception as e:
            print(f'[preload] 跳过 {fname}: {e}', flush=True)
    if loaded:
        ratio = (1 - total_gz / total_raw) * 100 if total_raw else 0
        print(f'[preload] ✅ 预加载 {loaded} 个静态文件 '
              f'({total_raw/1024:.0f}KB → {total_gz/1024:.0f}KB gzip, 节省 {ratio:.0f}%)', flush=True)

_WRITE_LOCK = threading.Lock()      # 2026-09-06 落盘串行化：防同文件并发写导致的截断/交叉

def _has_business_data():
    # 2026-09-07 防呆：业务数据全空时不再生成快照/邮件备份，避免"空快照"污染备份列表、误导恢复
    for f in (MAPPING_FILE, CONSUME_FILE, RECHARGE_FILE, PAYMENT_FILE, REFUND_FILE, SERVICE_FEE_FILE):
        try:
            if load_json(f): return True
        except Exception:
            return True
    return False


class _BodyRejected(Exception):
    # 2026-09-06 请求体被拒（已响应 413/400）时安全中断 handler
    pass


def save_json(path, data):
    global _DATA_VERSION
    tmp = '%s.%d.%d.tmp' % (path, os.getpid(), int(time.time() * 1e6) % 1000000)   # 唯一临时名，避免并发互相覆盖
    with _WRITE_LOCK:
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())        # 断电不丢已确认写入
        os.replace(tmp, path)
    _DATA_VERSION += 1          # 数据版本推进 → RAW/STATS 缓存自动失效
    _RAW_CACHE.pop(path, None)  # 本文件旧原文立即作废（同版本内也不会脏读）
    _STATS_CACHE.clear()        # 2026-09-10 根修：任何文件写入 → 统计缓存立刻清空
                                # （仅靠 bump 版本号仍有极小边界：同版本号内并发读写可能命中旧缓存；
                                #   直接 clear 成本极低，杜绝所有"删了充值但账号对账还显示"的边界问题）


# ============================================================
# 操作审计日志 + 系统设置（2026-09-04）
# ============================================================
OPS_LOG_FILE  = os.path.join(DATA_DIR, 'ops_log.json')
SETTINGS_FILE = os.path.join(DATA_DIR, 'settings.json')
BACKUP_DIR    = os.path.join(DATA_DIR, 'backups')
DEFAULT_SETTINGS = {'media_check': True, 'gap_rule': 'prev'}

def load_settings():
    try:
        s = load_json(SETTINGS_FILE)
    except Exception:
        s = {}
    out = dict(DEFAULT_SETTINGS); out.update(s or {})
    return out

def save_settings(s):
    save_json(SETTINGS_FILE, s or {})

DEFAULT_BACKUP = {'enabled': False, 'provider': 'custom', 'emails': '', 'smtp_host': '', 'smtp_port': 465,
                  'smtp_user': '', 'smtp_pass': '', 'smtp_ssl': True,
                  'freq': 'daily', 'send_time': '08:00', 'last_sent': '', 'last_result': ''}

def _check_user_password(u, password):
    """危险操作前复核当前登录账号密码"""
    if not u or not password:
        return False
    for x in load_json(USERS_FILE):
        if x.get('id') == u.get('id'):
            try:
                return x.get('password') == hash_password(password, x['password'].split('$')[0])
            except Exception:
                return False
    return False

def refresh_consume_stats_cache_clear():
    # 清空后立刻让旧统计缓存失效（save_json 已 bump 版本，此处双保险）
    try:
        _STATS_CACHE.clear()
    except Exception:
        pass


def get_backup_cfg():
    s = load_settings()
    b = dict(DEFAULT_BACKUP)
    b.update(s.get('backup') or {})
    return b

BACKUP_README = (
    '恢复步骤：\n'
    '1) 在目标电脑启动本系统（python3 server.py 3000）\n'
    '2) 管理员登录 → 系统设置 → 数据维护 → 恢复备份 → 选择本 zip + 输入当前登录密码 → 恢复\n'
    '   （系统会自动为恢复前的现有数据再打一个快照，可回退）\n'
    '或手工恢复：把 zip 内 JSON 覆盖到目标机 data/ 目录后重启服务\n\n'
    '说明：sessions.json（登录令牌）故意不打包；settings.json 中的 SMTP 授权码已置空，需重新填写'
)

def make_backup_zip(mask_secret=True, with_settings=True):
    """把 data/ 下全部 JSON 打包成时间戳压缩包，保留最近 3 份，返回 zip 路径。
    mask_secret=True 时 settings.json 的 smtp_pass 会被置空（防凭证外流）。"""
    os.makedirs(BACKUP_DIR, exist_ok=True)
    stamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    zpath = os.path.join(BACKUP_DIR, f'数据备份_{stamp}.zip')
    with zipfile.ZipFile(zpath, 'w', zipfile.ZIP_DEFLATED) as z:
        for fn in sorted(os.listdir(DATA_DIR)):
            # 安全：sessions.json 含有效登录令牌，不进备份包（防止邮箱泄露被用于会话劫持）
            if not (fn.endswith('.json') and fn != 'sessions.json'):
                continue
            if fn == 'settings.json' and not with_settings:
                continue
            full = os.path.join(DATA_DIR, fn)
            if fn == 'settings.json' and mask_secret:
                s = dict(load_json(full))
                b = dict(s.get('backup') or {})
                b['smtp_pass'] = ''
                s['backup'] = b
                z.writestr(fn, json.dumps(s, ensure_ascii=False, indent=2))
            else:
                z.write(full, arcname=fn)
        # 2026-09-06：转账凭证必须随备份走，否则换机/恢复后凭证丢失
        n_img = 0
        if os.path.isdir(ATTACH_DIR):
            for root, _dirs, files in os.walk(ATTACH_DIR):
                for fn0 in files:
                    full0 = os.path.join(root, fn0)
                    z.write(full0, arcname=os.path.relpath(full0, DATA_DIR).replace(os.sep, '/'))
                    n_img += 1
        if n_img: print('[backup] 已纳入 %d 个凭证文件' % n_img, flush=True)
        z.writestr('恢复说明.txt', BACKUP_README)
    # 本地只保留最近 3 份（用户要求，避免占用磁盘）
    zips = sorted([f for f in os.listdir(BACKUP_DIR) if f.endswith('.zip')])
    for old in zips[:-3]:
        try: os.remove(os.path.join(BACKUP_DIR, old))
        except Exception: pass
    return zpath

def send_backup_email(zpath, to_emails):
    """用配置的 SMTP 把备份压缩包发到邮箱；返回 (ok, message)"""
    cfg = get_backup_cfg()
    host, port = cfg.get('smtp_host',''), int(cfg.get('smtp_port') or 465)
    user, pwd = cfg.get('smtp_user',''), cfg.get('smtp_pass','')
    if not host or not user or not to_emails:
        return False, 'SMTP 未配置完整（服务器/账号/收件邮箱）'
    try:
        msg = MIMEMultipart()
        msg['Subject'] = f'FB广告系统数据备份 {datetime.now().strftime("%Y-%m-%d %H:%M")}'
        msg['From'] = user
        msg['To'] = to_emails
        msg.attach(MIMEText('系统自动备份：附件为全部业务数据（JSON + 转账凭证图片 uploads/）。\n'
                            '备份包中文名已转为 ASCII 附件名，恢复步骤见包内「恢复说明.txt」。', 'plain', 'utf-8'))
        with open(zpath, 'rb') as f:
            part = MIMEBase('application', 'zip')
            part.set_payload(f.read())
        encoders.encode_base64(part)
        # 2026-09-06 修复：RFC2231 的 (charset, lang, bytes) 三元组传中文/bytes 会抛
        #   "quote() doesn't support 'encoding' for bytes"，导致邮箱备份一直失败 → 改 ASCII 附件名
        part.add_header('Content-Disposition', 'attachment',
                        filename='ads-backup-%s.zip' % (os.path.basename(zpath)[5:-4] or 'latest'))
        msg.attach(part)
        if cfg.get('smtp_ssl', True):
            smtp = smtplib.SMTP_SSL(host, port, timeout=20)
        else:
            smtp = smtplib.SMTP(host, port, timeout=20)
            smtp.starttls()
        try:
            smtp.login(user, pwd)
            smtp.sendmail(user, [x.strip() for x in to_emails.split(',') if x.strip()], msg.as_string())
        finally:
            smtp.quit()
        return True, f'已发送至 {to_emails}'
    except Exception as e:
        return False, f'发送失败：{e}'

def run_backup_cycle(force=False):
    """定时备份调度：到期则打包并发送；返回 (executed, message)"""
    cfg = get_backup_cfg()
    today = datetime.now().strftime('%Y-%m-%d')
    if not force:
        if not cfg.get('enabled'):
            return False, '未启用'
        if cfg.get('last_sent') == today:
            return False, '今日已备份'
        send_time = cfg.get('send_time') or '08:00'
        if datetime.now().strftime('%H:%M') < send_time:
            return False, '未到发送时间'
        if cfg.get('freq') == 'weekly' and datetime.now().weekday() != 0:
            return False, '每周一发送，今天不是周一'
    zpath = make_backup_zip()
    ok, msg = send_backup_email(zpath, cfg.get('emails',''))
    s = load_settings()
    s.setdefault('backup', {}).update({'last_sent': today, 'last_result': (('✅ ' if ok else '❌ ') + msg + ' (' + os.path.basename(zpath) + ')')})
    save_settings(s)
    log_op({'username':'系统'}, '系统', '数据备份', f'{"发送成功" if ok else "发送失败"}：{msg}（{os.path.basename(zpath)}）')
    return True, (('✅ ' if ok else '❌ ') + msg)

# ============================================================
# Meta Marketing API · Token 管理 + 自动拉取消耗
# 多 BM 系统用户 Token 分别配置，后端加密存储，前端只读 mask
# ============================================================

def _meta_xor_cipher(plain_bytes: bytes, secret: str) -> bytes:
    """轻量 XOR 混淆 + HMAC-SHA256 签名（零依赖）。"""
    key_bytes = secret.encode('utf-8')
    out = bytearray(len(plain_bytes))
    for i, b in enumerate(plain_bytes):
        out[i] = b ^ key_bytes[i % len(key_bytes)]
    mac = hmac.new(key_bytes, bytes(out), hashlib.sha256).digest()
    return bytes(out) + mac

def _meta_xor_decipher(blob: bytes, secret: str) -> bytes:
    if len(blob) < 32: return b''
    cipher, mac = blob[:-32], blob[-32:]
    key_bytes = secret.encode('utf-8')
    expected = hmac.new(key_bytes, cipher, hashlib.sha256).digest()
    if not hmac.compare_digest(mac, expected):
        raise ValueError('Meta Token 签名校验失败')
    out = bytearray(len(cipher))
    for i, b in enumerate(cipher):
        out[i] = b ^ key_bytes[i % len(key_bytes)]
    return bytes(out)

def _encrypt_meta_token(plain: str) -> str:
    blob = _meta_xor_cipher(plain.encode('utf-8'), _META_SECRET)
    return base64.urlsafe_b64encode(blob).decode('ascii').rstrip('=')

def _decrypt_meta_token(enc: str) -> str:
    pad = '=' * (4 - len(enc) % 4) if len(enc) % 4 else ''
    blob = base64.urlsafe_b64decode(enc + pad)
    return _meta_xor_decipher(blob, _META_SECRET).decode('utf-8')

def mask_meta_token(token: str) -> str:
    if not token: return ''
    if len(token) <= 16: return token[:4] + '****'
    return token[:8] + '...' + token[-4:]

def load_meta_tokens():
    data = load_json(META_TOKENS_FILE)
    if not isinstance(data, list): return []
    safe = []
    for item in data:
        enc = item.get('token_enc', '')
        plain = _decrypt_meta_token(enc) if enc else ''
        s = {k: item.get(k) for k in ('id','name','note','bm_id','createdAt','lastCheckedAt','status')}
        s['token_mask'] = mask_meta_token(plain)
        safe.append(s)
    return safe

def load_meta_tokens_plain():
    data = load_json(META_TOKENS_FILE)
    if not isinstance(data, list): return []
    out = []
    for item in data:
        enc = item.get('token_enc', '')
        plain = _decrypt_meta_token(enc) if enc else ''
        row = {k: item.get(k) for k in ('id','name','note','bm_id','createdAt','lastCheckedAt','status')}
        row['token'] = plain
        out.append(row)
    return out

def _meta_save_tokens(items):
    persisted = []
    for it in items:
        plain = it.pop('token', '') if 'token' in it else ''
        enc = _encrypt_meta_token(plain) if plain else (it.get('token_enc') or '')
        persisted.append({
            'id': it.get('id'), 'name': (it.get('name') or '').strip(),
            'note': (it.get('note') or '').strip(),
            'bm_id': str(it.get('bm_id') or '').strip(),
            'token_enc': enc, 'createdAt': it.get('createdAt'),
            'lastCheckedAt': it.get('lastCheckedAt'), 'status': it.get('status'),
        })
    save_json(META_TOKENS_FILE, persisted)

# Meta 速率保护运行时状态
_META_LAST_REQUEST = 0.0          # 上次发请求的时间戳（全局单例，保证所有 Token 共用）
_META_429_COUNT = {}              # {bm_token_id: 连续429次数} — 熔断用
_META_429_LOCK = threading.Lock()

def _throttle_wait():
    """阻塞到全局最小间隔已满（Meta 限速桶共享，所有 Token 共用）"""
    global _META_LAST_REQUEST
    now = time.time()
    wait = (_META_LAST_REQUEST + META_MIN_INTERVAL) - now
    if wait > 0:
        time.sleep(wait)
    _META_LAST_REQUEST = time.time()

def _meta_http_get(url, params=None, timeout=META_HTTP_TIMEOUT, bm_token_id=None, _retry=0):
    """Meta Graph API 请求：
    - 自动节流（所有 Token 共用一个最小间隔桶）
    - HTTP 429 / 5xx 指数退避 + 最多 META_MAX_429_RETRY 次重试
    - 同一 BM 连续 META_429_CIRCUIT_BREAK 次 429 后熔断，后续请求直接被跳过（error='CIRCUIT_OPEN'）
    """
    if bm_token_id and _META_429_COUNT.get(bm_token_id, 0) >= META_429_CIRCUIT_BREAK:
        return None, 'CIRCUIT_OPEN: 该 BM Token 连续触发 %d 次限流，已熔断' % META_429_CIRCUIT_BREAK

    _throttle_wait()

    if params:
        qs = urllib.parse.urlencode(params)
        full = url + ('&' if '?' in url else '?') + qs
    else:
        full = url
    try:
        req = urllib.request.Request(full, headers={
            'User-Agent': 'FBAdsManager/3.911 v3.911',
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip',
        })
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            # 修正：请求头声明了 Accept-Encoding: gzip，Meta 会返回压缩体，必须显式解压
            # （此前未解压 → UnicodeDecodeError → 被兜底成 500）
            if (resp.headers.get('Content-Encoding') or '').lower() == 'gzip':
                try:
                    raw = gzip.decompress(raw)
                except Exception:
                    pass
            body = raw.decode('utf-8', 'replace')
            # 重置该 BM 的 429 计数（成功说明限流恢复）
            if bm_token_id:
                _META_429_COUNT[bm_token_id] = 0
            return json.loads(body), None
    except urllib.error.HTTPError as e:
        try:
            raw_err = e.read()
            if (e.headers.get('Content-Encoding') or '').lower() == 'gzip':
                try: raw_err = gzip.decompress(raw_err)
                except Exception: pass
            detail = raw_err.decode('utf-8', 'replace')
        except: detail = ''
        msg = 'HTTP %d: %s' % (e.code, detail[:200])
        # 429 限流 → 指数退避重试
        if e.code == 429 and _retry < META_MAX_429_RETRY:
            if bm_token_id:
                _META_429_COUNT[bm_token_id] = _META_429_COUNT.get(bm_token_id, 0) + 1
            backoff = min(2 ** _retry, 30)  # 1s → 2s → 4s → 8s... 最大 30s
            print(f'[Meta] 429 限流，退避 {backoff}s（第 {_retry+1}/{META_MAX_429_RETRY} 次）→ {url.split("v19.0")[-1][:60]}', flush=True)
            time.sleep(backoff)
            return _meta_http_get(url, params=params, timeout=timeout, bm_token_id=bm_token_id, _retry=_retry + 1)
        # 5xx 也重试一次（Meta 偶发 500）
        if 500 <= e.code < 600 and _retry == 0:
            time.sleep(1)
            return _meta_http_get(url, params=params, timeout=timeout, bm_token_id=bm_token_id, _retry=1)
        return None, msg
    except Exception as e:
        return None, str(e)

def test_meta_token(token: str, bm_id: str = '') -> dict:
    """校验 Meta Token 有效性。

    普通用户 Token → /me 返回 id+name
    系统用户 Token → /me 返回空对象 {}，必须通过 BM 节点回退验证
    """
    if not token or len(token) < 10:
        return {'ok': False, 'error': 'Token 长度不足，格式可疑'}
    url = 'https://graph.facebook.com/%s/me' % META_API_VERSION
    data, err = _meta_http_get(url, params={'access_token': token, 'fields': 'id,name'})
    if err:
        return {'ok': False, 'error': err}
    if isinstance(data, dict) and data.get('id'):
        return {'ok': True, 'name': data.get('name') or data.get('id'), 'id': data.get('id')}
    # /me 返回空对象（系统用户令牌）或无 id —— 回退到 BM 节点测试
    if isinstance(data, dict) and not data.get('id') and not data.get('error'):
        bm_id = str(bm_id or '').strip()
        if bm_id:
            test_url = 'https://graph.facebook.com/%s/%s/owned_ad_accounts' % (META_API_VERSION, bm_id)
            tdata, terr = _meta_http_get(test_url, params={'access_token': token, 'fields': 'id', 'limit': 1})
            if terr:
                return {'ok': False, 'error': 'Token 可连通 /me，但访问 BM(%s) 节点失败: %s' % (bm_id, terr[:150])}
            return {'ok': True, 'name': '(系统用户)', 'id': '', 'system_user': True}
        return {'ok': True, 'name': '(系统用户)', 'id': '', 'system_user': True, 'note': '/me 返回空对象，疑似系统用户令牌'}
    err_obj = (data or {}).get('error') or {}
    return {'ok': False, 'error': str(err_obj.get('message') or '未知错误')}

def discover_bm_ids(token: str) -> list:
    """尝试探测该 Token 能访问的 BM ID 列表（用于自动填充 bm_id）。

    注意：系统用户 Token 通常无法通过 /me/businesses 列出 BM（返回空），
    此时必须由用户手动填写 BM ID。本函数只是"能自动就自动"的便利手段。
    """
    ids = []
    for url in ('https://graph.facebook.com/%s/me/businesses' % META_API_VERSION,):
        data, err = _meta_http_get(url, params={'access_token': token, 'fields': 'id,name', 'limit': 100})
        if err or not isinstance(data, dict):
            continue
        for b in data.get('data', []) or []:
            bid = str(b.get('id') or '').strip()
            if bid and bid not in ids:
                ids.append(bid)
    return ids

def fetch_bm_ad_accounts(token: str, limit=500) -> list:
    all_accs = []
    url = 'https://graph.facebook.com/%s/me/adaccounts' % META_API_VERSION
    params = {'access_token': token, 'fields': 'id,name,account_id', 'limit': limit}
    while url:
        data, err = _meta_http_get(url, params=params if params else None)
        if err:
            print('[Meta] fetch_bm_ad_accounts fail: %s' % err, flush=True); break
        all_accs.extend(data.get('data', []))
        paging = data.get('paging', {})
        url = paging.get('next'); params = None
    return all_accs

def fetch_account_insights(token: str, acc_id: str, since: str, until: str) -> list:
    url = 'https://graph.facebook.com/%s/%s/insights' % (META_API_VERSION, acc_id)
    params = {
        'access_token': token, 'fields': 'date_start,spend',
        'time_increment': 1,
        'time_range': json.dumps({'since': since, 'until': until}),
        'limit': 500,
    }
    data, err = _meta_http_get(url, params=params)
    if err:
        print('[Meta] insights %s fail: %s' % (acc_id, err), flush=True); return []
    return [{'date': item.get('date_start',''), 'spend': float(item.get('spend') or 0)} for item in data.get('data', [])]

def _fetch_accounts_by_edge(token: str, edge_url: str, bm_token_id: str) -> list:
    """按给定的 Meta 边缘 URL（如 /{bm_id}/owned_ad_accounts）分页拉取广告账号（带节流）"""
    all_accs = []
    url = edge_url
    params = {'access_token': token, 'fields': 'id,name,account_id', 'limit': 500}
    while url:
        data, err = _meta_http_get(url, params=(params if params else None), bm_token_id=bm_token_id)
        if err:
            if err.startswith('CIRCUIT_OPEN'):
                print('[Meta] BM Token %s 已熔断，跳过剩余请求' % (str(bm_token_id)[:14] or '(?)'), flush=True)
                break
            print('[Meta] 拉取账号列表失败(%s): %s' % (edge_url, err), flush=True)
            break
        all_accs.extend(data.get('data', []))
        paging = data.get('paging', {})
        url = paging.get('next'); params = None
        if url: time.sleep(0.4)   # 分页间也要节流（Meta 每页一次调用）
    return all_accs

def fetch_bm_ad_accounts_safe(token: str, bm_token_id: str, bm_id: str = '') -> list:
    """拉取某个 BM 下的全部广告账号（带节流和分页）。

    关键：系统用户 Token 的 /me/adaccounts 恒为空，必须通过 BM 节点拉取：
      - 有 bm_id  → 依次合并 /{bm_id}/owned_ad_accounts（自有）+ /{bm_id}/client_ad_accounts（客户）
      - 无 bm_id  → 回退 /me/adaccounts（普通用户 Token 场景）
    获取 BM ID：Meta BM 后台 URL 里 business_id= 后面的数字，或调 /me/businesses。
    """
    if bm_id:
        bm_id = str(bm_id).strip()
        base = 'https://graph.facebook.com/%s/%s' % (META_API_VERSION, bm_id)
        accs = _fetch_accounts_by_edge(token, base + '/owned_ad_accounts', bm_token_id)
        accs += _fetch_accounts_by_edge(token, base + '/client_ad_accounts', bm_token_id)
        # 按账号 ID 去重（同一账号可能同时出现在 owned 与 client 边缘）
        seen = set(); uniq = []
        for a in accs:
            aid = a.get('id') or ''
            if aid and aid not in seen:
                seen.add(aid); uniq.append(a)
        return uniq
    return _fetch_accounts_by_edge(token, 'https://graph.facebook.com/%s/me/adaccounts' % META_API_VERSION, bm_token_id)

def fetch_account_insights_safe(token: str, bm_token_id: str, acc_id: str, since: str, until: str) -> list:
    url = 'https://graph.facebook.com/%s/%s/insights' % (META_API_VERSION, acc_id)
    params = {
        'access_token': token, 'fields': 'date_start,spend',
        'time_increment': 1,
        'time_range': json.dumps({'since': since, 'until': until}),
        'limit': 500,
    }
    data, err = _meta_http_get(url, params=params, bm_token_id=bm_token_id)
    if err:
        if err.startswith('CIRCUIT_OPEN'):
            return None, err   # None 表示熔断（不是单纯无数据）
        return [], err
    return [{'date': item.get('date_start',''), 'spend': float(item.get('spend') or 0)} for item in data.get('data', [])], None

def meta_persist_consume(rows: list) -> dict:
    """把 Meta Marketing API 同步到的消耗行，按「下户表」规则过滤后写入 consume.json。

    严格复用 BM Excel 导入 (_handle_consume_import) 的同一套规则：
      1. 只有在下户表 mapping.json 里登记过的账号才允许入库（按账号ID匹配，账号名称兜底）
      2. 按消耗日期命中下户归属区间，回填 client / channel / serviceRate
      3. 媒体一致性校验（settings.media_check）：Meta 恒为 Facebook，与下户段媒体不一致则拒绝
      4. 同 (日期, 账号ID) 已存在 → 更新（含回流消耗差分逻辑）；否则新增
    调用前必须已持有 _lock（本函数会读写 data/*.json）。
    返回：{added, updated, rejected, rejected_ids, totalConsume}
    """
    mapping_list = load_json(MAPPING_FILE)
    mapping_index = build_mapping_index(mapping_list)
    name_to_aid = {}
    for mm in mapping_list:
        nm = str(mm.get('name') or '').strip()
        aid = normalize_account_id(mm.get('accountId') or '')
        if nm and aid and nm not in name_to_aid:
            name_to_aid[nm] = aid

    consume = load_json(CONSUME_FILE)
    existing_keys = {(r.get('date', ''), r.get('accountId', ''), r.get('source', '')) for r in consume}
    media_check = load_settings().get('media_check', True)
    added = updated = rejected = 0
    rejected_ids = set()

    for r in rows:
        aid = normalize_account_id(r.get('accountId') or '')
        d = to_short_date(r.get('date') or '')
        if not aid or not d:
            rejected += 1
            if aid: rejected_ids.add(aid)
            continue
        raw = {
            '账号ID': aid,
            '账号名称': r.get('accountName') or '',
            '报告开始日期': d,
            '消耗金额': float(r.get('spend') or 0),
            '媒体': 'Facebook',
        }
        row = enrich_consume_row(raw, mapping_index, name_to_aid)
        if not row:
            rejected += 1
            rejected_ids.add(aid)
            continue
        m2 = pick_mapping_segment(mapping_index, row['accountId'], row['date'], platform_filter=platform)
        if m2:
            row['client'] = m2.get('client', '')
            row['channel'] = m2.get('channel', '')
            row['serviceRate'] = m2.get('rate', 0) or 0
            row['matched'] = True
            row['_segStart'] = m2.get('date', '') or ''
            row['_segEnd'] = m2.get('endDate', '') or ''
            row['segOut'] = seg_out_reason(str(row.get('date') or ''), m2)
            row['overdue'] = bool(row['segOut'])
            m_platform = normalize_platform(m2.get('platform') or m2.get('media') or '')
            if media_check and m_platform and row.get('media_from_file') and row['media_from_file'] != m_platform:
                rejected += 1
                rejected_ids.add(row['accountId'])
                continue
            row['platform'] = row.get('media_from_file') or m_platform or 'Facebook'
        # === 2026-09-24 TikTok/Google 新增：统一 source 标记 ===
        row['source'] = 'meta'
        row['importedAt'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        key = (row['date'], row['accountId'], source or row.get('source', ''))
        if key in existing_keys:
            for i, old_row in enumerate(consume):
                if (old_row.get('date'), old_row.get('accountId'), old_row.get('source', '')) == key:
                    old_included = old_row.get('spend_including_reflow', old_row.get('reflowSpend', old_row.get('spend', 0))) or 0
                    new_included = row.get('spend_including_reflow', row.get('spend', 0)) or 0
                    row['spend'] = float(new_included)
                    row['spend_including_reflow'] = float(new_included)
                    row['spend_usd_total'] = float(old_included)
                    row['reflow_difference'] = float(new_included) - float(old_included)
                    consume[i] = row
                    break
            updated += 1
        else:
            included = row.get('spend_including_reflow', row.get('spend', 0)) or 0
            row['spend'] = float(included)
            row['spend_including_reflow'] = float(included)
            row['spend_usd_total'] = float(included)
            row['reflow_difference'] = 0
            consume.append(row)
            existing_keys.add(key)
            added += 1

    consume.sort(key=lambda r: r.get('date', ''))
    save_json(CONSUME_FILE, consume)
    return {'added': added, 'updated': updated, 'rejected': rejected,
            'rejected_ids': sorted(rejected_ids)[:20], 'totalConsume': len(consume)}

def meta_sync_consume(bm_tokens: list, since: str, until: str) -> dict:
    """多 BM Token 循环同步消耗（保守节流策略适配几千账号规模）：
    - 全局请求最小间隔 META_MIN_INTERVAL（所有 Token 共用）
    - 每处理 META_ACC_BATCH 个账号后暂停 META_ACC_BATCH_SLEEP 秒
    - 切换 BM Token 额外暂停 META_BM_SWITCH_SLEEP 秒
    - 单 BM 连续 META_429_CIRCUIT_BREAK 次 429 后熔断跳过
    """
    result = {}
    total_accounts = 0
    for bm_idx, entry in enumerate(bm_tokens):
        tk_id = entry.get('id')
        bm_name = entry.get('name') or '(未命名 BM)'
        plain = entry.get('token') or ''
        one = {'bm_name': bm_name, 'accounts': 0, 'rows': 0, 'errors': []}
        # BM 切换间隔（让不同 App 的限速桶错开）
        if bm_idx > 0:
            time.sleep(META_BM_SWITCH_SLEEP)

        # 0) 先拿 BM ID（校验 Token 和拉账号都需要）
        bm_id = str(entry.get('bm_id') or '').strip()

        # 0.1) 校验 Token（带 bm_id 回退，系统用户 Token 的 /me 会返回空对象）
        v = test_meta_token(plain, bm_id)
        if not v.get('ok'):
            one['errors'].append('Token 失效: ' + str(v.get('error'))); result[tk_id] = one; continue
        one['meta_user'] = v.get('name')

        # 1) 拉 BM 下所有广告账号（系统用户 Token 必须带 bm_id，否则 /me/adaccounts 恒为空）
        if not bm_id:
            # 未配置 BM ID 时尝试自动探测（仅当该 Token 恰好能列出 BM 时才有效）
            ids = discover_bm_ids(plain)
            if len(ids) == 1:
                bm_id = ids[0]
                print('[Meta] %s：自动探测到 BM ID = %s' % (bm_name, bm_id), flush=True)
        if not bm_id:
            one['errors'].append('未配置 BM ID：系统用户 Token 必须填写 BM ID 才能拉账号（Meta BM 后台 URL 的 business_id= 后面的数字）')
        accs = fetch_bm_ad_accounts_safe(plain, tk_id, bm_id)
        one['bmId'] = bm_id
        one['accounts'] = len(accs)
        total_accounts += len(accs)
        if not accs:
            if len(bm_tokens) > 1:
                time.sleep(META_BM_SWITCH_SLEEP)
            one['dateFrom'] = since; one['dateTo'] = until
            result[tk_id] = one
            continue

        # 2) 逐个账号拉 insights（含批间节流）
        all_rows = []
        circuit_broken = False
        for i, acc in enumerate(accs):
            if circuit_broken: break
            aid = acc.get('id') or ''
            if not aid: continue
            rows, err = fetch_account_insights_safe(plain, tk_id, aid, since, until)
            if err and err.startswith('CIRCUIT_OPEN'):
                circuit_broken = True
                one['errors'].append('Token 被 Meta 限流熔断（连续 %d 次 429），本次 BM 后续 %d 个账号已跳过' % (
                    META_429_CIRCUIT_BREAK, len(accs) - i))
                break
            if err:
                # 单个账号失败不影响整体，计入 errors 列表
                one['errors'].append('账号 %s 拉取失败: %s' % (aid, err[:100]))
                if len(one['errors']) > 20:   # 错误太多就停，避免继续被限
                    one['errors'].append('…（错误过多已停止后续 %d 个账号）' % (len(accs) - i))
                    break
                continue
            for r in (rows or []):
                r['accountId'] = acc.get('account_id') or aid.replace('act_','')
                r['accountName'] = acc.get('name',''); r['bmName'] = bm_name
                all_rows.append(r)
            # 批间节流（每 N 个账号暂停）
            if (i + 1) % META_ACC_BATCH == 0:
                print('[Meta] %s：已处理 %d/%d 账号，暂停 %.1fs' % (bm_name, i+1, len(accs), META_ACC_BATCH_SLEEP), flush=True)
                time.sleep(META_ACC_BATCH_SLEEP)

        one['rows'] = len(all_rows)
        one['dateFrom'] = since; one['dateTo'] = until
        # 3) 落库：严格按「下户表」规则过滤后才写入 consume.json
        #    注意：网络请求阶段不持锁，避免几千账号同步（约30分钟）阻塞其它接口
        try:
            with _lock:
                persist = meta_persist_consume(all_rows)
            one['added'] = persist['added']
            one['updated'] = persist['updated']
            one['rejected'] = persist['rejected']
            one['rejected_ids'] = persist['rejected_ids']
            print('[Meta] %s：落库 新增 %d / 更新 %d / 未下户拒绝 %d' % (
                bm_name, persist['added'], persist['updated'], persist['rejected']), flush=True)
        except Exception as pe:
            one['errors'].append('落库失败: ' + str(pe)[:150])
            print('[Meta] %s 落库异常：%s' % (bm_name, str(pe)[:200]), flush=True)
        result[tk_id] = one

    # 汇总提示：告诉前端大概下次什么时候跑完
    est_secs = total_accounts * (META_MIN_INTERVAL + 0.05) + (len(bm_tokens) - 1) * META_BM_SWITCH_SLEEP + (total_accounts / META_ACC_BATCH) * META_ACC_BATCH_SLEEP
    result['_meta_sync_meta'] = {
        'totalAccounts': total_accounts,
        'tokenCount': len(bm_tokens),
        'estSeconds': round(est_secs),
        'minInterval': META_MIN_INTERVAL,
        'batchSize': META_ACC_BATCH,
    }
    return result


# ============================================================
# === 2026-09-24 TikTok for Business Marketing API 新增 ===
# ============================================================

def _tiktok_xor_cipher(plain_bytes: bytes, secret: str) -> bytes:
    """轻量 XOR 混淆 + HMAC-SHA256 签名（零依赖，复用 Meta 加密框架但密钥独立）"""
    key_bytes = secret.encode('utf-8')
    out = bytearray(len(plain_bytes))
    for i, b in enumerate(plain_bytes):
        out[i] = b ^ key_bytes[i % len(key_bytes)]
    mac = hmac.new(key_bytes, bytes(out), hashlib.sha256).digest()
    return bytes(out) + mac

def _tiktok_xor_decipher(blob: bytes, secret: str) -> bytes:
    if len(blob) < 32: return b''
    cipher, mac = blob[:-32], blob[-32:]
    key_bytes = secret.encode('utf-8')
    expected = hmac.new(key_bytes, cipher, hashlib.sha256).digest()
    if not hmac.compare_digest(mac, expected):
        raise ValueError('TikTok Token 签名校验失败')
    out = bytearray(len(cipher))
    for i, b in enumerate(cipher):
        out[i] = b ^ key_bytes[i % len(key_bytes)]
    return bytes(out)

def _encrypt_tiktok(plain: str) -> str:
    blob = _tiktok_xor_cipher(plain.encode('utf-8'), _TIKTOK_SECRET)
    return base64.urlsafe_b64encode(blob).decode('ascii').rstrip('=')

def _decrypt_tiktok(enc: str) -> str:
    if not enc: return ''
    pad = '=' * (4 - len(enc) % 4) if len(enc) % 4 else ''
    blob = base64.urlsafe_b64decode(enc + pad)
    return _tiktok_xor_decipher(blob, _TIKTOK_SECRET).decode('utf-8')

def mask_tiktok_token(token: str) -> str:
    if not token: return ''
    if len(token) <= 16: return token[:4] + '****'
    return token[:8] + '...' + token[-4:]

def load_tiktok_tokens():
    data = load_json(TIKTOK_TOKENS_FILE)
    if not isinstance(data, list): return []
    safe = []
    for item in data:
        plain_at = _decrypt_tiktok(item.get('access_token_enc', ''))
        plain_cs = _decrypt_tiktok(item.get('client_secret_enc', ''))
        plain_rs = _decrypt_tiktok(item.get('refresh_token_enc', ''))
        s = {k: item.get(k) for k in ('id','name','note','business_center_id',
                                       'createdAt','lastCheckedAt','status')}
        s['client_id_mask'] = mask_tiktok_token(_decrypt_tiktok(item.get('client_id_enc','')))
        s['access_token_mask'] = mask_tiktok_token(plain_at)
        s['has_client_secret'] = bool(plain_cs)
        s['has_refresh_token'] = bool(plain_rs)
        safe.append(s)
    return safe

def load_tiktok_tokens_plain():
    data = load_json(TIKTOK_TOKENS_FILE)
    if not isinstance(data, list): return []
    out = []
    for item in data:
        row = {k: item.get(k) for k in ('id','name','note','business_center_id',
                                         'createdAt','lastCheckedAt','status')}
        row['client_id'] = _decrypt_tiktok(item.get('client_id_enc', ''))
        row['client_secret'] = _decrypt_tiktok(item.get('client_secret_enc', ''))
        row['access_token'] = _decrypt_tiktok(item.get('access_token_enc', ''))
        row['refresh_token'] = _decrypt_tiktok(item.get('refresh_token_enc', ''))
        out.append(row)
    return out

def _tiktok_save_tokens(items):
    persisted = []
    for it in items:
        def _pick(plain_key, enc_key):
            plain = it.pop(plain_key, '') if plain_key in it else ''
            return _encrypt_tiktok(plain) if plain else (it.get(enc_key) or '')
        persisted.append({
            'id': it.get('id'), 'name': (it.get('name') or '').strip(),
            'note': (it.get('note') or '').strip(),
            'business_center_id': str(it.get('business_center_id') or '').strip(),
            'client_id_enc': _pick('client_id', 'client_id_enc'),
            'client_secret_enc': _pick('client_secret', 'client_secret_enc'),
            'access_token_enc': _pick('access_token', 'access_token_enc'),
            'refresh_token_enc': _pick('refresh_token', 'refresh_token_enc'),
            'createdAt': it.get('createdAt'),
            'lastCheckedAt': it.get('lastCheckedAt'),
            'status': it.get('status'),
        })
    save_json(TIKTOK_TOKENS_FILE, persisted)

def _tiktok_http_get(path, params=None, token=''):
    url = TIKTOK_API_BASE + path
    if params:
        params = dict(params)
        params.setdefault('access_token', token)
        qs = urllib.parse.urlencode(params)
        full = url + ('&' if '?' in url else '?') + qs
    else:
        full = url + ('?access_token=' + urllib.parse.quote(token) if token else '')
    try:
        req = urllib.request.Request(full, headers={
            'User-Agent': 'FBAdsManager/3.915',
            'Accept': 'application/json',
        })
        with urllib.request.urlopen(req, timeout=TIKTOK_HTTP_TIMEOUT) as resp:
            body = resp.read().decode('utf-8', 'replace')
            return json.loads(body), None
    except urllib.error.HTTPError as e:
        try:
            detail = e.read().decode('utf-8', 'replace')
        except: detail = ''
        return None, 'HTTP %d: %s' % (e.code, detail[:300])
    except Exception as e:
        return None, str(e)

def _tiktok_http_post(path, body_dict, token=''):
    url = TIKTOK_API_BASE + path + ('?access_token=' + urllib.parse.quote(token) if token else '')
    try:
        data_bytes = json.dumps(body_dict).encode('utf-8')
        req = urllib.request.Request(url, data=data_bytes, headers={
            'User-Agent': 'FBAdsManager/3.915',
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        })
        with urllib.request.urlopen(req, timeout=TIKTOK_HTTP_TIMEOUT) as resp:
            body = resp.read().decode('utf-8', 'replace')
            return json.loads(body), None
    except urllib.error.HTTPError as e:
        try:
            detail = e.read().decode('utf-8', 'replace')
        except: detail = ''
        return None, 'HTTP %d: %s' % (e.code, detail[:300])
    except Exception as e:
        return None, str(e)

def test_tiktok_token(token_plain: str, bc_id: str) -> dict:
    """校验 TikTok access_token 有效性：GET /business_center/?advertiser_id={bc_id}"""
    if not token_plain or len(token_plain) < 10:
        return {'ok': False, 'error': 'Token 长度不足'}
    bc_id = str(bc_id or '').strip()
    if not bc_id:
        # 没填 BC ID 时尝试一次 business_center 列表
        data, err = _tiktok_http_get('/business_center/', token=token_plain)
    else:
        data, err = _tiktok_http_get('/business_center/', params={'advertiser_id': bc_id, 'advertiser_id_type': 7}, token=token_plain)
    if err:
        return {'ok': False, 'error': err}
    # TikTok 返回 {"code":0,"message":"OK","data":{...}}
    if isinstance(data, dict) and data.get('code') == 0:
        info = (data.get('data') or {}).get('business_center_info', {})
        name = info.get('name') or info.get('bc_name') or bc_id or '(Business Center)'
        return {'ok': True, 'name': name, 'id': bc_id}
    return {'ok': False, 'error': str((data or {}).get('message') or '未知错误')}

def _tiktok_fetch_advertisers(token_plain: str, bc_id: str) -> list:
    """拉 TikTok BC 下所有 advertiser：GET /business/{bc_id}/advertiser/"""
    all_accs = []
    page = 1
    while True:
        data, err = _tiktok_http_get('/business/%s/advertiser/' % bc_id,
                                     params={'advertiser_id_type': 7, 'page': page, 'page_size': 1000},
                                     token=token_plain)
        if err:
            print('[TikTok] 拉 advertiser 列表失败(第%d页): %s' % (page, err), flush=True)
            break
        lst = ((data or {}).get('data') or {}).get('list') or []
        if not lst: break
        all_accs.extend(lst)
        page += 1
        if page > 50: break   # 安全上限
    return all_accs

def _tiktok_fetch_report(token_plain: str, advertiser_ids: list, since: str, until: str) -> list:
    """POST /report/integrated/get/ 拉取消耗数据，支持分页 + advertiser_ids 分批。
    TikTok API 单次 advertiser_ids 建议 ≤ 500 个，超过会被拒绝。
    """
    rows = []
    all_ids = [str(x) for x in advertiser_ids]
    BATCH = 500   # 每批最多 500 个，TikTok API 限制
    batches = [all_ids[i:i+BATCH] for i in range(0, len(all_ids), BATCH)]
    if len(batches) > 1:
        print('[TikTok] advertiser_ids=%d 分 %d 批拉 report' % (len(all_ids), len(batches)), flush=True)
    for bi, batch_ids in enumerate(batches, 1):
        page = 1
        while True:
            body = {
                'advertiser_ids': batch_ids,
                'start_date': since, 'end_date': until,
                'granularity': 'DAY',
                'metrics': ['spend','impressions','clicks','conversion','cost_per_conversion'],
                'dimensions': ['stat_time_day','advertiser_id','advertiser_name'],
                'page': page, 'page_size': 1000,
            }
            data, err = _tiktok_http_post('/report/integrated/get/', body, token=token_plain)
            if err:
                print('[TikTok] report 接口失败(batch %d page %d): %s' % (bi, page, err), flush=True); break
            lst = ((data or {}).get('data') or {}).get('list') or []
            if not lst: break
            rows.extend(lst)
            page += 1
            if page > 50: break
    return rows

def tiktok_sync_consume(tk_entries: list, since: str, until: str) -> dict:
    """TikTok 全量同步：校验 token → 拉 BC 下所有 advertiser → 按日期拉 report → 写入 consume.json"""
    result = {}
    for entry in tk_entries:
        tk_id = entry.get('id')
        name = entry.get('name') or '(未命名 BC)'
        plain_at = entry.get('access_token') or ''
        bc_id = str(entry.get('business_center_id') or '').strip()
        one = {'bc_name': name, 'accounts': 0, 'rows': 0, 'errors': []}
        if not plain_at:
            one['errors'].append('缺少 Access Token'); result[tk_id] = one; continue
        if not bc_id:
            one['errors'].append('缺少 Business Center ID'); result[tk_id] = one; continue
        # 校验 token
        v = test_tiktok_token(plain_at, bc_id)
        if not v.get('ok'):
            one['errors'].append('Token 失效: ' + str(v.get('error'))); result[tk_id] = one; continue
        one['bc_info'] = v.get('name')
        # 拉 advertiser 列表
        accs = _tiktok_fetch_advertisers(plain_at, bc_id)
        one['accounts'] = len(accs)
        acc_map = {str(a.get('advertiser_id') or ''): (a.get('advertiser_name') or '') for a in accs}
        if not accs:
            one['dateFrom'] = since; one['dateTo'] = until
            result[tk_id] = one; continue
        # 拉 report
        aid_list = list(acc_map.keys())
        all_rows = []
        # TikTok advertiser_ids 数组过大时建议分批，但常规 BC 不会超过 API 限制，这里直接一次
        try:
            raw = _tiktok_fetch_report(plain_at, aid_list, since, until)
        except Exception as e:
            one['errors'].append('report 异常: ' + str(e)[:150]); raw = []
        for item in raw:
            dims = item.get('dimensions') or {}
            metrics = item.get('metrics') or {}
            stat_day = str(dims.get('stat_time_day') or '')
            adv_id = str(dims.get('advertiser_id') or '')
            adv_name = str(dims.get('advertiser_name') or '') or acc_map.get(adv_id, '')
            if not stat_day or not adv_id: continue
            all_rows.append({
                'date': stat_day,
                'accountId': adv_id,
                'accountName': adv_name,
                'spend': float(metrics.get('spend', 0) or 0),
            })
        one['rows'] = len(all_rows)
        one['dateFrom'] = since; one['dateTo'] = until
        # 落库（与 Meta 独立的轻量 persist，设置 source='tiktok'）
        try:
            with _lock:
                persist = _platform_persist_consume(all_rows, platform='TikTok', source='tiktok')
            one['added'] = persist['added']
            one['updated'] = persist['updated']
            one['rejected'] = persist['rejected']
            one['rejected_ids'] = persist['rejected_ids']
        except Exception as pe:
            one['errors'].append('落库失败: ' + str(pe)[:150])
        result[tk_id] = one
    return result

def _platform_persist_consume(rows: list, platform: str = '', source: str = '') -> dict:
    """通用平台消耗落库（Meta/TikTok/Google 共用）。
    复用 Meta persist 的 mapping 匹配 + 媒体校验 + 同键 upsert 逻辑，
    额外写入 platform / source 字段。"""
    mapping_list = load_json(MAPPING_FILE)
    mapping_index = build_mapping_index(mapping_list)
    name_to_aid = {}
    for mm in mapping_list:
        nm = str(mm.get('name') or '').strip()
        aid = normalize_account_id(mm.get('accountId') or '')
        if nm and aid and nm not in name_to_aid:
            name_to_aid[nm] = aid
    consume = load_json(CONSUME_FILE)
    # 2026-09-24 BUGFIX: 唯一键必须带 source，否则不同渠道同一 date+accountId 会互相覆盖
    existing_keys = {(r.get('date', ''), r.get('accountId', ''), r.get('source', '')) for r in consume}
    media_check = load_settings().get('media_check', True)
    added = updated = rejected = 0
    rejected_ids = set()
    for r in rows:
        aid = normalize_account_id(r.get('accountId') or '')
        d = to_short_date(r.get('date') or '')
        if not aid or not d:
            rejected += 1
            if aid: rejected_ids.add(aid)
            continue
        raw = {
            '账号ID': aid,
            '账号名称': r.get('accountName') or '',
            '报告开始日期': d,
            '消耗金额': float(r.get('spend') or 0),
            '媒体': platform or '',
        }
        row = enrich_consume_row(raw, mapping_index, name_to_aid)
        if not row:
            rejected += 1
            rejected_ids.add(aid)
            continue
        # 2026-09-24: 跨平台同 accountId 时按 platform 过滤 mapping 段
        m2 = pick_mapping_segment(mapping_index, row['accountId'], row['date'], platform_filter=platform)
        if m2:
            row['client'] = m2.get('client', '')
            row['channel'] = m2.get('channel', '')
            row['serviceRate'] = m2.get('rate', 0) or 0
            row['matched'] = True
            row['_segStart'] = m2.get('date', '') or ''
            row['_segEnd'] = m2.get('endDate', '') or ''
            row['segOut'] = seg_out_reason(str(row.get('date') or ''), m2)
            row['overdue'] = bool(row['segOut'])
            m_platform = normalize_platform(m2.get('platform') or m2.get('media') or '')
            if media_check and m_platform and platform and m_platform != platform:
                rejected += 1
                rejected_ids.add(row['accountId'])
                continue
            row['platform'] = m_platform or platform
        if not row.get('platform'):
            row['platform'] = platform
        if source:
            row['source'] = source
        row['importedAt'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # 2026-09-24 BUGFIX: key 加 source 做三元组
        key = (row['date'], row['accountId'], source or row.get('source', ''))
        if key in existing_keys:
            for i, old_row in enumerate(consume):
                if (old_row.get('date'), old_row.get('accountId'), old_row.get('source', '')) == key:
                    old_included = old_row.get('spend_including_reflow', old_row.get('reflowSpend', old_row.get('spend', 0))) or 0
                    new_included = row.get('spend_including_reflow', row.get('spend', 0)) or 0
                    row['spend'] = float(new_included)
                    row['spend_including_reflow'] = float(new_included)
                    row['spend_usd_total'] = float(old_included)
                    row['reflow_difference'] = float(new_included) - float(old_included)
                    consume[i] = row
                    break
            updated += 1
        else:
            included = row.get('spend_including_reflow', row.get('spend', 0)) or 0
            row['spend'] = float(included)
            row['spend_including_reflow'] = float(included)
            row['spend_usd_total'] = float(included)
            row['reflow_difference'] = 0
            consume.append(row)
            existing_keys.add(key)
            added += 1
    consume.sort(key=lambda r: r.get('date', ''))
    save_json(CONSUME_FILE, consume)
    return {'added': added, 'updated': updated, 'rejected': rejected,
            'rejected_ids': sorted(rejected_ids)[:20], 'totalConsume': len(consume)}


# ============================================================
# === 2026-09-24 Google Ads API 新增 ===
# ============================================================

def _google_xor_cipher(plain_bytes: bytes, secret: str) -> bytes:
    key_bytes = secret.encode('utf-8')
    out = bytearray(len(plain_bytes))
    for i, b in enumerate(plain_bytes):
        out[i] = b ^ key_bytes[i % len(key_bytes)]
    mac = hmac.new(key_bytes, bytes(out), hashlib.sha256).digest()
    return bytes(out) + mac

def _google_xor_decipher(blob: bytes, secret: str) -> bytes:
    if len(blob) < 32: return b''
    cipher, mac = blob[:-32], blob[-32:]
    key_bytes = secret.encode('utf-8')
    expected = hmac.new(key_bytes, cipher, hashlib.sha256).digest()
    if not hmac.compare_digest(mac, expected):
        raise ValueError('Google Token 签名校验失败')
    out = bytearray(len(cipher))
    for i, b in enumerate(cipher):
        out[i] = b ^ key_bytes[i % len(key_bytes)]
    return bytes(out)

def _encrypt_google(plain: str) -> str:
    blob = _google_xor_cipher(plain.encode('utf-8'), _GOOGLE_SECRET)
    return base64.urlsafe_b64encode(blob).decode('ascii').rstrip('=')

def _decrypt_google(enc: str) -> str:
    if not enc: return ''
    pad = '=' * (4 - len(enc) % 4) if len(enc) % 4 else ''
    blob = base64.urlsafe_b64decode(enc + pad)
    return _google_xor_decipher(blob, _GOOGLE_SECRET).decode('utf-8')

def mask_google_token(token: str) -> str:
    if not token: return ''
    if len(token) <= 16: return token[:4] + '****'
    return token[:8] + '...' + token[-4:]

def load_google_tokens():
    data = load_json(GOOGLE_TOKENS_FILE)
    if not isinstance(data, list): return []
    safe = []
    for item in data:
        s = {k: item.get(k) for k in ('id','name','note','customer_id',
                                       'createdAt','lastCheckedAt','status')}
        s['developer_token_mask'] = mask_google_token(_decrypt_google(item.get('developer_token_enc','')))
        s['access_token_mask'] = mask_google_token(_decrypt_google(item.get('access_token_enc','')))
        safe.append(s)
    return safe

def load_google_tokens_plain():
    data = load_json(GOOGLE_TOKENS_FILE)
    if not isinstance(data, list): return []
    out = []
    for item in data:
        row = {k: item.get(k) for k in ('id','name','note','customer_id',
                                         'createdAt','lastCheckedAt','status')}
        row['developer_token'] = _decrypt_google(item.get('developer_token_enc', ''))
        row['client_id'] = _decrypt_google(item.get('client_id_enc', ''))
        row['client_secret'] = _decrypt_google(item.get('client_secret_enc', ''))
        row['refresh_token'] = _decrypt_google(item.get('refresh_token_enc', ''))
        row['access_token'] = _decrypt_google(item.get('access_token_enc', ''))
        out.append(row)
    return out

def _google_save_tokens(items):
    persisted = []
    for it in items:
        def _pick(plain_key, enc_key):
            plain = it.pop(plain_key, '') if plain_key in it else ''
            return _encrypt_google(plain) if plain else (it.get(enc_key) or '')
        persisted.append({
            'id': it.get('id'), 'name': (it.get('name') or '').strip(),
            'note': (it.get('note') or '').strip(),
            'customer_id': str(it.get('customer_id') or '').strip(),
            'developer_token_enc': _pick('developer_token', 'developer_token_enc'),
            'client_id_enc': _pick('client_id', 'client_id_enc'),
            'client_secret_enc': _pick('client_secret', 'client_secret_enc'),
            'refresh_token_enc': _pick('refresh_token', 'refresh_token_enc'),
            'access_token_enc': _pick('access_token', 'access_token_enc'),
            'createdAt': it.get('createdAt'),
            'lastCheckedAt': it.get('lastCheckedAt'),
            'status': it.get('status'),
        })
    save_json(GOOGLE_TOKENS_FILE, persisted)

def google_refresh_access_token(client_id: str, client_secret: str, refresh_token: str) -> dict:
    """用 refresh_token 换新 access_token，返回 {'access_token', 'expires_in'} 或 {'error': ...}"""
    if not client_id or not client_secret or not refresh_token:
        return {'error': '缺少 client_id/client_secret/refresh_token 任一'}
    try:
        params = urllib.parse.urlencode({
            'client_id': client_id, 'client_secret': client_secret,
            'refresh_token': refresh_token, 'grant_type': 'refresh_token',
        }).encode('ascii')
        req = urllib.request.Request(
            'https://oauth2.googleapis.com/token', data=params,
            headers={'Content-Type': 'application/x-www-form-urlencoded',
                     'User-Agent': 'FBAdsManager/3.915'})
        with urllib.request.urlopen(req, timeout=GOOGLE_HTTP_TIMEOUT) as resp:
            body = resp.read().decode('utf-8', 'replace')
            return json.loads(body)
    except urllib.error.HTTPError as e:
        try: detail = e.read().decode('utf-8', 'replace')
        except: detail = ''
        return {'error': 'HTTP %d: %s' % (e.code, detail[:200])}
    except Exception as e:
        return {'error': str(e)}

def _google_http(path, body_dict=None, dev_token='', access_token='', customer_id=None):
    """Google Ads API 请求（带 developer-token + Bearer Authorization）"""
    url = GOOGLE_API_BASE + path
    headers = {
        'User-Agent': 'FBAdsManager/3.915',
        'Accept': 'application/json',
    }
    if dev_token: headers['developer-token'] = dev_token
    if access_token: headers['Authorization'] = 'Bearer ' + access_token
    if body_dict is not None:
        headers['Content-Type'] = 'application/json'
        data = json.dumps(body_dict).encode('utf-8')
        method = 'POST'
    else:
        data = None
        method = 'GET'
    try:
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        with urllib.request.urlopen(req, timeout=GOOGLE_HTTP_TIMEOUT) as resp:
            body = resp.read().decode('utf-8', 'replace')
            return json.loads(body) if body else {}, None
    except urllib.error.HTTPError as e:
        try: detail = e.read().decode('utf-8', 'replace')
        except: detail = ''
        return None, 'HTTP %d: %s' % (e.code, detail[:300])
    except Exception as e:
        return None, str(e)

def test_google_token(plain_entry: dict) -> dict:
    """校验 Google Token 有效性：先尝试用 refresh_token 换新 access_token，再调 customerClients"""
    dev_token = plain_entry.get('developer_token') or ''
    access_token = plain_entry.get('access_token') or ''
    refresh_token = plain_entry.get('refresh_token') or ''
    client_id = plain_entry.get('client_id') or ''
    client_secret = plain_entry.get('client_secret') or ''
    customer_id = str(plain_entry.get('customer_id') or '').strip()
    if not dev_token:
        return {'ok': False, 'error': '缺少 Developer Token'}
    if not access_token and not refresh_token:
        return {'ok': False, 'error': '缺少 Access Token / Refresh Token'}
    new_at = None
    # 优先用 refresh_token 刷新（access_token 1 小时过期）
    if refresh_token and client_id and client_secret:
        r = google_refresh_access_token(client_id, client_secret, refresh_token)
        if r.get('access_token'):
            new_at = r['access_token']
            access_token = new_at
        else:
            print('[Google] refresh_token 刷新失败：%s' % str(r.get('error'))[:150], flush=True)
    if not access_token:
        return {'ok': False, 'error': '无可用 Access Token（refresh_token 也无法刷新）'}
    if not customer_id:
        return {'ok': False, 'error': '缺少 Customer ID（10 位数字）'}
    data, err = _google_http('/customers/%s/customerClients' % customer_id,
                             dev_token=dev_token, access_token=access_token)
    if err:
        return {'ok': False, 'error': err, 'new_access_token': new_at}
    clients = (data or {}).get('customerClients') or []
    info = next((c for c in clients if str(c.get('id','')) == customer_id), None)
    name = info.get('descriptiveName') if info else (plain_entry.get('name') or customer_id)
    return {'ok': True, 'name': name, 'id': customer_id, 'new_access_token': new_at}

def _google_fetch_customers(dev_token: str, access_token: str, customer_id: str) -> list:
    """拉 MCC 下所有子 customer：GET /customers/{customer_id}/customerClients"""
    data, err = _google_http('/customers/%s/customerClients' % customer_id,
                             dev_token=dev_token, access_token=access_token)
    if err:
        print('[Google] customerClients 失败: %s' % err, flush=True)
        return []
    return (data or {}).get('customerClients') or []

def _google_fetch_report(dev_token: str, access_token: str, customer_id: str,
                         since: str, until: str) -> list:
    """GQL 拉 campaign 级消耗数据"""
    query = (
        "SELECT segments.date, customer.id, customer.descriptive_name, "
        "metrics.cost_micros, metrics.impressions, metrics.clicks, metrics.conversions "
        "FROM campaign WHERE segments.date BETWEEN '%s' AND '%s'" % (since, until)
    )
    body = {'query': query}
    data, err = _google_http('/customers/%s/googleAds:search' % customer_id,
                             body_dict=body, dev_token=dev_token, access_token=access_token)
    if err:
        print('[Google] googleAds:search(%s) 失败: %s' % (customer_id, err), flush=True)
        return []
    return (data or {}).get('results') or []

def google_sync_consume(g_entries: list, since: str, until: str) -> dict:
    result = {}
    for entry in g_entries:
        tk_id = entry.get('id')
        name = entry.get('name') or '(未命名)'
        one = {'account_name': name, 'accounts': 0, 'rows': 0, 'errors': []}
        dev_token = entry.get('developer_token') or ''
        access_token = entry.get('access_token') or ''
        refresh_token = entry.get('refresh_token') or ''
        client_id = entry.get('client_id') or ''
        client_secret = entry.get('client_secret') or ''
        customer_id = str(entry.get('customer_id') or '').strip()
        if not dev_token:
            one['errors'].append('缺少 Developer Token'); result[tk_id] = one; continue
        if not customer_id:
            one['errors'].append('缺少 Customer ID'); result[tk_id] = one; continue
        # 尝试刷新 access_token
        refreshed = False
        if refresh_token and client_id and client_secret:
            r = google_refresh_access_token(client_id, client_secret, refresh_token)
            if r.get('access_token'):
                new_at = r['access_token']
                access_token = new_at
                # 写回明文到 entry（调用方持久化）
                entry['access_token'] = new_at
                refreshed = True
            elif not access_token:
                one['errors'].append('refresh_token 也无法刷新：' + str(r.get('error'))[:150])
                result[tk_id] = one; continue
        if not access_token:
            one['errors'].append('缺少可用 Access Token'); result[tk_id] = one; continue
        # 校验基础连通性
        v_data, v_err = _google_http('/customers/%s/customerClients' % customer_id,
                                     dev_token=dev_token, access_token=access_token)
        if v_err:
            one['errors'].append('Token 失效: ' + v_err); result[tk_id] = one; continue
        # 拉子客户（MCC 场景）
        customers = _google_fetch_customers(dev_token, access_token, customer_id)
        # 如果返回的列表为空或只有一个且 id==customer_id，当作普通账户处理
        target_customers = []
        if customers:
            for c in customers:
                cid = str(c.get('id') or '')
                if cid: target_customers.append(cid)
        else:
            target_customers = [customer_id]
        if not target_customers:
            target_customers = [customer_id]
        # 如果只发现自己一个 → 就是单账户，直接用 customer_id；否则遍历 MCC 子账户
        is_single = len(target_customers) == 1 and target_customers[0] == customer_id
        one['accounts'] = len(target_customers) if not is_single else 1
        all_rows = []
        for cid in target_customers:
            if len(all_rows) > 50000: break   # 安全上限
            raw = _google_fetch_report(dev_token, access_token, cid, since, until)
            for item in raw:
                camp = item.get('campaign') or {}
                seg = camp.get('segments') or {}
                cust = camp.get('customer') or {}
                met = camp.get('metrics') or {}
                stat_day = str(seg.get('date') or '')
                aid = str(cust.get('id') or '')
                aname = cust.get('descriptiveName') or ''
                cost_micros = float(met.get('costMicros') or 0)
                spend_usd = cost_micros / 1_000_000.0
                if not stat_day or not aid: continue
                all_rows.append({
                    'date': stat_day,
                    'accountId': aid,
                    'accountName': aname,
                    'spend': spend_usd,
                })
        one['rows'] = len(all_rows)
        one['dateFrom'] = since; one['dateTo'] = until
        if refreshed:
            one['access_token_refreshed'] = True
        try:
            with _lock:
                persist = _platform_persist_consume(all_rows, platform='Google', source='google')
            one['added'] = persist['added']
            one['updated'] = persist['updated']
            one['rejected'] = persist['rejected']
            one['rejected_ids'] = persist['rejected_ids']
        except Exception as pe:
            one['errors'].append('落库失败: ' + str(pe)[:150])
        result[tk_id] = one

    # Bug 修复(2026-09-24): 把刷新后的 access_token 持久化回加密存储
    any_refreshed = any(info.get('access_token_refreshed') for info in result.values() if isinstance(info, dict))
    if any_refreshed:
        try:
            current_list = load_json(GOOGLE_TOKENS_FILE)
            for entry in g_entries:
                new_at = entry.get('access_token') or ''
                if not new_at:
                    continue
                for i, saved in enumerate(current_list):
                    if saved.get('id') == entry.get('id'):
                        current_list[i]['access_token_enc'] = _encrypt_google(new_at)
                        break
            _google_save_tokens(current_list)
            print('[Google] ✅ access_token 刷新后已持久化', flush=True)
        except Exception as e:
            print('[Google] ⚠️ 刷新后持久化失败: %s' % str(e)[:150], flush=True)

    return result




# === 2026-09-24 三平台自动刷新 ===
def load_platform_auto_cfg() -> dict:
    """加载三平台自动刷新配置，不存在时返回默认值。
    如果旧 meta_auto_sync_cfg.json 存在且新文件不存在，迁移 meta 配置过来（兼容旧用户）。
    """
    default = {
        'meta':    {'enabled': False, 'interval_h': 12, 'days_back': 7, 'lastRun': '', 'lastError': ''},
        'tiktok':  {'enabled': False, 'interval_h': 12, 'days_back': 7, 'lastRun': '', 'lastError': ''},
        'google':  {'enabled': False, 'interval_h': 12, 'days_back': 7, 'lastRun': '', 'lastError': ''},
        'shared_lock_wait': 120,
    }
    cfg = load_json(_PLATFORM_AUTO_CFG_FILE)
    if not isinstance(cfg, dict):
        cfg = {}
        # 尝试从旧文件迁移 meta 配置
        old_cfg = load_json(META_AUTO_CFG_FILE)
        if isinstance(old_cfg, dict):
            print('[平台自动刷新] 迁移旧 meta_auto_sync_cfg.json → platform_auto_sync_cfg.json', flush=True)
            # 兼容旧配置（hour/minute）迁移到 interval_h
            if 'interval_h' not in old_cfg and ('hour' in old_cfg or 'minute' in old_cfg):
                old_hour = old_cfg.get('hour', 6)
                old_cfg['interval_h'] = 24 if old_hour is not None else 12
            meta_default = default['meta']
            meta_sub = {}
            for k in ('enabled', 'interval_h', 'days_back', 'lastRun'):
                if k in old_cfg:
                    meta_sub[k] = old_cfg[k]
                else:
                    meta_sub[k] = meta_default[k]
            meta_sub['lastError'] = old_cfg.get('lastError', '')
            cfg['meta'] = meta_sub

    for platform in ('meta', 'tiktok', 'google'):
        sub = cfg.get(platform)
        if not isinstance(sub, dict):
            sub = {}
        plat_default = default[platform]
        for k, v in plat_default.items():
            if k not in sub:
                sub[k] = v
        cfg[platform] = sub
    if 'shared_lock_wait' not in cfg:
        cfg['shared_lock_wait'] = default['shared_lock_wait']
    return cfg


def save_platform_auto_cfg(cfg: dict) -> None:
    """保存三平台自动刷新配置"""
    save_json(_PLATFORM_AUTO_CFG_FILE, cfg)


def load_meta_auto_cfg() -> dict:
    """旧接口 wrapper → 返回三平台配置里的 meta 子配置"""
    full = load_platform_auto_cfg()
    return full.get('meta', {'enabled': False, 'interval_h': 12, 'days_back': 7, 'lastRun': ''})


def save_meta_auto_cfg(cfg: dict) -> None:
    """旧接口 wrapper → 保存 meta 子配置到三平台文件"""
    full = load_platform_auto_cfg()
    full['meta'] = cfg
    save_platform_auto_cfg(full)


def _platform_auto_sync_worker() -> None:
    """后台线程：定时检查三平台（Meta/TikTok/Google）是否需要自动刷新。
    每分钟检查一次，哪个平台到期就跑哪个。各平台独立锁互不阻塞。
    """
    while True:
        try:
            time.sleep(60)
            cfg = load_platform_auto_cfg()
            now = datetime.now()

            for platform_key in ('meta', 'tiktok', 'google'):
                sub = cfg.get(platform_key, {})
                if not sub.get('enabled'):
                    continue

                interval_h = max(1, min(24, int(sub.get('interval_h', 12))))
                days_back = max(1, min(30, int(sub.get('days_back', 7))))
                last_run_str = sub.get('lastRun', '')

                # 判断是否到期
                if last_run_str:
                    try:
                        last = datetime.strptime(last_run_str, '%Y-%m-%d %H:%M:%S')
                        if (now - last).total_seconds() < interval_h * 3600:
                            continue  # 还没到时间
                    except Exception:
                        pass

                # 尝试拿锁（如果上一次还在跑就跳过）
                lock = _PLATFORM_AUTO_LOCKS[platform_key]
                if not lock.acquire(blocking=False):
                    print(f'[{platform_key}自动刷新] ⏸️  上次还在跑，跳过', flush=True)
                    continue

                try:
                    print(f'[{platform_key}自动刷新] 🚀 开始执行', flush=True)
                    with _PLATFORM_AUTO_STATE_LOCK:
                        _PLATFORM_AUTO_STATE[platform_key]['running'] = True
                        _PLATFORM_AUTO_STATE[platform_key]['startedAt'] = now.strftime('%Y-%m-%d %H:%M:%S')
                        _PLATFORM_AUTO_STATE[platform_key]['lastError'] = ''

                    since = (now.date() - timedelta(days=days_back)).strftime('%Y-%m-%d')
                    until = now.date().strftime('%Y-%m-%d')

                    # 同时更新旧 Meta 状态（兼容一键同步互斥锁逻辑）
                    if platform_key == 'meta':
                        with _META_SYNC_STATE_LOCK:
                            _META_SYNC_STATE['running'] = True
                            _META_SYNC_STATE['mode'] = 'auto'
                            _META_SYNC_STATE['startedAt'] = now.strftime('%Y-%m-%d %H:%M:%S')

                    # 调对应的同步函数
                    res = {}
                    if platform_key == 'meta':
                        # 自动刷新也要跟一键同步互斥
                        ok = _META_SYNC_LOCK.acquire(blocking=False)
                        if not ok:
                            print(f'[{platform_key}自动刷新] ⏸️  一键同步正在执行，跳过', flush=True)
                            continue
                        try:
                            tk_list = load_meta_tokens_plain()
                            if not tk_list:
                                print(f'[{platform_key}自动刷新] ⚠️  没有配置 Token，跳过', flush=True)
                                sub['lastError'] = '没有配置 Meta Token'
                                cfg[platform_key] = sub
                                save_platform_auto_cfg(cfg)
                                continue
                            res = meta_sync_consume(tk_list, since, until)
                        finally:
                            _META_SYNC_LOCK.release()
                    elif platform_key == 'tiktok':
                        tk_list = load_tiktok_tokens_plain()
                        if not tk_list:
                            print(f'[{platform_key}自动刷新] ⚠️  没有配置 Token，跳过', flush=True)
                            sub['lastError'] = '没有配置 TikTok Token'
                            cfg[platform_key] = sub
                            save_platform_auto_cfg(cfg)
                            continue
                        res = tiktok_sync_consume(tk_list, since, until)
                    elif platform_key == 'google':
                        tk_list = load_google_tokens_plain()
                        if not tk_list:
                            print(f'[{platform_key}自动刷新] ⚠️  没有配置 Token，跳过', flush=True)
                            sub['lastError'] = '没有配置 Google Token'
                            cfg[platform_key] = sub
                            save_platform_auto_cfg(cfg)
                            continue
                        res = google_sync_consume(tk_list, since, until)

                    total_rows = sum(info.get('rows', 0) for info in res.values() if isinstance(info, dict))

                    sub['lastRun'] = now.strftime('%Y-%m-%d %H:%M:%S')
                    sub['lastError'] = ''
                    cfg[platform_key] = sub
                    save_platform_auto_cfg(cfg)

                    # 同步更新旧 Meta 状态
                    if platform_key == 'meta':
                        with _META_SYNC_STATE_LOCK:
                            _META_SYNC_STATE['lastAutoSync'] = now.strftime('%Y-%m-%d %H:%M:%S')
                            _META_SYNC_STATE['lastAutoSyncError'] = ''

                    print(f'[{platform_key}自动刷新] ✅ 完成 {since}~{until}，{total_rows} 行', flush=True)

                except Exception as e:
                    err_msg = str(e)[:200]
                    print(f'[{platform_key}自动刷新] ❌ 异常: {err_msg}', flush=True)
                    sub['lastError'] = err_msg
                    sub['lastRun'] = now.strftime('%Y-%m-%d %H:%M:%S')
                    cfg[platform_key] = sub
                    save_platform_auto_cfg(cfg)
                    with _PLATFORM_AUTO_STATE_LOCK:
                        _PLATFORM_AUTO_STATE[platform_key]['lastError'] = err_msg
                    if platform_key == 'meta':
                        with _META_SYNC_STATE_LOCK:
                            _META_SYNC_STATE['lastAutoSyncError'] = err_msg
                finally:
                    with _PLATFORM_AUTO_STATE_LOCK:
                        _PLATFORM_AUTO_STATE[platform_key]['running'] = False
                    if platform_key == 'meta':
                        with _META_SYNC_STATE_LOCK:
                            _META_SYNC_STATE['running'] = False
                            _META_SYNC_STATE['mode'] = ''
                    lock.release()

        except Exception as e:
            print(f'[平台自动刷新Worker] 异常：{e}', flush=True)
            time.sleep(60)


# ============================================================


# 登录爆破限流：{ip: [失败时间戳]}，15 分钟窗口内 5 次失败 → 锁定
# 反爬虫/滥用：所有 /api/* 必须携带浏览器 Ajax 标识（X-Requested-With 或 Bearer），脚本裸抓直接 403。
_LOGIN_FAILS = {}
_LOGIN_LOCK = threading.Lock()
LOGIN_MAX_FAILS = 5
LOGIN_WINDOW = 900   # 秒

def _login_key(ip, username):
    """返回 (ip, username) 元组，username 为 None 时代表通用 IP 锁"""
    return (ip, username or '_')

def _login_status(ip, username=None):
    """返回当前登录限制状态，用于展示剩余次数或锁定到期时间。
    返回 dict：{allowed: bool, remaining: int, locked_until: float|None}
      - allowed=True  → 还能试，remaining 是剩余次数（0~LOGIN_MAX_FAILS-1）
      - allowed=False → 已锁定，locked_until 是解锁时间戳
    """
    with _LOGIN_LOCK:
        now = time.time()
        # 全局清理过期失败记录
        for k in list(_LOGIN_FAILS.keys()):
            _LOGIN_FAILS[k] = [t for t in _LOGIN_FAILS[k] if now - t < LOGIN_WINDOW]
            if not _LOGIN_FAILS[k]:
                del _LOGIN_FAILS[k]
        if username:
            key = _login_key(ip, username)
        else:
            key = _login_key(ip, None)
        arr = sorted([t for t in _LOGIN_FAILS.get(key, []) if now - t < LOGIN_WINDOW])
        fail_count = len(arr)
        if fail_count >= LOGIN_MAX_FAILS:
            locked_until = arr[-1] + LOGIN_WINDOW   # 最后一次失败时间 + 窗口
            return {'allowed': False, 'remaining': 0, 'locked_until': locked_until}
        remaining = LOGIN_MAX_FAILS - fail_count
        return {'allowed': True, 'remaining': remaining, 'locked_until': None}

def _login_fail(ip, username=None):
    """记录一次登录失败，并返回最新状态（用于告诉前端剩余次数）"""
    with _LOGIN_LOCK:
        key = _login_key(ip, username)
        _LOGIN_FAILS.setdefault(key, []).append(time.time())
    return _login_status(ip, username)

def log_op(user, module, action, detail=''):
    """操作审计：任何数据变更（新增/修改/删除/导入/清空/账号管理）都落一条记录"""
    try:
        with _WRITE_LOCK:
            log = []
            if os.path.exists(OPS_LOG_FILE):
                try:
                    with open(OPS_LOG_FILE, 'r', encoding='utf-8') as f:
                        log = json.load(f)
                except Exception:
                    log = []
            log.append({
                'time':   datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'user':   (user or {}).get('name') or (user or {}).get('username') or '-',
                'role':   (user or {}).get('role', ''),
                'module': module,
                'action': action,
                'detail': str(detail)[:200],
            })
            tmp = OPS_LOG_FILE + '.tmp'
            with open(tmp, 'w', encoding='utf-8') as f:
                json.dump(log[-2000:], f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, OPS_LOG_FILE)
    except Exception as e:
        print('[ops-log] 写入失败', e)


def gen_id():
    return str(int(time.time() * 1000)) + '_' + uuid.uuid4().hex[:6]


def save_import_error_file(rows, type_hint):
    """把导入被拒绝的原始行保存到独立文件，返回文件名（不含路径）；无行返回 None。"""
    if not rows:
        return None
    fname = f"{datetime.now().strftime('%Y%m%d%H%M%S')}_{type_hint}_{uuid.uuid4().hex[:8]}.json"
    fpath = os.path.join(IMPORT_ERROR_DIR, fname)
    try:
        with open(fpath, 'w', encoding='utf-8') as f:
            json.dump(rows, f, ensure_ascii=False)
        # 简单清理：保留最近 20 份错误文件
        try:
            allf = sorted(os.listdir(IMPORT_ERROR_DIR))
            while len(allf) > 20:
                os.remove(os.path.join(IMPORT_ERROR_DIR, allf.pop(0)))
        except OSError:
            pass
        return fname
    except OSError:
        return None


# ============================================================
# Mapping 归属区间工具（支持 1 个广告账号 → 多个客户段）
# ============================================================
def _parse_date(s):
    """把 'YYYY-MM-DD' / 'YYYY/MM/DD' 字符串解析为 datetime；空/非法返回 None"""
    if not s:
        return None
    s = str(s).strip().replace('/', '-')
    for fmt in ('%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%Y%m%d'):
        try:
            return datetime.strptime(s[:10], fmt)
        except Exception:
            continue
    return None


def _cmp_date(a, b):
    """日期比较：None 代表 无穷远（排在最后）。返回 -1/0/1"""
    if a is None and b is None: return 0
    if a is None: return 1          # 无穷大
    if b is None: return -1
    return -1 if a < b else (1 if a > b else 0)


def _normalize_mapping_segments(segments):
    """对同 accountId 的多个下户段进行排序+归一，避免区间重叠。
    输入: [ {date, endDate, client, ...}, ... ]
    返回: [ ... 按 date 升序，endDate 若跨到下一段 则 截断为 下一段 date-1 天 ]
    """
    if not segments:
        return []
    def key_of(x):
        d = _parse_date(x.get('date'))
        # 最早日期排前面，没日期（空）排最后
        return (0 if d else 1, d or datetime.max)
    arr = sorted(segments, key=key_of)
    result = []
    n = len(arr)
    for i, seg in enumerate(arr):
        seg_d = _parse_date(seg.get('date'))
        seg_e = _parse_date(seg.get('endDate'))
        # 下一段 start：若存在，计算其 date
        if i+1 < n:
            next_d = _parse_date(arr[i+1].get('date'))
            if next_d and (seg_e is None or (seg_d and seg_e and seg_e >= next_d)):
                # 自动把结束日期截止到下一段开始日前一天，避免两段重叠
                forced_e = next_d - timedelta(days=1)
                if seg_d and forced_e < seg_d:
                    forced_e = seg_d   # 至少等于 start
                seg = dict(seg)
                seg['endDate'] = forced_e.strftime('%Y-%m-%d')
                seg_e = forced_e
        result.append(seg)
    return result


def build_mapping_index(mapping_list):
    """构建 {accountId: [segments(升序+归一)]} 索引。
    同时对每个 item 补全 endDate=''（历史数据迁移）、date 默认空字符串。
    """
    by_id = {}
    for m in (mapping_list or []):
        item = dict(m)
        item.setdefault('endDate', '')
        item.setdefault('date', '')
        aid = normalize_account_id(item.get('accountId') or '')
        if not aid:
            continue
        by_id.setdefault(aid, []).append(item)
    for aid in by_id:
        by_id[aid] = _normalize_mapping_segments(by_id[aid])
    return by_id


def pick_mapping_segment(mapping_index, account_id, consume_date_str, platform_filter=None):
    """从 mapping_index 中按 consume_date_str 归属区间查找对应段。
    匹配规则（闭区间 [start, end]）：
      段.start(=date, 空=最早) ≤ consume_date ≤ 段.end(=endDate, 空=∞)
      同 accountId 多段时按 date 升序命中第一个满足的段；
    2026-09-24 BUGFIX: 新增 platform_filter 参数，支持按平台筛选 mapping 段。
      跨平台同 accountId 时（如 Facebook+TikTok 各有一段），必须传 platform_filter 才能命中对应段。
    兜底规则：
      - consume 日期早于首段 start → 取【首段】（最早下户者，保守归属，避免消耗无主）
      - consume 日期晚于所有段 end → 取【最后一段】（最晚下户者，用户常"忘记续期"）
      - consume 无日期 → 取最后一段
    """
    if not account_id:
        return None
    segs = mapping_index.get(normalize_account_id(account_id))
    if not segs:
        return None
    # 2026-09-24: platform_filter 过滤
    # 关键规则：传了 platform_filter 却没匹配到同平台段 → 直接返回 None
    #   （不能 fallback 到其他平台的段，会导致 A 平台消耗错误归属到 B 平台下户段）
    #   如果 mapping 里根本没给这个 accountId 设 platform 字段，才 fallback 兼容老数据
    if platform_filter:
        norm_plat = normalize_platform(platform_filter)
        filtered = [s for s in segs if normalize_platform(s.get('platform') or s.get('media') or '') == norm_plat]
        all_have_platform = all((s.get('platform') or s.get('media') or '').strip() for s in segs)
        if filtered:
            segs = filtered
        elif all_have_platform:
            # 所有 mapping 段都有明确 platform，但没一个匹配 → 说明 mapping 里没给这个平台下户
            return None
        # else: 老数据 mapping 段都没 platform 字段 → fallback 到所有段（兼容）
    cd = _parse_date(consume_date_str)
    if cd is None:
        return segs[-1]
    # 先尝试精确命中
    for seg in segs:
        sd = _parse_date(seg.get('date'))
        ed = _parse_date(seg.get('endDate'))   # None = 无期限
        # start ≤ date（sd 空=最小）
        ok_start = (sd is None) or (sd <= cd)
        # date ≤ end（ed 空=永远）
        ok_end   = (ed is None) or (cd <= ed)
        if ok_start and ok_end:
            return seg
    # 未命中 → 兜底归属（2026-09-04 转户规则修订）：
    #   ① date 早于所有段 start → 首段（保守归属，避免消耗无主）
    #   ② date 落在两段之间的「空窗期」（旧段已止、新段未始）→ 归【前一段】
    #      —— 转户语义：新客户从新段 start 才接管，空窗期账户仍由原客户负责
    #   ③ date 晚于所有段 end（且无更晚的段已开始）→ 尾段（防"忘记续期"）
    started = [seg for seg in segs if (seg.get('date') or '') and (_parse_date(seg.get('date')) or cd) <= cd]
    if started:
        # 空窗期归属规则（系统设置）：prev=归前一段（转户语义，默认）；last=归尾段（旧行为）
        if load_settings().get('gap_rule', 'prev') == 'prev':
            return started[-1]
        return segs[-1]
    earliest_start = _parse_date(segs[0].get('date'))
    if earliest_start and cd < earliest_start:
        return segs[0]
    return segs[-1]


def seg_out_reason(row_date, seg):
    # 2026-09-06 转户链路补盲：归属段「区间外」两种情形都要留痕，避免静默归责
    #   after  = 晚于该段结束日（含空窗归前段、忘记续期的尾段兜底）
    #   before = 早于该段起始日（漏登更早归属段 → 容易被后一任客户背锅）
    if not row_date or not seg: return ''
    st = str(seg.get('date') or ''); en = str(seg.get('endDate') or '')
    if en and row_date > en: return 'after'
    if st and row_date < st: return 'before'
    return ''


# ============================================================
# 鉴权工具
# ============================================================
def hash_password(password, salt=None):
    if salt is None:
        salt = secrets.token_hex(16)
    pw_hash = hashlib.pbkdf2_hmac('sha256', password.encode('utf-8'),
                                  salt.encode('utf-8'), 100000).hex()
    return f"{salt}${pw_hash}"


def verify_password(password, stored):
    try:
        salt, pw_hash = stored.split('$', 1)
        computed = hashlib.pbkdf2_hmac('sha256', password.encode('utf-8'),
                                       salt.encode('utf-8'), 100000).hex()
        return hmac.compare_digest(computed, pw_hash)
    except Exception:
        return False


def load_users():
    if not os.path.exists(USERS_FILE):
        default = [{
            'id': 'user_admin',
            'username': 'admin',
            'password': hash_password('admin123456'),
            'role': 'admin',
            'name': '超级管理员',
            'client': '',
            'active': True,
            'createdAt': datetime.now().isoformat(),
        }]
        save_json(USERS_FILE, default)
        print("⚠️  首次启动，已创建默认管理员：admin / admin123456")
    return load_json(USERS_FILE)


def load_sessions():
    if not os.path.exists(SESSIONS_FILE):
        with SESSIONS_LOCK:
            save_json(SESSIONS_FILE, {})
        return {}
    try:
        with SESSIONS_LOCK:
            with open(SESSIONS_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
    except Exception:
        return {}


def save_sessions(sessions):
    tmp = SESSIONS_FILE + '.tmp'
    with SESSIONS_LOCK:
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(sessions, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, SESSIONS_FILE)


def get_role_permission(role):
    return ROLES.get(role, ROLES[DEFAULT_ROLE])


def create_session(user_id):
    """原子创建会话：读-改-写全程持锁"""
    token = secrets.token_urlsafe(32)
    with SESSIONS_LOCK:
        sessions = {}
        if os.path.exists(SESSIONS_FILE):
            try:
                with open(SESSIONS_FILE, 'r', encoding='utf-8') as f:
                    sessions = json.load(f)
            except Exception:
                sessions = {}
        now = time.time()
        # 2026-09-06：登录时回收过期会话，并对总量设上限（防 sessions.json 无限增长）
        for k in [k for k, v in sessions.items() if now - float(v.get('createdAt') or 0) > SESSION_MAX_AGE]:
            sessions.pop(k, None)
        if len(sessions) > 500:
            for k, _v in sorted(sessions.items(), key=lambda kv: float(kv[1].get('lastSeen') or 0))[:len(sessions) - 500]:
                sessions.pop(k, None)
        sessions[token] = {'userId': user_id, 'createdAt': now, 'lastSeen': now}
        tmp = SESSIONS_FILE + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(sessions, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, SESSIONS_FILE)
    return token


def get_session(token):
    """原子获取会话：读-检查-写全程持锁"""
    if not token:
        return None
    with SESSIONS_LOCK:
        sessions = {}
        if os.path.exists(SESSIONS_FILE):
            try:
                with open(SESSIONS_FILE, 'r', encoding='utf-8') as f:
                    sessions = json.load(f)
            except Exception:
                return None
        sess = sessions.get(token)
        if not sess:
            return None
        if time.time() - sess.get('createdAt', 0) > SESSION_MAX_AGE:
            sessions.pop(token, None)
            tmp = SESSIONS_FILE + '.tmp'
            with open(tmp, 'w', encoding='utf-8') as f:
                json.dump(sessions, f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, SESSIONS_FILE)
            return None
        now = time.time()
        # 性能：lastSeen 落盘节流（>60s 才写）。否则每个请求都写 sessions.json 并 bump 数据版本，
        # 统计结果缓存将永远命中不了，且磁盘 IO 浪费。
        if now - sess.get('lastSeen', 0) > 60:
            sess['lastSeen'] = now
            sessions[token] = sess
            tmp = SESSIONS_FILE + '.tmp'
            with open(tmp, 'w', encoding='utf-8') as f:
                json.dump(sessions, f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, SESSIONS_FILE)
        return sess


def get_user_by_token(token):
    sess = get_session(token)
    if not sess:
        return None
    with _WRITE_LOCK:
        users = []
        if os.path.exists(USERS_FILE):
            try:
                with open(USERS_FILE, 'r', encoding='utf-8') as f:
                    users = json.load(f)
            except Exception:
                users = []
        for u in users:
            if u.get('id') == sess.get('userId') and u.get('active', True):
                safe = {k: v for k, v in u.items() if k != 'password'}
                return safe
        return None
    return None


def revoke_session(token):
    """原子撤销会话"""
    with SESSIONS_LOCK:
        sessions = {}
        if os.path.exists(SESSIONS_FILE):
            try:
                with open(SESSIONS_FILE, 'r', encoding='utf-8') as f:
                    sessions = json.load(f)
            except Exception:
                sessions = {}
        sessions.pop(token, None)
        tmp = SESSIONS_FILE + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(sessions, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, SESSIONS_FILE)


# ============================================================
# Excel 解析
# ============================================================
def _extract_uploaded_file(handler):
    """提取上传的文件内容和文件名。
    返回 (content_bytes, filename)；解析失败返回 (None, '')。
    """
    content_type = handler.headers.get("Content-Type", "")
    if "multipart/form-data" not in content_type:
        return None, ''
    boundary = content_type.split("boundary=")[-1].encode()
    body = handler._read_body()
    if not body:
        return None, ''
    parts = body.split(b"--" + boundary)
    for part in parts:
        if b"Content-Disposition" in part and b"filename=" in part:
            # 提取文件名：part 以 \r\n 开头，跳过空行后取 Content-Disposition 行
            filename = ''
            try:
                lines = part.split(b"\r\n")
                for ln in lines:
                    if b'Content-Disposition' in ln:
                        disp_line = ln.decode('utf-8', errors='ignore')
                        if 'filename="' in disp_line:
                            filename = disp_line.split('filename="')[1].split('"')[0]
                        break
            except Exception:
                filename = ''
            crlf = b"\r\n\r\n"
            header_end = part.find(crlf)
            if header_end > 0:
                content = part[header_end + len(crlf):]
                if content.endswith(b"\r\n"):
                    content = content[:-2]
                return content, filename
    return None, ''


def to_short_date(val):
    """将任意日期输入统一转为短日期 'YYYY-MM-DD' 字符串。
    支持：ISO 字符串(YYYY-MM-DD / YYYY/MM/DD)、带时间的字符串、
          Excel 日期序列号、datetime 对象；无法解析返回空串。
    """
    if val is None or val == '':
        return ''
    # datetime / date 对象
    if hasattr(val, 'strftime'):
        try: return val.strftime('%Y-%m-%d')
        except Exception: return ''
    # 数字 → Excel 序列号
    if isinstance(val, (int, float)) and not isinstance(val, bool):
        try:
            n = int(val)
            if 20000 <= n <= 60000:
                from datetime import date, timedelta
                return (date(1899, 12, 30) + timedelta(days=n)).isoformat()
        except Exception:
            pass
        return ''
    s = str(val).strip()
    if not s:
        return ''
    # 已匹配 YYYY-MM-DD 直接返回前 10 位
    import re as _re
    m = _re.match(r'^(\d{4})[-/](\d{1,2})[-/](\d{1,2})', s)
    if m:
        y, mo, d = m.group(1), m.group(2).zfill(2), m.group(3).zfill(2)
        return f'{y}-{mo}-{d}'
    # MM/DD/YYYY 或 DD/MM/YYYY（无法完全区分，按 MM/DD/YYYY 解析）
    m = _re.match(r'^(\d{1,2})[-/](\d{1,2})[-/](\d{4})', s)
    if m:
        mo, d, y = m.group(1).zfill(2), m.group(2).zfill(2), m.group(3)
        return f'{y}-{mo}-{d}'
    return ''


# IANA 时区名 → UTC 偏移量（忽略夏令时，取标准偏移）
_IANA_TO_OFFSET = {
    'UTC': '+0', 'GMT': '+0',
    'Asia/Shanghai': '+8', 'Asia/Taipei': '+8', 'Asia/Singapore': '+8',
    'Asia/Hong_Kong': '+8', 'Asia/Macau': '+8',
    'Asia/Tokyo': '+9', 'Asia/Seoul': '+9',
    'Asia/Dubai': '+4', 'Asia/Tbilisi': '+4',
    'Asia/Bangkok': '+7', 'Asia/Jakarta': '+7',
    'Asia/Kolkata': '+5.5', 'Asia/Calcutta': '+5.5',
    'Europe/London': '+0', 'Europe/Dublin': '+0',
    'Europe/Paris': '+1', 'Europe/Berlin': '+1', 'Europe/Madrid': '+1',
    'Europe/Moscow': '+3',
    'America/New_York': '-5', 'America/Toronto': '-5',
    'America/Chicago': '-6', 'America/Mexico_City': '-6',
    'America/Denver': '-7', 'America/Los_Angeles': '-8', 'America/Vancouver': '-8',
    'America/Sao_Paulo': '-3', 'America/Buenos_Aires': '-3',
    'Australia/Sydney': '+10', 'Australia/Melbourne': '+10',
}

def normalize_timezone(val):
    """将时区值统一为带符号的字符串偏移量格式，如 '+8'、'-5'、'+5.5'。
    空值默认 '+8'。支持：纯数字(8→+8)、带符号字符串('+8'/'-8')、IANA 时区名。"""
    if val is None or val == '' or str(val).strip() == '':
        return '+8'
    s = str(val).strip()
    # IANA 时区名转换
    if s in _IANA_TO_OFFSET:
        return _IANA_TO_OFFSET[s]
    # 尝试解析为数字偏移量
    try:
        n = float(s)
        sign = '+' if n >= 0 else '-'
        if n == int(n):
            return sign + str(abs(int(n)))
        return sign + str(abs(n))
    except ValueError:
        pass
    # 已经是带符号格式（如 '+8'、'-5.5'）直接返回
    if s[0] in '+-':
        return s
    return s


def normalize_account_id(val):
    """将账号ID统一标准化为纯数字字符串，保证匹配一致性。
    规则：
    1. 转字符串并去除首尾空白
    2. 去除所有非数字字符（如空格、逗号、科学计数法的 e/+ 等）
    3. 空值返回空字符串
    示例：
      '983782317896656'   → '983782317896656'
      '9.83782317896656E14' → '983782317896656'
      ' 983 782 317 '    → '983782317'
      123456789          → '123456789'
    """
    if val is None:
        return ''
    s = str(val).strip()
    if not s:
        return ''
    # 处理科学计数法：如 9.83782317896656E14 → 先转 float 再转 int 再转 str
    try:
        # 如果是纯数字+小数点+e/E 形式，可能是科学计数法
        if 'e' in s.lower() or 'E' in s:
            f = float(s)
            return str(int(f))
    except (ValueError, OverflowError):
        pass
    # 通用：提取所有数字字符
    digits = ''.join(ch for ch in s if ch.isdigit())
    return digits


ACCOUNT_ID_IMPORT_FIELDS = (
    '账号ID', '账户ID', '账户编号', 'accountId', 'Account ID', 'ID'
)


def _num_to_account_id(raw):
    """将数值型账号 ID（含科学计数法）转为纯数字字符串，非数字直接返回原值。"""
    if isinstance(raw, (int, float)):
        return str(int(raw))
    if isinstance(raw, str):
        s = raw.strip()
        try:
            f = float(s)
            return str(int(f))
        except (ValueError, OverflowError):
            pass
    return raw

def import_account_id_text(row):
    """读取导入表账号 ID，**只接受文本字符串**（拒绝 Excel 数值格式）。

    用户在 Excel 里必须把账号 ID 列设置为「文本」格式，否则 openpyxl 会读成 float，
    大数字还会变成科学计数法（2.785e+16），无法精确还原。
    """
    raw = ''
    for field in ACCOUNT_ID_IMPORT_FIELDS:
        value = row.get(field)
        if value not in (None, ''):
            raw = value
            break
    if raw == '':
        return '', '缺少账号ID'
    # 关键：只接受 str，拒绝 int/float
    if isinstance(raw, (int, float)):
        return '', '账号ID必须是文本格式，请在 Excel 中将该列设置为「文本」后重新导入（避免数字被转成科学计数法）'
    if not isinstance(raw, str):
        return '', '账号ID必须是文本格式'
    value = raw.strip()
    if not re.fullmatch(r'\d+', value):
        return '', '账号ID必须是纯数字文本，不能是科学计数法或带小数点'
    return value, ''


# 媒体/平台标准名称映射（统一前端展示与匹配口径）
_PLATFORM_ALIASES = {
    'facebook': 'Facebook', 'fb': 'Facebook', '脸书': 'Facebook',
    'google': 'Google', 'gg': 'Google', '谷歌': 'Google',
    'tiktok': 'TikTok', 'tt': 'TikTok', '抖音': 'TikTok', 'douyin': 'TikTok',
    'twitter': 'Twitter', 'x': 'Twitter',
    'instagram': 'Instagram', 'ins': 'Instagram',
    'youtube': 'YouTube', 'yt': 'YouTube',
    'snapchat': 'Snapchat', 'sc': 'Snapchat',
    'pinterest': 'Pinterest',
    'linkedin': 'LinkedIn',
}


def normalize_platform(val):
    """媒体/平台名称标准化。
    规则：去空白 → 转小写 → 查别名表 → 未命中则保留首字母大写原值。
    空值默认 'Facebook'。
    示例：'FB'/'facebook'/'脸书' → 'Facebook'，'GG' → 'Google'。
    """
    if val is None:
        return 'Facebook'
    s = str(val).strip()
    if not s:
        return 'Facebook'
    key = s.lower()
    if key in _PLATFORM_ALIASES:
        return _PLATFORM_ALIASES[key]
    # 未命中别名表：首字母大写，其余小写（如 'Facebook'）
    return s[0].upper() + s[1:].lower()


def normalize_channel(val):
    """渠道标准化：去首尾空白，转大写。空值返回空字符串。
    示例：' cx ' → 'CX'，'QQ' → 'QQ'。
    """
    if val is None:
        return ''
    s = str(val).strip()
    if not s:
        return ''
    return s.upper()


def normalize_client(val):
    """客户名称标准化：去首尾空白。空值返回空字符串。"""
    if val is None:
        return ''
    return str(val).strip()


def normalize_account_name(val):
    """账号名称标准化：去首尾空白。空值返回空字符串。"""
    if val is None:
        return ''
    return str(val).strip()


def normalize_rate(val):
    """服务点/费率标准化为小数（0~1）。
    规则（与前端录入口径一致：填 5 = 5% = 0.05）：
    1. 去除 % 号和空白
    2. 转 float
    3. 若值 > 1（如 5、1.5），除以 100 → 0.05、0.015
    4. 若值 ≤ 1（如 0.05），直接使用
    5. 解析失败返回 0
    示例：'5' → 0.05，'1.5' → 0.015，'5%' → 0.05，0.03 → 0.03。
    """
    if val is None:
        return 0.0
    s = str(val).strip().rstrip('%').strip()
    if not s:
        return 0.0
    try:
        n = float(s)
    except (ValueError, TypeError):
        return 0.0
    if n > 1:
        n = n / 100.0
    return round(n, 6)


def normalize_amount(val):
    """金额标准化为 float，兼容 Excel 文本金额及常见货币格式。"""
    if val is None:
        return 0.0
    if isinstance(val, bool):
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    s = unicodedata.normalize('NFKC', str(val)).strip()
    if not s:
        return 0.0
    # Excel 中以文本保存的金额可能带货币符号、千位分隔符、全角标点
    # 或用括号表示负数；先规范化这些展示格式，再交给 float 解析。
    negative = s.startswith('(') and s.endswith(')')
    if negative:
        s = s[1:-1].strip()
    s = (s.replace('\u00a0', '')
           .replace('\u202f', '')
           .replace(' ', '')
           .replace('\u3000', '')
           .replace('\u00a0', '')
           .replace('$', '')
           .replace('€', '')
           .replace('£', '')
           .replace('¥', '')
           .replace('元', '')
           .replace('USD', '')
           .replace('usd', '')
           .strip())
    if not s:
        return 0.0
    # 兼容 1.234,56（欧式）与 1,234.56（常规）两种分隔方式。
    if ',' in s and '.' in s and s.rfind(',') > s.rfind('.'):
        s = s.replace('.', '').replace(',', '.')
    else:
        s = s.replace(',', '')
    try:
        amount = float(s)
    except (ValueError, TypeError):
        # 有些系统导出的单元格是“USD 123.45 已到账”等文本，提取其中的数字。
        match = re.search(r'[-+]?(?:\d+(?:\.\d*)?|\.\d+)', s)
        if not match:
            return 0.0
        try:
            amount = float(match.group(0))
        except (ValueError, TypeError):
            return 0.0
    return -amount if negative else amount


def import_amount_cell(row, *names):
    """读取金额列；列名不标准时，从包含金额/amount 的列中选择可解析值。"""
    value = import_cell(row, *names)
    if value not in (None, '') and normalize_amount(value) != 0:
        return value
    candidates = []
    for key, raw in row.items():
        label = unicodedata.normalize('NFKC', str(key)).strip().lower()
        if '金额' in label or 'amount' in label or 'payment' in label:
            parsed = normalize_amount(raw)
            if parsed != 0:
                priority = 0 if ('打款' in label or '付款' in label or 'payment' in label) else 1
                candidates.append((priority, parsed, raw))
    if candidates:
        candidates.sort(key=lambda item: item[0])
        return candidates[0][2]
    return value


def import_cell(row, *names):
    """Read an import column with tolerant punctuation/spacing and common aliases."""
    normalized = {}
    for key, value in row.items():
        nk = ''.join(ch for ch in str(key).strip().lower() if ch not in ' \t\r\n-_（）()[]【】:')
        normalized[nk] = value
    for name in names:
        if name in row and row[name] not in (None, ''):
            return row[name]
        nk = ''.join(ch for ch in str(name).strip().lower() if ch not in ' \t\r\n-_（）()[]【】:') 
        if nk in normalized and normalized[nk] not in (None, ''):
            return normalized[nk]
    return ''


def parse_excel_bytes(data_bytes):
    try:
        from openpyxl import load_workbook
    except ImportError:
        raise Exception('需要安装 openpyxl: pip3 install openpyxl')

    def _excel_date_to_str(v):
        """Excel 日期序列号 → 'YYYY-MM-DD' 字符串。
        Excel 日期起点 1899-12-30，序列号 1 = 1900-01-01。
        常见范围：20000~60000 对应 1954~2064 年。"""
        try:
            n = int(v)
        except (TypeError, ValueError):
            return v
        if 20000 <= n <= 60000:
            from datetime import date, timedelta
            return (date(1899, 12, 30) + timedelta(days=n)).isoformat()
        return v

    wb = load_workbook(io.BytesIO(data_bytes), data_only=True)
    ws = wb.active

    header_row = None
    for row in ws.iter_rows(min_row=1, max_row=5, values_only=True):
        cells = [str(c).strip() if c else '' for c in row]
        if any(c for c in cells):
            header_row = cells
            break

    if not header_row:
        return []

    result = []
    for row in ws.iter_rows(min_row=2, values_only=True):
        if all(c is None or c == '' for c in row):
            continue
        d = {}
        for i, h in enumerate(header_row):
            if h and i < len(row):
                val = row[i]
                # 仅日期列允许转换 Excel 日期；金额列即使被设置为日期格式，也必须保留原始数字。
                header_key = ''.join(ch for ch in str(h).strip().lower()
                                     if ch not in ' \t\r\n-_（）()[]【】:')
                is_date_column = any(token in header_key for token in ('日期', 'date', '时间', 'time'))
                if is_date_column and hasattr(val, 'isoformat'):
                    val = val.isoformat()
                elif is_date_column and isinstance(val, (int, float)) and not isinstance(val, bool):
                    val = _excel_date_to_str(val)
                elif not is_date_column and hasattr(val, 'year') and hasattr(val, 'month') and hasattr(val, 'day'):
                    # openpyxl 对“日期格式”的金额单元格可能直接返回 datetime，转回 Excel 序列值。
                    try:
                        from openpyxl.utils.datetime import to_excel
                        val = float(to_excel(val))
                    except (TypeError, ValueError):
                        pass
                d[str(h).strip()] = val
        if d:
            result.append(d)
    return result


# ============================================================
# 匹配逻辑
# ============================================================
def guess_from_name(name):
    if not name:
        return {'client': '', 'channel': '', 'rate': 0}
    ch_patterns = [['QQ', 'QQ'], ['qq', 'QQ'], ['CX', 'CX'], ['cx', 'CX'],
                   ['FB+', 'FB'], ['FB-', 'FB'], ['卡台', '卡台'], ['卡台2', '卡台2'],
                   ['橘子', '橘子钱包'], ['云峰', '云峰'], ['直播户', '直播'],
                   ['Plus', 'PLUS'], ['PLUS', 'PLUS']]
    ch = ''
    for kw, v in ch_patterns:
        if kw in name:
            ch = v; break
    rate = 0
    m = re.search(r'[+\(](\d+\.?\d*)[+\)]', name)
    if m:
        try:
            r = float(m.group(1))
            if 0 < r < 100: rate = 0.05
        except ValueError: pass
    cl_patterns = [['MX', 'muxi'], ['muxi', 'muxi'], ['Ben', 'Ben'], ['环球', '环球'],
                   ['HQ', '环球'], ['DT', '环球'], ['VO', '实销'], ['实销', '实销']]
    cl = ''
    for kw, v in cl_patterns:
        if kw in name: cl = v; break
    if not cl and '巴基斯坦' in name: cl = 'fb'
    return {'client': cl, 'channel': ch, 'rate': rate}


def consume_row_date(raw):
    """从 BM 原始行取消耗日期（兼容多套表头别名），返回 YYYY-MM-DD；取不到/解析失败返回 ''。
    2026-09-09 抽为公共函数：导入层要用它把「未下户被拒」与「缺日期被拒」分开统计。"""
    row_date = str((raw or {}).get('报告开始日期') or (raw or {}).get('单日') or (raw or {}).get('日期')
                   or (raw or {}).get('报告日期') or (raw or {}).get('起始日期') or (raw or {}).get('Date') or '').strip()
    return to_short_date(row_date)


def consume_row_has_date(row):
    """消耗行是否带有效日期（date='' 的历史「幽灵行」不算）"""
    return bool(str((row or {}).get('date') or '').strip())


def fee_raw(spend, rate):
    """返回原始浮点服务费；只在最终展示/输出边界取整。"""
    return float(spend or 0) * float(rate or 0)


def row_fee_raw(row):
    """从一条消耗记录直接计算原始浮点服务费。"""
    return fee_raw(row.get('spend', 0) or 0, row.get('serviceRate', 0) or 0)


def enrich_consume_row(row, mapping_index, name_to_aid=None):
    """把 BM 原始消耗行 + mapping_index(按区间归属) 结合，生成一条标准消耗记录。
    - mapping_index 结构: {accountId: [segments]}，由 build_mapping_index() 产生
    - name_to_aid: {账号名称: accountId}，当账号ID匹配不到下户表时，用账号名称兜底匹配
    - 若同 accountId 有多个段，则按 consume 日期 ∈ [date, endDate||∞] 选段；
    - 无法匹配下户表的返回 None（会被过滤）。
    """
    # 2026-09-07 统一表头：报告开始日期、账号名称、账号ID、消耗金额（兼容旧列名别名）
    account_id = normalize_account_id(row.get('账号ID') or row.get('账户ID') or row.get('账户编号')
                     or row.get('accountId') or row.get('Account ID') or row.get('ID') or '')
    row_date = str(row.get('报告开始日期') or row.get('单日') or row.get('日期')
                   or row.get('报告日期') or row.get('起始日期') or row.get('Date') or '').strip()
    row_name = normalize_account_name(row.get('账号名称') or row.get('账户名称') or row.get('账户名') or '')
    if not account_id:
        return None
    consume_date = to_short_date(row_date)
    # 2026-09-09 修复「日消耗表幽灵行」：日期缺失/无法解析的行【不入库】。
    #   此前这类行会写成 date='' 的消耗记录（spend 通常为 0），导致：
    #   ① 日消耗明细多出一条空白日期 $0 行；② 「📅 天数」虚高；
    #   ③ 用区间查询时该行又消失 → 天数/合计前后跳变。
    #   常见来源：Excel 末尾的「合计」行、空行、日期列被改成文本格式。
    if not consume_date:
        return None
    m = pick_mapping_segment(mapping_index, account_id, consume_date)
    # 2026-09-07 兜底：账号ID没匹配到下户表时，用账号名称匹配下户表 name 字段
    if not m and row_name and name_to_aid:
        aid_by_name = normalize_account_id(name_to_aid.get(row_name) or '')
        if aid_by_name:
            account_id = aid_by_name
            m = pick_mapping_segment(mapping_index, aid_by_name, consume_date)
    if not m:
        # 没匹配到下户表 → 属于错误数据，不入库
        return None
    row_media = normalize_platform(row.get('媒体') or row.get('平台') or '')
    m_platform = normalize_platform(m.get('platform') or m.get('media') or '')
    # 2026-09-08 防呆：消耗金额列必须存在，否则判定为非 BM 消耗文件（如误导入充值表）→ 拒绝
    #   区分「列不存在」与「列存在但金额为 0」：后者允许（当天无消耗），前者拒绝
    # 2026-09-10 sed 全局替换把"消费"改成"消耗"时误伤了 BM 原始表头识别 → 下列加回"消费金额""消费"作为兼容性别名
    SPEND_COLUMNS = ('含回流消耗', '含回流消费',
                     '消耗金额', '消费金额', '消费',
                     '已花费金额 (USD)', '已花费金额', '花费',
                     'Amount Spent (USD)', 'Amount Spent', 'Spend (USD)', 'Spend',
                     '花费金额', '支出')
    has_spend_col = any(row.get(c) is not None for c in SPEND_COLUMNS)
    if not has_spend_col:
        return None
    raw_spend = 0
    for c in SPEND_COLUMNS:
        v = row.get(c)
        if v is not None and str(v).strip() != '':
            raw_spend = normalize_amount(v)
            if raw_spend:
                break
    # 2026-09-07：账号名称优先从下户表回填（Excel 空缺时用 mapping.name）
    final_name = row_name or normalize_account_name(m.get('name', '') or '')
    # 2026-09-10 加回 BM 原始表头别名「含回流消费」「回流消费」
    reflow_raw = next((row.get(c) for c in ('含回流消耗', '含回流消费', '回流消耗', '回流消费', '含回流金额') if row.get(c) not in (None, '')), raw_spend)
    included_spend = normalize_amount(reflow_raw)
    return {
        'date': consume_date,
        'accountName': final_name,
        'accountId': account_id,
        'spend': included_spend,
        'spend_including_reflow': included_spend,
        'spend_usd_total': included_spend,
        'reflow_difference': 0,
        'client': m.get('client', ''),
        'channel': m.get('channel', ''),
        # 2026-09-04 媒体一致性校验：原始文件里的媒体单独记录，由导入层与下户段比对（不一致 → 拒绝）
        'media_from_file': row_media,
        # 优先从原始 BM Excel 取，再回退下户段（一致时二者相同）
        'platform': row_media or m_platform,
        'serviceRate': m.get('rate', 0) or 0,
        'matched': True,
        # 可选：记录命中的下户段区间，便于调试（不会参与展示/汇总）
        '_segStart': m.get('date', '') or '',
        '_segEnd':   m.get('endDate', '') or '',
    }

# ============================================================
# HTTP Handler
# ============================================================

# 2026-09-12 公网穿透安全加固：自定义服务器类，加入连接数限制 + socket 调优
_MAX_CONCURRENT_CONN = 50   # 同时最大活跃连接数（穿透场景保守值）
_conn_count = 0
_conn_lock = threading.Lock()

class SecureServer(ThreadingHTTPServer):
    """ThreadingHTTPServer + 并发连接数限制 + socket SO_LINGER 关闭。"""
    allow_reuse_address = True
    daemon_threads = True

    def server_bind(self):
        # 关闭 socket  linger，快速回收 TIME_WAIT
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
        except (OSError, AttributeError):
            pass
        super().server_bind()

    def get_request(self):
        global _conn_count
        with _conn_lock:
            if _conn_count >= _MAX_CONCURRENT_CONN:
                print('[conn-limit] 并发连接已达上限 %d，拒绝新连接' % _MAX_CONCURRENT_CONN, flush=True)
                fake = FakeConn()
                return fake, self.server_address
            _conn_count += 1
        req, addr = super().get_request()
        # 使用 weakref.finalize 在 socket 被 GC 时自动递减计数
        # （macOS Python 3.9 的 socket.close 是只读属性，不能直接重写）
        try:
            import weakref
            weakref.finalize(req, _decrement_conn_count)
        except Exception:
            pass
        return req, addr


def _decrement_conn_count():
    global _conn_count
    with _conn_lock:
        _conn_count = max(_conn_count - 1, 0)


class FakeConn:
    """连接数超限时的假连接对象。"""
    def makefile(self, *a, **kw):
        raise OSError('Connection closed (limit reached)')
    def close(self): pass
    def settimeout(self, *a): pass
    def fileno(self): return -1
class ApiHandler(BaseHTTPRequestHandler):
    server_version = "srv"   # 中性化指纹（防针对性爬/攻击特征匹配）
    sys_version = ''
    def version_string(self):
        return 'srv'

    def log_message(self, fmt, *args):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {fmt % args}")

    def _send_json(self, data, status=200):
        body = json.dumps(data, ensure_ascii=False).encode('utf-8')
        etag = '"J' + hashlib.md5(body).hexdigest()[:16] + '"'
        if self.headers.get('If-None-Match', '') == etag:
            self.send_response(304)
            self.send_header('ETag', etag)
            self.send_header('Cache-Control', 'no-cache')
            self._security_headers()
            self.end_headers()
            return
        gz = None
        if len(body) >= 1024 and 'gzip' in (self.headers.get('Accept-Encoding') or ''):
            gz = gzip.compress(body, compresslevel=6)
        self.send_response(status)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Vary', 'Accept-Encoding')      # 2026-09-06：同一 URL 可能 gzip/明文，声明协商维度
        if gz is not None:
            self.send_header('Content-Encoding', 'gzip')
            self.send_header('Content-Length', str(len(gz)))
        else:
            self.send_header('Content-Length', str(len(body)))
        self.send_header('Cache-Control', 'no-cache')   # 带 ETag 协商缓存：未变 304 极快
        self.send_header('ETag', etag)
        self._security_headers()
        self.end_headers()
        self.wfile.write(gz if gz is not None else body)

    def _send_file(self, path, content_type='text/html; charset=utf-8'):
        # 2026-09-12 静态文件缓存命中：启动时预加载 + 预 gzip → 零磁盘 IO 零 CPU 压缩
        cache_entry = _STATIC_CACHE.get(path)
        if cache_entry is not None:
            etag = cache_entry['etag']
            if self.headers.get('If-None-Match', '') == etag:
                self.send_response(304)
                self.send_header('ETag', etag)
                self._security_headers()
                self.end_headers()
                return
            accept_gz = 'gzip' in (self.headers.get('Accept-Encoding') or '')
            data = cache_entry['raw']
            gz = cache_entry['gz'] if accept_gz else None
            self.send_response(200)
            self.send_header('Content-Type', content_type)
            self.send_header('Vary', 'Accept-Encoding')
            if gz is not None:
                self.send_header('Content-Encoding', 'gzip')
                self.send_header('Content-Length', str(len(gz)))
            else:
                self.send_header('Content-Length', str(len(data)))
            self.send_header('Cache-Control', 'no-store, no-cache, must-revalidate, max-age=0')
            self.send_header('Pragma', 'no-cache')
            self.send_header('ETag', etag)
            self._security_headers()
            self.end_headers()
            self.wfile.write(gz if gz is not None else data)
            return

        # ---- 未命中缓存（运行期动态生成的文件），走原始磁盘读取 + 按需 gzip ----
        with open(path, 'rb') as f:
            data = f.read()
        st = os.stat(path)
        etag = '"F%d-%d"' % (st.st_mtime_ns & 0xffffffff, len(data))   # 文件标识：内容不变 ETag 不变
        if self.headers.get('If-None-Match', '') == etag:
            self.send_response(304)
            self.send_header('ETag', etag)
            self._security_headers()
            self.end_headers()
            return
        gz = None
        if len(data) >= 1024 and 'gzip' in (self.headers.get('Accept-Encoding') or ''):
            gz = gzip.compress(data, compresslevel=6)   # 同一 URL 有 gzip/明文两种形态 → 必须声明 Vary（见下方 send_header）
        self.send_response(200)
        self.send_header('Content-Type', content_type)
        self.send_header('Vary', 'Accept-Encoding')      # 2026-09-06 协商维度声明（防中间代理串味）
        if gz is not None:
            self.send_header('Content-Encoding', 'gzip')
            self.send_header('Content-Length', str(len(gz)))
        else:
            self.send_header('Content-Length', str(len(data)))
        # HTML 强制不缓存（no-store）：修改后立即可见，避免开发/调试时缓存困扰
        self.send_header('Cache-Control', 'no-store, no-cache, must-revalidate, max-age=0')
        self.send_header('Pragma', 'no-cache')
        self.send_header('ETag', etag)
        self._security_headers()
        self.end_headers()
        self.wfile.write(gz if gz is not None else data)

    MAX_JSON_BODY = 16 * 1024 * 1024      # 2026-09-06：单次请求体上限（8MB 凭证的 base64 ≈ 10.7MB，留余量）

    def _read_body(self):
        # 2026-09-12 公网穿透安全加固：明确拒绝 Chunked Transfer-Encoding
        # Python 内置 HTTPHandler 不支持分块编码，有此头即视为恶意/畸形请求
        if self.headers.get('Transfer-Encoding', '').strip().lower() == 'chunked':
            self.close_connection = True
            self._send_json({'error': '不允许的编码方式'}, 400)
            raise _BodyRejected()
        try:
            length = int(self.headers.get('Content-Length', 0))
        except (TypeError, ValueError):
            self._send_json({'error': 'bad Content-Length'}, 400); return None
        if length == 0: return None
        if length > self.MAX_JSON_BODY:
            # 2026-09-06：超限不读体（防内存 DoS），并强制断连，避免残留请求体被当成下一个请求（请求走私）
            self.close_connection = True
            self._send_json({'error': '请求体过大（上限 %dMB）' % (self.MAX_JSON_BODY // 1024 // 1024)}, 413)
            return False                     # False = 已响应，调用方需中止
        return self.rfile.read(length)

    def _read_json(self):
        body = self._read_body()
        if body is False: raise _BodyRejected()      # 413/400 已响应 → 立即结束，避免二次写响应
        if not body: return {}
        try:
            return json.loads(body.decode('utf-8'))
        except Exception:
            self._send_json({'error': '请求体不是合法 JSON'}, 400)
            raise _BodyRejected()

    def _get_token(self):
        cookie = self.headers.get('Cookie', '')
        for part in cookie.split(';'):
            part = part.strip()
            if part.startswith('token='): return part[6:]
        auth = self.headers.get('Authorization', '')
        if auth.startswith('Bearer '): return auth[7:]
        return None

    def _me(self):
        return get_user_by_token(self._get_token())

    def _require_login(self):
        u = self._me()
        if not u:
            self._send_json({'error': '未登录', 'code': 401}, 401)
            return None
        return u

    def _require_perm(self, key):
        u = self._require_login()
        if not u: return None
        perms = get_role_permission(u.get('role', DEFAULT_ROLE))
        if not perms.get(key, False):
            self._send_json({'error': '没有权限', 'code': 403, 'need': key}, 403)
            return None
        return u

    def _att_visible(self, u, rec):
        # 凭证可见性 = 该笔账对该账号可见（复用统一数据范围过滤，客户角色只看得见自己的凭证）
        if rec is None: return False
        try:
            return bool(self._filter_for_user([rec], u))
        except Exception:
            return u.get('role') == 'admin'

    def _apply_account_status_locked(self, account_id, on_date, new_status, operator, now_str=''):
        """2026-09-06：账户状态只存下户段。把状态写回「account_id 在 on_date 命中的归属段」，
        新增充值(POST)与行内改状态(PUT)共用这一条逻辑。
        ⚠️ 调用方必须已持有 _lock —— _lock 不可重入，助手内部再取锁会自死锁。"""
        if not new_status or new_status not in MAPPING_STATUS_VALID or not account_id:
            return ''
        mapping_list = load_json(MAPPING_FILE)
        seg = pick_mapping_segment(build_mapping_index(mapping_list), account_id, str(on_date or ''))
        if not seg:
            return ''
        applied = False
        for mrow in mapping_list:
            if (str(mrow.get('accountId')) == str(seg.get('accountId'))
                    and str(mrow.get('date', '')) == str(seg.get('date', ''))):
                mrow['status'] = new_status
                mrow['updatedBy'] = operator
                mrow['updatedAt'] = now_str or datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                applied = True
                break
        if not applied:
            return ''
        save_json(MAPPING_FILE, mapping_list)
        self._refresh_consume_matching()
        return new_status

    def _require_money_delete(self, what='打款/退款记录'):
        # 资金凭证删除属高危：仅「管理员 + 财务」角色可删（后端强校验，不依赖前端隐藏按钮）
        u = self._require_login()
        if not u: return None
        perms = get_role_permission(u.get('role', DEFAULT_ROLE))
        if not perms.get('can_delete_money', False):
            self._send_json({'error': '仅管理员和财务可以删除%s' % what, 'code': 403, 'need': 'can_delete_money'}, 403)
            return None
        return u

    def _is_https_request(self):
        """判断当前请求是否通过 HTTPS（代理层会加 X-Forwarded-Proto=https 头）"""
        if FORCE_SECURE_COOKIE: return True
        proto = (self.headers.get('X-Forwarded-Proto') or '').lower()
        return proto.startswith('https')

    def _set_token_cookie(self, token):
        secure_flag = '; Secure' if self._is_https_request() else ''
        path = API_BASE_PATH + '/' if API_BASE_PATH else '/'
        self.send_header('Set-Cookie',
            f'token={token}; Path={path}; HttpOnly; SameSite=Lax; Max-Age={SESSION_MAX_AGE}{secure_flag}')

    def _clear_token_cookie(self):
        secure_flag = '; Secure' if self._is_https_request() else ''
        path = API_BASE_PATH + '/' if API_BASE_PATH else '/'
        self.send_header('Set-Cookie', f'token=; Path={path}; HttpOnly; Max-Age=0{secure_flag}')

    def _effective_perms(self, user):
        """2026-09-06 权限中心新增：账号级「导出数据」可选权限（can_export）。
        缺省（未设置）= 跟随角色默认；一旦在权限中心勾选/取消，即按账号生效。"""
        perms = dict(get_role_permission(user.get('role', DEFAULT_ROLE)))
        if user and 'can_export' in user:
            perms['can_export'] = bool(user.get('can_export'))
        return perms

    def _require_export(self):
        u = self._require_login()
        if not u: return None
        if not self._effective_perms(u).get('can_export', False):
            self._send_json({'error': '没有数据导出权限', 'code': 403, 'need': 'can_export'}, 403)
            return None
        return u

    def _channel_blind(self, user):
        #2026-09-06 渠道信息属于内部投放分工，不对客户/被屏蔽渠道的账号下发（服务端剥离，前端拿不到）
        if not user: return True
        if user.get('hide_channel'): return True
        role = user.get('role', DEFAULT_ROLE)
        if get_role_permission(role).get('can_see') == 'own': return True
        return False

    def _client_scope_allowed(self, user):
        """单客户数据展示规则：返回该账号可见的客户集合。
        - 客户角色：绑定客户 ∪ scope.clients（并集，任填其一即生效）；两者都空 = 空集合（看不到数据）
        - 其他角色：仅 scope.clients（未配置 = None = 不限制）
        """
        if not user: return None
        role = user.get('role', DEFAULT_ROLE)
        perms = get_role_permission(role)
        scope = user.get('scope') or {}
        allowed = set()
        if perms.get('can_see') == 'own' and user.get('client', ''):
            allowed.add(user.get('client'))
        allowed |= set(scope.get('clients') or [])
        if perms.get('can_see') == 'own' and not allowed:
            return set()          # 客户角色既无绑定也无范围 → 空集
        return allowed if allowed else None

    def _filter_for_user(self, data, user, client_key='client'):
        """数据展示过滤（单客户规则）：
        - 客户集合按 _client_scope_allowed（绑定客户 ∪ scope.clients）
        - 平台/渠道维度按 scope 逐维 AND 过滤；行内没有对应字段（如打款/退款）则不受该维限制。"""
        if not user: return data
        allowed = self._client_scope_allowed(user)
        if allowed is not None:
            data = [r for r in data if (r.get(client_key) or '') in allowed]
        scope = user.get('scope') or {}
        ps = scope.get('platforms') or []
        if ps:
            data = [r for r in data if ('platform' not in r) or ((r.get('platform') or '') in ps)]
        cs = scope.get('channels') or []
        if cs:
            data = [r for r in data if ('channel' not in r) or ((r.get('channel') or '') in cs)]
        return data

    def _security_headers(self):
        """安全响应头 + CORS（2026-09-11 新增：部署到跨域/子路径场景时自动生效）
        2026-09-12 穿透加固：补充 Permissions-Policy + Expect-CT 提示。"""
        self.send_header('X-Frame-Options', 'DENY')
        self.send_header('X-Content-Type-Options', 'nosniff')
        self.send_header('Referrer-Policy', 'no-referrer')
        self.send_header('X-Robots-Tag', 'noindex, nofollow, noarchive')
        self.send_header('Content-Security-Policy',
                         "default-src 'self'; script-src 'self' 'unsafe-inline'; "
                         "style-src 'self' 'unsafe-inline'; img-src 'self' data:; "
                         "connect-src 'self'; frame-ancestors 'none'; base-uri 'self'")
        # 2026-09-12 穿透加固：限制浏览器可访问的 API（摄像头/麦克风等）
        self.send_header('Permissions-Policy',
                         'camera=(), microphone=(), geolocation=(), payment=()')
        # --- CORS ---
        if CORS_ORIGIN:
            origin = self.headers.get('Origin', '')
            allow = CORS_ORIGIN
            if CORS_ORIGIN != '*' and origin:
                allowed = [o.strip() for o in CORS_ORIGIN.split(',')]
                if origin in allowed: allow = origin
                else: allow = ''   # 不在白名单 → 不发 CORS 头（浏览器不接受）
            if allow:
                self.send_header('Access-Control-Allow-Origin', allow)
                self.send_header('Access-Control-Allow-Credentials', 'true')
                self.send_header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS')
                self.send_header('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-Requested-With')
                self.send_header('Vary', 'Origin')

    def _api_guard(self):
        """反爬虫闸：/api/* 需浏览器 Ajax 标识。放行返回 True。
        速率限制已由 do_GET / do_POST 入口处统一处理，此处不再重复。"""
        try:
            parsed = urllib.parse.urlparse(self.path)
        except Exception:
            return True
        if not parsed.path.startswith('/api/'):
            return True
        if parsed.path == '/api/health':
            return True
        xrw = self.headers.get('X-Requested-With', '')
        auth = self.headers.get('Authorization', '')
        if xrw != 'XMLHttpRequest' and not auth.startswith('Bearer '):
            self._send_json({'error': 'forbidden'}, 403)
            return False
        return True

    def do_OPTIONS(self):
        """CORS 预检：返回 204 + CORS 头（2026-09-11 改）"""
        self.send_response(204)
        self.send_header('Content-Length', '0')
        self._security_headers()
        self.end_headers()

    # ----------------------------------------------------------
    # HEAD — 预览代理/健康检查用；BaseHTTPRequestHandler 默认返回 501 会被判定为不健康
    # ----------------------------------------------------------
    def do_HEAD(self):
        try:
            hp = urllib.parse.urlparse(self.path).path.rstrip('/')
        except Exception:
            hp = ''
        if hp not in ('', '/'):
            self.send_response(405)
            self.send_header('Content-Length', '0')
            self.end_headers()
            return
        self.send_response(200)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.send_header('Content-Length', '0')
        self._security_headers()
        self.end_headers()

    # ----------------------------------------------------------
    # GET
    # ----------------------------------------------------------
    def do_GET(self):
        # 2026-09-12 公网穿透安全加固：per-IP 通用速率限制（60s × 120次）
        real_ip = _extract_real_ip(self)
        ok, retry, cnt = _rate_check(real_ip, RATE_GENERAL_WINDOW, RATE_GENERAL_MAX)
        if not ok:
            self.send_response(429)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('Retry-After', str(retry))
            self.send_header('X-RateLimit-Remaining', '0')
            self.send_header('X-RateLimit-Reset', str(int(time.time()) + retry))
            self._security_headers()
            self.end_headers()
            self.wfile.write('{"error":"请求过于频繁，请稍后再试"}'.encode('utf-8'))
            print(f'[429] rate-limit GET ip={real_ip} count={cnt}', flush=True)
            return
        # 2026-09-06 统一异常兜底：内部异常只回 500 JSON（不外泄堆栈、不留半截响应）
        try:
            self._GET_impl()
        except _BodyRejected:
            return
        except (BrokenPipeError, ConnectionResetError):
            return
        except Exception as e:
            print('[500] GET', repr(e)[:220], flush=True)
            try:
                self._send_json({'error': '服务器处理失败，请稍后重试（详情见服务端日志）'}, 500)
            except Exception:
                pass

    def _GET_impl(self):
        if not self._api_guard(): return
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path.rstrip('/')
        # 2026-09-11：子路径部署时剥离 API_BASE_PATH 前缀（如 /myapp/api/login → /api/login）
        if API_BASE_PATH and path.startswith(API_BASE_PATH):
            path = path[len(API_BASE_PATH):] or '/'
            parsed = parsed._replace(path=path)
        real_ip = _extract_real_ip(self)
        cookie = self.headers.get('Cookie','')
        print(f'[GET] {path} from {real_ip} cookie=***' if cookie else f'[GET] {path} from {real_ip} no-cookie', flush=True)
        qs = urllib.parse.parse_qs(parsed.query)

        # 反爬虫第一道：robots.txt 全面禁止（配合 X-Robots-Tag）
        if path == '/robots.txt':
            body = b'User-agent: *\nDisallow: /\n'
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain; charset=utf-8')
            self.send_header('Content-Length', str(len(body)))
            self._security_headers()
            self.end_headers()
            self.wfile.write(body)
            return

        # 静态文件
        if path in ('', '/', '/login.html'):
            self._send_file(os.path.join(BASE_DIR, 'index.html'))
            return
        if path.startswith('/static/') or path.endswith(('.html','.js','.css','.ico','.png','.svg')):
            # 2026-09-04 安全加固：路径穿越防护 —— 解析真实路径后必须仍在 BASE_DIR 内
            file_path = os.path.realpath(os.path.join(BASE_DIR, path.lstrip('/')))
            base_real = os.path.realpath(BASE_DIR)
            if not (file_path == base_real or file_path.startswith(base_real + os.sep)):
                self._send_json({'error': 'not found'}, 404); return
            # 数据目录（含凭证图片与 JSON 账本）一律禁止静态直读：只能走带鉴权的 /attachments 路由
            if file_path == os.path.realpath(DATA_DIR) or file_path.startswith(os.path.realpath(DATA_DIR) + os.sep):
                self._send_json({'error': 'not found'}, 404); return
            if os.path.exists(file_path):
                ct = 'text/html' if file_path.endswith('.html') else \
                     'application/javascript' if file_path.endswith('.js') else \
                     'text/css' if file_path.endswith('.css') else \
                     'image/svg+xml' if file_path.endswith('.svg') else \
                     'application/octet-stream'
                self._send_file(file_path, ct + '; charset=utf-8')
                return

        # 原始凭证图片：非 /api 前缀便于 <img> 直读（自动带 cookie），但必须登录 + 数据范围可见
        if path.startswith('/attachments/'):
            u = self._me()
            if not u:
                self._send_json({'error': '未登录'}, 401); return
            seg = path.split('/')
            if len(seg) < 5 or seg[2] not in ATTACH_KIND_FILE:
                self._send_json({'error': 'not found'}, 404); return
            kind, rec_id, att_id = seg[2], seg[3], seg[4]
            rec = _att_find(kind, rec_id)
            if not rec or not self._att_visible(u, rec):
                self._send_json({'error': '无权查看该凭证'}, 403); return
            meta = next((x for x in (rec.get('attachments') or []) if str(x.get('id')) == str(att_id)), None)
            if not meta:
                self._send_json({'error': 'not found'}, 404); return
            fpath = os.path.realpath(os.path.join(_att_dir(kind, rec_id), os.path.basename(str(meta.get('file') or ''))))
            root = os.path.realpath(ATTACH_DIR)
            if not (fpath == root or fpath.startswith(root + os.sep)) or not os.path.isfile(fpath):
                self._send_json({'error': '文件缺失'}, 404); return
            with open(fpath, 'rb') as f:
                blob = f.read()
            self.send_response(200)
            self.send_header('Content-Type', meta.get('mime') or 'application/octet-stream')
            self.send_header('Content-Length', str(len(blob)))
            self.send_header('Cache-Control', 'private, max-age=86400')
            self.send_header('Vary', 'Cookie')            # 凭证按登录态返回，缓存需按用户区分
            self.send_header('X-Frame-Options', 'DENY')
            self.send_header('X-Content-Type-Options', 'nosniff')
            self.end_headers()
            self.wfile.write(blob)
            return

        # 公开 API
        if path == '/api/health':
            self._send_json({'ok': True, 'v': _DATA_VERSION}); return

        if path == '/api/auth/me':
            u = self._me()
            if u:
                role_info = self._effective_perms(u)   # 2026-09-06：含账号级 can_export 覆盖
                self._send_json({'user': u, 'roleInfo': role_info, 'permissions': role_info})
            else:
                self._send_json({'user': None})
            return
        if path == '/api/ops-log':
            u = self._require_perm('can_export')
            if not u: return
            log = load_json(OPS_LOG_FILE)
            mod = (qs.get('module') or [''])[0]
            if mod:
                log = [x for x in log if x.get('module') == mod]
            self._send_json(log[-500:][::-1]); return

        if path == '/api/settings':
            u = self._require_perm('can_manage_users')   # 含 SMTP 授权码，仅管理员可读
            if not u: return
            s = load_settings()
            s['backup'] = get_backup_cfg()
            self._send_json(s); return


        # --- Meta API Token 管理 ---
        if path == '/api/meta/tokens':
            u = self._require_perm('can_manage_users')
            if not u: return
            self._send_json({'ok': True, 'tokens': load_meta_tokens()}); return

        # === 2026-09-24 三平台自动刷新配置 GET ===
        if path == '/api/platform/auto-sync-cfg':
            u = self._require_perm('can_manage_users')
            if not u: return
            cfg = load_platform_auto_cfg()
            with _PLATFORM_AUTO_STATE_LOCK:
                state = {k: dict(v) for k, v in _PLATFORM_AUTO_STATE.items()}
            self._send_json({'ok': True, 'cfg': cfg, 'state': state}); return

        if path == '/api/meta/auto-sync-cfg':
            # 旧路由 wrapper → 返回 meta 子配置 + 旧 Meta 状态
            u = self._require_perm('can_manage_users')
            if not u: return
            full_cfg = load_platform_auto_cfg()
            cfg = full_cfg.get('meta', {})
            with _META_SYNC_STATE_LOCK:
                state = {k: v for k, v in _META_SYNC_STATE.items()}
            self._send_json({'ok': True, 'cfg': cfg, 'syncState': state}); return

        if path.startswith('/api/meta/tokens/') and path.endswith('/test'):
            u = self._require_perm('can_manage_users')
            if not u: return
            tk_id = path.split('/')[-2]
            items = load_json(META_TOKENS_FILE)
            target = next((x for x in items if x.get('id') == tk_id), None)
            if not target:
                self._send_json({'ok': False, 'error': 'Token 不存在'}, 404); return
            plain = _decrypt_meta_token(target.get('token_enc',''))
            res = test_meta_token(plain, target.get('bm_id', ''))
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            target['lastCheckedAt'] = now
            target['status'] = 'ok' if res.get('ok') else ('expired' if 'expired' in str(res.get('error','')).lower() else 'warn')
            _meta_save_tokens(items)
            log_op(u, 'Meta API', '校验Token', (target.get('name') or '') + ' -> ' + ('OK' if res.get('ok') else str(res.get('error'))))
            self._send_json(res); return

        if path.startswith('/api/meta/sync'):
            u = self._require_perm('can_manage_users')
            if not u: return
            since = (qs.get('since') or [''])[0]
            until = (qs.get('until') or [''])[0]
            mode = (qs.get('mode') or ['manual'])[0]

            # 2026-09-11：一键同步未传日期时默认拉取"上个月整月"
            if not since or not until:
                today = date.today()
                # 上个月最后一天 = 本月1号 - 1天
                last_month_end = (today.replace(day=1) - timedelta(days=1))
                last_month_start = last_month_end.replace(day=1)
                since = last_month_start.strftime('%Y-%m-%d')
                until = last_month_end.strftime('%Y-%m-%d')

            tk_list = load_meta_tokens_plain()
            if not tk_list:
                self._send_json({'ok': False, 'error': '请先在管理员设置里添加 Meta Token'}, 400); return

            # 互斥锁：一键同步与自动刷新不能同时执行
            acquired = _META_SYNC_LOCK.acquire(blocking=False)
            if not acquired:
                self._send_json({
                    'ok': False,
                    'error': '当前已有同步任务在执行中，请等待完成后再操作',
                    'syncState': {k: v for k, v in _META_SYNC_STATE.items()}
                }, 423); return

            try:
                with _META_SYNC_STATE_LOCK:
                    _META_SYNC_STATE['running'] = True
                    _META_SYNC_STATE['mode'] = mode
                    _META_SYNC_STATE['startedAt'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                res = meta_sync_consume(tk_list, since, until)
                summary = {}; rows_total = 0
                for tid, info in res.items():
                    if tid.startswith('_'): continue   # 跳过 _meta_sync_meta 等元数据 key
                    summary[tid] = {k: info.get(k) for k in ('bm_name','accounts','rows','errors','meta_user','dateFrom','dateTo')}
                    rows_total += info.get('rows', 0)
                self._send_json({'ok': True, 'summary': summary, 'totalRows': rows_total, 'detail': res, 'mode': mode}); return
            finally:
                with _META_SYNC_STATE_LOCK:
                    _META_SYNC_STATE['running'] = False
                    _META_SYNC_STATE['mode'] = ''
                _META_SYNC_LOCK.release()

        # === 2026-09-24 TikTok 新增：GET 路由 ===
        if path == '/api/tiktok/tokens':
            u = self._require_perm('can_manage_users')
            if not u: return
            self._send_json({'ok': True, 'tokens': load_tiktok_tokens()}); return

        if path.startswith('/api/tiktok/tokens/') and path.endswith('/test'):
            u = self._require_perm('can_manage_users')
            if not u: return
            tk_id = path.split('/')[-2]
            items = load_json(TIKTOK_TOKENS_FILE)
            target = next((x for x in items if x.get('id') == tk_id), None)
            if not target:
                self._send_json({'ok': False, 'error': 'Token 不存在'}, 404); return
            plain = {
                'access_token': _decrypt_tiktok(target.get('access_token_enc','')),
                'business_center_id': target.get('business_center_id',''),
            }
            res = test_tiktok_token(plain['access_token'], plain['business_center_id'])
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            target['lastCheckedAt'] = now
            target['status'] = 'ok' if res.get('ok') else ('expired' if 'expired' in str(res.get('error','')).lower() else 'warn')
            _tiktok_save_tokens(items)
            log_op(u, 'TikTok API', '校验Token', (target.get('name') or '') + ' -> ' + ('OK' if res.get('ok') else str(res.get('error'))))
            self._send_json(res); return

        if path.startswith('/api/tiktok/sync'):
            u = self._require_perm('can_manage_users')
            if not u: return
            since = (qs.get('since') or qs.get('fromDate') or [''])[0]
            until = (qs.get('until') or qs.get('toDate') or [''])[0]
            if not since or not until:
                today = date.today()
                last_month_end = (today.replace(day=1) - timedelta(days=1))
                last_month_start = last_month_end.replace(day=1)
                since = since or last_month_start.strftime('%Y-%m-%d')
                until = until or last_month_end.strftime('%Y-%m-%d')
            tk_list = load_tiktok_tokens_plain()
            if not tk_list:
                self._send_json({'ok': False, 'error': '请先在管理员设置里添加 TikTok Token'}, 400); return
            res = tiktok_sync_consume(tk_list, since, until)
            # 同步后若 token 有刷新 access_token 或其他字段变更，这里不写回（避免跨域改动；v1 简单策略）
            summary = {}; rows_total = 0
            for tid, info in res.items():
                summary[tid] = {k: info.get(k) for k in ('bc_name','accounts','rows','errors','dateFrom','dateTo')}
                rows_total += info.get('rows', 0)
            self._send_json({'ok': True, 'summary': summary, 'totalRows': rows_total, 'detail': res, 'source': 'tiktok'}); return

        # === 2026-09-24 Google Ads 新增：GET 路由 ===
        if path == '/api/google/tokens':
            u = self._require_perm('can_manage_users')
            if not u: return
            self._send_json({'ok': True, 'tokens': load_google_tokens()}); return

        if path.startswith('/api/google/tokens/') and path.endswith('/test'):
            u = self._require_perm('can_manage_users')
            if not u: return
            tk_id = path.split('/')[-2]
            items = load_json(GOOGLE_TOKENS_FILE)
            target = next((x for x in items if x.get('id') == tk_id), None)
            if not target:
                self._send_json({'ok': False, 'error': 'Token 不存在'}, 404); return
            plain = {
                'developer_token': _decrypt_google(target.get('developer_token_enc','')),
                'access_token': _decrypt_google(target.get('access_token_enc','')),
                'refresh_token': _decrypt_google(target.get('refresh_token_enc','')),
                'client_id': _decrypt_google(target.get('client_id_enc','')),
                'client_secret': _decrypt_google(target.get('client_secret_enc','')),
                'customer_id': target.get('customer_id',''),
                'name': target.get('name',''),
            }
            res = test_google_token(plain)
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            target['lastCheckedAt'] = now
            target['status'] = 'ok' if res.get('ok') else ('expired' if 'expired' in str(res.get('error','')).lower() else 'warn')
            # 如果测试时自动刷新了 access_token，这里写回
            if res.get('new_access_token'):
                target['access_token_enc'] = _encrypt_google(res['new_access_token'])
            _google_save_tokens(items)
            log_op(u, 'Google Ads API', '校验Token', (target.get('name') or '') + ' -> ' + ('OK' if res.get('ok') else str(res.get('error'))))
            self._send_json(res); return

        if path.startswith('/api/google/sync'):
            u = self._require_perm('can_manage_users')
            if not u: return
            since = (qs.get('since') or qs.get('fromDate') or [''])[0]
            until = (qs.get('until') or qs.get('toDate') or [''])[0]
            if not since or not until:
                today = date.today()
                last_month_end = (today.replace(day=1) - timedelta(days=1))
                last_month_start = last_month_end.replace(day=1)
                since = since or last_month_start.strftime('%Y-%m-%d')
                until = until or last_month_end.strftime('%Y-%m-%d')
            tk_list = load_google_tokens_plain()
            if not tk_list:
                self._send_json({'ok': False, 'error': '请先在管理员设置里添加 Google Token'}, 400); return
            res = google_sync_consume(tk_list, since, until)
            # 如果本次同步中有 token 刷新了 access_token，这里把明文写回加密存储
            changed = False
            plain_access_map = {e.get('id'): e.get('access_token') for e in tk_list}
            items = load_json(GOOGLE_TOKENS_FILE)
            for it in items:
                if it.get('id') in plain_access_map:
                    new_plain = plain_access_map[it['id']]
                    if new_plain and _decrypt_google(it.get('access_token_enc','')) != new_plain:
                        it['access_token_enc'] = _encrypt_google(new_plain)
                        changed = True
            if changed:
                _google_save_tokens(items)
            summary = {}; rows_total = 0
            for tid, info in res.items():
                summary[tid] = {k: info.get(k) for k in ('account_name','accounts','rows','errors','dateFrom','dateTo','access_token_refreshed')}
                rows_total += info.get('rows', 0)
            self._send_json({'ok': True, 'summary': summary, 'totalRows': rows_total, 'detail': res, 'source': 'google'}); return

        if path == '/api/auth/roles':
            if not self._require_login(): return     # 2026-09-06 角色权限矩阵不再对未登录者开放
            self._send_json({k: v for k, v in ROLES.items()})
            return

        # 需登录
        # 备份下载（另存为）：?all=1 附带历史备份包
        if path == '/api/backup/download':
            u = self._require_perm('can_manage_users')
            if not u: return
            want_all = (qs.get('all') or [''])[0] in ('1', 'true', 'yes')
            zpath = make_backup_zip(mask_secret=True)
            if want_all and os.path.isdir(BACKUP_DIR):
                # 「全量迁移包」= 当前备份 zip + 最近 10 个历史备份包 一并打包
                stamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                allpath = os.path.join(BACKUP_DIR, f'全量迁移包_{stamp}.zip')
                prev = sorted([f for f in os.listdir(BACKUP_DIR) if f.endswith('.zip')])[-10:]
                with zipfile.ZipFile(allpath, 'w', zipfile.ZIP_DEFLATED) as za:
                    for f in prev:
                        za.write(os.path.join(BACKUP_DIR, f), arcname=f)
                    za.writestr('恢复说明.txt', BACKUP_README)
                zpath = allpath
            size = os.path.getsize(zpath)
            self.send_response(200)
            self.send_header('Content-Type', 'application/zip')
            self.send_header('Content-Length', str(size))
            fn = urllib.parse.quote(os.path.basename(zpath))
            self.send_header('Content-Disposition', f"attachment; filename*=UTF-8''{fn}")
            self.send_header('Cache-Control', 'no-store')
            self._security_headers()
            self.end_headers()
            with open(zpath, 'rb') as f:
                self.wfile.write(f.read())
            return

        if path == '/api/users':
            u = self._require_perm('can_manage_users')
            if not u: return
            users = load_users()
            safe = [{k: v for k, v in x.items() if k != 'password'} for x in users]
            self._send_json(safe); return

        if path == '/api/mapping':
            u = self._require_login()
            if not u: return
            with _lock: data = load_json(MAPPING_FILE)
            data = self._filter_for_user(data, u)
            self._send_json(data); return

        if path == '/api/recharge':
            u = self._require_login()
            if not u: return
            with _lock:
                data = load_json(RECHARGE_FILE)
                mapping_list = load_json(MAPPING_FILE)
            # 数据互通（2026-09-04 转户规则）：为每行回填【充值日期所属段】的客户/媒体/渠道/状态/费率，
            # 保证「充值管理」表显客户与 概览统计 的归属一致（均按充值日期）
            midx = build_mapping_index(mapping_list)
            for r in data:
                aid = str(r.get('accountId','') or '')
                seg = pick_mapping_segment(midx, aid, str(r.get('date','') or '')) if aid else None
                # 2026-09-07：所有冗余字段（客户/账号名称/渠道/时区/状态/费率）都从下户表权威回填
                if seg:
                    r['periodClient']      = seg.get('client','')
                    r['periodAccountName'] = seg.get('name','')
                    r['periodPlatform']    = seg.get('platform') or 'Facebook'
                    r['periodChannel']     = seg.get('channel','')
                    r['periodStatus']      = seg.get('status') or '正常'
                    r['periodRate']        = seg.get('rate', 0) or 0
                    r['periodTimezone']    = seg.get('timezone','')
                    # 同时覆盖原始字段（前端优先读原始字段，period 作兜底）
                    r['accountName'] = seg.get('name','')
                    r['client']      = seg.get('client','')
                    r['channel']     = seg.get('channel','')
                    r['platform']    = seg.get('platform') or 'Facebook'
                    r['timezone']    = seg.get('timezone','')
            client = qs.get('client', [None])[0]
            if client: data = [r for r in data if (r.get('periodClient') or r.get('client')) == client]
            data = self._filter_for_user(data, u)
            self._send_json(data); return

        if path == '/api/payment':
            u = self._require_login()
            if not u: return
            with _lock: data = load_json(PAYMENT_FILE)
            client = qs.get('client', [None])[0]
            if client: data = [r for r in data if r.get('client') == client]
            data = self._filter_for_user(data, u)
            self._send_json(data); return

        if path == '/api/refund':
            u = self._require_login()
            if not u: return
            with _lock: data = load_json(REFUND_FILE)
            client = qs.get('client', [None])[0]
            if client: data = [r for r in data if r.get('client') == client]
            # 2026-09-04 统一权限链：退款同样走 _filter_for_user（绑定客户 ∪ 数据范围），
            # 退款行的 client 不因退款被改名，"已退客户凭证仍可见"由该行 client 本身保证
            data = self._filter_for_user(data, u)
            self._send_json(data); return

        if path == '/api/service-fee':
            u = self._require_login()
            if not u: return
            with _lock: data = load_json(SERVICE_FEE_FILE)
            self._send_json(self._filter_for_user(data, u)); return

        if path == '/api/client_status':
            u = self._require_login()
            if not u: return
            with _lock: data = load_json(CLIENT_STATUS_FILE)
            # 2026-09-04 查缺补漏：客户账号只能看到自己客户的退款/终止状态
            allowed = self._client_scope_allowed(u)
            if allowed is not None:
                data = {k: v for k, v in (data or {}).items() if k in allowed}
            self._send_json(data); return

        # 账户日消耗查询
        m = re.match(r'^/api/consume/account/(.+)$', path)
        if m:
            u = self._require_login()
            if not u: return
            aid = m.group(1)
            with _lock:
                consume  = load_json(CONSUME_FILE)
                recharge = load_json(RECHARGE_FILE)
                mapping  = load_json(MAPPING_FILE)
            consume  = self._filter_for_user(consume, u)
            recharge = self._filter_for_user(recharge, u)
            mapping  = self._filter_for_user(mapping, u)
            # 消耗明细（按日期升序）
            # 2026-09-09 修复：跳过历史遗留的 date='' 幽灵行 —— 否则日消耗明细会多出空白 $0 行、
            #   「📅 天数」虚高，叠加区间查询后行数还会跳变（读时过滤，不改写你的磁盘数据）
            rows = sorted([r for r in consume
                           if r.get('accountId') == aid and consume_row_has_date(r)],
                          key=lambda x: x.get('date', ''))
            # 充值清零明细
            recs = sorted([r for r in recharge if r.get('accountId') == aid],
                          key=lambda x: x.get('date', ''))
            # 账户信息
            info = next((m for m in mapping if str(m.get('accountId')) == aid), None)
            # 算累计消耗和余额变化（按日期序）
            cum_spend = 0
            daily = []
            for r in rows:
                included = r.get('spend_including_reflow', r.get('reflowSpend', r.get('spend', 0))) or 0
                total_usd = r.get('spend_usd_total', r.get('totalSpendUSD', r.get('spend', 0))) or 0
                cum_spend += included
                rate = r.get('serviceRate', 0) or 0
                daily.append({
                    'date':       r.get('date', ''),
                    'spend':      included,
                    'spend_including_reflow': included,
                    'spend_usd_total': total_usd,
                    'reflow_difference': included - total_usd,
                    'fee':        fee_raw(included, rate),
                    'rate':       rate,
                    'cumulative': round(cum_spend, 4),
                    'importDate': to_short_date(r.get('importedAt') or ''),
                    'overdue': bool(r.get('overdue')) or bool(r.get('segOut')),
                    'out': (r.get('segOut') or seg_out_reason(str(r.get('date') or ''),
                            {'date': r.get('_segStart') or '', 'endDate': r.get('_segEnd') or ''})),
                })
            # 算充值/清零累计
            cum_rech = 0; cum_clear = 0
            recs2 = []
            for r in recs:
                cum_rech  += r.get('amount', 0) or 0
                cum_clear += r.get('clear', 0) or 0
                recs2.append({
                    'date':     r.get('date', ''),
                    'amount':   r.get('amount', 0),
                    'clear':    r.get('clear', 0),
                    'cum_rech': round(cum_rech, 4),
                    'cum_clear':round(cum_clear, 4),
                })
            total_spend = cum_spend
            total_rech  = cum_rech
            total_clear = cum_clear
            # 2026-09-07 口径调整：单账号弹窗余额 = 累计充值 − 累计消耗
            #   清零/服务费仅作流水展示，不参与余额计算（用户明确口径）
            balance = total_rech - total_spend
            self._send_json({
                'accountId':    aid,
                'accountName':  (info or {}).get('name', '') or (rows[0].get('accountName') if rows else ''),
                'client':       (info or {}).get('client', ''),
                'channel':      ('' if self._channel_blind(u) else (info or {}).get('channel', '')),
                'platform':     (info or {}).get('platform', 'Facebook'),
                'rate':         (info or {}).get('rate', 0),
                'daily':        daily,
                'recharges':    recs2,
                'summary': {
                    'days':        len(daily),
                    'total_spend': round(total_spend, 4),
                    'total_fee':   round(sum(d['fee'] for d in daily), 2),
                    'total_rech':  round(total_rech, 4),
                    'total_clear': round(total_clear, 4),
                    'balance':     round(balance, 4),
                    # 审计修复 #3：超出归属期仍被兜底归属的天数（前端提示续期）
                    'overdue':     sum(1 for r in rows if r.get('overdue')),
                },
            }); return

        if path == '/api/consume':
            u = self._require_login()
            if not u: return
            with _lock: data = load_json(CONSUME_FILE)
            # 2026-09-09：date='' 的历史幽灵行不参与任何明细/透视/导出（读时过滤，磁盘数据不动）
            data = [r for r in data if consume_row_has_date(r)]
            date_from = qs.get('from', [None])[0]
            date_to   = qs.get('to',   [None])[0]
            client    = qs.get('client', [None])[0]
            account_id= qs.get('account_id', [None])[0]
            source    = qs.get('source', [None])[0]      # 2026-09-24: 新增渠道筛选
            platform  = qs.get('platform', [None])[0]    # 2026-09-24: 新增平台筛选
            limit     = int(qs.get('limit', [0])[0] or 0)
            if date_from: data = [r for r in data if r.get('date','') >= date_from]
            if date_to:   data = [r for r in data if r.get('date','') <= date_to]
            if client:    data = [r for r in data if r.get('client') == client]
            if account_id:data = [r for r in data if r.get('accountId') == account_id]
            if source:    data = [r for r in data if r.get('source','').startswith(source)]   # 支持前缀匹配
            if platform:  data = [r for r in data if normalize_platform(r.get('platform') or '') == normalize_platform(platform)]
            data = self._filter_for_user(data, u)
            # 2026-09-06：历史行若无 segOut，读时按已存段边界派生（只读内存，不写盘）
            for r in data:
                if 'segOut' not in r:
                    r['segOut'] = seg_out_reason(str(r.get('date') or ''),
                         {'date': r.get('_segStart') or '', 'endDate': r.get('_segEnd') or ''})
                if 'overdue' not in r:
                    r['overdue'] = bool(r.get('segOut'))
            if limit > 0: data = data[:limit]
            self._send_json({'total': len(data), 'rows': data}); return

        if path == '/api/consume/stats':
            u = self._require_login()
            if not u: return
            self._send_json(self._compute_consume_stats(u, refresh_matching=True,
                date_from=(qs.get('from') or [None])[0], date_to=(qs.get('to') or [None])[0])); return

        if path == '/api/consume/import-log':
            u = self._require_perm('can_export')   # 2026-09-04 查缺补漏：导入历史仅内部角色可见
            if not u: return
            log = load_json(IMPORT_LOG_FILE)
            self._send_json(log[-50:]); return

        if path == '/api/import/errors':
            u = self._require_perm('can_export')
            if not u: return
            fname = (qs.get('file') or [''])[0]
            # 路径穿越防护：只允许纯文件名，不含任何路径分隔符
            if not fname or '/' in fname or '\\' in fname or '..' in fname:
                self._send_json({'error': 'invalid filename'}, 400); return
            fpath = os.path.join(IMPORT_ERROR_DIR, fname)
            # 二次防护：解析后的绝对路径必须在 IMPORT_ERROR_DIR 内
            try:
                real = os.path.realpath(fpath)
                if not real.startswith(os.path.realpath(IMPORT_ERROR_DIR)):
                    self._send_json({'error': 'forbidden'}, 403); return
            except OSError:
                self._send_json({'error': 'not found'}, 404); return
            if not os.path.isfile(fpath):
                self._send_json({'error': 'not found'}, 404); return
            try:
                with open(fpath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                self._send_json(data); return
            except (OSError, ValueError):
                self._send_json({'error': 'read failed'}, 500); return

        if path == '/api/export':
            u = self._require_export()
            if not u: return
            # 权限区分：导出内容按账号数据范围过滤（客户角色只见自己客户）
            mp = self._filter_for_user(load_json(MAPPING_FILE), u)
            rc = self._filter_for_user(load_json(RECHARGE_FILE), u)
            cs = self._filter_for_user(load_json(CONSUME_FILE), u)
            self._send_json({
                'mapping': mp,
                'recharge': rc,
                'consume_count': len(cs),
            }); return

        if path == '/api/stats':
            u = self._require_login()
            if not u: return
            with _lock:
                mapping  = load_json(MAPPING_FILE)
                recharge = load_json(RECHARGE_FILE)
                consume  = load_json(CONSUME_FILE)
            mapping  = self._filter_for_user(mapping, u)
            recharge = self._filter_for_user(recharge, u)
            consume  = self._filter_for_user(consume, u)
            # 2026-09-09：date='' 的历史幽灵行不计入 条数/维度 统计（金额为 0，属口径一致性修复）
            consume  = [r for r in consume if consume_row_has_date(r)]
            total_spend    = sum(r.get('spend', 0) for r in consume)
            total_recharge = sum(r.get('amount', 0) for r in recharge)
            total_clear    = sum(r.get('clear', 0) for r in recharge)
            total_fee      = sum(row_fee_raw(r) for r in consume)
            self._send_json({
                'mapping_count':     len(mapping),
                'recharge_count':    len(recharge),
                'consume_count':     len(consume),
                'consume_total_spend': round(total_spend, 2),
                'recharge_total':      round(total_recharge, 2),
                'clear_total':         round(total_clear, 2),
                'fee_total':           round(total_fee, 2),
                'mapping_clients':     len(set(m.get('client') for m in mapping if m.get('client'))),
                'mapping_channels':    len(set(m.get('channel') for m in mapping if m.get('channel'))),
                'mapping_platforms':   len(set(m.get('platform') for m in mapping if m.get('platform'))),
                'payment_total':       round(sum(p.get('amount', 0) for p in load_json(PAYMENT_FILE)), 2),
                'payment_count':       len(load_json(PAYMENT_FILE)),
                'refund_total':        round(sum(r.get('amount', 0) for r in load_json(REFUND_FILE)), 2),
                'refund_count':        len(load_json(REFUND_FILE)),
                'consume_all_spend':   round(sum(r.get('spend',0) for r in load_json(CONSUME_FILE)), 2),
                'consume_all_fee':     round(sum(row_fee_raw(r) for r in load_json(CONSUME_FILE) if consume_row_has_date(r)), 2),
                'last_import':         load_json(IMPORT_LOG_FILE)[-1] if load_json(IMPORT_LOG_FILE) else None,
            }); return

        self._send_json({'error': 'not found', 'path': path}, 404)

    # ----------------------------------------------------------
    # POST
    # ----------------------------------------------------------
    def do_POST(self):
        # 2026-09-12 公网穿透安全加固：per-IP 通用速率限制（60s × 120次）
        real_ip = _extract_real_ip(self)
        ok, retry, cnt = _rate_check(real_ip, RATE_GENERAL_WINDOW, RATE_GENERAL_MAX)
        if not ok:
            self.send_response(429)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('Retry-After', str(retry))
            self.send_header('X-RateLimit-Remaining', '0')
            self.send_header('X-RateLimit-Reset', str(int(time.time()) + retry))
            self._security_headers()
            self.end_headers()
            self.wfile.write('{"error":"请求过于频繁，请稍后再试"}'.encode('utf-8'))
            print(f'[429] rate-limit POST ip={real_ip} count={cnt}', flush=True)
            return
        # 2026-09-06 统一异常兜底：内部异常只回 500 JSON（不外泄堆栈、不留半截响应）
        try:
            self._POST_impl()
        except _BodyRejected:
            return
        except (BrokenPipeError, ConnectionResetError):
            return
        except Exception as e:
            print('[500] POST', repr(e)[:220], flush=True)
            try:
                self._send_json({'error': '服务器处理失败，请稍后重试（详情见服务端日志）'}, 500)
            except Exception:
                pass

    def _POST_impl(self):
        if not self._api_guard(): return
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path.rstrip('/')
        # 2026-09-11：子路径部署时剥离 API_BASE_PATH 前缀
        if API_BASE_PATH and path.startswith(API_BASE_PATH):
            path = path[len(API_BASE_PATH):] or '/'
            parsed = parsed._replace(path=path)
        real_ip = _extract_real_ip(self)
        cookie = self.headers.get('Cookie','')
        print(f'[POST] {path} from {real_ip} cookie=***' if cookie else f'[POST] {path} from {real_ip} no-cookie', flush=True)

        # --- 登录 ---
        if path == '/api/auth/login':
            # 2026-09-12 公网穿透安全加固：登录接口专用速率限制（60s × 10次/IP）
            ok2, retry2, cnt2 = _rate_check(real_ip, RATE_LOGIN_WINDOW, RATE_LOGIN_MAX)
            if not ok2:
                log_op({'ip': real_ip}, '系统', '登录限流', f'IP {real_ip} 超过登录速率上限')
                self.send_response(429)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Retry-After', str(retry2))
                self._security_headers()
                self.end_headers()
                self.wfile.write(json.dumps({'error': '登录请求过于频繁，请稍后再试'}).encode('utf-8'))
                print(f'[429] rate-limit login ip={real_ip} count={cnt2}', flush=True)
                return
            data = self._read_json()
            if data is None: return
            username = _limit_str(data.get('username') or '', STR_LIMIT_USERNAME)
            password = data.get('password') or ''
            if not username or not password:
                self._send_json({'error': '用户名和密码必填'}, 400); return
            client_ip = real_ip   # 2026-09-12 统一用 _extract_real_ip 获取穿透后真实 IP
            lst = _login_status(client_ip, username)
            if not lst['allowed']:
                remain_s = max(0, int(lst['locked_until'] - time.time()))
                log_op({'username': username}, '系统', '登录锁定', f'IP {client_ip} 用户 {username} 连续失败触发限流，剩 {remain_s}s')
                self._send_json({
                    'error': f'密码错误次数过多，请 {remain_s // 60} 分 {remain_s % 60} 秒后再试',
                    'locked': True,
                    'retryAfter': remain_s,
                    'lockedUntil': lst['locked_until'],
                }, 429); return
            users = load_users()
            matched = None
            for x in users:
                if x.get('username') == username and x.get('active', True) \
                        and verify_password(password, x.get('password', '')):
                    matched = x; break
            if not matched:
                st = _login_fail(client_ip, username)
                log_op({'username': username}, '系统', '登录失败', f'IP {client_ip} 用户 {username} 密码错误')
                if st['allowed']:
                    hint = f'（还剩 {st["remaining"]} 次机会）' if st['remaining'] <= 2 else ''
                    self._send_json({'error': '用户名或密码错误' + hint, 'remaining': st['remaining']}, 401); return
                else:
                    remain_s = max(0, int(st['locked_until'] - time.time()))
                    self._send_json({
                        'error': f'密码错误 5 次，账号已锁定 {remain_s // 60} 分 {remain_s % 60} 秒',
                        'locked': True,
                        'retryAfter': remain_s,
                        'lockedUntil': st['locked_until'],
                    }, 429); return
            _LOGIN_FAILS.pop(_login_key(client_ip, username), None)   # 登录成功清空失败计数
            token = create_session(matched['id'])
            safe = {k: v for k, v in matched.items() if k != 'password'}
            roleInfo = self._effective_perms(matched)   # 2026-09-06：含账号级 can_export 覆盖
            body = json.dumps({'ok': True, 'token': token, 'user': safe, 'roleInfo': roleInfo},
                              ensure_ascii=False).encode('utf-8')
            self.send_response(200)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('Content-Length', str(len(body)))
            self._set_token_cookie(token)
            self.end_headers()
            self.wfile.write(body); return

        # 2026-09-12 公网穿透安全加固：上传接口统一限速（60s × 5次/IP），防大包 DoS + 爆破
        if path.endswith('/import') or path in ('/api/backup/run', '/api/backup/restore'):
            ok3, retry3, cnt3 = _rate_check(real_ip, RATE_UPLOAD_WINDOW, RATE_UPLOAD_MAX)
            if not ok3:
                self.send_response(429)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Retry-After', str(retry3))
                self._security_headers()
                self.end_headers()
                self.wfile.write(json.dumps({'error': '上传过于频繁，请稍后再试'}).encode('utf-8'))
                print(f'[429] rate-limit upload ip={real_ip} path={path} count={cnt3}', flush=True)
                return

        if path == '/api/auth/logout':
            token = self._get_token()
            if token: revoke_session(token)
            body = json.dumps({'ok': True}).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('Content-Length', str(len(body)))
            self._clear_token_cookie()
            self.end_headers()
            self.wfile.write(body); return

        if path == '/api/auth/change-password':
            u = self._require_login()
            if not u: return
            data = self._read_json()
            old_pw = data.get('oldPassword') or ''
            new_pw = data.get('newPassword') or ''
            if len(new_pw) < 6:
                self._send_json({'error': '新密码至少 6 位'}, 400); return
            users = load_users(); ok = False
            for x in users:
                if x.get('id') == u['id']:
                    if not verify_password(old_pw, x.get('password', '')):
                        self._send_json({'error': '原密码不对'}, 400); return
                    x['password'] = hash_password(new_pw); ok = True; break
            if ok: save_json(USERS_FILE, users)
            log_op(u, '系统', '修改密码', f"{u.get('username','')} 修改了自己的登录密码")
            self._send_json({'ok': True}); return

        # --- 数据备份（admin）---
        if path == '/api/backup/run':
            u = self._require_perm('can_manage_users')
            if not u: return
            executed, msg = run_backup_cycle(force=True)
            self._send_json({'ok': executed, 'message': msg}); return

        # 恢复：先暂存上传 zip，再用密码确认执行
        if path == '/api/backup/stage':
            u = self._require_perm('can_manage_users')
            if not u: return
            try: clen = int(self.headers.get('Content-Length', 0) or 0)
            except Exception: clen = 0
            if clen > 50 * 1024 * 1024:
                self._send_json({'error': '文件过大（上限 50MB）'}, 413); return
            file_data, _ = _extract_uploaded_file(self)
            if not file_data:
                self._send_json({'error': '需要上传 zip 备份包'}, 400); return
            sid = gen_id()
            sdir = os.path.join(DATA_DIR, '.stages'); os.makedirs(sdir, exist_ok=True)
            now = time.time()
            for f in os.listdir(sdir):
                try:
                    if now - os.path.getmtime(os.path.join(sdir, f)) > 7200: os.remove(os.path.join(sdir, f))
                except OSError: pass
            with open(os.path.join(sdir, sid + '.zip'), 'wb') as f: f.write(file_data)
            self._send_json({'ok': True, 'id': sid}); return

        if path == '/api/backup/restore':
            u = self._require_perm('can_manage_users')
            if not u: return
            data = self._read_json()
            if not _check_user_password(u, str(data.get('password') or '')):
                self._send_json({'error': '密码校验失败'}, 403); return
            sid = re.sub(r'[^a-zA-Z0-9_-]', '', str(data.get('id') or ''))
            spath = os.path.join(DATA_DIR, '.stages', sid + '.zip')
            if not sid or not os.path.exists(spath):
                self._send_json({'error': '暂存已过期，请重新上传'}, 400); return
            ALLOWED = {'mapping.json','consume.json','recharge.json','payment.json','refund.json','service_fee.json',
                       'client_status.json','import_log.json','ops_log.json','users.json','settings.json'}
            try:
                zf = zipfile.ZipFile(spath)
            except Exception:
                self._send_json({'error': 'zip 无效或已损坏'}, 400); return
            names = [n for n in zf.namelist() if not n.endswith('/')]
            # 路径安全：含目录分隔符/.. 的直接拒绝；非白名单 .json 也拒绝；说明文件忽略
            # 2026-09-06 允许 uploads/<kind>/<id>/<img> 一并恢复；仍拦 .. / 反斜杠 / 绝对路径
            susp = [n for n in names if '..' in n or chr(92) in n or n.startswith('/')]
            if susp:
                self._send_json({'error': '包内存在非法路径：' + '、'.join(susp[:5])}, 400); return
            IMG_EXT = ('.png', '.jpg', '.jpeg', '.gif', '.webp')
            def _ok_img(nm):
                parts = nm.split('/')
                return (len(parts) == 4 and parts[0] == 'uploads' and parts[1] in ATTACH_KIND_FILE
                        and all(parts[2:4]) and parts[3].lower().endswith(IMG_EXT))
            bad = [n for n in names if n.endswith('.json') and n not in ALLOWED]
            bad += [n for n in names if '/' in n and not _ok_img(n)]
            if bad:
                self._send_json({'error': '包含不允许的文件：' + '、'.join(bad[:5])}, 400); return
            apply_names = [n for n in names if n in ALLOWED]
            img_names = [n for n in names if _ok_img(n)]
            if not apply_names and not img_names:
                self._send_json({'error': '包内没有可恢复的数据文件'}, 400); return
            snapshot = make_backup_zip() if _has_business_data() else None   # 恢复前自动快照；数据本为空则跳过
            restored = []
            for n in apply_names:
                try:
                    obj = json.loads(zf.read(n).decode('utf-8'))
                except Exception:
                    self._send_json({'error': n + ' 内容不是合法 JSON'}, 400); return
                # 备份包内 settings 的 SMTP 授权码出于安全被置空；恢复到本机时保留本机现值
                if n == 'settings.json':
                    try:
                        cur = load_json(os.path.join(DATA_DIR, 'settings.json')) or {}
                        cc = ((cur.get('backup') or {}).get('smtp_pass')) or ''
                        if isinstance(obj, dict) and not ((obj.get('backup') or {}).get('smtp_pass')) and cc:
                            obj.setdefault('backup', {})['smtp_pass'] = cc
                    except Exception:
                        pass
                save_json(os.path.join(DATA_DIR, n), obj); restored.append(n)
            for n in img_names:                       # 凭证图片逐文件写回（realpath 防穿越 + 50MB/张上限）
                blob = zf.read(n)
                tgt = os.path.realpath(os.path.join(DATA_DIR, n))
                root_real = os.path.realpath(ATTACH_DIR)
                if not tgt.startswith(root_real + os.sep): continue
                if len(blob) > 16 * 1024 * 1024: continue
                os.makedirs(os.path.dirname(tgt), exist_ok=True)
                with open(tgt, 'wb') as fimg:
                    fimg.write(blob)
            restored += img_names
            zf.close()
            try: os.remove(spath)
            except OSError: pass
            log_op(u, '系统', '恢复备份', f'恢复 {len(restored)} 个文件' +
                  (f'；恢复前快照 {os.path.basename(snapshot)}' if snapshot else '；恢复前数据为空，未生成快照'))
            self._send_json({'ok': True, 'restored': restored,
                             'snapshot': (os.path.basename(snapshot) if snapshot else '')}); return

        # 一键清空：业务数据 + 备份（保留账号/登录/设置/审计日志，操作前先自动安全快照并保留该快照）
        if path == '/api/data/wipe':
            u = self._require_perm('can_manage_users')
            if not u: return
            data = self._read_json()
            if not _check_user_password(u, str(data.get('password') or '')):
                self._send_json({'error': '密码校验失败'}, 403); return
            # 2026-09-07 修订（用户选择）：取消 DELETE 关键词，只复核登录密码（密码错误仍 403）
            snapshot = make_backup_zip() if _has_business_data() else None   # 安全网：清空前自动本地快照；数据本为空则跳过
            snapname = os.path.basename(snapshot) if snapshot else ''
            # 2026-09-06 修复：一键清原来漏了 refund.json（说明文档承诺清退款），现补齐
            for fn in ('mapping.json','consume.json','recharge.json','payment.json','refund.json','service_fee.json','import_log.json'):
                save_json(os.path.join(DATA_DIR, fn), [])
            save_json(os.path.join(DATA_DIR, 'client_status.json'), {})
            _att_purge_kind('payment'); _att_purge_kind('refund')   # 凭证图片同步清空
            if snapshot:   # 2026-09-07 数据本为空时不动既有备份（防止误删唯一全量备份）
                for fn in os.listdir(BACKUP_DIR):
                    if fn.endswith('.zip') and fn != snapname:
                        try: os.remove(os.path.join(BACKUP_DIR, fn))
                        except OSError: pass
            log_op(u, '系统', '一键清空', (f'业务数据与历史备份已清空；安全快照保留：{snapname}（备份目录/下载中心可见）'
                     if snapshot else '业务数据本为空，已跳过清空（未生成快照、未删除既有备份）'))
            self._send_json({'ok': True, 'snapshot': snapname})
            refresh_consume_stats_cache_clear(); return

        if path == '/api/backup/test':
            u = self._require_perm('can_manage_users')
            if not u: return
            zpath = make_backup_zip()
            ok, msg = send_backup_email(zpath, get_backup_cfg().get('emails',''))
            self._send_json({'ok': ok, 'message': msg}); return

        # --- 用户管理（admin）---
        if path == '/api/users':
            u = self._require_perm('can_manage_users')
            if not u: return
            data = self._read_json()
            username = _limit_str(data.get('username') or '', STR_LIMIT_USERNAME)
            password = data.get('password') or secrets.token_hex(8)
            role = data.get('role') or DEFAULT_ROLE
            if not username: self._send_json({'error': '用户名必填'}, 400); return
            if data.get('password') and len(data['password']) < 6:
                self._send_json({'error': '密码至少 6 位'}, 400); return
            if role not in ROLES: self._send_json({'error': '无效角色'}, 400); return
            users = load_users()
            if any(x.get('username') == username for x in users):
                self._send_json({'error': '用户名已存在'}, 400); return
            new_user = {
                'id': 'user_' + gen_id(), 'username': username,
                'password': hash_password(password), 'role': role,
                'name': data.get('name') or username, 'client': data.get('client') or '',
                # 2026-09-04 权限中心：功能权限（导航模块列表，None=全部）+ 数据展示范围（空=不限制）
                'nav': data.get('nav') if isinstance(data.get('nav'), list) else None,
                'scope': data.get('scope') if isinstance(data.get('scope'), dict) else None,
                'hide_channel': bool(data.get('hide_channel')),
                # 2026-09-06 权限中心新增：导出数据可选权限（显式设置即按账号生效）
                'can_export': bool(data.get('can_export')),
                'active': True, 'createdAt': datetime.now().isoformat(),
            }
            users.append(new_user)
            save_json(USERS_FILE, users)
            log_op(u, '账号管理', '新建', f"{username} 角色={role}")
            safe = {k: v for k, v in new_user.items() if k != 'password'}
            self._send_json({'ok': True, 'user': safe, 'password': password}); return

        # --- 下户表 Excel 导入 ---
        if path == '/api/mapping/import':
            u = self._require_perm('can_edit_mapping')
            if not u: return
            try:
                clen = int(self.headers.get('Content-Length', 0) or 0)
            except Exception:
                clen = 0
            if clen > 50 * 1024 * 1024:
                self._send_json({'error': '上传文件过大（上限 50MB）'}, 413); return
            file_data, filename = _extract_uploaded_file(self)
            if not file_data:
                self._send_json({'error': '需要上传 Excel 文件'}, 400); return
            try:
                self._handle_mapping_import(file_data, u, filename=filename)
            except Exception as e:
                _rate_deduct(real_ip)   # 导入失败退还计数，不占用限流名额
                self._send_json({'error': str(e)}, 400)
            return

        # --- 充值表 Excel 导入 ---
        if path == '/api/recharge/import':
            u = self._require_perm('can_edit_recharge')
            if not u: return
            try:
                clen = int(self.headers.get('Content-Length', 0) or 0)
            except Exception:
                clen = 0
            if clen > 50 * 1024 * 1024:
                self._send_json({'error': '上传文件过大（上限 50MB）'}, 413); return
            file_data, filename = _extract_uploaded_file(self)
            if not file_data:
                self._send_json({'error': '需要上传 Excel 文件'}, 400); return
            try:
                self._handle_recharge_import(file_data, u, filename=filename)
            except Exception as e:
                _rate_deduct(real_ip)   # 导入失败退还计数，不占用限流名额
                self._send_json({'error': str(e)}, 400)
            return

        # --- 打款批量导入（2026-09-07 新增：不验证账号ID，只验证客户信息）---
        if path == '/api/payment/import':
            u = self._require_perm('can_edit_payment')
            if not u: return
            try:
                clen = int(self.headers.get('Content-Length', 0) or 0)
            except Exception:
                clen = 0
            if clen > 50 * 1024 * 1024:
                self._send_json({'error': '上传文件过大（上限 50MB）'}, 413); return
            file_data, filename = _extract_uploaded_file(self)
            if not file_data:
                self._send_json({'error': '需要上传 Excel 文件'}, 400); return
            try:
                self._handle_payment_import(file_data, u, filename=filename)
            except Exception as e:
                _rate_deduct(real_ip)   # 导入失败退还计数，不占用限流名额
                self._send_json({'error': str(e)}, 400)
            return

        # --- 退款/平账导入（2026-09-07 新增，不验证账号ID）---
        if path == '/api/refund/import':
            u = self._require_perm('can_refund')
            if not u: return
            try:
                clen = int(self.headers.get('Content-Length', 0) or 0)
            except Exception:
                clen = 0
            if clen > 50 * 1024 * 1024:
                self._send_json({'error': '上传文件过大（上限 50MB）'}, 413); return
            file_data, filename = _extract_uploaded_file(self)
            if not file_data:
                self._send_json({'error': '需要上传 Excel 文件'}, 400); return
            try:
                self._handle_refund_import(file_data, u, filename=filename)
            except Exception as e:
                _rate_deduct(real_ip)   # 导入失败退还计数，不占用限流名额
                self._send_json({'error': str(e)}, 400)
            return

        # --- 消耗导入 ---
        if path == '/api/consume/import':
            u = self._require_perm('can_upload_consume')
            if not u: return
            try:
                clen = int(self.headers.get('Content-Length', 0) or 0)
            except Exception:
                clen = 0
            if clen > 50 * 1024 * 1024:
                self._send_json({'error': '上传文件过大（上限 50MB）'}, 413); return
            file_data, filename = _extract_uploaded_file(self)
            if not file_data:
                self._send_json({'error': '需要上传 Excel 文件'}, 400); return
            try:
                self._handle_consume_import(file_data, u, filename=filename)
            except Exception as e:
                _rate_deduct(real_ip)   # 导入失败退还计数，不占用限流名额
                self._send_json({'error': str(e)}, 400)
            return

        # --- 下户表 ---
        if path == '/api/mapping/batch':
            u = self._require_perm('can_edit_mapping')
            if not u: return
            data = self._read_json()
            items = data.get('items', [])
            upsert = data.get('upsert', True)
            with _lock:
                mapping = load_json(MAPPING_FILE)
                existing = {normalize_account_id(m.get('accountId')): m for m in mapping}
                added = updated = 0
                operator = u.get('name') or u.get('username')
                now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                for item in items:
                    aid = normalize_account_id(item.get('accountId', ''))
                    if not aid: continue
                    item['id'] = item.get('id') or gen_id()
                    item['accountId'] = aid
                    item.setdefault('active', True)
                    item.setdefault('createdBy', operator)
                    item.setdefault('createdAt', now_str)
                    normalize_mapping_status(item)
                    if aid in existing and upsert:
                        for k, v in item.items(): existing[aid][k] = v
                        normalize_mapping_status(existing[aid])
                        updated += 1
                    else:
                        mapping.append(item); added += 1
                save_json(MAPPING_FILE, mapping)
                self._refresh_consume_matching()
            log_op(u, '下户表', '批量导入', f"新增 {added} / 更新 {updated}")
            self._send_json({'added': added, 'updated': updated, 'total': len(mapping)}); return

        # 2026-09-08 下户表批量删除
        if path == '/api/mapping/batch-delete':
            u = self._require_perm('can_edit_mapping')
            if not u: return
            data = self._read_json()
            ids = data.get('ids', [])
            if not ids:
                self._send_json({'error': '请选择要删除的记录'}, 400); return
            with _lock:
                mapping = load_json(MAPPING_FILE)
                id_set = set(str(x) for x in ids)
                removed = [m for m in mapping if str(m.get('id')) in id_set]
                new_list = [m for m in mapping if str(m.get('id')) not in id_set]
                save_json(MAPPING_FILE, new_list)
                # 2026-09-08 级联删除：下户表账号删除后，同步删除充值清零表中对应 accountId 的记录
                removed_acc_ids = set(str(m.get('accountId')) for m in removed if m.get('accountId'))
                recharge_deleted = 0
                if removed_acc_ids:
                    try:
                        recharge = load_json(RECHARGE_FILE)
                        rech_before = len(recharge)
                        recharge = [r for r in recharge if str(r.get('accountId')) not in removed_acc_ids]
                        recharge_deleted = rech_before - len(recharge)
                        if recharge_deleted > 0:
                            save_json(RECHARGE_FILE, recharge)
                    except Exception:
                        pass
                self._refresh_consume_matching()
            log_op(u, '下户表', '批量删除', f"删除 {len(removed)} 条" + (f"，级联删除充值清零 {recharge_deleted} 条" if recharge_deleted else ""))
            self._send_json({'ok': True, 'deleted': len(removed), 'total': len(new_list), 'recharge_deleted': recharge_deleted}); return

        # 2026-09-08 下户表批量更新状态
        if path == '/api/mapping/batch-update':
            u = self._require_perm('can_edit_mapping')
            if not u: return
            data = self._read_json()
            ids = data.get('ids', [])
            field = data.get('field', '')
            value = data.get('value')
            if not ids or not field:
                self._send_json({'error': '请选择记录并指定要更新的字段'}, 400); return
            if field not in ('status', 'client', 'platform', 'channel', 'timezone', 'rate', 'note'):
                self._send_json({'error': '不支持批量更新该字段'}, 400); return
            with _lock:
                mapping = load_json(MAPPING_FILE)
                id_set = set(str(x) for x in ids)
                now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                operator = u.get('name') or u.get('username')
                updated = 0
                for m in mapping:
                    if str(m.get('id')) in id_set:
                        m[field] = value
                        m['updatedBy'] = operator
                        m['updatedAt'] = now_str
                        if field == 'status':
                            normalize_mapping_status(m)
                        if field == 'platform':
                            m['platform'] = normalize_platform(value)
                        if field == 'channel':
                            m['channel'] = normalize_channel(value)
                        if field == 'rate':
                            m['rate'] = normalize_rate(value)
                        updated += 1
                save_json(MAPPING_FILE, mapping)
                self._refresh_consume_matching()
            log_op(u, '下户表', '批量更新', f"更新 {updated} 条的 {field}")
            self._send_json({'ok': True, 'updated': updated}); return

        if path == '/api/mapping':
            u = self._require_perm('can_edit_mapping')
            if not u: return
            data = self._read_json()
            aid = normalize_account_id(data.get('accountId', ''))
            if not aid: self._send_json({'error': 'accountId 必填'}, 400); return
            # 2026-09-04 用户规则：下户表 客户 与 账号ID 均为必填
            if not normalize_client(data.get('client', '')):
                self._send_json({'error': '客户必填（下户表归属客户不能为空）'}, 400); return
            data['id'] = data.get('id') or gen_id()
            data['accountId'] = aid
            data['name'] = normalize_account_name(data.get('name', ''))
            data['client'] = normalize_client(data.get('client', ''))
            data['channel'] = normalize_channel(data.get('channel', ''))
            data.setdefault('active', True)
            # 2026-09-07 修复：统一规范化日期和时区，避免前端 readXlsx 解析出的 Excel 序列号/带时区字符串被原样保存
            data['date'] = to_short_date(data.get('date', ''))
            data['endDate'] = to_short_date(data.get('endDate', ''))
            data['timezone'] = normalize_timezone(data.get('timezone', ''))
            # 2026-09-08 媒体标准化
            data['platform'] = normalize_platform(data.get('platform', ''))
            # 2026-09-08 服务点标准化（与前端 /100 口径一致）
            data['rate'] = normalize_rate(data.get('rate', 0))
            normalize_mapping_status(data)
            operator = u.get('name') or u.get('username')
            now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            seg_date = str(data.get('date', '')).strip()
            with _lock:
                mapping = load_json(MAPPING_FILE)
                # 幂等 upsert —— 只覆盖同 (accountId, date) 的段，保留其他历史段
                replaced = [m for m in mapping
                            if normalize_account_id(m.get('accountId')) == aid and str(m.get('date', '')).strip() == seg_date]
                if replaced:
                    # 覆盖已有段：保留首次创建人信息，记录本次修改人
                    old = replaced[0]
                    data['createdBy'] = old.get('createdBy') or operator
                    data['createdAt'] = old.get('createdAt') or now_str
                    op_action = '修改'
                else:
                    data['createdBy'] = operator
                    data['createdAt'] = now_str
                    op_action = '新增'
                data['updatedBy'] = operator
                data['updatedAt'] = now_str
                mapping = [m for m in mapping
                           if not (normalize_account_id(m.get('accountId')) == aid and str(m.get('date', '')).strip() == seg_date)]
                mapping.append(data)
                save_json(MAPPING_FILE, mapping)
                self._refresh_consume_matching()
            log_op(u, '下户表', op_action, f"{aid} 客户={data.get('client','')} 段日期={seg_date or '-'}")
            self._send_json({'ok': True, 'id': data['id']}); return

        if path == '/api/mapping/clear':
            # POST 兼容：前端历史函数 clearMapping() 使用 POST
            u = self._require_perm('can_edit_mapping')
            if not u: return
            with _lock:
                before = len(load_json(MAPPING_FILE))
                save_json(MAPPING_FILE, [])
                self._refresh_consume_matching()
                after = len(load_json(MAPPING_FILE))
            log_op(u, '下户表', '清空', f'删除全部 {before} 条下户记录')
            self._send_json({'ok': True, 'deleted': before - after, 'total': after}); return

        if path == '/api/mapping/clear-column':
            # POST 兼容（和 DELETE 逻辑一致）
            u = self._require_perm('can_edit_mapping')
            if not u: return
            data = self._read_json() or {}
            column = str(data.get('column', '')).strip()
            ALLOW_CLEAR_FIELDS = {
                'client','channel','platform','name','note',
                'rate','endDate','status','timezone',
            }
            if not column or column not in ALLOW_CLEAR_FIELDS:
                self._send_json({'error': f'column 必须为以下之一：{", ".join(sorted(ALLOW_CLEAR_FIELDS))}'}, 400); return
            with _lock:
                mapping = load_json(MAPPING_FILE)
                touched = 0
                for m in mapping:
                    if column == 'rate':
                        if float(m.get('rate', 0) or 0) != 0:
                            m['rate'] = 0; touched += 1
                    elif column == 'endDate':
                        if m.get('endDate'):
                            m['endDate'] = ''; touched += 1
                    elif column == 'status':
                        # 清状态 = 回到默认 '正常'
                        if m.get('status') != MAPPING_STATUS_DEFAULT:
                            m['status'] = MAPPING_STATUS_DEFAULT; touched += 1
                    else:
                        if m.get(column):
                            m[column] = ''; touched += 1
                if touched:
                    save_json(MAPPING_FILE, mapping)
                    log_op(u, '下户表', '清空字段', f"字段={column}，清空 {touched} 行")
                    self._refresh_consume_matching()
            self._send_json({'ok': True, 'field': column, 'touched': touched, 'total': len(load_json(MAPPING_FILE))}); return

        # --- 充值 ---
        if path == '/api/recharge/batch':
            u = self._require_perm('can_edit_recharge')
            if not u: return
            items = self._read_json().get('items', [])
            with _lock:
                recharge = load_json(RECHARGE_FILE)
                for item in items:
                    item['id'] = item.get('id') or gen_id()
                    item.setdefault('createdBy', u.get('name') or u.get('username') if u else '-')
                    item.setdefault('createdAt', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                    recharge.append(item)
                save_json(RECHARGE_FILE, recharge)
            log_op(u, '充值管理', '批量导入', f"批量新增 {len(items)} 条")
            self._send_json({'added': len(items), 'total': len(recharge)}); return

        # 2026-09-08 充值批量删除
        if path == '/api/recharge/batch-delete':
            u = self._require_perm('can_edit_recharge')
            if not u: return
            data = self._read_json()
            ids = data.get('ids', [])
            if not ids:
                self._send_json({'error': '请选择要删除的记录'}, 400); return
            with _lock:
                recharge = load_json(RECHARGE_FILE)
                id_set = set(str(x) for x in ids)
                removed = [r for r in recharge if str(r.get('id')) in id_set]
                new_list = [r for r in recharge if str(r.get('id')) not in id_set]
                save_json(RECHARGE_FILE, new_list)
            log_op(u, '充值管理', '批量删除', f"删除 {len(removed)} 条")
            self._send_json({'ok': True, 'deleted': len(removed), 'total': len(new_list)}); return

        # 2026-09-08 充值批量更新（状态/充值金额/清零金额）
        if path == '/api/recharge/batch-update':
            u = self._require_perm('can_edit_recharge')
            if not u: return
            data = self._read_json()
            ids = data.get('ids', [])
            field = data.get('field', '')
            value = data.get('value')
            if not ids or not field:
                self._send_json({'error': '请选择记录并指定要更新的字段'}, 400); return
            if field not in ('status', 'amount', 'clear', 'date', 'note'):
                self._send_json({'error': '不支持批量更新该字段'}, 400); return
            with _lock:
                recharge = load_json(RECHARGE_FILE)
                id_set = set(str(x) for x in ids)
                now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                operator = u.get('name') or u.get('username')
                updated = 0
                for r in recharge:
                    if str(r.get('id')) in id_set:
                        if field in ('amount', 'clear'):
                            r[field] = round(float(value or 0), 2)
                        elif field == 'date':
                            r[field] = to_short_date(value)
                        else:
                            r[field] = value
                        r['updatedBy'] = operator
                        r['updatedAt'] = now_str
                        # 状态同步到下户表归属段（与单条修改同通路）
                        if field == 'status' and value in MAPPING_STATUS_VALID:
                            try:
                                self._apply_account_status_locked(r.get('accountId'), str(r.get('date') or ''),
                                                                  value, operator, now_str)
                            except Exception:
                                pass
                        updated += 1
                save_json(RECHARGE_FILE, recharge)
            log_op(u, '充值管理', '批量更新', f"更新 {updated} 条的 {field}")
            self._send_json({'ok': True, 'updated': updated}); return

        if path == '/api/recharge':
            u = self._require_perm('can_edit_recharge')
            if not u: return
            data = self._read_json()
            if not normalize_account_id(data.get('accountId', '')):
                self._send_json({'error': 'accountId 必填（充值必须关联广告账号）'}, 400); return
            data['id'] = data.get('id') or gen_id()
            # 数据互通：手工充值也按下户表回填 客户/媒体（与导入口径一致，保证媒体维度筛选可用）
            aid = normalize_account_id(data.get('accountId', ''))
            data['accountId'] = aid
            data['amount'] = normalize_amount(data.get('amount', 0))
            data['accountName'] = normalize_account_name(data.get('accountName', ''))
            if aid:
                m = next((x for x in load_json(MAPPING_FILE) if normalize_account_id(x.get('accountId')) == aid), None)
                if m:
                    data['client'] = normalize_client(data.get('client') or m.get('client', '') or '无客户')
                    data['platform'] = normalize_platform(data.get('platform') or m.get('platform') or '')
                else:
                    data['client'] = normalize_client(data.get('client') or '无客户')
            operator = u.get('name') or u.get('username')
            now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            data['createdBy'] = operator          # 服务端强制写入，防客户端伪造录入人
            data['createdAt'] = now_str
            # 2026-09-08：清零金额兼容「清零」文本 → 0
            _rc = data.get('clear', 0)
            if isinstance(_rc, str) and _rc.strip() == '清零':
                data['clear'] = 0.0
            else:
                try:
                    data['clear'] = float(_rc) if _rc else 0.0
                except (ValueError, TypeError):
                    data['clear'] = 0.0
            # 状态联动（2026-09-04 用户规则）：带 status 时写回该账户「充值日期命中段」的状态；
            # 清零默认写清零，账户重新启用时在充值表单把状态改回正常
            new_status = str(data.get('status') or '').strip()
            applied_status = ''
            if new_status and new_status in MAPPING_STATUS_VALID:
                data.pop('status', None)          # 状态只存在下户段，充值流水不冗余
                with _lock:
                    applied_status = self._apply_account_status_locked(aid, str(data.get('date', '') or ''),
                                                                       new_status, operator, now_str)
            with _lock:
                recharge = load_json(RECHARGE_FILE)
                recharge.append(data)
                save_json(RECHARGE_FILE, recharge)
            log_op(u, '充值清零', '新增', f"{aid} 客户={data.get('client','')} 充值${data.get('amount',0)} 清零${data.get('clear',0)}" + (f" → 状态={applied_status}" if applied_status else ''))
            self._send_json({'ok': True, 'id': data['id']}); return

        if path == '/api/payment':
            u = self._require_perm('can_edit_payment')
            if not u: return
            data = self._read_json()
            if not normalize_client(data.get('client', '')):
                self._send_json({'error': '客户必填'}, 400); return
            if not normalize_amount(data.get('amount', 0)):
                self._send_json({'error': '打款金额必填且不能为 0'}, 400); return
            data['id'] = data.get('id') or gen_id()
            data['client'] = normalize_client(data.get('client', ''))
            data['amount'] = normalize_amount(data.get('amount', 0))
            operator = u.get('name') or u.get('username')
            now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            data['createdBy'] = operator          # 服务端强制写入
            data['createdAt'] = now_str
            with _lock:
                payment = load_json(PAYMENT_FILE)
                payment.append(data)
                save_json(PAYMENT_FILE, payment)
            log_op(u, '打款管理', '新增', f"{data.get('client','')} ${data.get('amount',0)} {data.get('method','')}")
            self._send_json({'ok': True, 'id': data['id']}); return

        if path == '/api/refund':
            u = self._require_perm('can_refund')
            if not u: return
            data = self._read_json()
            client = normalize_client(data.get('client'))
            if not client:
                self._send_json({'error': '客户必填'}, 400); return
            refund_amount = normalize_amount(data.get('amount') or 0)
            refund_type = data.get('type') or 'cash'  # cash=现金退款, offset=平账补差
            refund_id = gen_id()
            refund_record = {
                'id': refund_id,
                'createdBy': u.get('name') or u.get('username'),
                'createdAt': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'date': data.get('date') or datetime.now().strftime('%Y-%m-%d'),
                'client': client,
                'amount': refund_amount,
                'method': data.get('method', '银行转账'),
                'type': refund_type,
                'note': data.get('note', ''),
                'operator': u.get('name') or u.get('username'),
            }
            with _lock:
                refunds = load_json(REFUND_FILE)
                refunds.append(refund_record)
                save_json(REFUND_FILE, refunds)
                # 2026-09-05 业务规则修正（用户明确）：退款 ≠ 终止合作
                #   常见场景只是「某笔打款打多了，客户退回多余部分」，客户仍需继续投放/继续退款；
                #   因此不再写 client_status=refunded，也不做任何下拉屏蔽。
                #   备款/可退额度的扣减由前端按「打款 −（消耗+服务费）− 已退现金」实时计算。
                log_op(u, '退款管理', '退款' if refund_type=='cash' else '平账', f"{client} ${refund_amount} {data.get('note','')}")
                # ⚠️ 平账模式：不修改 mapping/consume，数据继续显示，只记录财务凭证。
                #   后续客户重新对账时，依然能看到历史所有充值/消耗。
            self._send_json({'ok': True, 'id': refund_id}); return

        if path == '/api/service-fee':
            u = self._require_perm('can_edit_payment')
            if not u: return
            data = self._read_json()
            client = normalize_client(data.get('client'))
            if not client:
                self._send_json({'error': '客户必填'}, 400); return
            amount = normalize_amount(data.get('amount') or 0)
            if not amount or amount < 0:
                self._send_json({'error': '服务费金额必填且不能为 0'}, 400); return
            record = {
                'id': data.get('id') or gen_id(),
                'date': to_short_date(data.get('date') or datetime.now().strftime('%Y-%m-%d')),
                'client': client,
                'amount': amount,
                'note': _limit_str(data.get('note') or '', STR_LIMIT_NOTE),
                'createdBy': u.get('name') or u.get('username'),
                'createdAt': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            }
            with _lock:
                rows = load_json(SERVICE_FEE_FILE)
                rows.append(record)
                save_json(SERVICE_FEE_FILE, rows)
            log_op(u, '服务费', '新增', f"{client} ${amount}")
            self._send_json({'ok': True, 'id': record['id']}); return

        # --- 清空（仅 admin）---
        if path.startswith('/api/attachment/'):
            # 上传原始凭证（转账截图）：/api/attachment/<payment|refund>/<记录id>
            seg = path.split('/')
            kind, rec_id = (seg[3], seg[4]) if len(seg) >= 5 else ('', '')
            if kind not in ATTACH_KIND_FILE:
                self._send_json({'error': '凭证只能挂在打款或退款记录上'}, 400); return
            u = self._require_perm(ATTACH_KIND_PERM[kind])
            if not u: return
            data = self._read_json()
            with _lock:
                rec = _att_find(kind, rec_id)
                if rec is None:
                    self._send_json({'error': '记录不存在（请刷新后重试）'}, 404); return
                if not self._att_visible(u, rec):
                    self._send_json({'error': '该笔账不在你的数据范围内'}, 403); return
                data_url = str(data.get('dataUrl') or '')
                if ',' in data_url: data_url = data_url.split(',', 1)[1]
                try:
                    raw = base64.b64decode(data_url)
                except Exception:
                    self._send_json({'error': '图片数据格式不正确'}, 400); return
                sniff = _att_sniff(raw)
                if not sniff:
                    self._send_json({'error': '只接受 png / jpg / gif / webp 图片（按文件头判定，改扩展名无效）'}, 400); return
                if not raw:
                    self._send_json({'error': '空文件'}, 400); return
                if len(raw) > ATTACH_MAX_BYTES:
                    self._send_json({'error': '单张凭证不能超过 8MB'}, 400); return
                atts = list(rec.get('attachments') or [])
                if len(atts) >= ATTACH_MAX_PER_DOC:
                    self._send_json({'error': '单笔最多 %d 张凭证，请先清理' % ATTACH_MAX_PER_DOC}, 400); return
                ext, mime = sniff
                att_id = gen_id()
                d = _att_dir(kind, rec_id)
                os.makedirs(d, exist_ok=True)
                fname = att_id + ext
                with open(os.path.join(d, fname), 'wb') as f:
                    f.write(raw)
                meta = {'id': att_id, 'file': fname, 'mime': mime, 'size': len(raw),
                        'name': _limit_str(data.get('name') or fname, STR_LIMIT_NAME),
                        'uploadedBy': u.get('name') or u.get('username'),
                        'uploadedAt': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                lst = load_json(ATTACH_KIND_FILE[kind])
                idx = next((j for j, r in enumerate(lst) if str(r.get('id')) == str(rec_id)), None)
                if idx is None:
                    self._send_json({'error': '记录不存在'}, 404); return
                lst[idx].setdefault('attachments', []).append(meta)
                save_json(ATTACH_KIND_FILE[kind], lst)
                log_op(u, ATTACH_KIND_LABEL[kind], '上传凭证', '%s $%s ← %s（%sKB）' % (
                    rec.get('client', ''), rec.get('amount', 0), meta['name'], round(len(raw) / 1024)))
                self._send_json({'ok': True, 'attachment': meta,
                                 'attachments': lst[idx].get('attachments')}); return

        if path in ('/api/consume/clear', '/api/recharge/clear', '/api/payment/clear', '/api/mapping/clear'):
            u = self._require_perm('can_manage_users')
            if not u: return
            with _lock:
                if path == '/api/consume/clear':
                    save_json(CONSUME_FILE, [])
                    save_json(IMPORT_LOG_FILE, [])
                elif path == '/api/recharge/clear':
                    save_json(RECHARGE_FILE, [])
                elif path == '/api/payment/clear':
                    save_json(PAYMENT_FILE, [])
                    _att_purge_kind('payment')   # 凭证文件同步清空，避免孤儿图
                elif path == '/api/mapping/clear':
                    save_json(MAPPING_FILE, [])
                    self._refresh_consume_matching()
            log_op(u, '系统', '清空数据', f"{path.replace('/api/','').replace('/clear','')} 全部清空")
            self._send_json({'ok': True}); return

        # --- Meta API Token 新增 ---
        if path == '/api/meta/tokens':
            data = self._read_json()
            if data is None: return
            u = self._require_perm('can_manage_users')
            if not u: return
            name = _limit_str(data.get('name') or '', STR_LIMIT_NAME)
            bm_id = _limit_str(data.get('bm_id') or '', STR_LIMIT_BMID)
            token = (data.get('token') or '').strip()
            if not name:
                self._send_json({'error': '请填写 BM 名称'}, 400); return
            if not bm_id:
                self._send_json({'error': '请填写 BM ID（必填）：Meta BM 后台 URL 里 business_id= 后面的数字'}, 400); return
            if not token:
                self._send_json({'error': '请填写 Token 值'}, 400); return
            items = load_json(META_TOKENS_FILE)
            new_id = gen_id()
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            items.append({
                'id': new_id, 'name': name,
                'note': (data.get('note') or '').strip(),
                'bm_id': bm_id,
                'token_enc': _encrypt_meta_token(token),
                'createdAt': now, 'lastCheckedAt': '', 'status': 'new',
            })
            _meta_save_tokens(items)
            log_op(u, 'Meta API', '新增Token', str(data.get('name') or '(未命名)'))
            self._send_json({'ok': True, 'id': new_id, 'tokens': load_meta_tokens()}); return

        # === 2026-09-24 TikTok 新增：Token 新增 ===
        if path == '/api/tiktok/tokens':
            data = self._read_json()
            if data is None: return
            u = self._require_perm('can_manage_users')
            if not u: return
            name = _limit_str(data.get('name') or '', STR_LIMIT_NAME)
            bc_id = _limit_str(str(data.get('business_center_id') or '').strip(), STR_LIMIT_ACCOUNTID)
            client_id = (data.get('client_id') or '').strip()
            client_secret = (data.get('client_secret') or '').strip()
            access_token = (data.get('access_token') or '').strip()
            refresh_token = (data.get('refresh_token') or '').strip()
            if not name: self._send_json({'error': '请填写 BC 名称'}, 400); return
            if not bc_id: self._send_json({'error': '请填写 Business Center ID'}, 400); return
            if not access_token: self._send_json({'error': '请填写 Access Token'}, 400); return
            if not client_id: self._send_json({'error': '请填写 Client ID'}, 400); return
            items = load_json(TIKTOK_TOKENS_FILE)
            new_id = gen_id()
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            items.append({
                'id': new_id, 'name': name,
                'note': _limit_str(data.get('note') or '', STR_LIMIT_NOTE),
                'business_center_id': bc_id,
                'client_id': client_id, 'client_secret': client_secret,
                'access_token': access_token, 'refresh_token': refresh_token,
                'createdAt': now, 'lastCheckedAt': '', 'status': 'new',
            })
            _tiktok_save_tokens(items)
            log_op(u, 'TikTok API', '新增Token', name)
            self._send_json({'ok': True, 'id': new_id, 'tokens': load_tiktok_tokens()}); return

        # === 2026-09-24 Google Ads 新增：Token 新增 ===
        if path == '/api/google/tokens':
            data = self._read_json()
            if data is None: return
            u = self._require_perm('can_manage_users')
            if not u: return
            name = _limit_str(data.get('name') or '', STR_LIMIT_NAME)
            customer_id = _limit_str(str(data.get('customer_id') or '').strip(), STR_LIMIT_ACCOUNTID)
            developer_token = (data.get('developer_token') or '').strip()
            client_id = (data.get('client_id') or '').strip()
            client_secret = (data.get('client_secret') or '').strip()
            refresh_token = (data.get('refresh_token') or '').strip()
            access_token = (data.get('access_token') or '').strip()
            if not name: self._send_json({'error': '请填写名称'}, 400); return
            if not customer_id: self._send_json({'error': '请填写 Customer ID'}, 400); return
            if not developer_token: self._send_json({'error': '请填写 Developer Token'}, 400); return
            if not client_id: self._send_json({'error': '请填写 Client ID'}, 400); return
            items = load_json(GOOGLE_TOKENS_FILE)
            new_id = gen_id()
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            items.append({
                'id': new_id, 'name': name,
                'note': _limit_str(data.get('note') or '', STR_LIMIT_NOTE),
                'customer_id': customer_id,
                'developer_token': developer_token, 'client_id': client_id,
                'client_secret': client_secret, 'refresh_token': refresh_token,
                'access_token': access_token,
                'createdAt': now, 'lastCheckedAt': '', 'status': 'new',
            })
            _google_save_tokens(items)
            log_op(u, 'Google Ads API', '新增Token', name)
            self._send_json({'ok': True, 'id': new_id, 'tokens': load_google_tokens()}); return

        # === 2026-09-24 三平台自动刷新配置 POST ===
        if path == '/api/platform/auto-sync-cfg':
            data = self._read_json()
            if data is None: return
            u = self._require_perm('can_manage_users')
            if not u: return
            full = load_platform_auto_cfg()

            # 支持两种 body 格式：{"platform":"meta", ...} 单平台，或 {"platforms": {...}} 批量
            if 'platform' in data:
                platform = data.get('platform')
                if platform not in ('meta', 'tiktok', 'google'):
                    self._send_json({'ok': False, 'error': 'platform 必须是 meta/tiktok/google 之一'}, 400); return
                old = full.get(platform, {})
                enabled = bool(data.get('enabled', old.get('enabled', False)))
                interval_h = max(1, min(24, int(data.get('interval_h', old.get('interval_h', 12)))))
                days_back = max(1, min(30, int(data.get('days_back', old.get('days_back', 7)))))
                sub = {
                    'enabled': enabled,
                    'interval_h': interval_h,
                    'days_back': days_back,
                    'lastRun': old.get('lastRun', ''),
                    'lastError': old.get('lastError', ''),
                }
                full[platform] = sub
                save_platform_auto_cfg(full)
                log_op(u, f'{platform.upper()} API', '更新自动刷新配置',
                       f'启用={enabled} 间隔={interval_h}h 回退={days_back}天')
                self._send_json({'ok': True, 'cfg': sub}); return
            elif 'platforms' in data:
                platforms_data = data.get('platforms', {})
                if not isinstance(platforms_data, dict):
                    self._send_json({'ok': False, 'error': 'platforms 必须是 dict'}, 400); return
                updated = {}
                for pk in ('meta', 'tiktok', 'google'):
                    if pk in platforms_data:
                        pd = platforms_data[pk]
                        if isinstance(pd, dict):
                            old = full.get(pk, {})
                            enabled = bool(pd.get('enabled', old.get('enabled', False)))
                            interval_h = max(1, min(24, int(pd.get('interval_h', old.get('interval_h', 12)))))
                            days_back = max(1, min(30, int(pd.get('days_back', old.get('days_back', 7)))))
                            full[pk] = {
                                'enabled': enabled,
                                'interval_h': interval_h,
                                'days_back': days_back,
                                'lastRun': old.get('lastRun', ''),
                                'lastError': old.get('lastError', ''),
                            }
                            updated[pk] = full[pk]
                save_platform_auto_cfg(full)
                log_op(u, '平台自动刷新', '批量更新配置', ','.join(updated.keys()))
                self._send_json({'ok': True, 'cfg': {k: full[k] for k in updated}}); return
            else:
                self._send_json({'ok': False, 'error': '需要 platform 或 platforms 字段'}, 400); return

        # --- Meta 自动刷新配置保存（旧路由 wrapper） ---
        if path == '/api/meta/auto-sync-cfg':
            data = self._read_json()
            if data is None: return
            u = self._require_perm('can_manage_users')
            if not u: return
            full = load_platform_auto_cfg()
            old = full.get('meta', {})
            enabled = bool(data.get('enabled', old.get('enabled', False)))
            interval_h = max(1, min(24, int(data.get('interval_h', old.get('interval_h', 12)))))
            days_back = max(1, min(30, int(data.get('days_back', old.get('days_back', 7)))))
            full['meta'] = {
                'enabled': enabled,
                'interval_h': interval_h,
                'days_back': days_back,
                'lastRun': old.get('lastRun', ''),
                'lastError': old.get('lastError', ''),
            }
            save_platform_auto_cfg(full)
            log_op(u, 'Meta API', '更新自动刷新配置', '启用=%s 间隔=%d小时 回退=%d天' % (enabled, interval_h, days_back))
            self._send_json({'ok': True, 'cfg': full['meta']}); return

        self._send_json({'error': 'not found'}, 404)

    # ----------------------------------------------------------
    # PUT / DELETE
    # ----------------------------------------------------------
    def do_PUT(self):
        # 2026-09-12 公网穿透安全加固：PUT 请求通用速率限制
        real_ip = _extract_real_ip(self)
        ok, retry, cnt = _rate_check(real_ip, RATE_GENERAL_WINDOW, RATE_GENERAL_MAX)
        if not ok:
            self.send_response(429)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('Retry-After', str(retry))
            self.send_header('X-RateLimit-Remaining', '0')
            self.send_header('X-RateLimit-Reset', str(int(time.time()) + retry))
            self._security_headers()
            self.end_headers()
            self.wfile.write('{"error":"请求过于频繁，请稍后再试"}'.encode('utf-8'))
            print(f'[429] rate-limit PUT ip={real_ip} count={cnt}', flush=True)
            return
        # 2026-09-06 统一异常兜底：内部异常只回 500 JSON（不外泄堆栈、不留半截响应）
        try:
            self._PUT_impl()
        except _BodyRejected:
            return
        except (BrokenPipeError, ConnectionResetError):
            return
        except Exception as e:
            print('[500] PUT', repr(e)[:220], flush=True)
            try:
                self._send_json({'error': '服务器处理失败，请稍后重试（详情见服务端日志）'}, 500)
            except Exception:
                pass

    def _PUT_impl(self):
        if not self._api_guard(): return
        data = self._read_json()
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path.rstrip('/')

        if path == '/api/settings':
            u = self._require_perm('can_manage_users')
            if not u: return
            cur = load_settings()
            if 'media_check' in data: cur['media_check'] = bool(data['media_check'])
            if data.get('gap_rule') in ('prev', 'last'): cur['gap_rule'] = data['gap_rule']
            if isinstance(data.get('backup'), dict):
                b = dict(get_backup_cfg())
                nb = data['backup']
                for k in ('enabled','smtp_ssl'):
                    if k in nb: b[k] = bool(nb[k])
                for k in ('emails','smtp_host','smtp_user','smtp_pass','freq','send_time','provider'):
                    if k in nb: b[k] = str(nb[k] or '')
                if nb.get('smtp_port'): b['smtp_port'] = int(nb['smtp_port'])
                if nb.get('last_sent') is not None and 'last_sent' not in nb: pass
                cur['backup'] = b
            save_settings(cur)
            log_op(u, '系统设置', '修改', f"媒体校验={cur['media_check']} 空窗规则={cur['gap_rule']}")
            # 空窗规则变化 → 历史消耗归属需按新规则重算
            try:
                if data.get('gap_rule') in ('prev', 'last'):
                    self._refresh_consume_matching()
            except Exception:
                pass
            self._send_json({'ok': True, 'settings': cur}); return


        # --- Meta Token 编辑/删除（放在 do_POST 后） ---
        if path.startswith('/api/meta/tokens/') and not path.endswith('/test'):
            u = self._require_perm('can_manage_users')
            if not u: return
            tk_id = path.split('/')[-1]
            items = load_json(META_TOKENS_FILE)
            target = next((x for x in items if x.get('id') == tk_id), None)
            if not target:
                self._send_json({'ok': False, 'error': 'Token 不存在'}, 404); return
            if self.command == 'PUT':
                if data.get('name'): target['name'] = str(data['name']).strip()
                if 'note' in data: target['note'] = _limit_str(data.get('note') or '', STR_LIMIT_NOTE)
                if 'bm_id' in data: target['bm_id'] = _limit_str(data.get('bm_id') or '', STR_LIMIT_BMID)
                new_tk = (data.get('token') or '').strip()
                if new_tk:
                    target['token_enc'] = _encrypt_meta_token(new_tk)
                    target['status'] = 'new'; target['lastCheckedAt'] = ''
                _meta_save_tokens(items)
                log_op(u, 'Meta API', '编辑Token', target.get('name',''))
                self._send_json({'ok': True, 'tokens': load_meta_tokens()}); return
            if self.command == 'DELETE':
                items.remove(target)
                _meta_save_tokens(items)
                log_op(u, 'Meta API', '删除Token', target.get('name',''))
                self._send_json({'ok': True, 'tokens': load_meta_tokens()}); return

        # === 2026-09-24 TikTok 新增：Token 编辑/删除 ===
        if path.startswith('/api/tiktok/tokens/') and not path.endswith('/test'):
            u = self._require_perm('can_manage_users')
            if not u: return
            tk_id = path.split('/')[-1]
            items = load_json(TIKTOK_TOKENS_FILE)
            target = next((x for x in items if x.get('id') == tk_id), None)
            if not target:
                self._send_json({'ok': False, 'error': 'Token 不存在'}, 404); return
            if self.command == 'PUT':
                if 'name' in data: target['name'] = _limit_str(data.get('name',''), STR_LIMIT_NAME)
                if 'note' in data: target['note'] = _limit_str(data.get('note',''), STR_LIMIT_NOTE)
                if 'business_center_id' in data: target['business_center_id'] = _limit_str(str(data.get('business_center_id','')).strip(), STR_LIMIT_ACCOUNTID)
                # 新 token 值（仅当有传时才覆盖）
                for fld, enc in [('client_id','client_id_enc'),('client_secret','client_secret_enc'),
                                 ('access_token','access_token_enc'),('refresh_token','refresh_token_enc')]:
                    nv = (data.get(fld) or '').strip() if fld in data else ''
                    if fld in data and nv:
                        target[fld] = nv
                        target['status'] = 'new'; target['lastCheckedAt'] = ''
                    elif fld in data and not nv and enc in target:
                        # 显式清空
                        target[enc] = ''
                _tiktok_save_tokens(items)
                log_op(u, 'TikTok API', '编辑Token', target.get('name',''))
                self._send_json({'ok': True, 'tokens': load_tiktok_tokens()}); return
            if self.command == 'DELETE':
                items.remove(target)
                _tiktok_save_tokens(items)
                log_op(u, 'TikTok API', '删除Token', target.get('name',''))
                self._send_json({'ok': True, 'tokens': load_tiktok_tokens()}); return

        # === 2026-09-24 Google Ads 新增：Token 编辑/删除 ===
        if path.startswith('/api/google/tokens/') and not path.endswith('/test'):
            u = self._require_perm('can_manage_users')
            if not u: return
            tk_id = path.split('/')[-1]
            items = load_json(GOOGLE_TOKENS_FILE)
            target = next((x for x in items if x.get('id') == tk_id), None)
            if not target:
                self._send_json({'ok': False, 'error': 'Token 不存在'}, 404); return
            if self.command == 'PUT':
                if 'name' in data: target['name'] = _limit_str(data.get('name',''), STR_LIMIT_NAME)
                if 'note' in data: target['note'] = _limit_str(data.get('note',''), STR_LIMIT_NOTE)
                if 'customer_id' in data: target['customer_id'] = _limit_str(str(data.get('customer_id','')).strip(), STR_LIMIT_ACCOUNTID)
                for fld in ('developer_token','client_id','client_secret','refresh_token','access_token'):
                    enc = fld + '_enc'
                    nv = (data.get(fld) or '').strip() if fld in data else ''
                    if fld in data and nv:
                        target[fld] = nv
                        target['status'] = 'new'; target['lastCheckedAt'] = ''
                    elif fld in data and not nv and enc in target:
                        target[enc] = ''
                _google_save_tokens(items)
                log_op(u, 'Google Ads API', '编辑Token', target.get('name',''))
                self._send_json({'ok': True, 'tokens': load_google_tokens()}); return
            if self.command == 'DELETE':
                items.remove(target)
                _google_save_tokens(items)
                log_op(u, 'Google Ads API', '删除Token', target.get('name',''))
                self._send_json({'ok': True, 'tokens': load_google_tokens()}); return

        if path.startswith('/api/users/'):
            u = self._require_perm('can_manage_users')
            if not u: return
            target_id = path.split('/')[-1]
            users = load_users()
            for i, x in enumerate(users):
                if x.get('id') == target_id:
                    if 'can_export' in data:
                        data['can_export'] = bool(data['can_export'])   # 2026-09-06 只允许布尔覆盖
                    for k, v in data.items():
                        if k == 'password':
                            users[i][k] = hash_password(v) if v else x[k]
                        else:
                            users[i][k] = v
                    save_json(USERS_FILE, users)
                    log_op(u, '账号管理', '修改', f"{x.get('username','')} 字段={','.join(data.keys())}")
                    safe = {k: v for k, v in users[i].items() if k != 'password'}
                    self._send_json({'ok': True, 'user': safe}); return
            self._send_json({'error': '用户不存在'}, 404); return

        if path.startswith('/api/mapping/') and path not in ('/api/mapping/clear','/api/mapping/clear-column','/api/mapping/import','/api/mapping/batch'):
            u = self._require_perm('can_edit_mapping')
            if not u: return
            method = str(getattr(self, 'command', '') or '').upper()
            target_id = path.split('/')[-1]
            with _lock:
                mapping = load_json(MAPPING_FILE)
                if method == 'DELETE':
                    before = len(mapping)
                    removed = [m for m in mapping if str(m.get('id')) == target_id or str(m.get('accountId')) == target_id]
                    mapping = [m for m in mapping if str(m.get('id')) != target_id and str(m.get('accountId')) != target_id]
                    if len(mapping) == before:
                        self._send_json({'error':'not found'}, 404); return
                    save_json(MAPPING_FILE, mapping)
                    # 2026-09-08 级联删除：下户表账号删除后，同步删除充值清零表中该 accountId 的记录
                    removed_acc_ids = set(str(m.get('accountId')) for m in removed if m.get('accountId'))
                    recharge_deleted = 0
                    if removed_acc_ids:
                        try:
                            recharge = load_json(RECHARGE_FILE)
                            rech_before = len(recharge)
                            recharge = [r for r in recharge if str(r.get('accountId')) not in removed_acc_ids]
                            recharge_deleted = rech_before - len(recharge)
                            if recharge_deleted > 0:
                                save_json(RECHARGE_FILE, recharge)
                        except Exception:
                            pass
                    try: self._refresh_consume_matching()
                    except Exception: pass
                    log_op(u, '下户表', '删除', '；'.join(f"{m.get('accountId')}({m.get('client','')})" for m in removed[:3]))
                    self._send_json({'ok': True, 'deleted': before - len(mapping), 'total': len(mapping), 'recharge_deleted': recharge_deleted}); return
                # PUT / POST（按 id 或 accountId 匹配更新）
                for i, m in enumerate(mapping):
                    if str(m.get('id')) == target_id or str(m.get('accountId')) == target_id:
                        _ALLOW = set(['date','endDate','client','name','accountId','platform','channel','rate','status','note','timezone'])   # 2026-09-06 字段白名单：拒绝注入 id/createdBy/attachments 等内部字段；2026-09-07 +timezone
                        for k, v in data.items():
                            if k in _ALLOW:
                                if k == 'timezone':
                                    mapping[i][k] = normalize_timezone(v)
                                elif k in ('date', 'endDate'):
                                    # 2026-09-07 修复：日期字段统一规范化为 YYYY-MM-DD
                                    mapping[i][k] = to_short_date(v)
                                elif k == 'platform':
                                    # 2026-09-07 修复：platform 空值兜底为 Facebook，防止媒体数统计为 0
                                    mapping[i][k] = str(v or '').strip() or 'Facebook'
                                else:
                                    mapping[i][k] = v
                        # 操作人戳：记录谁在什么时候改了这条归属
                        mapping[i]['updatedBy'] = u.get('name') or u.get('username')
                        mapping[i]['updatedAt'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        normalize_mapping_status(mapping[i])
                        save_json(MAPPING_FILE, mapping)
                        try: self._refresh_consume_matching()
                        except Exception: pass
                        log_op(u, '下户表', '修改', f"{mapping[i].get('accountId')} 字段={','.join(data.keys())}")
                        self._send_json({'ok': True}); return
            self._send_json({'error': 'not found'}, 404); return

        if path.startswith('/api/recharge/'):
            u = self._require_perm('can_edit_recharge')
            if not u: return
            target_id = path.split('/')[-1]
            with _lock:
                recharge = load_json(RECHARGE_FILE)
                for i, r in enumerate(recharge):
                    if str(r.get('id')) == target_id:
                        # 2026-09-06 账户状态可编辑：状态只落下户段（与新增充值同一条通路），流水不冗余存 status
                        _ALLOW = set(['date','accountId','accountName','amount','clear','client','platform','note'])   # 字段白名单：拒绝注入 id/createdBy/attachments 等内部字段
                        new_st = str(data.pop('status', '') or '').strip() if isinstance(data, dict) else ''
                        new_tz = normalize_timezone(data.pop('timezone', '') or '') if isinstance(data, dict) else '+8'
                        if new_st and new_st not in MAPPING_STATUS_VALID:
                            self._send_json({'error': '非法状态（只能 正常/挂户/清零/清零不回收）'}, 400); return
                        if new_st:
                            applied = self._apply_account_status_locked(r.get('accountId'), str(r.get('date') or ''),
                                                                new_st, u.get('name') or u.get('username'),
                                                                datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                            if not applied:
                                self._send_json({'error': '该账户在充值日期 %s 没有归属段，请先在「下户表」登记归属' % (r.get('date') or '')}, 400); return
                        # 2026-09-07 timezone 写回下户表（充值日期命中的归属段）—— 与 status 同通路
                        if new_tz or (isinstance(data, dict) and 'timezone' in data and data.get('timezone') == ''):
                            _mapping_list = load_json(MAPPING_FILE)
                            _idx = build_mapping_index(_mapping_list)
                            _seg = pick_mapping_segment(_idx, str(r.get('accountId')), str(r.get('date') or ''))
                            if _seg:
                                for _mrow in _mapping_list:
                                    if (str(_mrow.get('accountId')) == str(_seg.get('accountId'))
                                            and str(_mrow.get('date', '')) == str(_seg.get('date', ''))):
                                        _mrow['timezone'] = new_tz
                                        _mrow['updatedBy'] = u.get('name') or u.get('username')
                                        _mrow['updatedAt'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                        break
                                save_json(MAPPING_FILE, _mapping_list)
                                self._refresh_consume_matching()
                        for k, v in data.items():
                            if k in _ALLOW: recharge[i][k] = v
                        recharge[i]['updatedBy'] = u.get('name') or u.get('username')
                        recharge[i]['updatedAt'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        save_json(RECHARGE_FILE, recharge)
                        log_op(u, '充值管理', '修改', f"{r.get('accountId','')} 字段={','.join(data.keys())}")
                        self._send_json({'ok': True}); return
            self._send_json({'error': 'not found'}, 404); return

        if path.startswith('/api/payment/'):
            u = self._require_perm('can_edit_payment')
            if not u: return
            target_id = path.split('/')[-1]
            with _lock:
                payment = load_json(PAYMENT_FILE)
                for i, r in enumerate(payment):
                    if str(r.get('id')) == target_id:
                        _ALLOW = set(['date','client','amount','method','note'])   # 2026-09-06 字段白名单：拒绝注入 id/createdBy/attachments 等内部字段
                        for k, v in data.items():
                            if k in _ALLOW: payment[i][k] = v
                        payment[i]['updatedBy'] = u.get('name') or u.get('username')
                        payment[i]['updatedAt'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        save_json(PAYMENT_FILE, payment)
                        log_op(u, '打款管理', '修改', f"{r.get('client','')} 字段={','.join(data.keys())}")
                        self._send_json({'ok': True}); return
            self._send_json({'error': 'not found'}, 404); return

        if path.startswith('/api/refund/'):
            u = self._require_perm('can_refund')
            if not u: return
            target_id = path.split('/')[-1]
            with _lock:
                refunds = load_json(REFUND_FILE)
                for i, r in enumerate(refunds):
                    if str(r.get('id')) == target_id:
                        if set(data) - {'note'}:
                            self._send_json({'error': '退款记录仅允许修改备注'}, 400); return
                        refunds[i]['note'] = _limit_str(data.get('note') or '', STR_LIMIT_NOTE)
                        refunds[i]['updatedBy'] = u.get('name') or u.get('username')
                        refunds[i]['updatedAt'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        save_json(REFUND_FILE, refunds)
                        log_op(u, '退款管理', '修改', f"{r.get('client','')} 字段=note")
                        self._send_json({'ok': True}); return
            self._send_json({'error': 'not found'}, 404); return

        if path.startswith('/api/service-fee/'):
            u = self._require_perm('can_edit_payment')
            if not u: return
            target_id = path.split('/')[-1]
            with _lock:
                rows = load_json(SERVICE_FEE_FILE)
                for i, r in enumerate(rows):
                    if str(r.get('id')) == target_id:
                        allowed = {'note'}
                        if 'client' in data or 'amount' in data:
                            self._send_json({'error': '服务费记录添加后，客户和金额不可修改'}, 400); return
                        if 'note' in data:
                            data['note'] = _limit_str(data.get('note') or '', STR_LIMIT_NOTE)
                        for k, v in data.items():
                            if k in allowed: rows[i][k] = v
                        rows[i]['updatedBy'] = u.get('name') or u.get('username')
                        rows[i]['updatedAt'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        save_json(SERVICE_FEE_FILE, rows)
                        log_op(u, '服务费', '修改', f"{rows[i].get('client','')} 字段={','.join(data.keys())}")
                        self._send_json({'ok': True}); return
            self._send_json({'error': 'not found'}, 404); return

        self._send_json({'error': 'not found'}, 404)

    def do_DELETE(self):
        # 2026-09-12 公网穿透安全加固：DELETE 请求通用速率限制
        real_ip = _extract_real_ip(self)
        ok, retry, cnt = _rate_check(real_ip, RATE_GENERAL_WINDOW, RATE_GENERAL_MAX)
        if not ok:
            self.send_response(429)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('Retry-After', str(retry))
            self.send_header('X-RateLimit-Remaining', '0')
            self.send_header('X-RateLimit-Reset', str(int(time.time()) + retry))
            self._security_headers()
            self.end_headers()
            self.wfile.write('{"error":"请求过于频繁，请稍后再试"}'.encode('utf-8'))
            print(f'[429] rate-limit DELETE ip={real_ip} count={cnt}', flush=True)
            return
        # 2026-09-06 统一异常兜底：内部异常只回 500 JSON（不外泄堆栈、不留半截响应）
        try:
            self._DELETE_impl()
        except _BodyRejected:
            return
        except (BrokenPipeError, ConnectionResetError):
            return
        except Exception as e:
            print('[500] DELETE', repr(e)[:220], flush=True)
            try:
                self._send_json({'error': '服务器处理失败，请稍后重试（详情见服务端日志）'}, 500)
            except Exception:
                pass

    def _DELETE_impl(self):
        if not self._api_guard(): return
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path.rstrip('/')

        if path == '/api/payment/batch-delete':
            u = self._require_money_delete('打款记录')
            if not u: return
            data = self._read_json()
            ids = data.get('ids') if isinstance(data, dict) else []
            if not isinstance(ids, list) or not ids:
                self._send_json({'error': '请选择要删除的打款记录'}, 400); return
            wanted = {str(x) for x in ids}
            with _lock:
                payment = load_json(PAYMENT_FILE)
                removed = [r for r in payment if str(r.get('id')) in wanted]
                if not removed:
                    self._send_json({'error': '未找到要删除的打款记录'}, 404); return
                save_json(PAYMENT_FILE, [r for r in payment if str(r.get('id')) not in wanted])
                for r in removed: _att_purge_doc('payment', r.get('id'))
            total = round(sum(float(r.get('amount') or 0) for r in removed), 2)
            log_op(u, '打款管理', '批量删除', f"删除 {len(removed)} 条，累计打款 -${total}")
            self._send_json({'ok': True, 'deleted': len(removed), 'amount': total}); return


        # --- Meta Token 编辑/删除（放在 do_POST 后） ---
        if path.startswith('/api/meta/tokens/') and not path.endswith('/test'):
            u = self._require_perm('can_manage_users')
            if not u: return
            tk_id = path.split('/')[-1]
            items = load_json(META_TOKENS_FILE)
            target = next((x for x in items if x.get('id') == tk_id), None)
            if not target:
                self._send_json({'ok': False, 'error': 'Token 不存在'}, 404); return
            if self.command == 'PUT':
                if data.get('name'): target['name'] = str(data['name']).strip()
                if 'note' in data: target['note'] = _limit_str(data.get('note') or '', STR_LIMIT_NOTE)
                if 'bm_id' in data: target['bm_id'] = _limit_str(data.get('bm_id') or '', STR_LIMIT_BMID)
                new_tk = (data.get('token') or '').strip()
                if new_tk:
                    target['token_enc'] = _encrypt_meta_token(new_tk)
                    target['status'] = 'new'; target['lastCheckedAt'] = ''
                _meta_save_tokens(items)
                log_op(u, 'Meta API', '编辑Token', target.get('name',''))
                self._send_json({'ok': True, 'tokens': load_meta_tokens()}); return
            if self.command == 'DELETE':
                items.remove(target)
                _meta_save_tokens(items)
                log_op(u, 'Meta API', '删除Token', target.get('name',''))
                self._send_json({'ok': True, 'tokens': load_meta_tokens()}); return

        # === 2026-09-24 TikTok 新增：Token 删除（_DELETE_impl） ===
        if path.startswith('/api/tiktok/tokens/') and not path.endswith('/test'):
            u = self._require_perm('can_manage_users')
            if not u: return
            tk_id = path.split('/')[-1]
            items = load_json(TIKTOK_TOKENS_FILE)
            target = next((x for x in items if x.get('id') == tk_id), None)
            if not target:
                self._send_json({'ok': False, 'error': 'Token 不存在'}, 404); return
            if self.command == 'DELETE':
                items.remove(target)
                _tiktok_save_tokens(items)
                log_op(u, 'TikTok API', '删除Token', target.get('name',''))
                self._send_json({'ok': True, 'tokens': load_tiktok_tokens()}); return

        # === 2026-09-24 Google Ads 新增：Token 删除（_DELETE_impl） ===
        if path.startswith('/api/google/tokens/') and not path.endswith('/test'):
            u = self._require_perm('can_manage_users')
            if not u: return
            tk_id = path.split('/')[-1]
            items = load_json(GOOGLE_TOKENS_FILE)
            target = next((x for x in items if x.get('id') == tk_id), None)
            if not target:
                self._send_json({'ok': False, 'error': 'Token 不存在'}, 404); return
            if self.command == 'DELETE':
                items.remove(target)
                _google_save_tokens(items)
                log_op(u, 'Google Ads API', '删除Token', target.get('name',''))
                self._send_json({'ok': True, 'tokens': load_google_tokens()}); return

        if path.startswith('/api/users/'):
            u = self._require_perm('can_manage_users')
            if not u: return
            target_id = path.split('/')[-1]
            users = load_users()
            if target_id == u['id']:
                self._send_json({'error': '不能删除自己'}, 400); return
            for x in users:
                if x.get('id') == target_id:
                    x['active'] = False
                    save_json(USERS_FILE, users)
                    log_op(u, '账号管理', '停用', f"{x.get('username','')}")
                    self._send_json({'ok': True}); return
            self._send_json({'error': '用户不存在'}, 404); return

        if path == '/api/mapping/clear':
            # ===== 危险操作：清空所有下户记录 =====
            u = self._require_perm('can_edit_mapping')
            if not u: return
            with _lock:
                before = len(load_json(MAPPING_FILE))
                save_json(MAPPING_FILE, [])
                self._refresh_consume_matching()
                after = len(load_json(MAPPING_FILE))
            log_op(u, '下户表', '清空', f'删除全部 {before} 条下户记录')
            self._send_json({'ok': True, 'deleted': before - after, 'total': after}); return

        if path == '/api/mapping/clear-column':
            # ===== 按列清空：把所有行的指定字段 column 置空/0 =====
            u = self._require_perm('can_edit_mapping')
            if not u: return
            try:
                data = self._read_json() or {}
            except Exception:
                # 兼容 query string 方式
                qs = urllib.parse.parse_qs(parsed.query)
                data = {'column': (qs.get('column') or [''])[0]}
            column = str(data.get('column', '')).strip()
            # 白名单：只允许清空下面这些字段（不允许动 id/accountId/date 等关键结构）
            ALLOW_CLEAR_FIELDS = {
                'client','channel','platform','name','note',
                'rate','endDate','status','timezone',
            }
            if not column or column not in ALLOW_CLEAR_FIELDS:
                self._send_json({'error': f'column 必须为以下之一：{", ".join(sorted(ALLOW_CLEAR_FIELDS))}'}, 400); return
            with _lock:
                mapping = load_json(MAPPING_FILE)
                touched = 0
                for m in mapping:
                    old = m.get(column)
                    if column == 'rate':
                        # 数值类：清 0
                        if float(m.get('rate',0) or 0) != 0:
                            m['rate'] = 0; touched += 1
                    elif column == 'endDate':
                        # endDate：清为空字符串
                        if m.get('endDate'):
                            m['endDate'] = ''; touched += 1
                    elif column == 'status':
                        # 清状态 = 回到默认 '正常'
                        if m.get('status') != MAPPING_STATUS_DEFAULT:
                            m['status'] = MAPPING_STATUS_DEFAULT; touched += 1
                    else:
                        # 文本类：清为空字符串
                        if m.get(column):
                            m[column] = ''; touched += 1
                if touched:
                    save_json(MAPPING_FILE, mapping)
                    log_op(u, '下户表', '清空字段', f"字段={column}，清空 {touched} 行")
                    self._refresh_consume_matching()
            self._send_json({'ok': True, 'field': column, 'touched': touched, 'total': len(load_json(MAPPING_FILE))}); return

        if path.startswith('/api/mapping/'):
            u = self._require_perm('can_edit_mapping')
            if not u: return
            target_id = path.split('/')[-1]
            with _lock:
                mapping = load_json(MAPPING_FILE)
                removed = [m for m in mapping if str(m.get('id')) == target_id or str(m.get('accountId')) == target_id]
                new_list = [m for m in mapping
                            if str(m.get('id')) != target_id and str(m.get('accountId')) != target_id]
                if len(new_list) < len(mapping):
                    save_json(MAPPING_FILE, new_list)
                    # 2026-09-08 级联删除：下户表账号删除后，同步删除充值清零表中该 accountId 的记录
                    removed_acc_ids = set(str(m.get('accountId')) for m in removed if m.get('accountId'))
                    recharge_deleted = 0
                    if removed_acc_ids:
                        try:
                            recharge = load_json(RECHARGE_FILE)
                            rech_before = len(recharge)
                            recharge = [r for r in recharge if str(r.get('accountId')) not in removed_acc_ids]
                            recharge_deleted = rech_before - len(recharge)
                            if recharge_deleted > 0:
                                save_json(RECHARGE_FILE, recharge)
                        except Exception:
                            pass
                    self._refresh_consume_matching()
                    log_op(u, '下户表', '删除', '；'.join(f"{m.get('accountId')}({m.get('client','')})" for m in removed[:3]))
                    self._send_json({'ok': True, 'deleted': len(removed), 'recharge_deleted': recharge_deleted}); return
            self._send_json({'error': 'not found'}, 404); return

        if path.startswith('/api/recharge/'):
            u = self._require_perm('can_edit_recharge')
            if not u: return
            target_id = path.split('/')[-1]
            with _lock:
                recharge = load_json(RECHARGE_FILE)
                removed = [r for r in recharge if str(r.get('id')) == target_id]
                new_list = [r for r in recharge if str(r.get('id')) != target_id]
                if len(new_list) < len(recharge):
                    save_json(RECHARGE_FILE, new_list)
                    log_op(u, '充值管理', '删除', '；'.join(f"{r.get('accountId')} ${r.get('amount',0)}" for r in removed[:3]))
                    self._send_json({'ok': True}); return
            self._send_json({'error': 'not found'}, 404); return

        if path.startswith('/api/payment/'):
            u = self._require_money_delete('打款记录')
            if not u: return
            target_id = path.split('/')[-1]
            with _lock:
                payment = load_json(PAYMENT_FILE)
                removed = [r for r in payment if str(r.get('id')) == target_id]
                new_list = [r for r in payment if str(r.get('id')) != target_id]
                if len(new_list) < len(payment):
                    save_json(PAYMENT_FILE, new_list)
                    for r in removed: _att_purge_doc('payment', r.get('id'))
                    # 删除即金额回退：累计打款 / 客户池分摊 / 备款一览 / 按日期汇总 全部由剩余记录重算（派生值，无残留）
                    amt = round(sum(float(r.get('amount') or 0) for r in removed), 2)
                    log_op(u, '打款管理', '删除', '；'.join(f"{r.get('client')} ${r.get('amount',0)}" for r in removed[:3]) + '（累计打款 -$%s，备款同步回退）' % amt)
                    self._send_json({'ok': True, 'removed': removed[0], 'restoredPayment': amt}); return
            self._send_json({'error': 'not found'}, 404); return

        if path.startswith('/api/service-fee/'):
            u = self._require_money_delete('服务费记录')
            if not u: return
            target_id = path.split('/')[-1]
            with _lock:
                rows = load_json(SERVICE_FEE_FILE)
                removed = [r for r in rows if str(r.get('id')) == target_id]
                new_rows = [r for r in rows if str(r.get('id')) != target_id]
                if len(new_rows) == len(rows):
                    self._send_json({'error': 'not found'}, 404); return
                save_json(SERVICE_FEE_FILE, new_rows)
            log_op(u, '服务费', '删除', '；'.join(f"{r.get('client')} ${r.get('amount',0)}" for r in removed[:3]))
            self._send_json({'ok': True, 'removed': removed[0]}); return

        if path.startswith('/api/attachment/'):
            # 删除凭证：与凭证删除同权限（管理员/财务），并留痕
            u = self._require_money_delete('凭证附件')
            if not u: return
            seg = path.split('/')
            if len(seg) < 6 or seg[3] not in ATTACH_KIND_FILE:
                self._send_json({'error': 'not found'}, 404); return
            kind, rec_id, att_id = seg[3], seg[4], seg[5]
            with _lock:
                lst = load_json(ATTACH_KIND_FILE[kind])
                idx = next((i for i, r in enumerate(lst) if str(r.get('id')) == str(rec_id)), None)
                if idx is None:
                    self._send_json({'error': '记录不存在'}, 404); return
                if not self._att_visible(u, lst[idx]):
                    self._send_json({'error': '该笔账不在你的数据范围内'}, 403); return
                atts = [a for a in (lst[idx].get('attachments') or []) if str(a.get('id')) != str(att_id)]
                gone = [a for a in (lst[idx].get('attachments') or []) if str(a.get('id')) == str(att_id)]
                if not gone:
                    self._send_json({'error': '凭证不存在'}, 404); return
                for g in gone:
                    try:
                        os.remove(os.path.join(_att_dir(kind, rec_id), os.path.basename(str(g.get('file') or ''))))
                    except Exception: pass
                lst[idx]['attachments'] = atts
                save_json(ATTACH_KIND_FILE[kind], lst)
                if not atts:
                    try: os.rmdir(_att_dir(kind, rec_id))   # 末张凭证删掉后不留空目录
                    except OSError: pass
                log_op(u, ATTACH_KIND_LABEL[kind], '删除凭证', '%s $%s → %s' % (
                    lst[idx].get('client', ''), lst[idx].get('amount', 0), gone[0].get('name', '')))
                self._send_json({'ok': True, 'attachments': atts}); return

        if path.startswith('/api/refund/'):
            # 2026-09-05 新增：删除退款/平账凭证（管理员+财务）；删除后该笔金额冲回累计退款并恢复可退备款
            u = self._require_money_delete('退款记录')
            if not u: return
            target_id = path.split('/')[-1]
            with _lock:
                refunds = load_json(REFUND_FILE)
                removed = [r for r in refunds if str(r.get('id')) == target_id]
                new_list = [r for r in refunds if str(r.get('id')) != target_id]
                if len(new_list) >= len(refunds):
                    self._send_json({'error': 'not found'}, 404); return
                save_json(REFUND_FILE, new_list)
                for r in removed: _att_purge_doc('refund', r.get('id'))
                restored = 0.0
                cs = load_json(CLIENT_STATUS_FILE); cs_changed = False
                for r in removed:
                    tp = r.get('type') or 'cash'
                    amt = float(r.get('amount') or 0)
                    restored += amt if tp == 'cash' else -amt
                    # 历史副作用回滚：早期版本会因现金退款把客户标成「已终止」，删掉这条退款即撤销该标记
                    cl = r.get('client')
                    if cl and cs.get(cl) and str(cs[cl].get('refundId') or '') == str(target_id):
                        cs.pop(cl, None); cs_changed = True
                if cs_changed:
                    save_json(CLIENT_STATUS_FILE, cs)
                log_op(u, '退款管理', '删除', '；'.join(
                    '%s $%s（%s）' % (r.get('client'), r.get('amount', 0), r.get('type') or 'cash') for r in removed[:3]) +
                    '（累计退款 -$%s，可退备款回补）' % round(sum(float(r.get('amount') or 0) for r in removed), 2))
                self._send_json({'ok': True, 'removed': removed[0],
                                 'restoredRefund': round(restored, 2),
                                 'clientRestored': bool(cs_changed)}); return

        self._send_json({'error': 'not found'}, 404)

    # ----------------------------------------------------------
    # 内部方法
    # ----------------------------------------------------------
    def _handle_mapping_import(self, file_data, u=None, filename=''):
        rows = parse_excel_bytes(file_data)
        if not rows:
            self._send_json({'ok': True, 'raw_rows': 0, 'added': 0, 'updated': 0, 'total': 0}); return
        with _lock:
            mapping = load_json(MAPPING_FILE)
            # 2026-09-24 BUGFIX: 去重键必须加 platform 变成 (accountId, date, platform)
            #   跨平台同 accountId（Facebook+TikTok 各有一段同日期区间）时，旧 key=(accountId, date) 会把第二段跳过
            #   2026-09-08 原始规则基础上扩展：三字段相同才跳过
            added = 0
            skipped = 0
            id_format_rejected = 0
            rejected_rows = []
            def key_of(i):
                return (normalize_account_id(i.get('accountId','')),
                        str(i.get('date','')).strip(),
                        normalize_platform(i.get('platform') or i.get('media') or ''))
            existing_keys = { key_of(m) for m in mapping }
            for r in rows:
                aid, aid_error = import_account_id_text(r)
                if aid_error:
                    skipped += 1
                    id_format_rejected += 1
                    rejected_rows.append({**r, '__reject_reason': '账号ID格式无效'})
                    continue
                # 2026-09-04 用户规则：客户/账号ID 必填 → Excel 行缺客户直接跳过并计数
                client_val = normalize_client(r.get('客户') or '')
                if not client_val:
                    skipped += 1
                    rejected_rows.append({**r, '__reject_reason': '客户为空'})
                    continue
                item = {
                    'date':     to_short_date(r.get('下户日期') or r.get('日期') or r.get('startDate') or ''),
                    'endDate':  to_short_date(r.get('结束日期') or r.get('endDate') or r.get('归属截止日') or ''),
                    'client':   normalize_client(client_val),
                    'name':     normalize_account_name(r.get('账号名称') or r.get('账户名称') or r.get('账户的名字')
                                    or r.get('名称') or ''),
                    'accountId': aid,
                    'channel':  normalize_channel(r.get('渠道') or ''),
                    'timezone': normalize_timezone(r.get('时区') or r.get('timezone') or ''),
                    'platform': normalize_platform(r.get('媒体') or r.get('platform') or ''),
                    'rate':     normalize_rate(r.get('服务点') or r.get('服务率') or r.get('费率') or 0),
                    'note':     str(r.get('备注') or r.get('note') or '').strip(),
                    # 审计修复 #1：模板已有「状态」列，导入时不再丢弃
                    'status':   str(r.get('状态') or r.get('status') or '').strip() or MAPPING_STATUS_DEFAULT,
                    'active':   True,
                }
                normalize_mapping_status(item)
                if key_of(item) in existing_keys:
                    continue
                item['id'] = gen_id()
                item['createdBy'] = u.get('name') or u.get('username') if u else '-'
                item['createdAt'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                mapping.append(item)
                existing_keys.add(key_of(item))
                added += 1
            save_json(MAPPING_FILE, mapping)
            self._refresh_consume_matching()
            # 写入导入日志
            ilog = load_json(IMPORT_LOG_FILE)
            err_file = save_import_error_file(rejected_rows, 'mapping')
            ilog.append({'time': datetime.now().isoformat(), 'type': 'mapping', 'file': filename,
                         'raw_rows': len(rows), 'added': added, 'updated': 0,
                         'rejected': skipped, 'id_format_rejected': id_format_rejected,
                         'rejected_ids': [], 'error_file': err_file})
            save_json(IMPORT_LOG_FILE, ilog)
        log_op(u, '下户表', '导入', f"新增 {added} 条，跳过 {skipped} 条（缺客户/账号ID）")
        self._send_json({'ok': True, 'raw_rows': len(rows),
                         'added': added, 'updated': 0, 'skipped': skipped,
                         'id_format_rejected': id_format_rejected,
                         'total': len(mapping)})

    def _handle_recharge_import(self, file_data, u=None, filename=''):
        """充值批量导入（2026-09-07 增强）。
        规则：按 账户ID + 充值日期 命中下户段，回填 客户/账号名称/渠道/媒体/时区；
        2026-09-08 用户规则：充值账号ID必须在下户表中存在，否则拒绝（下户表为权威数据源）。
        """
        rows = parse_excel_bytes(file_data)
        if not rows:
            self._send_json({'ok': True, 'raw_rows': 0, 'added': 0, 'rejected': 0, 'total': 0}); return
        with _lock:
            recharge = load_json(RECHARGE_FILE)
            mapping_list = load_json(MAPPING_FILE)
            mapping_index = build_mapping_index(mapping_list)
            # 简单 fallback：同 aid 取第一段
            mapping_by_aid = {}
            for m in mapping_list:
                key = normalize_account_id(m.get('accountId', ''))
                if key and key not in mapping_by_aid:
                    mapping_by_aid[key] = m
            # 2026-09-10 去重规则已移除：充值批量导入不再限制同日期同账号重复，直接全部导入
            added_count = 0
            rejected = 0
            rejected_ids = set()
            dup_count = 0
            id_format_rejected = 0
            rejected_rows = []
            for r in rows:
                aid, aid_error = import_account_id_text(r)
                if aid_error:
                    rejected += 1
                    id_format_rejected += 1
                    rejected_rows.append({**r, '__reject_reason': '账号ID格式无效'})
                    continue
                if not aid:
                    rejected_rows.append({**r, '__reject_reason': '账号ID为空'})
                    continue
                rdate = to_short_date(r.get('日期') or r.get('充值日期') or r.get('Date') or '')
                # 2026-09-07：优先按日期 + aid 匹配下户段，fallback 到同 aid 第一段
                m = pick_mapping_segment(mapping_index, aid, rdate) or mapping_by_aid.get(aid)
                if m is None:
                    # 2026-09-08：账号ID不在下户表 → 拒绝（下户表为权威数据源）
                    rejected += 1
                    rejected_ids.add(aid)
                    rejected_rows.append({**r, '__reject_reason': '账号ID未在下户表登记'})
                    continue
                # 回填优先级：Excel原值 > 下户段值
                _name   = normalize_account_name(r.get('账号名称') or r.get('账户名称') or '')
                _client = normalize_client(r.get('客户') or '')
                _chan   = normalize_channel(r.get('渠道') or '')
                _plat   = normalize_platform(r.get('媒体平台') or r.get('平台') or '')
                _tz     = normalize_timezone(r.get('时区') or '')
                # 2026-09-08：清零金额列兼容「清零」文本 → 转为 0
                _raw_clear = r.get('清零金额') or r.get('清零') or 0
                if isinstance(_raw_clear, str) and _raw_clear.strip() == '清零':
                    _clear_val = 0.0
                else:
                    try:
                        _clear_val = float(_raw_clear) if _raw_clear else 0.0
                    except (ValueError, TypeError):
                        _clear_val = 0.0
                _amount = normalize_amount(r.get('充值金额') or r.get('充值') or r.get('金额') or 0)
                # 2026-09-10 重复判断已移除：不再拒绝同日期同账号的重复数据，直接导入
                recharge.append({
                    'id':       gen_id(),
                    'date':     rdate,
                    'accountName': _name   or (m.get('name','') if m else ''),
                    'accountId': aid,
                    'amount':   _amount,
                    'clear':    _clear_val,
                    'client':   _client or (m.get('client','') if m else '') or '无客户',
                    'channel':  _chan   or (m.get('channel','') if m else ''),
                    'platform': _plat   or (m.get('platform','') if m else '') or 'Facebook',
                    # 2026-09-07：回填时区和状态（充值本身不改状态，只读展示）
                    'timezone': _tz or (m.get('timezone','') if m else ''),
                    'status':   str(r.get('状态') or '').strip() or '正常',
                    'note':     str(r.get('备注') or '').strip(),
                    'createdBy': u.get('name') or u.get('username') if u else '-',
                    'createdAt': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                })
                added_count += 1
            save_json(RECHARGE_FILE, recharge)
            # 写入导入日志
            ilog = load_json(IMPORT_LOG_FILE)
            err_file = save_import_error_file(rejected_rows, 'recharge')
            ilog.append({'time': datetime.now().isoformat(), 'type': 'recharge', 'file': filename,
                         'raw_rows': len(rows), 'added': added_count, 'updated': 0,
                         'rejected': rejected, 'rejected_ids': sorted(rejected_ids)[:20],
                         'duplicated': dup_count, 'id_format_rejected': id_format_rejected,
                         'error_file': err_file})
            save_json(IMPORT_LOG_FILE, ilog)
        log_op(u, '充值管理', '导入', f"新增 {added_count} 条" + (f"，拒绝 {rejected} 条（未下户/重复）" if rejected else ""))
        self._send_json({'ok': True, 'raw_rows': len(rows),
                         'added': added_count, 'total': len(recharge),
                         'rejected': rejected,
                         'duplicated': dup_count,
                         'id_format_rejected': id_format_rejected,
                         'rejected_ids': sorted(rejected_ids)[:20]})

    def _handle_payment_import(self, file_data, u=None, filename=''):
        """打款批量导入（2026-09-07 新增）。
        规则：不验证账号ID，只验证「客户」必填 + 「金额」必填且非零；
        支持列名：日期/客户/金额/打款方式/备注（兼容别名）。
        """
        rows = parse_excel_bytes(file_data)
        if not rows:
            self._send_json({'ok': True, 'raw_rows': 0, 'added': 0, 'skipped': 0}); return
        operator = (u.get('name') or u.get('username')) if u else '-'
        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        added = 0
        skipped = 0
        skip_reasons = []
        rejected_rows = []
        with _lock:
            payment = load_json(PAYMENT_FILE)
            for r in rows:
                client = normalize_client(import_cell(r, '客户', '客户名称', '收款客户', '付款客户'))
                amount = normalize_amount(import_amount_cell(
                    r, '金额', '金额USD', '金额(USD)', '金额（USD）', '打款金额',
                    '打款金额USD', '到账金额', '实际到账金额', '实际打款金额',
                    '入账金额', '付款金额', 'Amount'
                ) or 0)
                # 只验证客户和金额，不验证账号ID
                if not client:
                    skipped += 1
                    skip_reasons.append('客户为空')
                    rejected_rows.append({**r, '__reject_reason': '客户为空'})
                    continue
                if not amount:
                    skipped += 1
                    skip_reasons.append(f"客户「{client}」金额为0或空")
                    rejected_rows.append({**r, '__reject_reason': '金额为0或空'})
                    continue
                payment.append({
                    'id':        gen_id(),
                    'date':      to_short_date(import_cell(r, '日期', '打款日期', '付款日期', 'Date') or ''),
                    'client':    client,
                    'amount':    amount,
                    'method':    str(import_cell(r, '打款方式', '方式', '付款方式', 'Method') or '').strip(),
                    'note':      str(import_cell(r, '备注', '说明', 'Note') or '').strip(),
                    'createdBy': operator,
                    'createdAt': now_str,
                })
                added += 1
            save_json(PAYMENT_FILE, payment)
            # 写入导入日志
            ilog = load_json(IMPORT_LOG_FILE)
            err_file = save_import_error_file(rejected_rows, 'payment')
            ilog.append({'time': datetime.now().isoformat(), 'type': 'payment', 'file': filename,
                         'raw_rows': len(rows), 'added': added, 'updated': 0,
                         'rejected': skipped, 'rejected_ids': [], 'skip_reasons': skip_reasons[:100],
                         'error_file': err_file})
            save_json(IMPORT_LOG_FILE, ilog)
        log_op(u, '打款管理', '批量导入', f"新增 {added} 条" + (f"，跳过 {skipped} 条" if skipped else ""))
        self._send_json({'ok': True, 'raw_rows': len(rows),
                         'added': added, 'skipped': skipped,
                         'skip_reasons': skip_reasons[:100]})

    def _handle_refund_import(self, file_data, u=None, filename=''):
        """退款/平账批量导入（2026-09-07 新增）。
        规则：不验证账号ID，只验证「客户」必填 + 「金额」必填且非零；
        支持列名：日期/客户/金额/类型/方式/备注（兼容别名）。
        类型：cash=现金退款，offset=平账补差（默认 cash）。
        """
        rows = parse_excel_bytes(file_data)
        if not rows:
            self._send_json({'ok': True, 'raw_rows': 0, 'added': 0, 'skipped': 0}); return
        operator = (u.get('name') or u.get('username')) if u else '-'
        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        added = 0
        skipped = 0
        skip_reasons = []
        rejected_rows = []
        with _lock:
            refund = load_json(REFUND_FILE)
            for r in rows:
                client = normalize_client(r.get('客户') or r.get('客户名称') or '')
                amount = normalize_amount(r.get('金额') or r.get('退款金额') or r.get('平账金额') or 0)
                # 只验证客户和金额，不验证账号ID
                if not client:
                    skipped += 1
                    skip_reasons.append('客户为空')
                    rejected_rows.append({**r, '__reject_reason': '客户为空'})
                    continue
                if not amount:
                    skipped += 1
                    skip_reasons.append(f"客户「{client}」金额为0或空")
                    rejected_rows.append({**r, '__reject_reason': '金额为0或空'})
                    continue
                rtype = str(r.get('类型') or r.get('type') or 'cash').strip()
                if rtype not in ('cash', 'offset'):
                    rtype = 'cash'
                refund.append({
                    'id':        gen_id(),
                    'date':      to_short_date(r.get('日期') or r.get('退款日期') or r.get('平账日期') or r.get('Date') or ''),
                    'client':    client,
                    'amount':    amount,
                    'type':      rtype,
                    'method':    str(r.get('方式') or r.get('method') or r.get('打款方式') or '').strip(),
                    'note':      str(r.get('备注') or r.get('Note') or '').strip(),
                    'createdBy': operator,
                    'createdAt': now_str,
                })
                added += 1
            save_json(REFUND_FILE, refund)
            # 写入导入日志
            ilog = load_json(IMPORT_LOG_FILE)
            err_file = save_import_error_file(rejected_rows, 'refund')
            ilog.append({'time': datetime.now().isoformat(), 'type': 'refund', 'file': filename,
                         'raw_rows': len(rows), 'added': added, 'updated': 0,
                         'rejected': skipped, 'rejected_ids': [], 'error_file': err_file})
            save_json(IMPORT_LOG_FILE, ilog)
        log_op(u, '退款管理', '批量导入', f"新增 {added} 条" + (f"，跳过 {skipped} 条" if skipped else ""))
        self._send_json({'ok': True, 'raw_rows': len(rows),
                         'added': added, 'skipped': skipped,
                         'skip_reasons': skip_reasons[:10]})

    def _guess_excel_source(self, filename: str, first_row: dict) -> str:
        """从 Excel 文件名 + 行内媒体列猜渠道 source 值。
        返回: excel_facebook / excel_tiktok / excel_google / excel_unknown
        """
        fname_lower = (filename or '').lower()
        # 1) 先看文件名关键词
        tiktok_kw  = ['tiktok', 'tt-', 'tt_', 'bc ', 'bc_', 'business center', 'tiktok ads']
        google_kw  = ['google', 'google ads', 'googleads', 'ads ', 'ads_', 'googleads', 'mcc']
        facebook_kw = ['facebook', 'fb ', 'fb_', 'bm ', 'business manager', '绿通', 'media_budget', 'meta_ads']
        if any(k in fname_lower for k in tiktok_kw):  return 'excel_tiktok'
        if any(k in fname_lower for k in google_kw):  return 'excel_google'
        if any(k in fname_lower for k in facebook_kw): return 'excel_facebook'
        # 2) 再看 Excel 行内的媒体列（各渠道下载的 Excel 表头会带 "媒体/Media/平台/Platform"）
        if first_row:
            for col in ('媒体', 'Media', 'media', '平台', 'Platform', 'platform'):
                v = first_row.get(col) or ''
                if v:
                    vl = str(v).lower()
                    if 'tiktok' in vl: return 'excel_tiktok'
                    if 'google' in vl or 'ads' in vl: return 'excel_google'
                    if 'facebook' in vl or 'instagram' in vl or vl == 'fb': return 'excel_facebook'
        return 'excel_unknown'

    def _handle_consume_import(self, file_data, u=None, filename=''):
        raw_rows = parse_excel_bytes(file_data)
        # 2026-09-24: 自动识别 Excel 渠道 → 写入 source 字段（解决跨平台同 accountId 冲突）
        _excel_source = self._guess_excel_source(filename, raw_rows[0] if raw_rows else {})
        print(f'[Excel导入] filename={filename!r} → source={_excel_source}, rows={len(raw_rows)}', flush=True)
        with _lock:
            mapping_list = load_json(MAPPING_FILE)
            mapping_index = build_mapping_index(mapping_list)
            # 2026-09-07 构建 账号名称→accountId 映射，供账号ID匹配失败时兜底
            name_to_aid = {}
            for mm in mapping_list:
                nm = str(mm.get('name') or '').strip()
                aid = normalize_account_id(mm.get('accountId') or '')
                if nm and aid and nm not in name_to_aid:
                    name_to_aid[nm] = aid
            consume = load_json(CONSUME_FILE)
            # 2026-09-24 BUGFIX: Excel 导入唯一键也必须带 source（否则与 API 拉取互相覆盖）
            existing_keys = {(r.get('date', ''), r.get('accountId', ''), r.get('source', '')) for r in consume}

            added = updated = rejected = 0
            no_date = 0          # 2026-09-09：因缺日期被跳过的行数（与「未下户被拒」分开统计）
            overdue = 0
            media_mismatch = 0
            mismatched = []
            rejected_ids = set()
            id_format_rejected = 0
            rejected_rows = []
            for raw in raw_rows:
                # 2026-09-07 统一表头：优先账号ID，其次账号名称
                aid_candidate, aid_error = import_account_id_text(raw)
                if aid_error:
                    rejected += 1
                    id_format_rejected += 1
                    rejected_rows.append({**raw, '__reject_reason': '账号ID格式无效'})
                    continue
                row = enrich_consume_row(raw, mapping_index, name_to_aid)
                if not row:
                    if aid_candidate and not consume_row_date(raw):
                        # 有账号、但日期列为空/不是日期（Excel 末尾「合计」行、空行、日期列变文本）
                        #   → 不是"未下户"问题，单独计数，否则会误报"广告ID未在下户表登记"
                        no_date += 1
                        rejected_rows.append({**raw, '__reject_reason': '日期列为空/无法解析'})
                    else:
                        rejected += 1
                        if aid_candidate: rejected_ids.add(aid_candidate)
                        rejected_rows.append({**raw, '__reject_reason': '账号ID未在下户表登记'})
                    continue
                # 2026-09-24: 写入 source（Excel 导入自动识别）
                row['source'] = _excel_source
                # 2026-09-24 BUGFIX: key 加 source 做三元组
                key = (row['date'], row['accountId'], _excel_source)
                # 再次用 mapping_index + 该条消耗日期刷新，保证客户/渠道字段一定按区间命中
                # 2026-09-24: 按 Excel 媒体 / source 推导平台来过滤 mapping 段
                _excel_platform = row.get('media_from_file') or _excel_source.replace('excel_','') or 'Facebook'
                m2 = pick_mapping_segment(mapping_index, row['accountId'], row['date'], platform_filter=_excel_platform)
                if m2:
                    row['client']      = m2.get('client', '')
                    row['channel']     = m2.get('channel', '')
                    row['serviceRate'] = m2.get('rate', 0) or 0
                    row['matched']     = True
                    row['_segStart']   = m2.get('date', '') or ''
                    row['_segEnd']     = m2.get('endDate', '') or ''
                    # 审计修复 #3 + 2026-09-06 补盲：区间外（晚于段截止 / 早于段起始）都留痕，segOut 记方向
                    row['segOut'] = seg_out_reason(str(row.get('date') or ''), m2)
                    row['overdue'] = bool(row['segOut'])
                    # 2026-09-04 用户规则：BM 导入前校验「媒体一致性」——
                    #   Excel 行自带的媒体 与 下户表归属段的媒体 不一致 → 拒绝导入（防止数据挂错媒体）
                    #   下户段媒体留空 = 不设限（以 Excel 媒体为准）
                    m_platform = (m2.get('platform') or m2.get('media') or '')
                    if load_settings().get('media_check', True) and row.get('media_from_file') and m_platform and row['media_from_file'] != m_platform:
                        media_mismatch += 1
                        mismatched.append(f"{row['accountId']}({row['media_from_file']}≠{m_platform})")
                        rejected_rows.append({**raw, '__reject_reason': f'媒体不一致（Excel:{row["media_from_file"]} ≠ 下户段:{m_platform}）'})
                        continue
                    row['platform'] = row.get('media_from_file') or m_platform or 'Facebook'
                if row.get('overdue'): overdue += 1
                # 2026-09-07 新增：记录本条消耗数据的导入时间（数据概览「导入日期」列取该账户所有 consume 记录的最大值）
                row['importedAt'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                if key in existing_keys:
                    # 同日期+账号+同 source 再次上传 → upsert（回流消耗更新逻辑）
                    for i, old_row in enumerate(consume):
                        if (old_row.get('date'), old_row.get('accountId'), old_row.get('source', '')) == key:
                            old_included = old_row.get(
                                'spend_including_reflow',
                                old_row.get('reflowSpend', old_row.get('spend', 0)),
                            ) or 0
                            new_included = row.get('spend_including_reflow', row.get('spend', 0)) or 0
                            row['spend'] = float(new_included)
                            row['spend_including_reflow'] = float(new_included)
                            row['spend_usd_total'] = float(old_included)
                            row['reflow_difference'] = float(new_included) - float(old_included)
                            consume[i] = row; break
                    updated += 1
                else:
                    # 新日期或新账号首次录入：当前值同时作为初始总消耗，差额为 0。
                    included = row.get('spend_including_reflow', row.get('spend', 0)) or 0
                    row['spend'] = float(included)
                    row['spend_including_reflow'] = float(included)
                    row['spend_usd_total'] = float(included)
                    row['reflow_difference'] = 0
                    consume.append(row)
                    existing_keys.add(key)
                    added += 1

            consume.sort(key=lambda r: r.get('date', ''))
            save_json(CONSUME_FILE, consume)

            log = load_json(IMPORT_LOG_FILE)
            err_file = save_import_error_file(rejected_rows, 'consume')
            log.append({'time': datetime.now().isoformat(), 'type': 'consume', 'file': filename,
                        'raw_rows': len(raw_rows), 'added': added, 'updated': updated,
                        'rejected': rejected, 'no_date': no_date,
                        'id_format_rejected': id_format_rejected,
                        'rejected_ids': sorted(list(rejected_ids))[:20],
                        'media_mismatch': media_mismatch, 'mismatched': mismatched[:20],
                        'error_file': err_file})
            save_json(IMPORT_LOG_FILE, log)
        log_op(u, 'BM消耗导入', '导入',
               f"新增 {added} / 更新 {updated} / 拒绝 {rejected}（未下户）/ 跳过 {no_date}（缺日期）/ 媒体不一致 {media_mismatch}")

        self._send_json({'ok': True, 'raw_rows': len(raw_rows),
                         'added': added, 'updated': updated,
                         'rejected': rejected,
                         'id_format_rejected': id_format_rejected,
                         # 2026-09-09：日期列为空/无法解析而被跳过的行数（这些行不再写成 date='' 的幽灵消耗记录）
                         'no_date': no_date,
                         # 2026-09-04 BUGFIX: 前端要显示"哪些 ID 没在下户表里录入 → 被拒绝"的清单（最多前 20 个，防止超长）
                         'rejected_ids': sorted(list(rejected_ids))[:20],
                         # 审计修复 #3：本次导入中"超出归属期被尾段兜底吸收"的行数
                         'overdue': overdue,
                         # 2026-09-04 媒体一致性校验：Excel媒体 ≠ 下户段媒体 → 拒绝
                         'media_mismatch': media_mismatch,
                         'mismatched': mismatched[:20],
                         'total_consume': len(consume)})

    def _refresh_consume_matching(self):
        """每当下户表变更后重跑：把所有消耗记录按「消耗日期 ∈ 下户归属区间」重新分配客户/渠道。
        若同 accountId 多段，按区间命中；无匹配段（没有下户）则剔除该消耗记录。
        """
        mapping_list = load_json(MAPPING_FILE)
        mapping_index = build_mapping_index(mapping_list)
        consume = load_json(CONSUME_FILE)
        consume_before = len(consume)
        changed = 0
        filtered = []
        for r in consume:
            aid = str(r.get('accountId', '')).strip()
            cdate = str(r.get('date', '')).strip()
            # 2026-09-24: 重跑归属时按 consume 行自身的 platform 过滤 mapping 段
            seg = pick_mapping_segment(mapping_index, aid, cdate, platform_filter=r.get('platform'))
            if not seg:
                # 没匹配到下户表 → 剔除（业务上不允许没有归属的消耗）
                continue
            nc   = seg.get('client', '')  or ''
            nch  = seg.get('channel', '') or ''
            nr   = float(seg.get('rate', 0) or 0)
            nsd  = seg.get('date', '')    or ''
            ned  = seg.get('endDate', '') or ''
            # 审计修复 #3 + 2026-09-06 补盲：重跑归属时刷新标记与方向（含早于段起始）
            r['segOut'] = seg_out_reason(str(cdate or ''), seg)
            r['overdue'] = bool(r['segOut'])
            # 比较 5 个字段，任一变动则记一次 update
            if (r.get('client') != nc or r.get('channel') != nch
                    or (r.get('serviceRate',0) or 0) != nr
                    or r.get('_segStart','') != nsd
                    or r.get('_segEnd','')   != ned
                    or not r.get('matched')):
                r['client'] = nc
                r['channel'] = nch
                r['serviceRate'] = nr
                r['_segStart']   = nsd
                r['_segEnd']   = ned
                r['matched'] = True
                changed += 1
            filtered.append(r)
        if changed or len(filtered) != consume_before:
            save_json(CONSUME_FILE, filtered)   # 无变化不写盘 → 不 bump 版本，让统计缓存能命中
        print(f"[刷新匹配] 保留 {len(filtered)} 条, 更新了 {changed} 条")

    def _compute_consume_stats(self, user=None, refresh_matching=False, date_from=None, date_to=None):
        # 2026-09-04 性能：全量聚合结果按 (数据版本 + 用户 + 区间) 缓存。
        # 数据有任何写入（save_json bump 版本）自动失效；命中时零计算返回。
        key = (_DATA_VERSION, (user or {}).get('id', 'anon'), str(date_from or ''), str(date_to or ''))
        hit = _STATS_CACHE.get(key)
        if hit is not None:
            _STATS_CACHE.move_to_end(key)
            return hit
        result = self._compute_consume_stats_raw(user=user, refresh_matching=refresh_matching,
                                                 date_from=date_from, date_to=date_to)
        _STATS_CACHE[key] = result
        while len(_STATS_CACHE) > 48:      # FIFO 限量，防内存增长
            _STATS_CACHE.popitem(last=False)
        return result

    def _compute_consume_stats_raw(self, user=None, refresh_matching=False, date_from=None, date_to=None):
        with _lock:
            data = load_json(CONSUME_FILE)
            mapping_list = load_json(MAPPING_FILE)
            recharge = load_json(RECHARGE_FILE)
        # 关键：下户表改了状态/客户/渠道/服务点后，必须先重跑消耗归属，否则读取的是历史消耗文件缓存（之前可能写入了旧的归属段 client/status）
        if refresh_matching:
            try:
                self._refresh_consume_matching()
                with _lock:
                    data = load_json(CONSUME_FILE)
            except Exception:
                pass
        # 2026-09-09：date='' 的历史幽灵行不进任何聚合（否则 账户数/更新日期/天数 会被空行污染；
        #   其 spend 通常为 0，金额不受影响，但维度计数与「按日期汇总」的天数会失真）
        for row in data:
            included = row.get('spend_including_reflow', row.get('reflowSpend', row.get('spend', 0))) or 0
            total_usd = row.get('spend_usd_total', row.get('totalSpendUSD', row.get('spend', 0))) or 0
            row['spend_including_reflow'] = float(included)
            row['spend_usd_total'] = float(total_usd)
            row['reflow_difference'] = row['spend_including_reflow'] - row['spend_usd_total']
        data = [r for r in data if consume_row_has_date(r)]
        data = self._filter_for_user(data, user)
        mapping_list = self._filter_for_user(mapping_list, user)
        recharge = self._filter_for_user(recharge, user)
        # 2026-09-04 区间查询：消耗/充值/打款/退款全部按日期区间过滤（口径=区间内发生的业务）
        if date_from: data = [r for r in data if (r.get('date') or '') >= date_from]
        if date_to:   data = [r for r in data if (r.get('date') or '') <= date_to]
        if date_from: recharge = [r for r in recharge if (r.get('date') or '') >= date_from]
        if date_to:   recharge = [r for r in recharge if (r.get('date') or '') <= date_to]
        mapping_index = build_mapping_index(mapping_list)
        # 权威客户/渠道/服务点/备注/状态：从"今日归属段"取；空取最新段；再空取最早段；兜底=正常
        # 与消耗归属 pick_mapping_segment 完全同源，保证多段 mapping 下账户表与下户表一致
        today_str = datetime.now().strftime('%Y-%m-%d')

        def _mapping_meta(aid):
            aid_s = str(aid) if aid is not None else ''
            segments = mapping_index.get(aid_s, [])
            if not segments:
                return {'client': '', 'channel': '', 'platform': 'Facebook', 'rate': 0, 'name': '', 'note': '', 'status': '正常'}
            # 1) 今日归属段
            seg = pick_mapping_segment(mapping_index, aid_s, today_str) if today_str else None
            if not seg and segments:
                # 2) 最新段（start 最大，没有 start 的兜底用 index 最大）
                segs_sorted = sorted(segments, key=lambda s: (s.get('date') or '', 0), reverse=False)
                seg = segs_sorted[-1]
            if not seg and segments:
                seg = segments[0]
            seg = seg or {}
            return {
                'client':   seg.get('client') or '',
                'channel':  seg.get('channel') or '',
                'platform': seg.get('platform') or (seg.get('media') or 'Facebook'),
                'rate':     seg.get('rate', 0) or 0,
                'name':     seg.get('accountName') or seg.get('name') or '',
                'note':     seg.get('note', '') or '',
                'status':   normalize_mapping_status(seg.get('status')),
            }

        mapping = {str(m.get('accountId')): m for m in mapping_list}

        # 2026-09-04：raw_payments 必须在"账户分摊 payment"之前加载（否则 L1588 会 UnboundLocalError）
        # 财务类数据不受停用客户过滤影响，直接读原始文件；加锁读以保证一致性
        with _lock:
            raw_payments = load_json(PAYMENT_FILE)
            raw_refunds = load_json(REFUND_FILE)
            raw_recharge = load_json(RECHARGE_FILE)
        # 2026-09-04 权限修复：财务流水同样按账号的数据范围过滤——
        # 否则客户账号的 打款/充值/清零/备款 全是全公司口径（Hero 卡数字错误）
        raw_payments = self._filter_for_user(raw_payments, user)
        raw_refunds  = self._filter_for_user(raw_refunds, user)
        raw_recharge = self._filter_for_user(raw_recharge, user)
        if date_from:
            raw_payments = [p for p in raw_payments if (p.get('date') or '') >= date_from]
            raw_refunds  = [r for r in raw_refunds if (r.get('date') or '') >= date_from]
            raw_recharge = [r for r in raw_recharge if (r.get('date') or '') >= date_from]
        if date_to:
            raw_payments = [p for p in raw_payments if (p.get('date') or '') <= date_to]
            raw_refunds  = [r for r in raw_refunds if (r.get('date') or '') <= date_to]
            raw_recharge = [r for r in raw_recharge if (r.get('date') or '') <= date_to]

        cl_agg = {}; ch_agg = {}; daily = {}; acc_agg = {}
        def included_spend(row):
            return row.get('spend_including_reflow', row.get('reflowSpend', row.get('spend', 0))) or 0
        for r in data:
            sp = included_spend(r); cl = r.get('client',''); ch = r.get('channel','')
            dt = r.get('date',''); aid = r.get('accountId',''); rate = r.get('serviceRate',0) or 0
            total_usd = r.get('spend_usd_total', r.get('totalSpendUSD', r.get('spend', 0))) or 0
            # 服务费按原始浮点累加，最终由接口/前端展示边界统一保留两位小数。
            fc = fee_raw(sp, rate)
            if cl:
                g = cl_agg.setdefault(cl, {'spend':0,'totalSpendUSD':0,'reflowDifference':0,'fee':0,'rows':0})
                g['spend'] += sp; g['totalSpendUSD'] += total_usd
                g['reflowDifference'] += sp - total_usd; g['fee'] += fc; g['rows'] += 1
            if ch:
                g = ch_agg.setdefault(ch, {'spend':0,'totalSpendUSD':0,'reflowDifference':0,'accounts':set()})
                g['spend'] += sp; g['totalSpendUSD'] += total_usd
                g['reflowDifference'] += sp - total_usd; g['accounts'].add(aid)
            if dt:
                g = daily.setdefault(dt, {'spend':0,'totalSpendUSD':0,'reflowDifference':0,'fee':0})
                g['spend'] += sp; g['totalSpendUSD'] += total_usd
                g['reflowDifference'] += sp - total_usd; g['fee'] += fc
            if aid:
                g = acc_agg.setdefault(aid, {'spend':0,'totalSpendUSD':0,'reflowDifference':0,'fee':0,'name':r.get('accountName',''), 'client':''})
                g['spend'] += sp; g['totalSpendUSD'] += total_usd
                g['reflowDifference'] += sp - total_usd; g['fee'] += fc
                if r.get('accountName'): g['name'] = r.get('accountName')
                if cl: g['client'] = cl
        for k in ch_agg: ch_agg[k]['accounts'] = len(ch_agg[k]['accounts'])

        # 2026-09-04 审计修复 #4/#5：
        #   balances 按 (账户ID × 归属段客户) 拆行 —— 多段跨客户账户（如 M1 段1→甲、段2→乙）
        #   在账户表/打款分摊/备款中保持与行级消耗归属一致；服务费 = 行级 spend×serviceRate 汇总，
        #   修复"全段消耗×今日段费率"的费率错配。
        def _meta_for(aid, client):
            """该账户指定客户的权威段：今日段优先（客户相符时），否则该客户最新段"""
            segs = [s for s in (mapping_index.get(str(aid)) or [])
                    if (s.get('client') or '') == (client or '')]
            if not segs:
                return None
            today_seg = pick_mapping_segment(mapping_index, aid, today_str) if today_str else None
            if today_seg and (today_seg.get('client') or '') == (client or ''):
                return today_seg
            return sorted(segs, key=lambda s: (s.get('date') or ''))[-1]

        def _bal_key(aid, cl):
            return str(aid) + '\u0001' + str(cl or '')

        bal_map = {}
        # ① 充值：挂 (账户, 【充值日期】所属归属段客户)——转户后充值/清零归对应期间的客户；
        #    无下户 → 用充值行自带客户并标记 no_mapping
        for r in recharge:
            aid = str(r.get('accountId',''))
            if not aid: continue
            meta = _mapping_meta(aid)
            no_map = not mapping_index.get(aid)
            # 转户规则（2026-09-04）：按充值日期命中归属段，充值/清零归该段客户；
            # 空窗期/无段时回退 今日段客户 → 充值行自带客户
            rseg = pick_mapping_segment(mapping_index, aid, str(r.get('date','') or ''))
            seg_meta = rseg or meta
            cl = (rseg.get('client') if rseg else '') or meta.get('client') or r.get('client','')
            bg = bal_map.setdefault(_bal_key(aid, cl), {
                'accountId': aid,
                'accountName': (seg_meta.get('name') if seg_meta else None) or r.get('accountName',''),
                'client':      cl,
                'channel':     (seg_meta.get('channel') if seg_meta else None) or r.get('channel',''),
                'platform':    (seg_meta.get('platform') if seg_meta else None) or r.get('platform','Facebook'),
                'timezone':    (seg_meta.get('timezone') if seg_meta else None) or '',
                'rate':        (seg_meta.get('rate', 0) if seg_meta else 0) or 0,
                'status':      (seg_meta.get('status') if seg_meta else None) or '正常',
                'no_mapping':  no_map,
                'lastDate':    '',
                'importDate':  '',
                'payment': 0, 'recharge': 0, 'clear': 0, 'spend': 0,
                'totalSpendUSD': 0, 'reflowDifference': 0, 'fee': 0,
            })
            bg['recharge'] += r.get('amount',0); bg['clear'] += r.get('clear',0)
            bg['no_mapping'] = bg.get('no_mapping') or no_map

        # ② 消耗：按 (账户, 行级客户=归属段客户) 累计；服务费按行级费率
        for r in data:
            aid = str(r.get('accountId','') or '')
            if not aid: continue
            cl = (r.get('client') or '').strip()
            meta = _meta_for(aid, cl) or _mapping_meta(aid)
            no_map = not mapping_index.get(aid)
            bg = bal_map.setdefault(_bal_key(aid, cl), {
                'accountId': aid,
                'accountName': (meta.get('name') if meta else None) or r.get('accountName',''),
                'client':      cl,
                'channel':     (meta.get('channel') if meta else None) or (r.get('channel') or ''),
                'platform':    (meta.get('platform') if meta else None) or r.get('platform','Facebook'),
                'timezone':    (meta.get('timezone') if meta else None) or '',
                'rate':        (meta.get('rate', 0) if meta else 0) or 0,
                'status':      (meta.get('status') if meta else None) or '正常',
                'no_mapping':  no_map,
                'lastDate':    '',
                'importDate':  '',
                'payment': 0, 'recharge': 0, 'clear': 0, 'spend': 0,
                'totalSpendUSD': 0, 'reflowDifference': 0, 'fee': 0,
            })
            sp = included_spend(r)
            bg['spend'] += sp
            total_usd = r.get('spend_usd_total', r.get('totalSpendUSD', sp)) or 0
            bg['totalSpendUSD'] += total_usd
            bg['reflowDifference'] += sp - total_usd
            bg['fee'] += fee_raw(sp, r.get('serviceRate', 0) or 0)
            d = to_short_date(r.get('date'))
            if d and d > (bg.get('lastDate') or ''):
                bg['lastDate'] = d                              # 最近一次日消耗日期（排序用）
            # 2026-09-07 导入日期：取该账户所有 consume 记录中最大的 importedAt，统一短日期 YYYY-MM-DD
            _imp = r.get('importedAt') or ''
            if _imp:
                _imp_date = to_short_date(_imp)
                if _imp_date > (bg.get('importDate') or ''):
                    bg['importDate'] = _imp_date
            if r.get('accountName') and meta:
                bg['accountName'] = meta.get('name') or bg['accountName']

        # ③ 补齐：下户表有但 bal_map 没有的账号（无消耗、无充值、无清零 → 数据概况也需展示）
        # 2026-09-12 用户要求：无消耗数据的账号ID也要在数据概况中显示
        for mid, mrow in mapping_index.items():
            # 该账号在 mapping 中属于哪个客户（取最新归属段）
            m_segments = mapping_index.get(mid) or []
            if not m_segments: continue
            latest_seg = max(m_segments, key=lambda s: (s.get('date') or ''))
            cl = (latest_seg.get('client') or '').strip() or '-'
            _k = _bal_key(mid, cl)
            if _k in bal_map: continue   # 已有消耗/充值 → 跳过
            # 补一个全零条目，所有元数据从下户表取
            bal_map[_k] = {
                'accountId':   mid,
                'accountName': latest_seg.get('name', ''),
                'client':      cl,
                'channel':     latest_seg.get('channel', ''),
                'platform':    latest_seg.get('platform', 'Facebook'),
                'timezone':    latest_seg.get('timezone', ''),
                'rate':        latest_seg.get('rate', 0) or 0,
                'status':      normalize_mapping_status(latest_seg.get('status')) or '正常',
                'no_mapping':  False,
                'lastDate':    '',
                'importDate':  '',
                'payment': 0, 'recharge': 0, 'clear': 0, 'spend': 0,
                'totalSpendUSD': 0, 'reflowDifference': 0, 'fee': 0,
            }

        # ④ 打款分摊：客户池 → 客户内条目按 (消+服) 占比分摊（条目键为二元组，逻辑不变）
        # 客户维度汇总
        payByClient = {}  # {client: payment_total}
        for p in raw_payments:
            cl = p.get('client', '') or ''
            if not cl: continue
            payByClient[cl] = (payByClient.get(cl, 0.0) or 0.0) + (p.get('amount', 0.0) or 0.0)
        # 先算每客户的 consume+fee 总额（用于分摊打款）
        clSF = {}
        for key, g in bal_map.items():
            cl = g.get('client', '') or ''
            sf = (g.get('spend',0.0) or 0.0) + (g.get('fee',0.0) or 0.0)
            clSF[cl] = (clSF.get(cl, 0.0) or 0.0) + sf
        # 每客户内的条目列表（用于分摊）
        clAids = {}
        for key, g in bal_map.items():
            cl = g.get('client', '') or ''
            clAids.setdefault(cl, []).append(key)
        # 给每个条目分配 payment
        for cl, keys in clAids.items():
            totalPay = payByClient.get(cl, 0.0) or 0.0
            totalSF  = clSF.get(cl, 0.0) or 0.0
            if totalSF > 0 and keys:
                # 按 (消+服) 占比分摊；四舍五入的尾差归入占比最大的条目，保证 Σ分摊 == 客户池
                alloc = {}
                biggest = keys[0]
                for key in keys:
                    g = bal_map[key]
                    share = ((g.get('spend',0) or 0) + (g.get('fee',0) or 0)) / totalSF
                    alloc[key] = round(totalPay * share, 2)
                    sf_k = (g.get('spend',0) or 0) + (g.get('fee',0) or 0)
                    sf_b = (bal_map[biggest].get('spend',0) or 0) + (bal_map[biggest].get('fee',0) or 0)
                    if sf_k > sf_b: biggest = key
                residual = round(totalPay - sum(alloc.values()), 2)
                alloc[biggest] = round(alloc[biggest] + residual, 2)
                for key in keys:
                    bal_map[key]['payment'] = alloc[key]
            elif keys:
                # 客户没消耗 → 均分（少见：只充值清零无消耗）
                share = 1.0 / len(keys)
                each = round(totalPay * share, 2)
                residual = round(totalPay - each * len(keys), 2)
                for i, key in enumerate(keys):
                    bal_map[key]['payment'] = round(each + (residual if i == 0 else 0), 2)

        for key, bg in bal_map.items():
            aid = bg['accountId']
            # ✅ 2026-09-09 用户定稿：广告账号余额 = 账号累计充值(recharge) − 账号累计消耗(spend)
            #   不扣服务费 fee（服务费属于财务备款口径），不扣清零 clear（清零仅作账户流水展示）
            #   注：payment 字段（打款分摊值）仍保留给客户维度备款使用，但不再参与账户余额
            bg['balance'] = (bg.get('recharge',0) or 0) - bg['spend']
            # 备注与 status 权威：从该客户归属段取
            meta = _meta_for(aid, bg['client'])
            if meta and meta.get('note'):
                bg['note'] = meta['note']
            else:
                m = mapping.get(str(aid))
                bg['note'] = (m or {}).get('note', '')
            # 状态：始终从归属段取权威值，段为 None 则统一兜底 正常（100% 字段非空）
            bg['status'] = normalize_mapping_status(meta.get('status') if meta else None) or '正常'
            bg.setdefault('status', '正常')

        # 2026-09-07 最终权威覆盖：遍历所有 bal_map 条目，从下户表覆盖账号名称/渠道/时区/媒体
        # 确保数据概览「账号对账」表格的所有冗余字段 100% 来自下户表（权威数据源）
        for key, bg in bal_map.items():
            aid = bg['accountId']
            # 优先用今日段 + 客户匹配的段，fallback 到 mapping 通用查询
            meta_c = _meta_for(aid, bg['client'])
            meta_g = _mapping_meta(aid)
            meta = meta_c or meta_g or {}
            if meta:
                if meta.get('name'):     bg['accountName'] = meta['name']
                if meta.get('channel'):  bg['channel']     = meta['channel']
                if meta.get('timezone'): bg['timezone']    = meta['timezone']
                if meta.get('platform'): bg['platform']    = meta['platform']
                if meta.get('note'):     bg['note']        = meta['note']

        # 汇总指标（包含日期过滤）
        total_spend = sum(r.get('spend_including_reflow', r.get('spend', 0)) for r in data)
        total_spend_usd = sum(r.get('spend_usd_total', r.get('spend', 0)) for r in data)
        total_reflow_difference = sum(r.get('reflow_difference', 0) for r in data)
        total_recharge = sum(r.get('amount',0) for r in recharge)
        total_clear = sum(r.get('clear',0) for r in recharge)
        total_fee = sum(row_fee_raw(r) for r in data)

        # 财务类原始流水已经在本函数开头加载好（保证 payment 分摊阶段可用，也避免重复 IO 加锁）
        pay_all_total = sum(p.get('amount',0) for p in raw_payments)
        refund_all_total = sum(r.get('amount',0) for r in raw_refunds)
        rech_all_total = sum(r.get('amount',0) for r in raw_recharge)
        clear_all_total = sum(r.get('clear',0) for r in raw_recharge)

        # 备款口径：剩余备款 = 打款 − 退款 − 消耗 − 服务费。
        # 充值/清零仅作账户流水展示，不参与备款公式。
        #   与前端 renderReserve / renderCli / renderDaily / renderPay 同源（经验 100029509）
        consume_all_spend = round(sum(included_spend(r) for r in data), 2)
        consume_all_fee   = round(sum(fee_raw(included_spend(r), r.get('serviceRate', 0) or 0) for r in data), 2)
        remaining_reserve = round(pay_all_total - refund_all_total - consume_all_spend - consume_all_fee, 2)
        # 赊账场景允许备款为负数（红色警示 UI），这里不做 max(0) 截断

        return {
            'totalSpend': total_spend,
            'totalSpendUSD': total_spend_usd,
            'reflowDifference': total_reflow_difference,
            'totalRows': len(data),
            'totalAccounts': len({b['accountId'] for b in bal_map.values()}),
            'clients': list(cl_agg.keys()),
            'channels': list(ch_agg.keys()),
            'clientGroups': cl_agg, 'channelGroups': ch_agg, 'dailyGroups': daily,
            'unmatched': [],  # 已剔除
            'balances': list(bal_map.values()),
            'summary': {
                'spend_total': round(total_spend, 2),
                'spend_usd_total': round(total_spend_usd, 2),
                'reflow_difference': round(total_reflow_difference, 2),
                'recharge_total': round(total_recharge, 2),
                'clear_total': round(total_clear, 2),
                'fee_total': round(total_fee, 2),
                'client_count': len(set(m.get('client') for m in mapping_list if m.get('client'))),
                'channel_count': len(set(m.get('channel') for m in mapping_list if m.get('channel'))),
                'account_count': len(mapping_list),
                'platform_count': len(set(r.get('platform') for r in data if r.get('platform'))),
                # 财务口径：全局原始文件，不过滤停用
                'payment_total': round(pay_all_total, 2),
                'payment_count': len(raw_payments),
                'refund_total': round(refund_all_total, 2),
                'refund_count': len(raw_refunds),
                'recharge_all_total': round(rech_all_total, 2),
                'clear_all_total': round(clear_all_total, 2),
                # 消耗全量（不经过停用/日期过滤，用于备款绝对公式）
                'consume_all_spend': consume_all_spend,
                'consume_all_fee':   consume_all_fee,
                'remaining_reserve': remaining_reserve,
                # 每个客户的累计消耗+服务费（备款计算用）
                'client_consume_totals': {
                    c: {
                        'spend': round(v.get('spend',0), 2),
                        'fee':   round(v.get('fee',0), 2),
                        'total': round((v.get('spend',0) or 0) + (v.get('fee',0) or 0), 2),
                    } for c,v in cl_agg.items()
                },
            },
        }


# ============================================================
# 启动
# ============================================================
def main():
    port = int(os.environ.get('PORT') or (sys.argv[1] if len(sys.argv) > 1 else 8765))
    for fpath in (MAPPING_FILE, RECHARGE_FILE, CONSUME_FILE, IMPORT_LOG_FILE):
        if not os.path.exists(fpath):
            save_json(fpath, [])
    load_users()

    # —— 把 stdout / stderr 同步写入 data/server.log，便于前端查看（teed tee）——
    class _TeeWriter:
        """每次 write 同时写到原 stream 和日志文件（带时间戳）。"""
        def __init__(self, primary, log_fp):
            self.primary = primary
            self.log_fp = log_fp
        def write(self, s):
            try:
                self.primary.write(s)
                self.primary.flush()
                if not isinstance(s, str):
                    s = str(s)
                if s and not s.endswith('\n'): s = s + '\n'   # 自动换行
                stamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                for line in s.splitlines() or ['']:
                    self.log_fp.write('[%s] %s\n' % (stamp, line))
                self.log_fp.flush()
            except Exception:
                pass
        def flush(self):
            try: self.primary.flush()
            except Exception: pass
            try: self.log_fp.flush()
            except Exception: pass
    log_fp = open(SERVER_LOG_FILE, 'a', encoding='utf-8', buffering=1)   # 行缓冲
    sys.stdout = _TeeWriter(sys.__stdout__, log_fp)
    sys.stderr = _TeeWriter(sys.__stderr__, log_fp)
    print('[boot] 日志重定向已启用 -> ' + SERVER_LOG_FILE, flush=True)
    # 数据备份定时线程：每 10 分钟检查一次是否到达发送时间（daemon，随主进程退出）
    def _backup_worker():
        while True:
            try:
                cfg = get_backup_cfg()
                if cfg.get('enabled') and cfg.get('emails') and cfg.get('smtp_host') and _has_business_data():
                    run_backup_cycle(force=False)
            except Exception as e:
                print('[backup-worker] 异常', e)
            time.sleep(600)
    threading.Thread(target=_backup_worker, daemon=True).start()
    # === 2026-09-24 三平台自动刷新定时线程：每分钟检查 Meta/TikTok/Google 三个平台
    threading.Thread(target=_platform_auto_sync_worker, daemon=True).start()
    # 2026-09-12 启动时预加载静态文件到内存（穿透提速关键优化）
    # 2026-09-12 公网穿透安全加固：速率限制后台清理线程（daemon=True 随主进程退出）
    threading.Thread(target=_rate_cleanup, daemon=True).start()
    _preload_static()
    server = SecureServer(('0.0.0.0', port), ApiHandler)
    print("=" * 55)
    print(f"  FB 广告账户管理系统 v4 · 单机完整版")
    print(f"  地址: http://localhost:{port}")
    print(f"  数据目录: {DATA_DIR}")
    print(f"  默认账号: admin / admin123456")
    print(f"  按 Ctrl+C 停止")
    print("=" * 55)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n✅ 已停止")
        server.server_close()


if __name__ == '__main__':
    main()
