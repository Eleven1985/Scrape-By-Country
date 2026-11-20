import asyncio
import base64
import json
import logging
import os
import re
import shutil
from datetime import datetime

import aiohttp
import pytz
from bs4 import BeautifulSoup

# 获取当前脚本所在目录的绝对路径
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..'))

# 使用绝对路径引用文件
URLS_FILE = os.path.join(SCRIPT_DIR, 'urls.txt')
KEYWORDS_FILE = os.path.join(SCRIPT_DIR, 'key.json')
PROTOCOL_OUTPUT_DIR = os.path.join(PROJECT_ROOT, 'configs/protocols')
README_FILE = os.path.join(PROJECT_ROOT, 'README.md')

REQUEST_TIMEOUT = 20
CONCURRENT_REQUESTS = 10
FETCH_RETRIES = 3
MAX_CONFIG_LENGTH = 1500
MIN_PERCENT25_COUNT = 15

GITHUB_REPO_PATH = 'looomix/Scrape-By-Country'
GITHUB_BRANCH = 'main'

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

PROTOCOL_CATEGORIES = [
    "Vmess", "Vless", "Trojan", "ShadowSocks", "ShadowSocksR",
    "Tuic", "Hysteria2", "WireGuard"
]

# 用于判断一段文本是否真的是订阅内容
PROTOCOL_MARKERS = (
    'vmess://', 'vless://', 'trojan://', 'ss://', 'ssr://',
    'shadowsocks://', 'tuic://', 'tuic5://', 'hy2://',
    'hysteria2://', 'wireguard://',
)

UUID_PATTERN = re.compile(
    r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-'
    r'[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
)


def decode_base64_subscription(text):
    """整段响应若是 base64 订阅内容则解码为明文，否则返回 None。"""
    if not isinstance(text, str) or not text.strip():
        return None

    compact = re.sub(r'\s+', '', text)
    if len(compact) < 16:
        return None

    normalized = compact.replace('-', '+').replace('_', '/')
    normalized += '=' * (-len(normalized) % 4)
    try:
        decoded = base64.b64decode(normalized, validate=False).decode('utf-8')
    except (ValueError, UnicodeDecodeError):
        return None

    # 解码结果必须确实包含节点链接，避免把普通文本误判为 base64
    if not any(marker in decoded.lower() for marker in PROTOCOL_MARKERS):
        return None

    printable = sum(1 for ch in decoded if ch.isprintable() or ch in '\r\n\t')
    if printable / max(len(decoded), 1) < 0.95:
        return None
    return decoded


def extract_text_from_html(html):
    soup = BeautifulSoup(html, 'html.parser')
    for tag in soup(['script', 'style', 'noscript']):
        tag.decompose()
    # stripped_strings 逐行取文本，避免嵌套标签导致的重复
    return '\n'.join(soup.stripped_strings)


def normalize_body(body, content_type):
    """把响应体统一转换成待匹配的纯文本，兼容 HTML 页面与 base64 订阅。"""
    text = body.decode('utf-8', 'ignore')
    looks_like_html = 'html' in (content_type or '').lower() and '<' in text

    if looks_like_html:
        plain_text = extract_text_from_html(text)
        decoded = decode_base64_subscription(text)
        # 页面被 base64 包裹时，解码后的节点会更多
        if decoded and decoded.count('://') > plain_text.count('://'):
            return decoded
        return plain_text

    decoded = decode_base64_subscription(text)
    if decoded:
        return decoded
    return text


def filter_reason(config):
    """返回被过滤的原因；返回 None 表示保留。"""
    if 'i_love_' in config.lower():
        return 'i_love_'
    if config.count('%25') >= MIN_PERCENT25_COUNT:
        return 'percent25'
    if len(config) >= MAX_CONFIG_LENGTH:
        return 'too_long'
    if '%2525' in config:
        return 'double_encoded'
    return None


def is_mislabeled_ss(config):
    """部分源站把 Vless 节点错写成 ss://（凭据为 UUID），这里做识别。

    合法的 Shadowsocks 凭据一定是 base64/百分号编码的 "加密方式:密码"，
    不可能是一个裸 UUID，因此凭据是 UUID 的 ss:// 必为错标。
    """
    if not config.startswith('ss://'):
        return False

    body = config[len('ss://'):]
    if '@' not in body:
        return False

    userinfo = body.split('@', 1)[0]
    if not UUID_PATTERN.match(userinfo):
        return False

    # 带 SS 插件参数的保持原样，避免误改
    return 'plugin=' not in config.lower()


async def fetch_url(session, url):
    for attempt in range(1, FETCH_RETRIES + 1):
        try:
            async with session.get(url, timeout=REQUEST_TIMEOUT) as response:
                response.raise_for_status()
                body = await response.read()
                content_type = response.headers.get('Content-Type', '')
                text = normalize_body(body, content_type)
                logging.info(f"Successfully fetched: {url} "
                             f"(attempt {attempt}/{FETCH_RETRIES}, {len(body)} bytes)")
                return url, text
        except Exception as e:
            if attempt < FETCH_RETRIES:
                logging.warning(f"Failed to fetch or process {url} "
                                f"(attempt {attempt}/{FETCH_RETRIES}): {e}. Retrying...")
                await asyncio.sleep(1)
            else:
                logging.error(f"Failed to fetch or process {url} "
                              f"after {FETCH_RETRIES} attempts: {e}")
    return url, None


def find_matches(text, protocol_patterns):
    """按协议正则提取节点，并统计被过滤掉的条目。"""
    matches = {}
    stats = {'i_love_': 0, 'percent25': 0, 'too_long': 0,
             'double_encoded': 0, 'reclassified': 0}

    for category, patterns in protocol_patterns.items():
        found_configs = set()
        for pattern_str in patterns:
            if not isinstance(pattern_str, str):
                continue
            try:
                pattern = re.compile(pattern_str, re.IGNORECASE | re.MULTILINE)
            except re.error as e:
                logging.error(f"Regex error for '{pattern_str}' in category '{category}': {e}")
                continue

            for item in pattern.findall(text):
                item = item.strip()
                if not item:
                    continue
                reason = filter_reason(item)
                if reason:
                    stats[reason] += 1
                    continue
                found_configs.add(item)

        if found_configs:
            matches[category] = found_configs

    # 把被源站错标成 ss:// 的 Vless 节点归位，避免生成无法使用的链接
    ss_configs = matches.get('ShadowSocks', set())
    if ss_configs:
        mislabeled = {c for c in ss_configs if is_mislabeled_ss(c)}
        if mislabeled:
            matches['ShadowSocks'] = ss_configs - mislabeled
            if not matches['ShadowSocks']:
                del matches['ShadowSocks']
            matches.setdefault('Vless', set()).update(
                'vless://' + c[len('ss://'):] for c in mislabeled
            )
            stats['reclassified'] += len(mislabeled)

    return matches, stats


def save_to_file(directory, category_name, items_set):
    if not items_set:
        return 0
    file_path = os.path.join(directory, f"{category_name}.txt")
    count = len(items_set)
    try:
        with open(file_path, 'w', encoding='utf-8', newline='\n') as f:
            for item in sorted(items_set):
                f.write(f"{item}\n")
        logging.info(f"Saved {count} items to {file_path}")
        return count
    except Exception as e:
        logging.error(f"Failed to write file {file_path}: {e}")
        return 0


def generate_readme(protocol_counts, github_repo_path=GITHUB_REPO_PATH,
                    github_branch=GITHUB_BRANCH):
    tz = pytz.timezone('Asia/Shanghai')
    now = datetime.now(tz)
    time_str = now.strftime("%H:%M")
    date_str = now.strftime("%Y-%m-%d")
    timestamp = f"最后更新: {time_str} {date_str}"

    raw_github_base_url_protocols = (
        f"https://raw.githubusercontent.com/{github_repo_path}"
        f"/refs/heads/{github_branch}/configs/protocols"
    )

    total_configs = sum(protocol_counts.values())

    md_content = f"""# 🚀 Scrape-By-Country

<p align="center">
  <img src="https://img.shields.io/github/license/{github_repo_path}?style=flat-square&color=blue" alt="License" />
  <img src="https://img.shields.io/badge/python-3.9%2B-3776AB?style=flat-square&logo=python" alt="Python 3.9+" />
  <img src="https://img.shields.io/github/actions/workflow/status/{github_repo_path}/main.yml?style=flat-square" alt="GitHub Workflow Status" />
  <img src="https://img.shields.io/github/last-commit/{github_repo_path}?style=flat-square" alt="Last Commit" />
  <br>
  <img src="https://img.shields.io/github/issues/{github_repo_path}?style=flat-square" alt="GitHub Issues" />
  <img src="https://img.shields.io/badge/Configs-{total_configs}-blue?style=flat-square" alt="Total Configs" />
  <img src="https://img.shields.io/github/stars/{github_repo_path}?style=social" alt="GitHub Stars" />
  <img src="https://img.shields.io/badge/status-active-brightgreen?style=flat-square" alt="Project Status" />
  <img src="https://img.shields.io/badge/language-中文%20%26%20English-007EC6?style=flat-square" alt="Language" />
</p>

## {timestamp}

---

## 📖 关于项目
这个项目自动从各种来源收集并按协议分类 VPN 配置（如 V2Ray、Trojan 和 Shadowsocks 等不同协议）。我们的目标是为用户提供最新且可靠的配置。

---

## 📁 协议配置
{f'目前有 {total_configs} 个配置可用。' if total_configs else '未找到任何协议配置。'}

<div align="center">

| 协议 | 数量 | 下载链接 |
|:-------:|:-----:|:------------:|
| **汇总** | **{total_configs}** | [`AllProtocols.txt`]({raw_github_base_url_protocols}/AllProtocols.txt) |
"""

    if protocol_counts:
        for category_name, count in sorted(protocol_counts.items()):
            file_link = f"{raw_github_base_url_protocols}/{category_name}.txt"
            md_content += f"| {category_name} | {count} | [`{category_name}.txt`]({file_link}) |\n"
    else:
        md_content += "| - | - | - |\n"

    md_content += """</div>

---

## 🛠️ 使用方法
1. **下载配置**: 从上方表格中，下载您需要的协议文件。
2. **推荐客户端**:
   - **V2Ray**: [v2rayNG](https://github.com/2dust/v2rayNG) (安卓)，[Hiddify](https://github.com/hiddify/hiddify-app/releases) (Mac)，[V2RayN](https://github.com/2dust/v2rayN/releases) (Windows)
   - **NekoRey_pro**: [NekoRey](https://github.com/Mahdi-zarei/nekoray/releases) (Mac)，[Karing](https://github.com/KaringX/karing/releases)
   - **sing-box**: [Sing-Box](https://github.com/SagerNet/sing-box/releases)
3. 将配置文件导入您的客户端并测试连接。

> **建议**: 为获得最佳性能，请定期检查和更新配置。

---
## 📢 注意事项
- 本项目仅用于学习和研究目的。
- 请根据您所在国家的法律负责任地使用配置。
- 如遇问题或建议，请使用 [Issues](https://github.com/{repo}/issues) 部分。
""".format(repo=github_repo_path)

    try:
        with open(README_FILE, 'w', encoding='utf-8', newline='\n') as f:
            f.write(md_content)
        logging.info(f"Successfully generated {README_FILE}")
    except Exception as e:
        logging.error(f"Failed to write {README_FILE}: {e}")


async def main():
    if not os.path.exists(URLS_FILE) or not os.path.exists(KEYWORDS_FILE):
        logging.critical("Input files not found.")
        return

    with open(URLS_FILE, 'r', encoding='utf-8') as f:
        urls = [line.strip() for line in f if line.strip()]
    with open(KEYWORDS_FILE, 'r', encoding='utf-8') as f:
        all_patterns = json.load(f)

    protocol_patterns = {
        category: patterns
        for category, patterns in all_patterns.items()
        if category in PROTOCOL_CATEGORIES
    }

    missing = [c for c in PROTOCOL_CATEGORIES if c not in protocol_patterns]
    if missing:
        logging.warning(f"key.json 中缺少以下协议的正则: {', '.join(missing)}")

    logging.info(f"Loaded {len(urls)} URLs and "
                 f"{len(protocol_patterns)} protocol categories from key.json.")

    sem = asyncio.Semaphore(CONCURRENT_REQUESTS)

    async def fetch_with_sem(session, url_to_fetch):
        async with sem:
            return await fetch_url(session, url_to_fetch)

    async with aiohttp.ClientSession() as session:
        fetched_pages = await asyncio.gather(*[fetch_with_sem(session, u) for u in urls])

    final_configs = {category: set() for category in protocol_patterns}
    filter_totals = {'i_love_': 0, 'percent25': 0, 'too_long': 0,
                     'double_encoded': 0, 'reclassified': 0}

    logging.info("Processing pages for config extraction...")
    for url, text in fetched_pages:
        if not text:
            logging.warning(f"No content parsed from: {url}")
            continue

        page_matches, stats = find_matches(text, protocol_patterns)
        page_total = 0
        for category, configs in page_matches.items():
            final_configs[category].update(configs)
            page_total += len(configs)
        for key, value in stats.items():
            filter_totals[key] += value

        logging.info(f"Parsed {page_total} configs from {url}")

    filtered = {k: v for k, v in filter_totals.items() if k != 'reclassified' and v}
    if filtered:
        logging.info("Filtered out: " + ", ".join(f"{k}={v}" for k, v in filtered.items()))
    if filter_totals['reclassified']:
        logging.info(f"Reclassified {filter_totals['reclassified']} mislabeled "
                     f"ss:// configs as vless://")

    if os.path.exists(PROTOCOL_OUTPUT_DIR):
        shutil.rmtree(PROTOCOL_OUTPUT_DIR)
    os.makedirs(PROTOCOL_OUTPUT_DIR, exist_ok=True)
    logging.info(f"Saving protocol files to directory: {PROTOCOL_OUTPUT_DIR}")

    protocol_counts = {}
    for category in PROTOCOL_CATEGORIES:
        count = save_to_file(PROTOCOL_OUTPUT_DIR, category, final_configs.get(category, set()))
        if count:
            protocol_counts[category] = count

    # 汇总所有协议到一个文件
    all_protocols_combined = set()
    for configs in final_configs.values():
        all_protocols_combined.update(configs)
    save_to_file(PROTOCOL_OUTPUT_DIR, "AllProtocols", all_protocols_combined)

    generate_readme(protocol_counts, github_repo_path=GITHUB_REPO_PATH,
                    github_branch=GITHUB_BRANCH)

    logging.info(f"Total unique configs: {len(all_protocols_combined)}")
    logging.info("--- Script Finished ---")


if __name__ == "__main__":
    asyncio.run(main())
