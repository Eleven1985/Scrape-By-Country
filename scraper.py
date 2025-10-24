import asyncio
import aiohttp
import json
import re
import logging
from bs4 import BeautifulSoup
import os
import shutil
from datetime import datetime
import pytz
import base64
from urllib.parse import parse_qs, unquote

# --- 配置常量 ---
CONFIG_DIR = 'config'  # 配置文件夹，用于存放输入文件
URLS_FILE = os.path.join(CONFIG_DIR, 'urls.txt')
KEYWORDS_FILE = os.path.join(CONFIG_DIR, 'keywords.json') # 应包含国家的两字母代码
OUTPUT_DIR = 'output_configs'
COUNTRY_SUBDIR = 'countries'  # 国家配置文件夹
PROTOCOL_SUBDIR = 'protocols' # 协议配置文件夹
README_FILE = 'README.md'
REQUEST_TIMEOUT = 15
CONCURRENT_REQUESTS = 10
MAX_CONFIG_LENGTH = 1500
MIN_PERCENT25_COUNT = 15
FILTERED_PHRASE = 'i_love_'  # 要过滤的特定短语

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# --- 协议类别 ---
PROTOCOL_CATEGORIES = [
    "Vmess", "Vless", "Trojan", "ShadowSocks", "ShadowSocksR",
    "Tuic", "Hysteria2", "WireGuard"
]
# 预编译协议前缀列表，提高性能
PROTOCOL_PREFIXES = [p.lower() + "://" for p in PROTOCOL_CATEGORIES]

# --- 检查非英语文本的辅助函数 ---
def is_non_english_text(text):
    """检查文本是否包含非英语字符（如波斯语、阿拉伯语等特殊字符）"""
    if not isinstance(text, str) or not text.strip():
        return False
    
    # 定义非拉丁字符范围，但排除常见的国家名称和代码可能使用的字符
    # 我们需要更精确地识别真正需要过滤的字符
    problematic_char_ranges = [
        ('\u0600', '\u06FF'),  # 阿拉伯语及波斯语
        ('\u0750', '\u077F'),  # 阿拉伯文补充
        ('\u08A0', '\u08FF'),  # 阿拉伯文扩展-A
    ]
    
    # 检查是否包含问题字符
    for char in text:
        # 只检查真正可能导致问题的字符范围
        for start, end in problematic_char_ranges:
            if start <= char <= end:
                return True
    
    # 只过滤零宽连接符等真正的问题字符
    problematic_chars = ['\u200C', '\u200D']  # 零宽连接符
    for char in text:
        if char in problematic_chars:
            return True
    
    # 保留常见的国家名称字符，包括中文、日语、韩语等
    # 这些字符对于国家识别很重要，不应该被过滤
    return False

# --- Base64 Decoding Helper ---
def decode_base64(data):
    """安全地解码Base64字符串，处理URL安全的Base64格式"""
    if not data or not isinstance(data, str):
        return None
    try:
        # 替换URL安全的Base64字符
        data = data.replace('_', '/').replace('-', '+')
        # 添加必要的填充
        missing_padding = len(data) % 4
        if missing_padding:
            data += '=' * (4 - missing_padding)
        return base64.b64decode(data).decode('utf-8')
    except Exception:
        return None

# --- 协议名称提取辅助函数 ---
def get_vmess_name(vmess_config):
    """
    从VMess配置中提取名称信息
    参数:
        vmess_config: VMess配置字符串
    返回:
        提取的名称字符串或None
    """
    try:
        # 确保输入是字符串
        if not isinstance(vmess_config, str) or not vmess_config.startswith('vmess://'):
            return None
        
        # 移除前缀
        encoded_part = vmess_config[8:]
        
        # 尝试解码
        try:
            # 添加必要的填充
            padded = encoded_part + '=' * ((4 - len(encoded_part) % 4) % 4)
            decoded = base64.b64decode(padded).decode('utf-8')
        except Exception:
            # 如果标准解码失败，尝试URL解码后再base64解码
            try:
                encoded_part = unquote(encoded_part)
                padded = encoded_part + '=' * ((4 - len(encoded_part) % 4) % 4)
                decoded = base64.b64decode(padded).decode('utf-8')
            except Exception:
                return None
        
        # 解析JSON并尝试获取名称
        try:
            vmess_data = json.loads(decoded)
            # 尝试从不同字段获取名称
            for name_field in ['ps', 'name', 'remarks', 'tag']:
                if name_field in vmess_data and isinstance(vmess_data[name_field], str):
                    return vmess_data[name_field].strip()
        except Exception:
            return None
        
        return None
    except Exception:
        return None

def get_ssr_name(ssr_config):
    """
    从SSR配置中提取名称信息
    参数:
        ssr_config: SSR配置字符串
    返回:
        提取的名称字符串或None
    """
    try:
        # 确保输入是字符串
        if not isinstance(ssr_config, str) or not ssr_config.startswith('ssr://'):
            return None
        
        # 移除前缀
        encoded_part = ssr_config[6:]
        
        # 尝试解码
        try:
            # 添加必要的填充
            padded = encoded_part + '=' * ((4 - len(encoded_part) % 4) % 4)
            decoded = base64.b64decode(padded).decode('utf-8')
        except Exception:
            # 如果标准解码失败，尝试URL解码后再base64解码
            try:
                encoded_part = unquote(encoded_part)
                padded = encoded_part + '=' * ((4 - len(encoded_part) % 4) % 4)
                decoded = base64.b64decode(padded).decode('utf-8')
            except Exception:
                return None
        
        # SSR格式: server:port:protocol:method:obfs:password_base64/?params
        parts = decoded.split('/?')
        if len(parts) < 2:
            return None
            
        # 解析参数部分并获取remarks
        params = parse_qs(parts[1])
        if 'remarks' in params:
            try:
                remarks_encoded = params['remarks'][0]
                # 解码remarks
                padded_remarks = remarks_encoded + '=' * ((4 - len(remarks_encoded) % 4) % 4)
                return base64.b64decode(padded_remarks).decode('utf-8', errors='ignore')
            except Exception:
                return None
        
        return None
    except Exception:
        return None

def get_trojan_name(trojan_config):
    """
    从Trojan配置中提取名称信息
    参数:
        trojan_config: Trojan配置字符串
    返回:
        提取的名称字符串或None
    """
    try:
        # 确保输入是字符串
        if not isinstance(trojan_config, str) or not trojan_config.startswith('trojan://'):
            return None
        
        # Trojan URL 格式: trojan://password@hostname:port#name
        # 检查是否有 # 后的名称部分
        if '#' in trojan_config:
            try:
                name_part = trojan_config.split('#', 1)[1]
                return unquote(name_part).strip()
            except Exception:
                pass
        
        # 尝试从URL路径或查询参数中提取名称
        parts = trojan_config.split('?')
        if len(parts) > 1:
            try:
                params = parse_qs(parts[1])
                for name_key in ['name', 'remarks', 'ps']:
                    if name_key in params:
                        return unquote(params[name_key][0]).strip()
            except Exception:
                pass
        
        return None
    except Exception:
        return None

def get_vless_name(vless_config):
    """
    从VLESS配置中提取名称信息
    参数:
        vless_config: VLESS配置字符串
    返回:
        提取的名称字符串或None
    """
    try:
        # 确保输入是字符串
        if not isinstance(vless_config, str) or not vless_config.startswith('vless://'):
            return None
        
        # 检查是否有 # 后的名称部分
        if '#' in vless_config:
            try:
                name_part = vless_config.split('#', 1)[1]
                return unquote(name_part).strip()
            except Exception:
                pass
        
        # 尝试从URL查询参数中提取名称
        parts = vless_config.split('?')
        if len(parts) > 1:
            try:
                params = parse_qs(parts[1])
                for name_key in ['name', 'remarks', 'ps']:
                    if name_key in params:
                        return unquote(params[name_key][0]).strip()
            except Exception:
                pass
        
        return None
    except Exception:
        return None

def get_shadowsocks_name(ss_config):
    """
    从Shadowsocks配置中提取名称信息
    参数:
        ss_config: Shadowsocks配置字符串
    返回:
        提取的名称字符串或None
    """
    try:
        # 确保输入是字符串
        if not isinstance(ss_config, str) or not ss_config.startswith('ss://'):
            return None
        
        # 检查是否有 # 后的名称部分
        if '#' in ss_config:
            try:
                name_part = ss_config.split('#', 1)[1]
                return unquote(name_part).strip()
            except Exception:
                pass
        
        return None
    except Exception:
        return None

# --- New Filter Function ---
def should_filter_config(config):
    """根据特定规则过滤无效或低质量的配置"""
    if not config or not isinstance(config, str):
        return True
    
    # 检查是否包含过滤短语
    if FILTERED_PHRASE in config.lower():
        return True
    
    # 放宽URL编码检查，减少误判
    percent25_count = config.count('%25')
    if percent25_count >= MIN_PERCENT25_COUNT * 2:  # 提高阈值以减少误判
        return True
    
    # 检查配置长度，适当放宽限制
    if len(config) >= MAX_CONFIG_LENGTH * 2:  # 提高阈值以减少误判
        return True
    
    # 基本的有效性检查：确保配置包含协议前缀
    has_valid_protocol = False
    for protocol_prefix in PROTOCOL_PREFIXES:
        if protocol_prefix in config.lower():
            has_valid_protocol = True
            break
    
    if not has_valid_protocol:
        return True
    
    return False

async def fetch_url(session, url):
    """异步获取URL内容并提取文本"""
    try:
        async with session.get(url, timeout=REQUEST_TIMEOUT) as response:
            response.raise_for_status()
            
            # 尝试处理不同的内容类型
            content_type = response.headers.get('Content-Type', '')
            
            # 如果是JSON内容，直接处理
            if 'application/json' in content_type:
                try:
                    json_data = await response.json()
                    # 将JSON转换为字符串以方便后续处理
                    text_content = json.dumps(json_data, ensure_ascii=False)
                    logging.debug(f"处理JSON内容: {url}")
                except json.JSONDecodeError:
                    # 如果无法解析为JSON，回退到文本处理
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    text_content = soup.get_text(separator='\n', strip=True)
            else:
                # 处理HTML或纯文本
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                
                # 优先从代码相关标签提取内容
                text_content = ""
                code_elements = soup.find_all(['pre', 'code'])
                if code_elements:
                    for element in code_elements:
                        text_content += element.get_text(separator='\n', strip=True) + "\n"
                
                # 如果没有足够的代码内容，再提取其他文本元素
                if not text_content or len(text_content) < 100:
                    for element in soup.find_all(['p', 'div', 'li', 'span', 'td']):
                        text_content += element.get_text(separator='\n', strip=True) + "\n"
                
                # 最后的备用方案
                if not text_content: 
                    text_content = soup.get_text(separator=' ', strip=True)
                    
            logging.info(f"成功获取: {url}")
            return url, text_content
    except asyncio.TimeoutError:
        logging.warning(f"Request timed out for {url}")
    except aiohttp.ClientError as e:
        logging.warning(f"Client error fetching {url}: {e}")
    except Exception as e:
        logging.warning(f"Unexpected error fetching {url}: {e}")
    return url, None

def find_matches(text, categories_data):
    """根据正则表达式模式在文本中查找匹配项，优化内存使用"""
    if not text or not isinstance(text, str):
        return {}
    
    matches = {}
    
    for category, patterns in categories_data.items():
        if not patterns or not isinstance(patterns, list):
            continue
            
        category_matches = set()
        
        for pattern_str in patterns:
            if not isinstance(pattern_str, str):
                continue
                
            try:
                # 使用预编译的协议前缀列表提高性能
                is_protocol_pattern = any(proto_prefix in pattern_str.lower() for proto_prefix in PROTOCOL_PREFIXES)
                
                if category in PROTOCOL_CATEGORIES or is_protocol_pattern:
                    # 优化正则表达式性能
                    pattern = re.compile(pattern_str, re.IGNORECASE | re.MULTILINE | re.DOTALL)
                    found = pattern.findall(text)
                    
                    for item in found:
                        if item and isinstance(item, str):
                            cleaned_item = item.strip()
                            if cleaned_item:
                                category_matches.add(cleaned_item)
            except re.error as e:
                logging.error(f"正则表达式错误 - 模式在类别 '{category}': {e}")
                continue
        
        if category_matches:
            matches[category] = category_matches
    
    return matches

def save_to_file(directory, category_name, items_set):
    """将项目集合保存到指定目录的文本文件中"""
    if not items_set:
        logging.debug(f"跳过空集合的保存: {category_name}")
        return False, 0
        
    try:
        # 确保目录存在
        os.makedirs(directory, exist_ok=True)
        file_path = os.path.join(directory, f"{category_name}.txt")
        count = len(items_set)
        
        # 使用写入模式直接覆盖文件，这在大多数情况下已经足够
        with open(file_path, 'w', encoding='utf-8', newline='') as f:
            # 一次性写入排序后的项目列表
            for item in sorted(list(items_set)):
                f.write(f"{item}\n")
        
        # 简单验证文件是否成功写入
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            abs_file_path = os.path.abspath(file_path)
            logging.debug(f"已成功保存 {count} 项到 {abs_file_path}")
            return True, count
        else:
            logging.error(f"文件创建失败或为空: {file_path}")
            return False, 0
    except Exception as e:
        # 合并所有异常处理，避免代码过于复杂
        logging.error(f"保存文件时发生错误 {file_path}: {str(e)[:100]}...")
        
        # 只保留简单的备用方法
        try:
            # 使用临时文件方法作为备用
            temp_file = os.path.join(directory, f"temp_{category_name}.txt")
            with open(temp_file, 'w', encoding='utf-8') as f:
                for item in sorted(list(items_set)):
                    f.write(f"{item}\n")
            
            # 重命名临时文件到目标位置
            target_file = os.path.join(directory, f"{category_name}.txt")
            if os.path.exists(target_file):
                os.remove(target_file)
            os.rename(temp_file, target_file)
            
            logging.info(f"备用方法: 已保存 {count} 项到 {target_file}")
            return True, count
        except Exception:
            # 备用方法也失败，返回错误
            return False, 0

# --- 使用旗帜图像生成简单的README函数 ---
def generate_simple_readme(protocol_counts, country_counts, all_keywords_data, use_local_paths=True):
    """生成README.md文件，展示抓取结果统计信息"""
    # 确保输入参数是字典类型
    if not isinstance(protocol_counts, dict):
        protocol_counts = {}
    if not isinstance(country_counts, dict):
        country_counts = {}
    
    tz = pytz.timezone('Asia/Shanghai')
    now = datetime.now(tz)
    timestamp = now.strftime("%Y-%m-%d %H:%M:%S %Z")
    
    # 计算统计信息
    total_protocol_configs = sum(protocol_counts.values())
    total_country_configs = sum(country_counts.values())
    countries_with_data = len(country_counts)
    protocols_with_data = len(protocol_counts)

    # 构建子目录的路径
    if use_local_paths:
        protocol_base_url = f"{OUTPUT_DIR}/{PROTOCOL_SUBDIR}"
        country_base_url = f"{OUTPUT_DIR}/{COUNTRY_SUBDIR}"
    else:
        # 保留GitHub远程路径支持作为备用
        github_repo_path = "miladtahanian/V2RayScrapeByCountry"
        github_branch = "main"
        protocol_base_url = f"https://raw.githubusercontent.com/{github_repo_path}/refs/heads/{github_branch}/{OUTPUT_DIR}/{PROTOCOL_SUBDIR}"
        country_base_url = f"https://raw.githubusercontent.com/{github_repo_path}/refs/heads/{github_branch}/{OUTPUT_DIR}/{COUNTRY_SUBDIR}"

    md_content = f"# 📊 提取结果 (最后更新: {timestamp})\n\n"
    md_content += "此文件是自动生成的。\n\n"
    md_content += f"## 📋 统计概览\n\n"
    md_content += f"- **配置总数**: {total_protocol_configs}\n"
    md_content += f"- **有数据的协议数**: {protocols_with_data}\n"
    md_content += f"- **国家相关配置数**: {total_country_configs}\n"
    md_content += f"- **有配置的国家数**: {countries_with_data}\n\n"
    
    md_content += "## ℹ️ 说明\n\n"
    md_content += "国家文件仅包含在**配置名称**中找到国家名称/旗帜的配置。配置名称首先从链接的`#`部分提取，如果不存在，则从内部名称(对于Vmess/SSR)提取。\n\n"
    md_content += "过度URL编码的配置(包含大量`%25`、过长或包含特定关键词的)已从结果中删除。\n\n"
    md_content += "所有输出文件已按类别整理到不同目录中，便于查找和使用。\n\n"

    md_content += "## 📁 协议文件\n\n"
    if protocol_counts:
        md_content += "| 协议 | 总数 | 链接 |\n"
        md_content += "|---|---|---|\n"
        for category_name, count in sorted(protocol_counts.items()):
            file_link = f"{protocol_base_url}/{category_name}.txt"
            md_content += f"| {category_name} | {count} | [`{category_name}.txt`]({file_link}) |\n"
    else:
        md_content += "没有找到协议配置。\n"
    md_content += "\n"

    md_content += "## 🌍 国家文件 (包含配置)\n\n"
    if country_counts:
        md_content += "| 国家 | 相关配置数量 | 链接 |\n"
        md_content += "|---|---|---|\n"
        for country_category_name, count in sorted(country_counts.items()):
            flag_image_markdown = "" # 用于保存旗帜图像HTML标签
            
            # 查找国家的两字母ISO代码用于旗帜图像URL
            if country_category_name in all_keywords_data:
                keywords_list = all_keywords_data[country_category_name]
                if keywords_list and isinstance(keywords_list, list):
                    for item in keywords_list:
                        if isinstance(item, str) and len(item) == 2 and item.isupper() and item.isalpha():
                            iso_code_lowercase_for_url = item.lower()
                            # 使用flagcdn.com，宽度为20像素
                            flag_image_url = f"https://flagcdn.com/w20/{iso_code_lowercase_for_url}.png"
                            flag_image_markdown = f'<img src="{flag_image_url}" width="20" alt="{country_category_name} flag">'
                            break 

            # 为"国家"列构建最终文本
            display_parts = []
            # 如果旗帜图像标签已创建
            if flag_image_markdown:
                display_parts.append(flag_image_markdown)
            
            display_parts.append(country_category_name) # 原始名称 (键)
            
            country_display_text = " ".join(display_parts)
            
            file_link = f"{country_base_url}/{country_category_name}.txt"
            link_text = f"{country_category_name}.txt"
            md_content += f"| {country_display_text} | {count} | [`{link_text}`]({file_link}) |\n"
    else:
        md_content += "没有找到与国家相关的配置。\n"
    md_content += "\n"

    try:
        with open(README_FILE, 'w', encoding='utf-8') as f:
            f.write(md_content)
        logging.info(f"成功生成 {README_FILE}")
    except Exception as e:
        logging.error(f"写入 {README_FILE} 失败: {e}")

# main函数和其他函数实现
async def main():
    """主函数，协调整个抓取和处理流程"""
    # 确保配置文件夹存在
    try:
        os.makedirs(CONFIG_DIR, exist_ok=True)
    except Exception as e:
        logging.error(f"创建配置文件夹 '{CONFIG_DIR}' 失败: {e}")
    
    # 检查必要的输入文件是否存在
    if not os.path.exists(URLS_FILE) or not os.path.exists(KEYWORDS_FILE):
        missing_files = []
        if not os.path.exists(URLS_FILE):
            missing_files.append(f"URLs文件: {URLS_FILE}")
        if not os.path.exists(KEYWORDS_FILE):
            missing_files.append(f"关键词文件: {KEYWORDS_FILE}")
        
        logging.critical(f"未找到输入文件:\n- {chr(10)}- ".join(missing_files))
        logging.info(f"请确保这些文件已放在 {CONFIG_DIR} 文件夹中")
        return

    # 加载URL和关键词数据
    try:
        with open(URLS_FILE, 'r', encoding='utf-8') as f:
            urls = [line.strip() for line in f if line.strip()]
            
        if not urls:
            logging.critical("URLs文件为空，没有要抓取的URL。")
            return
            
        logging.info(f"已从 {URLS_FILE} 加载 {len(urls)} 个URL")
        
        with open(KEYWORDS_FILE, 'r', encoding='utf-8') as f:
            categories_data = json.load(f)
            
        # 验证categories_data是字典类型
        if not isinstance(categories_data, dict):
            logging.critical("keywords.json必须包含字典格式的数据。")
            return
            
        # 验证协议类别是否在配置中
        missing_protocols = [p for p in PROTOCOL_CATEGORIES if p not in categories_data]
        if missing_protocols:
            logging.warning(f"keywords.json中缺少以下协议类别的配置: {', '.join(missing_protocols)}")
            
        # 验证每个值都是列表
        invalid_entries = [(k, v) for k, v in categories_data.items() if not isinstance(v, list)]
        if invalid_entries:
            logging.warning(f"keywords.json包含非列表格式的值: {invalid_entries}")
            # 过滤掉非列表的值
            categories_data = {k: v for k, v in categories_data.items() if isinstance(v, list)}
            
        if not categories_data:
            logging.critical("keywords.json中没有有效的类别数据。")
            return
            
    except json.JSONDecodeError as e:
        logging.critical(f"解析keywords.json文件失败: {e}")
        return
    except IOError as e:
        logging.critical(f"读取输入文件时出错: {e}")
        return

    # 分离协议模式和国家关键词
    protocol_patterns_for_matching = {
        cat: patterns for cat, patterns in categories_data.items() if cat in PROTOCOL_CATEGORIES
    }
    country_keywords_for_naming = {
        cat: patterns for cat, patterns in categories_data.items() if cat not in PROTOCOL_CATEGORIES
    }
    country_category_names = list(country_keywords_for_naming.keys())

    logging.info(f"已加载 {len(urls)} 个URL和 "
                 f"{len(categories_data)} 个总类别从keywords.json。")

    # 异步获取所有页面
    sem = asyncio.Semaphore(CONCURRENT_REQUESTS)  # 限制并发请求数
    
    async def fetch_with_semaphore(session, url_to_fetch):
        """使用信号量限制并发的fetch_url"""
        async with sem:
            return await fetch_url(session, url_to_fetch)
    
    # 创建HTTP会话并执行所有获取任务
    async with aiohttp.ClientSession() as session:
        logging.info(f"开始获取 {len(urls)} 个URLs (最大并发: {CONCURRENT_REQUESTS})...")
        fetched_pages = await asyncio.gather(
            *[fetch_with_semaphore(session, u) for u in urls],
            return_exceptions=True  # 即使某些任务失败也继续执行
        )
        
        # 过滤出成功获取的页面并统计失败情况
        success_count = 0
        exception_count = 0
        filtered_pages = []
        
        for result in fetched_pages:
            if isinstance(result, tuple) and len(result) == 2 and isinstance(result[0], str) and result[1] is not None:
                filtered_pages.append(result)
                success_count += 1
            elif isinstance(result, Exception):
                exception_count += 1
                logging.warning(f"URL获取任务异常: {type(result).__name__}: {result}")
            else:
                logging.debug(f"无效的URL获取结果: {type(result)}")
        
        fetched_pages = filtered_pages
        logging.info(f"URL获取完成: 成功 {success_count}, 异常 {exception_count}, 总计 {len(filtered_pages)} 个页面待处理")

    # 初始化结果集合
    final_configs_by_country = {cat: set() for cat in country_category_names}
    final_all_protocols = {cat: set() for cat in PROTOCOL_CATEGORIES}

    logging.info("处理页面并关联配置名称...")
    
    # 统计成功处理的页面数量
    processed_pages = 0
    found_configs = 0
    filtered_out_configs = 0
    
    for url, text in fetched_pages:
        if not text:
            continue
            
        processed_pages += 1
        page_protocol_matches = find_matches(text, protocol_patterns_for_matching)
        all_page_configs_after_filter = set()
        
        # 处理找到的协议配置
        page_filtered_count = 0
        for protocol_cat_name, configs_found in page_protocol_matches.items():
            if protocol_cat_name in PROTOCOL_CATEGORIES:
                for config in configs_found:
                    if not should_filter_config(config):
                        all_page_configs_after_filter.add(config)
                        final_all_protocols[protocol_cat_name].add(config)
                    else:
                        page_filtered_count += 1
        
        found_configs += len(all_page_configs_after_filter)
        filtered_out_configs += page_filtered_count
        
        # 每10个页面输出一次进度
        if processed_pages % 10 == 0:
            logging.info(f"处理进度: {processed_pages}/{len(fetched_pages)} 页面, " \
                      f"已找到 {found_configs} 配置, 已过滤 {filtered_out_configs} 配置")

        # 为每个配置关联国家信息
        for config in all_page_configs_after_filter:
            name_to_check = None
            
            # 1. 首先尝试从URL片段中提取名称（#后面的部分）
            if '#' in config:
                try:
                    potential_name = config.split('#', 1)[1]
                    name_to_check = unquote(potential_name).strip()
                    if not name_to_check:
                        name_to_check = None
                except (IndexError, Exception) as e:
                    logging.debug(f"从URL片段提取名称失败: {e}")

            # 2. 如果URL片段中没有名称，尝试从协议特定字段提取
            if not name_to_check:
                if config.startswith('ssr://'):
                    name_to_check = get_ssr_name(config)
                elif config.startswith('vmess://'):
                    name_to_check = get_vmess_name(config)
                elif config.startswith('trojan://'):
                    name_to_check = get_trojan_name(config)
                elif config.startswith('vless://'):
                    name_to_check = get_vless_name(config)
                elif config.startswith('ss://'):
                    name_to_check = get_shadowsocks_name(config)
                # 其他协议的名称提取支持

            # 如果无法获取名称，跳过此配置
            if not name_to_check or not isinstance(name_to_check, str):
                continue
                
            current_name_to_check_str = name_to_check.strip()

            # 遍历每个国家的关键词列表，寻找匹配
            country_matched = False
            for country_name_key, keywords_for_country_list in country_keywords_for_naming.items():
                # 只处理有效的关键词列表
                if not isinstance(keywords_for_country_list, list):
                    continue
                    
                # 准备此国家的文本关键词，过滤无效和重复的关键词
                text_keywords_for_country = []
                for kw in keywords_for_country_list:
                    if isinstance(kw, str) and kw.strip():
                        if kw not in text_keywords_for_country:  # 避免重复关键词
                            text_keywords_for_country.append(kw)
                
                # 检查是否匹配任何关键词
                match_found = False
                current_name_lower = current_name_to_check_str.lower()
                
                for keyword in text_keywords_for_country:
                    if not isinstance(keyword, str) or not keyword.strip():
                        continue
                        
                    keyword = keyword.strip()
                    keyword_lower = keyword.lower()
                    
                    # 简单有效的匹配策略
                    # 1. 对于缩写使用特殊处理
                    if len(keyword) in [2, 3] and keyword.isupper() and keyword.isalpha():
                        # 检查是否作为独立部分出现
                        if keyword_lower in current_name_lower:
                            parts = re.split(r'[^a-zA-Z]', current_name_lower)
                            if keyword_lower in parts:
                                match_found = True
                                break
                    # 2. 对于普通关键词使用简单的包含匹配
                    elif (keyword_lower in current_name_lower or 
                          keyword in current_name_to_check_str):
                        match_found = True
                        break
                
                if match_found:
                    final_configs_by_country[country_name_key].add(config)
                    country_matched = True
                    logging.debug(f"配置已关联到国家: {country_name_key}")
                    # 移除这里的break，允许配置匹配多个国家
                
            # 移除这里的break，确保每个配置都能被完全处理

    # 统计信息日志
    logging.info(f"成功处理 {processed_pages}/{len(fetched_pages)} 个页面，找到 {found_configs} 个有效配置，过滤掉 {filtered_out_configs} 个无效配置")
    

    # 准备输出目录结构
    country_dir = os.path.join(OUTPUT_DIR, COUNTRY_SUBDIR)
    protocol_dir = os.path.join(OUTPUT_DIR, PROTOCOL_SUBDIR)
    
    # 简洁的目录处理逻辑
    # 1. 尝试删除旧目录（如果存在）
    if os.path.exists(OUTPUT_DIR):
        try:
            shutil.rmtree(OUTPUT_DIR)
            logging.info(f"已删除旧输出目录")
        except Exception as e:
            logging.warning(f"无法删除旧输出目录，将在创建新目录时覆盖: {str(e)[:50]}...")
    
    # 2. 创建必要的目录结构
    try:
        os.makedirs(country_dir, exist_ok=True)
        os.makedirs(protocol_dir, exist_ok=True)
        logging.info(f"输出目录已准备就绪: {OUTPUT_DIR}")
    except (PermissionError, OSError) as e:
        logging.critical(f"无法创建输出目录: {e}")
        return

    # 保存协议配置文件
    protocol_counts = {}
    protocol_category_count = 0
    
    logging.info(f"开始保存协议配置文件")
    
    # 预先过滤出非空协议类别
    non_empty_protocols = {cat: items for cat, items in final_all_protocols.items() if items}
    
    for category, items in non_empty_protocols.items():
        items_count = len(items)
        logging.debug(f"保存协议 {category} 的 {items_count} 个配置")
        
        saved, count = save_to_file(protocol_dir, category, items)
        if saved:
            protocol_counts[category] = count
            protocol_category_count += 1
    
    total_protocol_configs = sum(protocol_counts.values())
    logging.info(f"协议配置保存完成: 成功 {protocol_category_count}/{len(non_empty_protocols)} 个类别, 总计 {total_protocol_configs} 项")
    
    # 保存国家配置文件
    country_counts = {}
    countries_with_configs = 0
    total_country_configs = 0
    
    logging.info(f"开始保存国家配置文件")
    
    # 预先过滤出非空国家类别
    non_empty_countries = {cat: items for cat, items in final_configs_by_country.items() if items}
    
    for category, items in non_empty_countries.items():
        actual_count = len(items)
        logging.debug(f"保存国家 {category} 的 {actual_count} 个配置")
        
        saved, count = save_to_file(country_dir, category, items)
        if saved:
            country_counts[category] = actual_count
            countries_with_configs += 1
            total_country_configs += actual_count
    
    logging.info(f"国家配置保存完成: 成功 {countries_with_configs}/{len(non_empty_countries)} 个国家, 总计 {total_country_configs} 项")
    
    # 生成README文件
    try:
        generate_simple_readme(protocol_counts, country_counts, categories_data, use_local_paths=True)
    except Exception as e:
        logging.error(f"生成README文件时出错: {e}")
        # 继续执行，不中断程序
    
    # 输出完成信息
    logging.info(f"=== 抓取完成 ===")
    logging.info(f"找到并保存的协议配置: {total_protocol_configs}")
    logging.info(f"有配置的国家数量: {countries_with_configs}")
    logging.info(f"国家相关配置总数: {total_country_configs}")
    logging.info(f"输出目录: {OUTPUT_DIR}")
    logging.info(f"README文件已更新")

if __name__ == "__main__":
    try:
        logging.info("=== V2Ray配置抓取工具开始运行 ===")
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("程序被用户中断")
    except Exception as e:
        logging.critical(f"程序执行出错: {e}")
    finally:
        logging.info("=== 程序结束 ===")
