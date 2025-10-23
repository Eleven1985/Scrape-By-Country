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
# 额外的协议别名前缀
ADDITIONAL_PROTOCOL_PREFIXES = {
    "Hysteria2": ["hy2://"],
    "WireGuard": ["wg://"]
}

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
        
        # 尝试从URL查询参数中提取名称
        parts = ss_config.split('?')
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

def get_tuic_name(tuic_config):
    """
    从Tuic配置中提取名称信息
    参数:
        tuic_config: Tuic配置字符串
    返回:
        提取的名称字符串或None
    """
    try:
        # 确保输入是字符串
        if not isinstance(tuic_config, str) or not tuic_config.startswith('tuic://'):
            return None
        
        # 检查是否有 # 后的名称部分
        if '#' in tuic_config:
            try:
                name_part = tuic_config.split('#', 1)[1]
                return unquote(name_part).strip()
            except Exception:
                pass
        
        # 尝试从URL查询参数中提取名称
        parts = tuic_config.split('?')
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

def get_hysteria2_name(hy2_config):
    """
    从Hysteria2配置中提取名称信息
    参数:
        hy2_config: Hysteria2配置字符串
    返回:
        提取的名称字符串或None
    """
    try:
        # 确保输入是字符串
        if not isinstance(hy2_config, str):
            return None
        
        # 支持hy2://和hysteria2://前缀
        config_lower = hy2_config.lower()
        if not (config_lower.startswith('hy2://') or config_lower.startswith('hysteria2://')):
            return None
        
        # 检查是否有 # 后的名称部分
        if '#' in hy2_config:
            try:
                name_part = hy2_config.split('#', 1)[1]
                return unquote(name_part).strip()
            except Exception:
                pass
        
        # 尝试从URL查询参数中提取名称
        parts = hy2_config.split('?')
        if len(parts) > 1:
            try:
                params = parse_qs(parts[1])
                for name_key in ['name', 'remarks', 'ps', 'tag']:
                    if name_key in params:
                        return unquote(params[name_key][0]).strip()
            except Exception:
                pass
        
        return None
    except Exception:
        return None

def get_wireguard_name(wg_config):
    """
    从WireGuard配置中提取名称信息
    参数:
        wg_config: WireGuard配置字符串
    返回:
        提取的名称字符串或None
    """
    try:
        # 确保输入是字符串
        if not isinstance(wg_config, str):
            return None
        
        # 支持wireguard://和wg://前缀
        config_lower = wg_config.lower()
        if not (config_lower.startswith('wireguard://') or config_lower.startswith('wg://')):
            return None
        
        # 检查是否有 # 后的名称部分
        if '#' in wg_config:
            try:
                name_part = wg_config.split('#', 1)[1]
                return unquote(name_part).strip()
            except Exception:
                pass
        
        # 尝试从URL查询参数中提取名称
        parts = wg_config.split('?')
        if len(parts) > 1:
            try:
                params = parse_qs(parts[1])
                for name_key in ['name', 'remarks', 'ps', 'tag']:
                    if name_key in params:
                        return unquote(params[name_key][0]).strip()
            except Exception:
                pass
        
        # 对于base64编码的WireGuard配置，尝试解码查找名称
        try:
            prefix = 'wireguard://' if config_lower.startswith('wireguard://') else 'wg://'
            encoded_part = wg_config[len(prefix):]
            
            # 尝试解码base64部分
            decoded = decode_base64(encoded_part)
            if decoded:
                # 尝试从解码后的配置中查找名称相关信息
                for line in decoded.split('\n'):
                    if line.strip().lower().startswith('#'):
                        # 注释行可能包含名称信息
                        comment_text = line.strip()[1:].strip()
                        if comment_text:
                            return comment_text
                    elif '=' in line:
                        key, value = line.split('=', 1)
                        key = key.strip().lower()
                        value = value.strip().strip('"').strip("'")
                        if key in ['name', 'remarks', 'ps', 'tag', 'description']:
                            return value
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
    
    # 进一步放宽URL编码检查，减少误判
    percent25_count = config.count('%25')
    if percent25_count >= MIN_PERCENT25_COUNT * 4:  # 再次提高阈值
        return True
    
    # 进一步放宽配置长度限制
    if len(config) >= MAX_CONFIG_LENGTH * 3:  # 再次提高阈值
        return True
    
    # 增强的协议前缀检查
    has_valid_protocol = False
    config_lower = config.lower()
    
    # 检查标准协议前缀
    for protocol_prefix in PROTOCOL_PREFIXES:
        if protocol_prefix in config_lower:
            has_valid_protocol = True
            break
    
    # 如果没有匹配标准前缀，检查别名前缀
    if not has_valid_protocol:
        for protocol, aliases in ADDITIONAL_PROTOCOL_PREFIXES.items():
            for alias in aliases:
                if alias in config_lower:
                    has_valid_protocol = True
                    break
            if has_valid_protocol:
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
    """根据正则表达式模式在文本中查找匹配项，优化内存使用并增强协议识别"""
    if not text or not isinstance(text, str):
        return {}
        
    # 增强的协议模式定义
    PROTOCOL_PATTERNS = {
        "Vmess": [
            r'vmess:\/\/[^ \n\r<"\']+',
            r'vmess:\/\/[a-zA-Z0-9_\-\.\~]+(?:%[0-9a-fA-F]{2})*(?:[a-zA-Z0-9_\-\.\~\/\?\#\[\]\@\!\$\&\'\(\)\*\+\,\;\=\:]|%[0-9a-fA-F]{2})*'
        ],
        "Vless": [
            r'vless:\/\/[^ \n\r<"\']+',
            r'vless:\/\/[a-zA-Z0-9_\-\.\~]+(?:%[0-9a-fA-F]{2})*(?:[a-zA-Z0-9_\-\.\~\/\?\#\[\]\@\!\$\&\'\(\)\*\+\,\;\=\:]|%[0-9a-fA-F]{2})*'
        ],
        "Trojan": [
            r'trojan:\/\/[^ \n\r<"\']+',
            r'trojan:\/\/[a-zA-Z0-9_\-\.\~]+(?:%[0-9a-fA-F]{2})*(?:[a-zA-Z0-9_\-\.\~\/\?\#\[\]\@\!\$\&\'\(\)\*\+\,\;\=\:]|%[0-9a-fA-F]{2})*'
        ],
        "ShadowSocks": [
            r'ss:\/\/[^ \n\r<"\']+',
            r'ss:\/\/[a-zA-Z0-9_\-\.\~]+(?:%[0-9a-fA-F]{2})*(?:[a-zA-Z0-9_\-\.\~\/\?\#\[\]\@\!\$\&\'\(\)\*\+\,\;\=\:]|%[0-9a-fA-F]{2})*'
        ],
        "ShadowSocksR": [
            r'ssr:\/\/[^ \n\r<"\']+',
            r'ssr:\/\/[a-zA-Z0-9_\-\.\~]+(?:%[0-9a-fA-F]{2})*(?:[a-zA-Z0-9_\-\.\~\/\?\#\[\]\@\!\$\&\'\(\)\*\+\,\;\=\:]|%[0-9a-fA-F]{2})*'
        ],
        "Tuic": [
            r'tuic:\/\/[^ \n\r<"\']+',
            r'tuic:\/\/[a-zA-Z0-9_\-\.\~]+(?:%[0-9a-fA-F]{2})*(?:[a-zA-Z0-9_\-\.\~\/\?\#\[\]\@\!\$\&\'\(\)\*\+\,\;\=\:]|%[0-9a-fA-F]{2})*'
        ],
        "Hysteria2": [
            r'hy2:\/\/[^ \n\r<"\']+',
            r'hysteria2:\/\/[^ \n\r<"\']+',
            r'hy2:\/\/[a-zA-Z0-9_\-\.\~]+(?:%[0-9a-fA-F]{2})*(?:[a-zA-Z0-9_\-\.\~\/\?\#\[\]\@\!\$\&\'\(\)\*\+\,\;\=\:]|%[0-9a-fA-F]{2})*',
            r'hysteria2:\/\/[a-zA-Z0-9_\-\.\~]+(?:%[0-9a-fA-F]{2})*(?:[a-zA-Z0-9_\-\.\~\/\?\#\[\]\@\!\$\&\'\(\)\*\+\,\;\=\:]|%[0-9a-fA-F]{2})*'
        ],
        "WireGuard": [
            r'wireguard:\/\/[^ \n\r<"\']+',
            r'wg:\/\/[^ \n\r<"\']+',
            r'wireguard:\/\/[a-zA-Z0-9_\-\.\~]+(?:%[0-9a-fA-F]{2})*(?:[a-zA-Z0-9_\-\.\~\/\?\#\[\]\@\!\$\&\'\(\)\*\+\,\;\=\:]|%[0-9a-fA-F]{2})*',
            r'wg:\/\/[a-zA-Z0-9_\-\.\~]+(?:%[0-9a-fA-F]{2})*(?:[a-zA-Z0-9_\-\.\~\/\?\#\[\]\@\!\$\&\'\(\)\*\+\,\;\=\:]|%[0-9a-fA-F]{2})*'
        ]
    }
    
    # 只初始化有模式的类别，节省内存
    matches = {}
    
    # 增强的协议匹配策略：先使用直接前缀匹配，再使用正则表达式
    # 1. 首先使用直接字符串搜索提取所有可能的协议链接
    all_protocol_matches = {}
    
    # 收集所有协议前缀
    all_prefixes = []
    for protocol in PROTOCOL_CATEGORIES:
        all_prefixes.append(protocol.lower() + "://")
    
    # 添加别名前缀
    for protocol, aliases in ADDITIONAL_PROTOCOL_PREFIXES.items():
        all_prefixes.extend(aliases)
    
    # 直接搜索所有协议前缀
    for prefix in all_prefixes:
        prefix_lower = prefix.lower()
        text_lower = text.lower()
        start_pos = 0
        
        while True:
            pos = text_lower.find(prefix_lower, start_pos)
            if pos == -1:
                break
            
            # 找到前缀后，提取完整的URL直到遇到空白字符或结束符
            end_pos = pos + len(prefix_lower)
            while end_pos < len(text) and text[end_pos] not in [' ', '\n', '\r', '\t', '<', '"', "'"]:
                end_pos += 1
            
            # 提取完整的配置字符串
            full_config = text[pos:end_pos]
            if full_config:
                # 确定协议类别
                protocol_category = None
                if prefix_lower.startswith('vmess://'):
                    protocol_category = "Vmess"
                elif prefix_lower.startswith('vless://'):
                    protocol_category = "Vless"
                elif prefix_lower.startswith('trojan://'):
                    protocol_category = "Trojan"
                elif prefix_lower.startswith('ss://') and not prefix_lower.startswith('ssr://'):
                    protocol_category = "ShadowSocks"
                elif prefix_lower.startswith('ssr://'):
                    protocol_category = "ShadowSocksR"
                elif prefix_lower.startswith('tuic://'):
                    protocol_category = "Tuic"
                elif prefix_lower.startswith('hy2://') or prefix_lower.startswith('hysteria2://'):
                    protocol_category = "Hysteria2"
                elif prefix_lower.startswith('wireguard://') or prefix_lower.startswith('wg://'):
                    protocol_category = "WireGuard"
                
                if protocol_category:
                    if protocol_category not in all_protocol_matches:
                        all_protocol_matches[protocol_category] = set()
                    all_protocol_matches[protocol_category].add(full_config)
            
            start_pos = end_pos + 1
    
    # 2. 使用增强的正则表达式进行匹配
    for category, patterns in categories_data.items():
        # 只处理非空的模式列表
        if not patterns or not isinstance(patterns, list):
            continue
            
        category_matches = set()
        
        # 如果已经有直接匹配的结果，先添加它们
        if category in all_protocol_matches:
            category_matches.update(all_protocol_matches[category])
        
        # 添加内置的协议模式
        if category in PROTOCOL_PATTERNS:
            for pattern_str in PROTOCOL_PATTERNS[category]:
                try:
                    pattern = re.compile(pattern_str, re.IGNORECASE | re.MULTILINE | re.DOTALL)
                    found = pattern.findall(text)
                    if found:
                        for item in found:
                            if item and isinstance(item, str):
                                cleaned_item = item.strip()
                                if cleaned_item:
                                    category_matches.add(cleaned_item)
                except re.error as e:
                    logging.error(f"内置正则表达式错误 - 模式 '{pattern_str}' 在类别 '{category}': {e}")
                    continue
        
        # 然后处理用户提供的模式
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
                    
                    if found:
                        # 清理并去重匹配结果
                        for item in found:
                            if item and isinstance(item, str):
                                cleaned_item = item.strip()
                                if cleaned_item:
                                    category_matches.add(cleaned_item)
            except re.error as e:
                logging.error(f"正则表达式错误 - 模式 '{pattern_str}' 在类别 '{category}': {e}")
                continue
        
        if category_matches:
            matches[category] = category_matches
    
    # 只返回非空的匹配结果
    return {k: v for k, v in matches.items() if v}

def save_to_file(directory, category_name, items_set):
    """将项目集合保存到指定目录的文本文件中"""
    if not items_set:
        logging.debug(f"跳过空集合的保存: {category_name}")
        return False, 0
        
    # 确保目录存在
    try:
        os.makedirs(directory, exist_ok=True)
        file_path = os.path.join(directory, f"{category_name}.txt")
        count = len(items_set)
        
        # 写入排序后的项目，每行一个
        with open(file_path, 'w', encoding='utf-8') as f:
            for item in sorted(list(items_set)): 
                f.write(f"{item}\n")
        
        logging.info(f"已保存 {count} 项到 {file_path}")
        return True, count
    except IOError as e:
        logging.error(f"写入文件失败 {file_path}: {e}")
    except Exception as e:
        logging.error(f"保存文件时发生意外错误 {file_path}: {e}")
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

    # 定义增强的协议模式
    PROTOCOL_PATTERNS = {
        "Vmess": [r'vmess://[^\s]+'],
        "Vless": [r'vless://[^\s]+'],
        "Trojan": [r'trojan://[^\s]+'],
        "ShadowSocks": [r'ss://[^\s]+(?!r://)'],
        "ShadowSocksR": [r'ssr://[^\s]+'],
        "Tuic": [r'tuic://[^\s]+'],
        "Hysteria2": [r'(?:hysteria2|hy2)://[^\s]+'],
        "WireGuard": [r'(?:wireguard|wg)://[^\s]+']
    }
    
    # 使用增强的协议模式进行匹配，仅从keywords.json中加载国家关键词
    protocol_patterns_for_matching = PROTOCOL_PATTERNS
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
            config_lower = config.lower()
            if config_lower.startswith('ssr://'):
                name_to_check = get_ssr_name(config)
            elif config_lower.startswith('vmess://'):
                name_to_check = get_vmess_name(config)
            elif config_lower.startswith('trojan://'):
                name_to_check = get_trojan_name(config)
            elif config_lower.startswith('vless://'):
                name_to_check = get_vless_name(config)
            elif config_lower.startswith('ss://'):
                name_to_check = get_shadowsocks_name(config)
            elif config_lower.startswith('tuic://'):
                name_to_check = get_tuic_name(config)
            elif config_lower.startswith('hy2://') or config_lower.startswith('hysteria2://'):
                name_to_check = get_hysteria2_name(config)
            elif config_lower.startswith('wireguard://') or config_lower.startswith('wg://'):
                name_to_check = get_wireguard_name(config)
            # 所有协议都有名称提取支持了

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
                    
                # 准备此国家的文本关键词，保留所有有效的关键词
                text_keywords_for_country = []
                for kw in keywords_for_country_list:
                    if isinstance(kw, str) and kw.strip():
                        # 移除过度的过滤，只过滤掉空字符串和纯表情符号
                        # 允许所有有效的国家关键词，包括非英语字符
                        if len(kw.strip()) > 0:
                            # 只添加唯一的关键词
                            if kw not in text_keywords_for_country:
                                text_keywords_for_country.append(kw)
                
                # 检查是否匹配任何关键词
                match_found = False
                current_name_lower = current_name_to_check_str.lower()
                
                # 添加调试日志
                if processed_pages % 50 == 0:
                    logging.debug(f"处理配置名称: '{current_name_to_check_str}' 长度: {len(current_name_to_check_str)}")
                
                for keyword in text_keywords_for_country:
                    if not isinstance(keyword, str):
                        continue
                        
                    # 移除关键词前后空格
                    keyword = keyword.strip()
                    if not keyword:
                        continue
                        
                    # 对缩写使用单词边界匹配，对普通词使用包含匹配
                    is_abbr = (len(keyword) in [2, 3]) and keyword.isupper() and keyword.isalpha()
                    keyword_lower = keyword.lower()
                    
                    if is_abbr:
                        # 对于缩写，使用更灵活的匹配策略
                        try:
                            # 尝试精确匹配缩写
                            pattern = r'\b' + re.escape(keyword) + r'\b'
                            if re.search(pattern, current_name_to_check_str, re.IGNORECASE):
                                match_found = True
                                logging.debug(f"国家'{country_name_key}' 匹配缩写: '{keyword}'")
                                break
                            # 尝试另一种方式：在配置名称中查找国家代码，允许前后有非字母字符
                            if keyword_lower in current_name_lower:
                                # 检查是否是独立的国家代码，避免匹配到其他单词中包含的字母组合
                                parts = re.split(r'[^a-zA-Z]', current_name_to_check_str.lower())
                                if keyword_lower in parts:
                                    match_found = True
                                    logging.debug(f"国家'{country_name_key}' 匹配分割后缩写: '{keyword}'")
                                    break
                        except Exception:
                            # 静默跳过正则匹配错误
                            pass
                    else:
                        # 对于普通关键词，使用更精确的匹配
                        # 对于多语言关键词，使用更宽松的匹配策略
                        if not is_non_english_text(keyword):
                            # 英语关键词使用严格的包含检查
                            if keyword_lower in current_name_lower:
                                match_found = True
                                logging.debug(f"国家'{country_name_key}' 匹配英语关键词: '{keyword}'")
                                break
                        else:
                            # 非英语关键词使用直接比较
                            if keyword in current_name_to_check_str or keyword_lower in current_name_lower:
                                match_found = True
                                logging.debug(f"国家'{country_name_key}' 匹配非英语关键词: '{keyword}'")
                                break
                
                if match_found:
                    final_configs_by_country[country_name_key].add(config)
                    country_matched = True
                    logging.debug(f"配置已关联到国家: {country_name_key}")
                    # 移除这里的break，允许配置匹配多个国家
                
            # 移除这里的break，确保每个配置都能被完全处理

    # 统计信息日志
    logging.info(f"成功处理 {processed_pages}/{len(fetched_pages)} 个页面，找到 {found_configs} 个有效配置，过滤掉 {filtered_out_configs} 个无效配置")
    
    # 确保删除任何可能的旧国家计数数据，重新基于集合大小计算
    country_counts = {}
    
    # 国家计数将在保存文件时基于集合大小计算，此处删除重复代码
    
    # 准备输出目录结构
    country_dir = os.path.join(OUTPUT_DIR, COUNTRY_SUBDIR)
    protocol_dir = os.path.join(OUTPUT_DIR, PROTOCOL_SUBDIR)
    
    if os.path.exists(OUTPUT_DIR):
        try:
            shutil.rmtree(OUTPUT_DIR)
            logging.info(f"已删除旧的输出目录: {OUTPUT_DIR}")
        except (PermissionError, OSError) as e:
            logging.warning(f"无法删除旧输出目录: {e}，尝试使用新目录名")
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_dir = f"{OUTPUT_DIR}_backup_{timestamp}"
            try:
                shutil.move(OUTPUT_DIR, backup_dir)
                logging.info(f"已将旧目录重命名为: {backup_dir}")
            except Exception as inner_e:
                logging.error(f"重命名旧目录失败: {inner_e}")
                # 继续执行，让os.makedirs处理可能的目录存在情况
    
    # 确保输出目录结构存在
    try:
        os.makedirs(country_dir, exist_ok=True)
        os.makedirs(protocol_dir, exist_ok=True)
        logging.info(f"正在保存文件到目录: {OUTPUT_DIR}")
        logging.info(f"国家配置将保存到: {country_dir}")
        logging.info(f"协议配置将保存到: {protocol_dir}")
    except (PermissionError, OSError) as e:
        logging.critical(f"无法创建输出目录: {e}")
        return

    # 保存协议配置文件
    protocol_counts = {}
    for category, items in final_all_protocols.items():
        if items:  # 只保存非空集合
            saved, count = save_to_file(protocol_dir, category, items)
            if saved:
                protocol_counts[category] = count
    
    # 保存国家配置文件并确保计数准确
    country_counts = {}
    countries_with_configs = 0
    total_country_configs = 0
    
    for category, items in final_configs_by_country.items():
        if items:  # 只保存非空集合
            # 确保使用集合的实际大小作为计数
            actual_count = len(items)
            saved, count = save_to_file(country_dir, category, items)
            if saved:
                country_counts[category] = actual_count
                countries_with_configs += 1
                total_country_configs += actual_count
                logging.debug(f"已保存国家配置: {category}, 节点数量: {actual_count}")
    
    # 生成README文件
    try:
        generate_simple_readme(protocol_counts, country_counts, categories_data, use_local_paths=True)
    except Exception as e:
        logging.error(f"生成README文件时出错: {e}")
        # 继续执行，不中断程序
    
    # 输出完成信息
    logging.info(f"=== 抓取完成 ===")
    logging.info(f"找到并保存的协议配置: {sum(protocol_counts.values())}")
    logging.info(f"有配置的国家数量: {countries_with_configs}")
    logging.info(f"国家相关配置总数: {total_country_configs}")
    logging.info(f"输出目录结构:")
    logging.info(f"- 协议配置: {os.path.join(OUTPUT_DIR, PROTOCOL_SUBDIR)}")
    logging.info(f"- 国家配置: {os.path.join(OUTPUT_DIR, COUNTRY_SUBDIR)}")
    logging.info(f"README文件已更新: {README_FILE}")

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
