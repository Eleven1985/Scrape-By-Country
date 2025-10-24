# 标准库导入
import asyncio
import aiohttp
import json
import re
import logging
import os
import shutil


from datetime import datetime
import pytz
import base64

from urllib.parse import parse_qs, unquote

# 第三方库导入
import psutil  # 用于内存监控

# BeautifulSoup暂时保留，可能在后续功能扩展中使用
from bs4 import BeautifulSoup

# --- 配置常量 ---
# 使用绝对路径以避免路径解析问题
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_DIR = os.path.join(BASE_DIR, 'config')  # 配置文件夹，用于存放输入文件
URLS_FILE = os.path.join(CONFIG_DIR, 'urls.txt')
KEYWORDS_FILE = os.path.join(CONFIG_DIR, 'keywords.json') # 应包含国家的两字母代码
OUTPUT_DIR = os.path.join(BASE_DIR, 'output_configs')  # 使用绝对路径
COUNTRY_SUBDIR = 'countries'  # 国家配置文件夹
PROTOCOL_SUBDIR = 'protocols' # 协议配置文件夹
README_FILE = os.path.join(BASE_DIR, 'README.md')  # 使用绝对路径

# 运行时配置
REQUEST_TIMEOUT = 15  # HTTP请求超时时间（秒）
CONCURRENT_REQUESTS = 10  # 最大并发请求数
MAX_CONFIG_LENGTH = 1500  # 配置最大长度
MIN_PERCENT25_COUNT = 15  # 最小%25出现次数（用于检测过度URL编码）
FILTERED_PHRASE = 'i_love_'  # 要过滤的特定短语

# 性能优化设置
MAX_PAGE_SIZE = 5 * 1024 * 1024  # 最大页面大小(5MB)，防止过大的页面消耗过多内存
MAX_TOTAL_CONFIGS = 100000  # 最大总配置数量，防止内存溢出

# 协议前缀常量 - 确保键名与PROTOCOL_CATEGORIES一致
PROTOCOL_PREFIXES = {
    'vmess': ['vmess://'],
    'vless': ['vless://'],
    'trojan': ['trojan://'],
    'shadowsocks': ['ss://'],
    'shadowsocksr': ['ssr://'],
    'wireguard': ['wg://', 'wireguard://'],
    'tuic': ['tuic://'],
    'hysteria2': ['hy2://', 'hysteria2://']
}

# --- Logging Setup ---
# 创建日志目录
LOG_DIR = os.path.join(BASE_DIR, 'logs')
os.makedirs(LOG_DIR, exist_ok=True)

# 生成带有时间戳的日志文件名
log_filename = datetime.now().strftime("%Y%m%d_%H%M%S_scraper.log")
log_file_path = os.path.join(LOG_DIR, log_filename)

# 创建logger实例
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# 清除现有的处理器
logger.handlers.clear()

# 创建控制台处理器
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# 创建文件处理器
file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
file_handler.setLevel(logging.DEBUG)  # 文件记录所有级别的日志

# 设置格式器
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

# 添加处理器到logger
logger.addHandler(console_handler)
logger.addHandler(file_handler)

# 覆盖默认的logging模块，使所有调用使用我们的logger
logging = logger

# ---# 协议类别 - 与PROTOCOL_PREFIXES和PROTOCOL_REGEX_PATTERNS保持一致
PROTOCOL_CATEGORIES = [
    "Vmess", "Vless", "Trojan", "ShadowSocks", "ShadowSocksR",
    "WireGuard", "Tuic", "Hysteria2"
] # --- 检查非英语文本的辅助函数 ---
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
        if not isinstance(hy2_config, str) or not (hy2_config.startswith('hy2://') or hy2_config.startswith('hysteria2://')):
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
                for name_key in ['name', 'remarks', 'ps']:
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
        if not isinstance(wg_config, str) or not wg_config.startswith('wireguard://'):
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
                for name_key in ['name', 'remarks', 'ps']:
                    if name_key in params:
                        return unquote(params[name_key][0]).strip()
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
    
    # 快速检查空配置
    config_stripped = config.strip()
    if not config_stripped:
        return True
    
    # 检查是否包含过滤短语
    if FILTERED_PHRASE in config_stripped.lower():
        logging.debug(f"配置因包含过滤短语 '{FILTERED_PHRASE}' 被过滤: {config_stripped[:100]}...")
        return True
    
    # 检查URL编码情况
    percent25_count = config_stripped.count('%25')
    if percent25_count >= MIN_PERCENT25_COUNT * 2:  # 提高阈值以减少误判
        logging.debug(f"配置因过度URL编码 ({percent25_count}个%25) 被过滤: {config_stripped[:100]}...")
        return True
    
    # 检查配置长度
    if len(config_stripped) >= MAX_CONFIG_LENGTH * 2:  # 提高阈值以减少误判
        logging.debug(f"配置因过长 ({len(config_stripped)}字符) 被过滤")
        return True
    
    # 优化的协议前缀检查
    # 优化的协议前缀检查 - 使用更高效的方式
    config_lower = config_stripped.lower()
    found_protocol = None
    
    # 扁平化PROTOCOL_PREFIXES字典中的所有前缀
    all_protocol_prefixes = [prefix for prefix_list in PROTOCOL_PREFIXES.values() for prefix in prefix_list]
    
    # 优化前缀匹配逻辑：必须从字符串开头匹配协议前缀
    for protocol_prefix in all_protocol_prefixes:
        if config_lower.startswith(protocol_prefix):
            found_protocol = protocol_prefix
            break
    
    if not found_protocol:
        logging.debug(f"配置因缺少有效协议前缀被过滤: {config_stripped[:100]}...")
        return True
    
    # 对不同协议进行基本格式验证 - 使用映射表简化逻辑
    protocol_validation = {
        'vmess://': (8, lambda c: not c[8:].strip()),
        'vless://': (8, lambda c: not c[8:].strip()),
        'trojan://': (0, lambda c: '@' not in c),  # Trojan格式必须包含@
        'ss://': (5, lambda c: not c[5:].strip()),
        'ssr://': (6, lambda c: not c[6:].strip()),
        'tuic://': (7, lambda c: not c[7:].strip()),
        'hy2://': (5, lambda c: not c[5:].strip()),
        'hysteria2://': (12, lambda c: not c[12:].strip()),
        'wireguard://': (12, lambda c: not c[12:].strip()),
        'wg://': (5, lambda c: not c[5:].strip())  # 添加wireguard的wg://前缀验证
    }
    
    # 检查协议是否需要特殊验证且验证失败
    if found_protocol in protocol_validation:
        _, validate_func = protocol_validation[found_protocol]
        if validate_func(config):
            return True
    
    return False

async def fetch_url(session, url, max_retries=2):
    """异步获取URL内容并提取文本，支持重试机制"""
    # 验证URL格式（更严格的验证）
    if not url or not isinstance(url, str) or not url.startswith(('http://', 'https://')):
        logging.warning(f"无效的URL格式: {url}")
        return url, None
    
    retry_count = 0
    last_exception = None
    
    # 优化的浏览器头部，增加更多伪装信息
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    }
    
    while retry_count <= max_retries:
        try:
            # 添加请求超时和重定向处理
            timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
            async with session.get(url, timeout=timeout, headers=headers, allow_redirects=True) as response:
                # 即使状态码不是2xx，也尝试获取内容
                text_content = None
                
                # 检查是否为成功响应
                if response.status >= 200 and response.status < 300:
                    # 检查内容长度，避免过大的响应
                    content_length = response.headers.get('Content-Length')
                    if content_length:
                        try:
                            if int(content_length) > MAX_PAGE_SIZE:
                                logging.warning(f"页面过大 (>{MAX_PAGE_SIZE/1024/1024:.1f}MB), 跳过: {url}")
                                return url, None
                        except ValueError:
                            pass
                    
                    # 尝试处理不同的内容类型
                    content_type = response.headers.get('Content-Type', '')
                    
                    try:
                        # 优化内容处理逻辑
                        if 'application/json' in content_type:
                            try:
                                json_data = await response.json()
                                text_content = json.dumps(json_data, ensure_ascii=False)
                                logging.debug(f"处理JSON内容: {url}")
                            except json.JSONDecodeError:
                                # 回退到文本处理
                                html = await response.text()
                                soup = BeautifulSoup(html, 'html.parser')
                                text_content = soup.get_text(separator='\n', strip=True)
                        else:
                            # 处理HTML或纯文本
                            html = await response.text(max_chars=MAX_PAGE_SIZE)  # 限制读取大小
                            
                            # 再次检查内容大小
                            if len(html) >= MAX_PAGE_SIZE:
                                logging.warning(f"页面内容过大, 已部分读取: {url}")
                            
                            # 优化BeautifulSoup解析
                            soup = BeautifulSoup(html, 'lxml')  # 使用lxml解析器更快
                            
                            # 优化内容提取策略
                            # 1. 优先从代码块提取
                            code_blocks = soup.find_all(['pre', 'code'])
                            if code_blocks:
                                text_content = '\n'.join(block.get_text().strip() for block in code_blocks)
                            else:
                                # 2. 提取所有文本，但避免重复
                                text_content = soup.get_text(separator='\n', strip=True)
                                
                    except UnicodeDecodeError:
                        # 处理编码错误
                        logging.warning(f"解码错误，尝试使用二进制模式: {url}")
                        content = await response.read()
                        try:
                            text_content = content.decode('utf-8', errors='replace')
                        except:
                            text_content = str(content)[:MAX_PAGE_SIZE]
                else:
                    # 非成功响应也记录状态码
                    logging.warning(f"URL返回非成功状态码: {response.status}, URL: {url}")
                    # 尝试获取错误页面内容
                    try:
                        text_content = await response.text(max_chars=1000)
                        if text_content and len(text_content.strip()) > 0:
                            logging.info(f"成功获取: {url}")
                            return url, text_content
                    except Exception:
                        logging.debug(f"无法获取错误页面内容: {url}")
                    
        except asyncio.TimeoutError:
            last_exception = "请求超时"
            logging.warning(f"获取URL超时: {url}, 第{retry_count+1}次尝试")
        except aiohttp.ClientError as e:
            last_exception = f"客户端错误: {str(e)}"
            logging.warning(f"获取URL客户端错误: {url}, 错误: {str(e)}, 第{retry_count+1}次尝试")
        except Exception as e:
            last_exception = f"未知错误: {type(e).__name__}: {str(e)}"
            logging.warning(f"获取URL时出错: {url}, 错误类型: {type(e).__name__}, 第{retry_count+1}次尝试")
        
        retry_count += 1
        # 只有在还没达到最大重试次数时才延迟
        if retry_count <= max_retries:
            # 指数退避策略，增加随机因子避免雪崩
            delay = min(2 ** retry_count + random.uniform(0, 1), 10)
            logging.info(f"将在{delay:.2f}秒后重试获取URL: {url}")
            await asyncio.sleep(delay)
    
    logging.error(f"在{max_retries+1}次尝试后获取URL失败: {url}, 最后错误: {last_exception}")
    return url, None

def get_memory_usage():
    """获取当前进程的内存使用情况（MB）"""
    try:
        process = psutil.Process(os.getpid())
        mem_info = process.memory_info()
        return mem_info.rss / 1024 / 1024  # 转换为MB
    except Exception as e:
        logging.warning(f"无法获取内存使用情况: {e}")
        return None

# 预编译协议前缀的正则表达式，提高性能 - 确保键名与PROTOCOL_PREFIXES一致
PROTOCOL_REGEX_PATTERNS = {
    'vmess': re.compile(r'vmess://[-_a-zA-Z0-9+/=]+'),
    'vless': re.compile(r'vless://[-_a-zA-Z0-9+/=?&]+'),
    'trojan': re.compile(r'trojan://[-_a-zA-Z0-9+/=?&@.]+'),
    'shadowsocks': re.compile(r'ss://[-_a-zA-Z0-9+/=?&@.]+'),
    'shadowsocksr': re.compile(r'ssr://[-_a-zA-Z0-9+/=?&@.]+'),
    'wireguard': re.compile(r'(wg://|wireguard://)[-_a-zA-Z0-9+/=?&@.\s]+'),
    'tuic': re.compile(r'tuic://[-_a-zA-Z0-9+/=?&@.]+'),
    'hysteria2': re.compile(r'(hy2://|hysteria2://)[-_a-zA-Z0-9+/=?&@.]+')
}

def find_matches(text, categories_data):
    """根据正则表达式模式在文本中查找匹配项，优化内存使用和性能"""
    if not text or not isinstance(text, str):
        return {}
    
    matches = {}
    text_lower = text.lower()  # 预计算小写文本以提高效率
    
    # 1. 快速扫描：使用预编译的协议正则表达式进行初步匹配
    # 这比用户自定义的模式更高效，用于快速过滤和初步提取
    for protocol, regex in PROTOCOL_REGEX_PATTERNS.items():
        if protocol in categories_data:
            try:
                # 快速提取协议链接
                quick_matches = regex.findall(text)
                if quick_matches and protocol not in matches:
                    matches[protocol] = set()
                matches[protocol].update(quick_matches)
            except Exception as e:
                logging.debug(f"协议快速匹配错误 ({protocol}): {e}")
    
    # 2. 应用用户自定义的模式进行更精确的匹配
    for category, patterns in categories_data.items():
        if not patterns or not isinstance(patterns, list):
            continue
            
        category_matches = set()
        is_protocol_category = category in PROTOCOL_CATEGORIES
        
        # 如果已经通过快速匹配找到了结果，只在必要时应用自定义模式
        if category in matches and len(matches[category]) > 0 and len(patterns) > 3:
            # 如果已经有足够的结果且模式很多，跳过以提高性能
            continue
        
        for pattern_str in patterns:
            if not isinstance(pattern_str, str) or not pattern_str.strip():
                continue
                
            try:
                # 对于国家类别，使用原始正则表达式
                pattern = re.compile(
                    pattern_str, 
                    re.IGNORECASE | re.MULTILINE | re.DOTALL | re.UNICODE
                )
                found = pattern.findall(text)
                
                # 批量添加匹配项，减少循环开销
                valid_items = {item.strip() for item in found 
                            if item and isinstance(item, str) and item.strip()}
                category_matches.update(valid_items)
                
            except re.error as e:
                logging.error(f"正则表达式错误 - 模式 '{pattern_str[:50]}...' 在类别 '{category}': {e}")
                continue
                
        if category_matches:  # 只添加非空集合
            if category not in matches:
                matches[category] = set()
            matches[category].update(category_matches)
    
    # 3. 清理：确保所有结果都是有效的URL格式
    for category in list(matches.keys()):
        # 检查是否是协议类别
        category_lower = category.lower()
        for proto, prefixes in PROTOCOL_PREFIXES.items():
            if proto == category_lower:
                valid_configs = {config for config in matches[category] 
                               if any(config.startswith(prefix) for prefix in prefixes)}
                matches[category] = valid_configs
                break
    
    return matches

def save_to_file(directory, category_name, items_set):
    """将项目集合保存到指定目录的文本文件中"""
    if not items_set:
        logging.debug(f"跳过空集合的保存: {category_name}")
        return False, 0
        
    # 确保使用绝对路径
    abs_directory = os.path.abspath(directory)
    abs_file_path = os.path.join(abs_directory, f"{category_name}.txt")
    count = len(items_set)
    
    # 添加日志，记录将保存的内容数量和目标位置
    logging.debug(f"准备保存 {count} 项到: {abs_file_path}")
    
    # 记录将要保存的文件的绝对路径
    logging.info(f"准备保存 {count} 项到: {abs_file_path}")
    
    try:
        # 确保目录存在
        os.makedirs(abs_directory, exist_ok=True)
        logging.debug(f"确认目录存在: {abs_directory}")
        
        # 直接写入文件
        with open(abs_file_path, 'w', encoding='utf-8', newline='') as f:
            for item in sorted(list(items_set)):
                f.write(f"{item}\n")
        
        # 强制刷新文件系统缓存
        import io
        io.open(abs_file_path).close()
        
        # 验证文件是否成功写入
        if os.path.exists(abs_file_path):
            file_size = os.path.getsize(abs_file_path)
            if file_size > 0:
                logging.info(f"✓ 成功保存 {count} 项到 {abs_file_path} (大小: {file_size} 字节)")
                return True, count
            else:
                logging.error(f"✗ 文件创建成功但为空: {abs_file_path}")
                return False, 0
        else:
            logging.error(f"✗ 文件不存在: {abs_file_path}")
            
            # 检查目录是否可写
            if not os.access(abs_directory, os.W_OK):
                logging.error(f"✗ 目录不可写: {abs_directory}")
            else:
                logging.debug(f"目录可写，但文件创建失败")
            
            return False, 0
    except Exception as e:
        logging.error(f"✗ 保存文件时发生错误: {str(e)}")
        
        # 使用备用方法 - 写入到临时文件并立即检查
        try:
            temp_file = os.path.join(abs_directory, f"temp_{category_name}.txt")
            logging.info(f"尝试备用方法，写入临时文件: {temp_file}")
            
            with open(temp_file, 'w', encoding='utf-8') as f:
                # 写入少量内容用于测试
                sample_items = list(items_set)[:10]  # 取前10个样本
                for item in sorted(sample_items):
                    f.write(f"{item}\n")
            
            if os.path.exists(temp_file):
                logging.info(f"备用方法测试成功，临时文件已创建")
                # 现在写入完整内容
                with open(temp_file, 'w', encoding='utf-8') as f:
                    for item in sorted(list(items_set)):
                        f.write(f"{item}\n")
                
                # 重命名到目标位置
                if os.path.exists(abs_file_path):
                    os.remove(abs_file_path)
                os.rename(temp_file, abs_file_path)
                
                logging.info(f"✓ 备用方法: 已保存 {count} 项到 {abs_file_path}")
                return True, count
            else:
                logging.error(f"✗ 备用方法失败: 临时文件未创建")
                return False, 0
        except Exception as backup_e:
            logging.error(f"✗ 备用方法也失败: {str(backup_e)}")
            return False, 0

# --- 使用旗帜图像生成简单的README函数 ---
def generate_simple_readme(protocol_counts, country_counts, all_keywords_data, use_local_paths=True):
    """生成增强版README.md文件，展示抓取结果统计信息"""
    # 输入验证和默认值处理
    protocol_counts = protocol_counts if isinstance(protocol_counts, dict) else {}
    country_counts = country_counts if isinstance(country_counts, dict) else {}
    all_keywords_data = all_keywords_data if isinstance(all_keywords_data, dict) else {}
    
    try:
        # 获取带时区的当前时间
        tz = pytz.timezone('Asia/Shanghai')
        now = datetime.now(tz)
        timestamp = now.strftime("%Y-%m-%d %H:%M:%S %Z")
        date_only = now.strftime("%Y-%m-%d")
        
        # 计算详细统计信息
        total_protocol_configs = sum(protocol_counts.values())
        total_country_configs = sum(country_counts.values())
        countries_with_data = len(country_counts)
        protocols_with_data = len(protocol_counts)
        
        # 计算平均每国配置数
        avg_configs_per_country = total_country_configs / countries_with_data if countries_with_data > 0 else 0
        
        # 找出配置最多的国家和协议
        top_country = max(country_counts.items(), key=lambda x: x[1], default=("无", 0))
        top_protocol = max(protocol_counts.items(), key=lambda x: x[1], default=("无", 0))

        # 构建子目录的路径
        if use_local_paths:
            # 使用相对路径，便于本地查看
            protocol_base_url = f"{PROTOCOL_SUBDIR}"
            country_base_url = f"{COUNTRY_SUBDIR}"
            logging.debug(f"README使用本地相对路径: protocols={protocol_base_url}, countries={country_base_url}")
        else:
            # GitHub远程路径支持
            github_repo_path = "miladtahanian/V2RayScrapeByCountry"
            github_branch = "main"
            protocol_base_url = f"https://raw.githubusercontent.com/{github_repo_path}/refs/heads/{github_branch}/{OUTPUT_DIR}/{PROTOCOL_SUBDIR}"
            country_base_url = f"https://raw.githubusercontent.com/{github_repo_path}/refs/heads/{github_branch}/{OUTPUT_DIR}/{COUNTRY_SUBDIR}"
            logging.debug(f"README使用GitHub路径: protocols={protocol_base_url}, countries={country_base_url}")

        # 构建README内容，使用更现代化的格式
        md_content = [
            f"# 📊 V2Ray 配置抓取结果 ({date_only})\n",
            "\n",
            f"*最后更新: {timestamp}*\n",
            "\n",
            "> 此文件由自动脚本生成，包含从多个来源抓取和分类的 V2Ray 配置信息。\n",
            "\n",
            "## 📋 详细统计概览\n",
            "\n",
            f"- **总配置数量**: **{total_protocol_configs:,}**\n",
            f"- **有数据的协议类型**: {protocols_with_data}\n",
            f"- **国家相关配置数**: {total_country_configs:,}\n",
            f"- **有配置的国家/地区**: {countries_with_data}\n",
            f"- **平均每国配置数**: {avg_configs_per_country:.1f}\n",
            f"- **配置最多的国家**: {top_country[0]} ({top_country[1]:,} 个配置)\n",
            f"- **配置最多的协议**: {top_protocol[0]} ({top_protocol[1]:,} 个配置)\n",
            "\n",
            "## ℹ️ 说明\n",
            "\n",
            "- 国家文件仅包含在**配置名称**中找到国家名称/标识的配置\n",
            "- 配置名称首先从链接的`#`部分提取，如果不存在，则从内部名称(对于Vmess/SSR)提取\n",
            "- 所有配置已按类别整理到不同目录中，便于查找和使用\n",
            "- 配置可能随时失效，请及时更新\n",
            "\n",
        ]

        # 添加协议文件表格
        md_content.append("## 📁 协议配置文件\n")
        md_content.append("\n")
        
        if protocol_counts:
            # 按配置数量排序
            sorted_protocols = sorted(protocol_counts.items(), key=lambda x: x[1], reverse=True)
            
            md_content.append("| 协议类型 | 配置数量 | 占比 | 文件链接 |")
            md_content.append("|---------|---------|------|----------|")
            
            for category_name, count in sorted_protocols:
                # 计算占比
                percentage = (count / total_protocol_configs * 100) if total_protocol_configs > 0 else 0
                file_link = f"{protocol_base_url}/{category_name}.txt"
                md_content.append(f"| **{category_name}** | {count:,} | {percentage:.1f}% | [`{category_name}.txt`]({file_link}) |")
        else:
            md_content.append("*没有找到协议配置。*\n")
        
        md_content.append("\n")

        # 添加国家文件表格
        md_content.append("## 🌍 国家/地区配置文件\n")
        md_content.append("\n")
        
        if country_counts:
            # 按配置数量排序
            sorted_countries = sorted(country_counts.items(), key=lambda x: x[1], reverse=True)
            
            md_content.append("| 国家/地区 | 配置数量 | 文件链接 |")
            md_content.append("|----------|---------|----------|")
            
            for country_category_name, count in sorted_countries:
                country_display_text = []
                
                # 1. 查找国家的两字母ISO代码用于旗帜图像
                flag_image_markdown = ""
                if country_category_name in all_keywords_data:
                    keywords_list = all_keywords_data[country_category_name]
                    if isinstance(keywords_list, list):
                        for item in keywords_list:
                            if isinstance(item, str) and len(item) == 2 and item.isupper() and item.isalpha():
                                iso_code = item.lower()
                                flag_image_url = f"https://flagcdn.com/w20/{iso_code}.png"
                                flag_image_markdown = f'<img src="{flag_image_url}" width="20" height="15" alt="{country_category_name} flag" align="absmiddle">'
                                country_display_text.append(flag_image_markdown)
                                break
                
                # 2. 提取中文名称（如果有）
                display_name = country_category_name
                if country_category_name in all_keywords_data:
                    keywords_list = all_keywords_data[country_category_name]
                    if isinstance(keywords_list, list):
                        # 查找包含中文的条目
                        for item in keywords_list:
                            if isinstance(item, str):
                                # 提取纯中文部分
                                chinese_chars = ''.join(char for char in item if '\u4e00' <= char <= '\u9fff')
                                if chinese_chars:
                                    display_name = f"{country_category_name}（{chinese_chars}）"
                                    break
                
                country_display_text.append(display_name)
                full_display = " ".join(country_display_text)
                
                # 构建文件链接
                file_link = f"{country_base_url}/{country_category_name}.txt"
                md_content.append(f"| {full_display} | {count:,} | [`{country_category_name}.txt`]({file_link}) |")
        else:
            md_content.append("*没有找到与国家相关的配置。*\n")
        
        md_content.append("\n")
        
        # 添加底部信息
        md_content.append("## 📝 备注\n")
        md_content.append("\n")
        md_content.append("- 本项目仅供学习和研究使用\n")
        md_content.append("- 请遵守相关法律法规\n")
        md_content.append("- 定期更新以获取最新配置\n")
        
        # 合并内容
        full_content = ''.join(md_content)
        
        # 确保README文件目录存在
        readme_dir = os.path.dirname(README_FILE)
        if readme_dir and not os.path.exists(readme_dir):
            try:
                os.makedirs(readme_dir, exist_ok=True)
                logging.debug(f"创建README目录: {readme_dir}")
            except Exception as e:
                logging.error(f"创建README目录失败: {e}")
                return False
        
        # 写入文件
        try:
            with open(README_FILE, 'w', encoding='utf-8', newline='') as f:
                f.write(full_content)
            
            # 验证文件是否成功写入
            if os.path.exists(README_FILE):
                file_size = os.path.getsize(README_FILE) / 1024  # KB
                logging.info(f"✅ 成功生成README文件: {os.path.abspath(README_FILE)} ({file_size:.2f} KB)")
                return True
            else:
                logging.error(f"❌ README文件写入失败: 文件不存在")
                return False
                
        except IOError as e:
            logging.error(f"❌ 写入README文件时发生IO错误: {e}")
            return False
        except Exception as e:
            logging.error(f"❌ 生成README时发生未知错误: {e}")
            return False
            
    except Exception as e:
        logging.error(f"❌ README生成过程中发生异常: {e}")
        return False

# main函数和其他函数实现
async def main():
    """主函数，协调整个抓取和处理流程"""
    start_time = time.time()
    logging.info(f"日志文件已创建: {os.path.abspath(log_file_path)}")
    logging.info(f"当前工作目录: {os.getcwd()}")
    
    # 确保配置文件夹存在
    try:
        os.makedirs(CONFIG_DIR, exist_ok=True)
        logging.info(f"配置文件夹: {os.path.abspath(CONFIG_DIR)}")
    except Exception as e:
        logging.error(f"创建配置文件夹失败: {e}")
    
    # 检查必要的输入文件是否存在
    urls_file_abs = os.path.abspath(URLS_FILE)
    keywords_file_abs = os.path.abspath(KEYWORDS_FILE)
    
    if not os.path.exists(urls_file_abs) or not os.path.exists(keywords_file_abs):
        missing_files = []
        if not os.path.exists(urls_file_abs):
            missing_files.append(f"URLs文件: {urls_file_abs}")
        if not os.path.exists(keywords_file_abs):
            missing_files.append(f"关键词文件: {keywords_file_abs}")
        
        logging.critical(f"未找到输入文件:\n- {chr(10)}- ".join(missing_files))
        logging.info(f"请确保这些文件已放在正确的位置")
        return
    
    # 检查文件读取权限
    if not os.access(urls_file_abs, os.R_OK):
        logging.critical(f"没有权限读取URLs文件: {urls_file_abs}")
        return
    if not os.access(keywords_file_abs, os.R_OK):
        logging.critical(f"没有权限读取关键词文件: {keywords_file_abs}")
        return

    # 加载URL和关键词数据
    try:
        # 更健壮的URL加载，跳过注释行和无效URL
        urls = []
        with open(urls_file_abs, 'r', encoding='utf-8', errors='replace') as f:
            for line_num, line in enumerate(f, 1):
                stripped_line = line.strip()
                # 跳过空行和注释行
                if stripped_line and not stripped_line.startswith('#'):
                    # 基本URL格式验证
                    if stripped_line.startswith(('http://', 'https://')):
                        urls.append(stripped_line)
                    else:
                        logging.warning(f"第{line_num}行包含无效URL格式: {stripped_line}")
            
        if not urls:
            logging.critical("URLs文件中没有有效的URL。")
            return
            
        logging.info(f"已从 {URLS_FILE} 加载 {len(urls)} 个有效URL")
        
        # 更安全的JSON加载
        categories_data = {}
        with open(keywords_file_abs, 'r', encoding='utf-8', errors='replace') as f:
            try:
                categories_data = json.load(f)
            except json.JSONDecodeError as e:
                logging.critical(f"解析keywords.json文件失败: {e}")
                # 尝试提供更多帮助信息
                logging.info("请检查文件格式是否为有效的JSON")
                return
            
        # 验证categories_data是字典类型
        if not isinstance(categories_data, dict):
            logging.critical("keywords.json必须包含字典格式的数据。")
            return
            
        # 验证协议类别是否在配置中
        missing_protocols = [p for p in PROTOCOL_CATEGORIES if p not in categories_data]
        if missing_protocols:
            logging.warning(f"keywords.json中缺少以下协议类别的配置: {', '.join(missing_protocols)}")
            # 为缺失的协议类别创建空列表，以便程序能够继续运行
            for protocol in missing_protocols:
                categories_data[protocol] = []
            
        # 验证每个值都是列表
        invalid_entries = []
        valid_categories_data = {}
        for k, v in categories_data.items():
            if isinstance(v, list):
                # 过滤掉空字符串和非字符串元素
                filtered_list = [item for item in v if isinstance(item, str) and item.strip()]
                if filtered_list:  # 只保留非空列表
                    valid_categories_data[k] = filtered_list
            else:
                invalid_entries.append((k, type(v).__name__))
        
        if invalid_entries:
            logging.warning(f"keywords.json包含非列表格式的值: {invalid_entries}")
            
        categories_data = valid_categories_data
        
        if not categories_data:
            logging.critical("keywords.json中没有有效的类别数据。")
            return
            
        # 统计有效关键词信息
        total_patterns = sum(len(patterns) for patterns in categories_data.values())
        logging.info(f"成功加载 {len(categories_data)} 个类别，共 {total_patterns} 个模式")
            
    except IOError as e:
        logging.critical(f"读取输入文件时出错: {e}")
        return
    except Exception as e:
        logging.critical(f"加载配置数据时发生未预期错误: {e}")
        return

    # 分离协议模式和国家关键词
    protocol_patterns_for_matching = {
        cat: patterns for cat, patterns in categories_data.items() if cat in PROTOCOL_CATEGORIES
    }
    country_keywords_for_naming = {
        cat: patterns for cat, patterns in categories_data.items() if cat not in PROTOCOL_CATEGORIES
    }
    country_category_names = list(country_keywords_for_naming.keys())

    logging.info(f"已加载 {len(urls)} 个URL和 {len(categories_data)} 个总类别")
    logging.info(f"协议类别数量: {len(protocol_patterns_for_matching)}")
    logging.info(f"国家类别数量: {len(country_keywords_for_naming)}")

    # URL去重（使用有序字典保留顺序）
    unique_urls = list(dict.fromkeys(urls))  # Python 3.7+ 中字典保持插入顺序
    if len(unique_urls) < len(urls):
        duplicate_count = len(urls) - len(unique_urls)
        logging.info(f"已去除 {duplicate_count} 个重复URL，剩余 {len(unique_urls)} 个唯一URL")
        urls = unique_urls
    
    # 异步获取所有页面
    # 动态调整并发数，根据URL数量
    dynamic_concurrency = min(CONCURRENT_REQUESTS, max(5, len(urls) // 10))
    sem = asyncio.Semaphore(dynamic_concurrency)
    
    async def fetch_with_semaphore(session, url_to_fetch):
        """使用信号量限制并发的fetch_url，添加超时和重试控制"""
        async with sem:
            try:
                # 为每个URL记录开始时间
                start_fetch_time = time.time()
                result = await fetch_url(session, url_to_fetch)
                fetch_time = time.time() - start_fetch_time
                logging.debug(f"URL获取完成: {url_to_fetch[:50]}..., 耗时: {fetch_time:.2f}秒")
                return result
            except asyncio.CancelledError:
                # 处理任务取消
                logging.warning(f"URL获取任务被取消: {url_to_fetch}")
                return url_to_fetch, None
            except Exception as e:
                logging.error(f"URL获取任务异常: {url_to_fetch[:50]}..., 错误类型: {type(e).__name__}")
                return url_to_fetch, None
    
    # 创建优化的HTTP会话
    timeout = aiohttp.ClientTimeout(
        total=60,      # 总超时
        connect=15,    # 连接超时
        sock_connect=15,  # socket连接超时
        sock_read=30   # socket读取超时
    )
    
    connector = aiohttp.TCPConnector(
        limit=dynamic_concurrency,  # 最大并发连接数
        limit_per_host=min(5, dynamic_concurrency // 2),  # 每个主机的最大连接数
        enable_cleanup_closed=True,  # 启用连接清理
        ttl_dns_cache=300  # DNS缓存时间
    )
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        logging.info(f"开始获取 {len(urls)} 个URLs (最大并发: {dynamic_concurrency})...")
        
        # 批量处理URL，根据URL数量动态调整批次大小
        batch_size = min(20, max(5, len(urls) // 5))
        filtered_pages = []
        success_count = 0
        exception_count = 0
        
        for i in range(0, len(urls), batch_size):
            batch_urls = urls[i:i+batch_size]
            batch_num = i//batch_size + 1
            total_batches = (len(urls) + batch_size - 1) // batch_size
            logging.info(f"处理URL批次 {batch_num}/{total_batches}, 包含 {len(batch_urls)} 个URL")
            
            # 异步获取本批次URL的内容
            batch_results = await asyncio.gather(
                *[fetch_with_semaphore(session, u) for u in batch_urls],
                return_exceptions=True  # 即使某些任务失败也继续执行
            )
            
            # 处理本批次结果
            for j, result in enumerate(batch_results):
                url = batch_urls[j]
                if isinstance(result, tuple) and len(result) == 2 and isinstance(result[0], str) and result[1] is not None:
                    filtered_pages.append(result)
                    success_count += 1
                    logging.debug(f"成功获取URL: {url}")
                elif isinstance(result, Exception):
                    exception_count += 1
                    logging.warning(f"URL获取任务异常: {url}, {type(result).__name__}: {result}")
                else:
                    logging.debug(f"无效的URL获取结果: {url}, {type(result)}")
            
            # 记录批次进度
            logging.info(f"批次 {batch_num}/{total_batches} 完成: 成功 {success_count}, 异常 {exception_count}, 累计有效页面 {len(filtered_pages)}")
            
            # 避免请求过于频繁，在批次之间添加小延迟
            if i + batch_size < len(urls):
                logging.debug(f"在批次之间添加1秒延迟")
                await asyncio.sleep(1)
        
        fetched_pages = filtered_pages
        logging.info(f"URL获取完成: 成功 {success_count}, 异常 {exception_count}, 总计 {len(filtered_pages)} 个页面待处理")

    # 初始化结果集合
    final_configs_by_country = {cat: set() for cat in country_category_names}
    final_all_protocols = {cat: set() for cat in PROTOCOL_CATEGORIES}
    # 全局去重集合，提高去重效率
    global_config_set = set()

    logging.info("处理页面并关联配置名称...")
    
    # 统计成功处理的页面数量
    processed_pages = 0
    found_configs = 0
    filtered_out_configs = 0
    
    # 处理前获取内存使用情况
    initial_memory = get_memory_usage()
    if initial_memory:
        logging.debug(f"初始内存使用: {initial_memory:.2f} MB")
    
    for page_idx, (url, text) in enumerate(fetched_pages, 1):
        if not text:
            continue
            
        processed_pages += 1
        page_start_time = time.time()
        
        # 优化的匹配处理
        try:
            page_protocol_matches = find_matches(text, protocol_patterns_for_matching)
        except Exception as e:
            logging.error(f"处理页面时出错: {url}, 错误: {e}")
            continue
        
        # 处理找到的协议配置
        page_found_count = 0
        page_filtered_count = 0
        
        # 检查总配置数是否超过限制，防止内存溢出
        total_current_configs = sum(len(configs) for configs in final_all_protocols.values())
        if total_current_configs >= MAX_TOTAL_CONFIGS:
            logging.warning(f"已达到最大配置数限制 ({MAX_TOTAL_CONFIGS})，停止处理新配置")
            break
        
        # 批量处理配置，减少重复计算
        for protocol_cat_name, configs_found in page_protocol_matches.items():
            if protocol_cat_name in PROTOCOL_CATEGORIES:
                # 对每个协议类别的配置进行批量处理
                valid_configs = []
                for config in configs_found:
                    # 快速检查是否已存在于全局集合中
                    if config in global_config_set:
                        continue
                        
                    # 过滤无效配置
                    if not should_filter_config(config):
                        valid_configs.append(config)
                        global_config_set.add(config)
                        page_found_count += 1
                    else:
                        page_filtered_count += 1
                
                # 批量添加到结果集合
                if valid_configs:
                    final_all_protocols[protocol_cat_name].update(valid_configs)
        
        found_configs += page_found_count
        filtered_out_configs += page_filtered_count
        
        page_processing_time = time.time() - page_start_time
        
        # 更智能的进度输出
        if processed_pages % 10 == 0 or page_idx == len(fetched_pages):
            current_memory = get_memory_usage()
            memory_info = f"内存: {current_memory:.2f} MB" if current_memory else ""
            logging.info(f"处理进度: {processed_pages}/{len(fetched_pages)} 页面, "
                      f"已找到 {found_configs} 配置, 已过滤 {filtered_out_configs} 配置, "
                      f"此页耗时: {page_processing_time:.2f}秒 {memory_info}")
        elif page_processing_time > 5:  # 记录处理慢的页面
            logging.warning(f"页面处理较慢: {url[:50]}..., 耗时: {page_processing_time:.2f}秒")

        # 配置已通过global_config_set进行去重，无需额外操作
        if len(unique_configs) < len(all_page_configs_after_filter):
            logging.info(f"去重前配置数量: {len(all_page_configs_after_filter)}, 去重后: {len(unique_configs)}")
            all_page_configs_after_filter = unique_configs
        
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
                # 使用字典映射协议类型到对应的名称提取函数，提高可维护性
                protocol_handlers = {
                    # Vmess协议
                    'vmess://': get_vmess_name,
                    # Vless协议
                    'vless://': get_vless_name,
                    # Trojan协议
                    'trojan://': get_trojan_name,
                    # ShadowSocks协议
                    'ss://': get_shadowsocks_name,
                    # ShadowSocksR协议
                    'ssr://': get_ssr_name,
                    # WireGuard协议
                    'wireguard://': get_wireguard_name,
                    'wg://': get_wireguard_name,
                    # Tuic协议
                    'tuic://': get_tuic_name,
                    # Hysteria2协议
                    'hy2://': get_hysteria2_name,
                    'hysteria2://': get_hysteria2_name
                }
                
                for prefix, handler_func in protocol_handlers.items():
                    if config.startswith(prefix):
                        name_to_check = handler_func(config)
                        break
                # 其他协议的名称提取支持

            # 如果无法获取名称，记录并跳过此配置
            if not name_to_check or not isinstance(name_to_check, str):
                logging.debug(f"无法从配置中提取有效名称，跳过: {config[:100]}...")
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

    # 详细统计信息日志，同时写入日志文件和控制台
    logging.info(f"处理统计:")
    logging.info(f"  - 成功处理页面: {processed_pages}/{len(fetched_pages)}")
    logging.info(f"  - 找到有效配置: {found_configs}")
    logging.info(f"  - 过滤无效配置: {filtered_out_configs}")
    logging.info(f"  - 过滤率: {filtered_out_configs/(found_configs+filtered_out_configs)*100:.1f}%" if (found_configs+filtered_out_configs) > 0 else "  - 无配置找到")
    

    # 准备输出目录结构
    country_dir = os.path.join(OUTPUT_DIR, COUNTRY_SUBDIR)
    protocol_dir = os.path.join(OUTPUT_DIR, PROTOCOL_SUBDIR)
    
    # 使用绝对路径
    abs_output_dir = os.path.abspath(OUTPUT_DIR)
    abs_country_dir = os.path.abspath(country_dir)
    abs_protocol_dir = os.path.abspath(protocol_dir)
    
    # 增强的目录处理逻辑
    try:
        # 1. 安全地删除旧目录（如果存在）
        def safe_rmtree(path):
            if os.path.exists(path):
                try:
                    shutil.rmtree(path)
                    return True
                except Exception as e:
                    logging.warning(f"无法删除目录 {path}: {e}")
                    return False
            return True
        
        # 先删除子目录，再删除父目录（如果需要）
        safe_rmtree(abs_country_dir)
        safe_rmtree(abs_protocol_dir)
        
        # 2. 确保父目录存在且可写
        parent_dir = os.path.dirname(abs_output_dir)
        if parent_dir and not os.path.exists(parent_dir):
            try:
                os.makedirs(parent_dir, exist_ok=True)
                logging.debug(f"创建父目录: {parent_dir}")
            except Exception as e:
                logging.error(f"无法创建父目录: {parent_dir}, 错误: {e}")
                return
        
        # 3. 检查写入权限
        if not os.access(parent_dir or os.getcwd(), os.W_OK):
            logging.error(f"错误: 没有写入权限: {parent_dir or os.getcwd()}")
            return
        
        # 4. 创建新的目录结构
        for dir_path in [abs_output_dir, abs_country_dir, abs_protocol_dir]:
            try:
                os.makedirs(dir_path, exist_ok=True)
                logging.debug(f"确保目录存在: {dir_path}")
            except Exception as e:
                logging.error(f"创建目录失败: {dir_path}, 错误: {e}")
                return
        
        # 5. 最终权限验证
        for dir_path in [abs_output_dir, abs_country_dir, abs_protocol_dir]:
            if not os.path.isdir(dir_path) or not os.access(dir_path, os.W_OK):
                logging.error(f"目录检查失败: {dir_path}")
                return
        
        logging.info(f"目录结构准备完成:")
        logging.info(f"  - 输出目录: {abs_output_dir}")
        logging.info(f"  - 国家目录: {abs_country_dir}")
        logging.info(f"  - 协议目录: {abs_protocol_dir}")
        
    except Exception as e:
        logging.error(f"目录处理失败: {e}")
        return
        
        # 创建测试文件验证写入权限
        test_file = os.path.join(abs_output_dir, "test_write.txt")
        try:
            with open(test_file, 'w') as f:
                f.write("测试写入权限")
            if os.path.exists(test_file):
                logging.info(f"✓ 写入测试成功")
                os.remove(test_file)  # 清理测试文件
            else:
                logging.error(f"✗ 写入测试失败: 文件未创建")
        except Exception as write_test_e:
            logging.error(f"✗ 写入测试失败: {str(write_test_e)}")

    # 保存协议配置文件
    protocol_counts = {}
    protocol_category_count = 0
    
    logging.info(f"开始保存协议配置文件到: {abs_protocol_dir}")
    
    # 预先过滤出非空协议类别
    non_empty_protocols = {cat: items for cat, items in final_all_protocols.items() if items}
    
    # 按协议类型排序，提高可预测性
    for category, items in sorted(non_empty_protocols.items()):
        items_count = len(items)
        logging.info(f"保存协议 {category}: {items_count} 个配置")
        
        saved, count = save_to_file(protocol_dir, category, items)
        if saved:
            protocol_counts[category] = count
            protocol_category_count += 1
        else:
            logging.error(f"保存协议 {category} 失败")
        
        # 内存优化：保存后清理大型集合
        final_all_protocols[category].clear()
    
    total_protocol_configs = sum(protocol_counts.values())
    logging.info(f"协议配置保存完成: 成功 {protocol_category_count}/{len(non_empty_protocols)} 个类别, 总计 {total_protocol_configs} 项")
    
    # 保存国家配置文件
    country_counts = {}
    countries_with_configs = 0
    total_country_configs = 0
    
    logging.info(f"开始保存国家配置文件到: {abs_country_dir}")
    
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
    logging.info(f"输出目录: {abs_output_dir}")
    logging.info(f"国家配置目录: {abs_country_dir}")
    logging.info(f"协议配置目录: {abs_protocol_dir}")
    logging.info(f"README文件已更新")

async def cleanup_tasks():
    """安全清理所有正在运行的异步任务，带超时和错误处理"""
    try:
        # 获取当前事件循环，处理可能的RuntimeError
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # 如果没有正在运行的循环，使用当前循环或创建新循环
            loop = asyncio.get_event_loop()
        
        # 获取所有任务并过滤出需要取消的任务
        tasks = asyncio.all_tasks(loop=loop)
        current_task = asyncio.current_task(loop=loop)
        
        # 只取消未完成且不是当前任务的任务
        tasks_to_cancel = [task for task in tasks if task != current_task and not task.done()]
        
        if not tasks_to_cancel:
            logging.debug("没有需要清理的异步任务")
            return
        
        logging.info(f"开始清理 {len(tasks_to_cancel)} 个正在运行的异步任务")
        
        # 取消所有需要清理的任务
        for task in tasks_to_cancel:
            task.cancel()
        
        # 使用带超时的gather，避免无限等待
        try:
            # 设置10秒超时，防止任务清理过程卡住
            results = await asyncio.wait_for(
                asyncio.gather(*tasks_to_cancel, return_exceptions=True),
                timeout=10.0
            )
            
            # 统计清理结果
            cancelled_count = sum(1 for r in results if isinstance(r, asyncio.CancelledError))
            error_count = sum(1 for r in results if isinstance(r, Exception) and not isinstance(r, asyncio.CancelledError))
            
            logging.info(f"任务清理完成: 已取消 {cancelled_count}, 错误 {error_count}")
            
            # 记录非取消的异常（如果有）
            for i, result in enumerate(results):
                if isinstance(result, Exception) and not isinstance(result, asyncio.CancelledError):
                    logging.debug(f"任务清理时发生异常: {result}")
                    
        except asyncio.TimeoutError:
            logging.warning("任务清理超时，可能有任务仍在运行")
        except Exception as e:
            logging.error(f"任务清理过程中发生错误: {e}")
            
    except Exception as e:
        logging.error(f"清理任务函数本身发生异常: {e}")

def safe_cleanup():
    """安全的资源清理函数，确保所有资源被正确释放"""
    try:
        logging.info("=== 开始资源清理 ===")
        
        # 1. 清理异步任务
        loop = None
        try:
            # 尝试获取或创建事件循环
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                # 没有现有循环，创建新循环
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                
            # 安全运行清理任务
            if loop.is_running():
                # 在运行中的循环使用call_soon_threadsafe
                logging.debug("在运行中的事件循环上调度清理任务")
                loop.call_soon_threadsafe(lambda: asyncio.create_task(cleanup_tasks()))
                
                # 给任务一些时间执行（非阻塞）
                import time
                time.sleep(0.1)  # 短暂延迟
            else:
                # 在未运行的循环上直接执行
                try:
                    logging.debug("在新事件循环上运行清理任务")
                    # 使用run_until_complete运行清理任务
                    loop.run_until_complete(cleanup_tasks())
                    
                    # 确保异步生成器被关闭
                    try:
                        loop.run_until_complete(loop.shutdown_asyncgens())
                    except AttributeError:
                        # Python 3.6及更早版本没有shutdown_asyncgens
                        pass
                        
                except Exception as e:
                    logging.error(f"运行清理任务时出错: {e}")
                    
                    logging.debug(f"清理任务错误详情: {traceback.format_exc()}")
                    
        except Exception as e:
            logging.error(f"事件循环操作时出错: {e}")
        finally:
            # 确保关闭循环
            try:
                if loop and not loop.is_closed():
                    loop.close()
                    logging.debug("事件循环已关闭")
            except Exception as e:
                logging.error(f"关闭事件循环时出错: {e}")
        
        # 2. 清理其他资源（如果有）
        # 可以在这里添加其他资源清理逻辑
        
        logging.info("=== 资源清理完成 ===")
        
    except Exception as e:
        logging.error(f"清理过程中发生未捕获的异常: {e}")

if __name__ == "__main__":
    start_time = time.time()
    
    try:
        # 导入必要的模块
        import sys
        import warnings
        
        # 捕获并记录警告
        warnings.filterwarnings('always')
        
        # 打印启动信息
        logging.info("=== V2Ray配置抓取工具启动 ===")
        logging.info(f"当前工作目录: {os.getcwd()}")
        logging.info(f"Python版本: {sys.version}")
        logging.info(f"脚本路径: {os.path.abspath(__file__)}")
        
        # 检查Python版本兼容性
        if sys.version_info < (3, 7):
            logging.warning("警告: 推荐使用Python 3.7或更高版本以获得最佳性能")
            
        # 运行主程序
        asyncio.run(main())
        
        # 计算执行时间
        execution_time = time.time() - start_time
        logging.info(f"程序成功完成！总执行时间: {execution_time:.2f} 秒")
        
    except KeyboardInterrupt:
        logging.info("⚠️  程序被用户中断")
    except asyncio.TimeoutError:
        logging.error("⏱️  程序执行超时")
        
        logging.debug(f"超时错误详情: {traceback.format_exc()}")
    except aiohttp.ClientError as e:
        logging.error(f"🌐 HTTP客户端错误: {str(e)}")
        
        logging.debug(f"HTTP错误详情: {traceback.format_exc()}")
    except ValueError as e:
        logging.error(f"📊 数据处理错误: {str(e)}")
        
        logging.debug(f"数据错误详情: {traceback.format_exc()}")
    except FileNotFoundError as e:
        logging.error(f"📁 文件未找到: {str(e)}")
        
        logging.debug(f"文件错误详情: {traceback.format_exc()}")
    except IOError as e:
        logging.error(f"💾 IO错误: {str(e)}")
        
        logging.debug(f"IO错误详情: {traceback.format_exc()}")
    except PermissionError as e:
        logging.error(f"🔒 权限错误: {str(e)}")
        
        logging.debug(f"权限错误详情: {traceback.format_exc()}")
    except OSError as e:
        logging.error(f"🖥️  操作系统错误: {str(e)}")
        
        logging.debug(f"OS错误详情: {traceback.format_exc()}")
    except Exception as e:
        logging.critical(f"❌ 程序执行出错: {e}")
        import traceback
        logging.debug(f"错误详细信息: {traceback.format_exc()}")
        
        # 提供用户友好的错误信息
        print("\n程序发生错误。请尝试以下解决方法:")
        print("1. 检查网络连接是否正常")
        print("2. 确保有足够的磁盘空间和文件权限")
        print("3. 更新依赖库: pip install -r requirements.txt")
        print("4. 查看日志文件获取详细信息")
    finally:
        # 计算总执行时间
        total_time = time.time() - start_time
        logging.info(f"=== 程序结束 === 总运行时间: {total_time:.2f} 秒")
        
        # 执行安全清理
        safe_cleanup()
