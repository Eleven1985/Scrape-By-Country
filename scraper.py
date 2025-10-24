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

# --- 协议类别 ---
PROTOCOL_CATEGORIES = [
    "Vmess", "Vless", "Trojan", "ShadowSocks", "ShadowSocksR",
    "Tuic", "Hysteria2", "WireGuard"
]
# 定义正确的协议前缀映射
PROTOCOL_PREFIXES = [
    "vmess://", "vless://", "trojan://", "ss://", "ssr://",
    "tuic://", "hy2://", "hysteria2://", "wireguard://"
]

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
    
    # 检查是否包含过滤短语
    if FILTERED_PHRASE in config.lower():
        logging.debug(f"配置因包含过滤短语 '{FILTERED_PHRASE}' 被过滤: {config[:100]}...")
        return True
    
    # 检查URL编码情况
    percent25_count = config.count('%25')
    if percent25_count >= MIN_PERCENT25_COUNT * 2:  # 提高阈值以减少误判
        logging.debug(f"配置因过度URL编码 ({percent25_count}个%25) 被过滤: {config[:100]}...")
        return True
    
    # 检查配置长度
    if len(config) >= MAX_CONFIG_LENGTH * 2:  # 提高阈值以减少误判
        logging.debug(f"配置因过长 ({len(config)}字符) 被过滤")
        return True
    
    # 基本的有效性检查：确保配置包含协议前缀
    has_valid_protocol = False
    found_protocol = None
    for protocol_prefix in PROTOCOL_PREFIXES:
        if protocol_prefix in config.lower():
            has_valid_protocol = True
            found_protocol = protocol_prefix
            break
    
    if not has_valid_protocol:
        logging.debug(f"配置因缺少有效协议前缀被过滤: {config[:100]}...")
        return True
    
    # 对不同协议进行基本格式验证
    if found_protocol == 'vmess://' and not config[8:].strip():  # 确保有内容在协议前缀后
        return True
    elif found_protocol == 'vless://' and not config[8:].strip():
        return True
    elif found_protocol == 'trojan://' and '@' not in config:  # Trojan格式必须包含@
        return True
    elif found_protocol == 'ss://' and not config[5:].strip():
        return True
    elif found_protocol == 'ssr://' and not config[6:].strip():
        return True
    elif found_protocol == 'tuic://' and not config[7:].strip():
        return True
    elif found_protocol == 'hy2://' and not config[5:].strip():
        return True
    elif found_protocol == 'hysteria2://' and not config[12:].strip():
        return True
    elif found_protocol == 'wireguard://' and not config[12:].strip():
        return True
    
    return False

async def fetch_url(session, url, max_retries=2):
    """异步获取URL内容并提取文本，支持重试机制"""
    # 验证URL格式
    if not url.startswith(('http://', 'https://')):
        logging.warning(f"无效的URL格式: {url}")
        return url, None
    
    retry_count = 0
    last_exception = None
    
    # 使用头部模拟浏览器请求，避免被阻止
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    while retry_count <= max_retries:
        try:
            async with session.get(url, timeout=REQUEST_TIMEOUT, headers=headers) as response:
                response.raise_for_status()
                
                # 检查内容长度，避免过大的响应
                content_length = response.headers.get('Content-Length')
                if content_length and int(content_length) > MAX_PAGE_SIZE:
                    logging.warning(f"页面过大 (>{MAX_PAGE_SIZE/1024/1024:.1f}MB), 跳过: {url}")
                    return url, None
                
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
                    
                    # 再次检查内容大小
                    if len(html) > MAX_PAGE_SIZE:
                        logging.warning(f"页面内容过大 (>{MAX_PAGE_SIZE/1024/1024:.1f}MB), 跳过详细处理: {url}")
                        return url, None
                    
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
            last_exception = "请求超时"
            logging.warning(f"获取URL超时: {url}, 第{retry_count+1}次尝试")
        except aiohttp.ClientError as e:
            last_exception = f"客户端错误: {str(e)}"
            logging.warning(f"获取URL客户端错误: {url}, 错误: {str(e)}, 第{retry_count+1}次尝试")
        except Exception as e:
            last_exception = f"未知错误: {str(e)}"
            logging.warning(f"获取URL时出错: {url}, 错误: {str(e)}, 第{retry_count+1}次尝试")
        
        retry_count += 1
        # 只有在还没达到最大重试次数时才延迟
        if retry_count <= max_retries:
            delay = min(2 ** retry_count, 10)  # 指数退避策略，最多等待10秒
            logging.info(f"将在{delay}秒后重试获取URL: {url}")
            await asyncio.sleep(delay)
    
    logging.error(f"在{max_retries+1}次尝试后获取URL失败: {url}, 最后错误: {last_exception}")
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
                for item in sorted(list(items_set))[:10]:  # 只写入少量内容用于测试
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
        # 使用相对路径，避免README中的绝对路径问题
        protocol_base_url = f"{PROTOCOL_SUBDIR}"
        country_base_url = f"{COUNTRY_SUBDIR}"
        logging.debug(f"README使用本地相对路径: protocols={protocol_base_url}, countries={country_base_url}")
    else:
        # 保留GitHub远程路径支持作为备用
        github_repo_path = "miladtahanian/V2RayScrapeByCountry"
        github_branch = "main"
        protocol_base_url = f"https://raw.githubusercontent.com/{github_repo_path}/refs/heads/{github_branch}/{OUTPUT_DIR}/{PROTOCOL_SUBDIR}"
        country_base_url = f"https://raw.githubusercontent.com/{github_repo_path}/refs/heads/{github_branch}/{OUTPUT_DIR}/{COUNTRY_SUBDIR}"
        logging.debug(f"README使用GitHub路径: protocols={protocol_base_url}, countries={country_base_url}")

    md_content = f"# 📊 提取结果 (最后更新: {timestamp})\n\n"
    md_content += "此文件是自动生成的。\n\n"
    md_content += f"## 📋 统计概览\n\n"
    md_content += f"- **配置总数**: {total_protocol_configs}\n"
    md_content += f"- **有数据的协议数**: {protocols_with_data}\n"
    md_content += f"- **国家相关配置数**: {total_country_configs}\n"
    md_content += f"- **有配置的国家数**: {countries_with_data}\n\n"
    
    md_content += "## ℹ️ 说明\n\n"
    md_content += "国家文件仅包含在**配置名称**中找到国家名称/旗帜的配置。配置名称首先从链接的`#`部分提取，如果不存在，则从内部名称(对于Vmess/SSR)提取。\n\n"
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
            
            # 原始名称 (键)，为所有国家添加中文标识
            display_name = country_category_name
            
            # 国家英文名到中文名的映射
            country_chinese_names = {
                "Canada": "Canada（加拿大）",
                "China": "China（中国）",
                "Finland": "Finland（芬兰）",
                "France": "France（法国）",
                "Germany": "Germany（德国）",
                "Iran": "Iran（伊朗）",
                "Ireland": "Ireland（爱尔兰）",
                "Israel": "Israel（以色列）",
                "Japan": "Japan（日本）",
                "Luxembourg": "Luxembourg（卢森堡）",
                "Poland": "Poland（波兰）",
                "Portugal": "Portugal（葡萄牙）",
                "Russia": "Russia（俄罗斯）",
                "Singapore": "Singapore（新加坡）",
                "SouthKorea": "SouthKorea（韩国）",
                "Spain": "Spain（西班牙）",
                "Switzerland": "Switzerland（瑞士）",
                "Taiwan": "Taiwan（台湾）",
                "UK": "UK（英国）",
                "USA": "USA（美国）"
            }
            
            # 查找对应的中文名称
            if country_category_name in country_chinese_names:
                display_name = country_chinese_names[country_category_name]
                
            display_parts.append(display_name)
            
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
    logging.info(f"日志文件已创建: {log_file_path}")
    # 确保配置文件夹存在
    try:
        os.makedirs(CONFIG_DIR, exist_ok=True)
        logging.info(f"配置文件夹: {CONFIG_DIR}")
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
            try:
                return await fetch_url(session, url_to_fetch)
            except Exception as e:
                logging.error(f"URL获取任务异常: {url_to_fetch}, 错误: {e}")
                return url_to_fetch, None
    
    # 添加URL去重
    unique_urls = list(set(urls))
    if len(unique_urls) < len(urls):
        logging.info(f"去重前URL数量: {len(urls)}, 去重后: {len(unique_urls)}")
        urls = unique_urls
    
    # 创建HTTP会话并批处理URL请求
    async with aiohttp.ClientSession() as session:
        logging.info(f"开始获取 {len(urls)} 个URLs (最大并发: {CONCURRENT_REQUESTS})...")
        
        # 批量处理URL，控制并发数量
        batch_size = 10
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
        
        # 检查总配置数是否超过限制，防止内存溢出
        total_current_configs = sum(len(configs) for configs in final_all_protocols.values())
        if total_current_configs >= MAX_TOTAL_CONFIGS:
            logging.warning(f"已达到最大配置数限制 ({MAX_TOTAL_CONFIGS})，停止处理新配置")
            break
            
        for protocol_cat_name, configs_found in page_protocol_matches.items():
            if protocol_cat_name in PROTOCOL_CATEGORIES:
                for config in configs_found:
                    # 检查总配置数是否超过限制
                    if sum(len(configs) for configs in final_all_protocols.values()) >= MAX_TOTAL_CONFIGS:
                        break
                        
                    if not should_filter_config(config):
                        all_page_configs_after_filter.add(config)
                        final_all_protocols[protocol_cat_name].add(config)
                    else:
                        page_filtered_count += 1
            if sum(len(configs) for configs in final_all_protocols.values()) >= MAX_TOTAL_CONFIGS:
                break
        
        found_configs += len(all_page_configs_after_filter)
        filtered_out_configs += page_filtered_count
        
        # 每10个页面输出一次进度
        if processed_pages % 10 == 0:
            logging.info(f"处理进度: {processed_pages}/{len(fetched_pages)} 页面, " \
                      f"已找到 {found_configs} 配置, 已过滤 {filtered_out_configs} 配置")

        # 使用集合进行配置去重
        unique_configs = list(dict.fromkeys(all_page_configs_after_filter))
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
                    'ssr://': get_ssr_name,
                    'vmess://': get_vmess_name,
                    'trojan://': get_trojan_name,
                    'vless://': get_vless_name,
                    'ss://': get_shadowsocks_name,
                    'tuic://': get_tuic_name,
                    'hy2://': get_hysteria2_name,
                    'hysteria2://': get_hysteria2_name,
                    'wireguard://': get_wireguard_name,
                    'wg://': get_wireguard_name
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
        # 使用绝对路径创建目录
        abs_output_dir = os.path.abspath(OUTPUT_DIR)
        abs_country_dir = os.path.abspath(country_dir)
        abs_protocol_dir = os.path.abspath(protocol_dir)
        
        os.makedirs(abs_output_dir, exist_ok=True)
        os.makedirs(abs_country_dir, exist_ok=True)
        os.makedirs(abs_protocol_dir, exist_ok=True)
        
        # 验证目录是否创建成功并可写
        for dir_path in [abs_output_dir, abs_country_dir, abs_protocol_dir]:
            if os.path.exists(dir_path):
                writable = os.access(dir_path, os.W_OK)
                logging.info(f"目录检查: {dir_path} {'(可写)' if writable else '(不可写)'}")
                if not writable:
                    logging.error(f"目录不可写: {dir_path}")
                    # 尝试修复权限问题
                    try:
                        # 仅在Windows系统尝试更改权限
                        if os.name == 'nt':  # Windows系统
                            logging.warning(f"Windows系统检测到目录不可写，尝试继续执行")
                        else:  # Linux/Unix系统
                            os.chmod(dir_path, 0o755)
                            logging.warning(f"已尝试修改目录权限: {dir_path}")
                    except Exception as perm_e:
                        logging.error(f"修改目录权限失败: {perm_e}")
            else:
                logging.error(f"目录创建失败: {dir_path}")
        
        logging.info(f"输出目录已准备就绪: {abs_output_dir}")
        
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
            
    except (PermissionError, OSError) as e:
        logging.critical(f"无法创建输出目录: {e}")
        return

    # 保存协议配置文件
    protocol_counts = {}
    protocol_category_count = 0
    
    logging.info(f"开始保存协议配置文件")
    
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

async def cleanup_tasks():
    """清理所有正在运行的异步任务"""
    tasks = asyncio.all_tasks()
    current_task = asyncio.current_task()
    tasks_to_cancel = [task for task in tasks if task != current_task and not task.done()]
    
    if tasks_to_cancel:
        logging.info(f"清理 {len(tasks_to_cancel)} 个正在运行的异步任务")
        for task in tasks_to_cancel:
            task.cancel()
        try:
            await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
        except Exception:
            pass

if __name__ == "__main__":
    try:
        logging.info("=== V2Ray配置抓取工具开始运行 ===")
        logging.info(f"当前工作目录: {os.getcwd()}")
        logging.info(f"Python版本: {os.sys.version}")
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("程序被用户中断")
    except asyncio.TimeoutError:
        logging.error("程序执行超时")
    except aiohttp.ClientError as e:
        logging.error(f"HTTP客户端错误: {str(e)}")
        import traceback
        logging.debug(f"错误详细信息: {traceback.format_exc()}")
    except ValueError as e:
        logging.error(f"数据处理错误: {str(e)}")
        import traceback
        logging.debug(f"错误详细信息: {traceback.format_exc()}")
    except FileNotFoundError as e:
        logging.error(f"文件未找到: {str(e)}")
    except IOError as e:
        logging.error(f"IO错误: {str(e)}")
    except Exception as e:
        logging.critical(f"程序执行出错: {e}")
        import traceback
        logging.debug(f"错误详细信息: {traceback.format_exc()}")
    finally:
        logging.info("=== 程序结束 ===")
        # 清理异步任务
        try:
            if asyncio.get_event_loop().is_running():
                asyncio.create_task(cleanup_tasks())
            else:
                asyncio.run(cleanup_tasks())
        except Exception as cleanup_e:
            logging.warning(f"清理异步任务时出错: {cleanup_e}")
        
        # 确保所有日志都被写入文件
        for handler in logging.handlers:
            try:
                handler.flush()
                handler.close()
            except Exception as handler_e:
                print(f"关闭日志处理器时出错: {handler_e}")  # 避免在日志关闭时再记录日志
