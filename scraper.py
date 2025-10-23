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
URLS_FILE = 'urls.txt'
KEYWORDS_FILE = 'keywords.json' # 应包含国家的两字母代码
OUTPUT_DIR = 'output_configs'
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
    """检查文本是否包含非英语字符（主要是波斯语等）"""
    if not isinstance(text, str) or not text.strip():
        return False
    has_non_latin_char = False
    has_latin_char = False
    for char in text:
        if '\u0600' <= char <= '\u06FF' or char in ['\u200C', '\u200D']: # 非拉丁字符范围和零宽连接符
            has_non_latin_char = True
        elif 'a' <= char.lower() <= 'z':
            has_latin_char = True
    return has_non_latin_char and not has_latin_char

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
def get_vmess_name(vmess_link):
    """从Vmess链接中提取配置名称"""
    if not vmess_link or not vmess_link.startswith("vmess://"):
        return None
        
    try:
        b64_part = vmess_link[8:]  # 移除"vmess://"前缀
        decoded_str = decode_base64(b64_part)
        
        if decoded_str:
            try:
                vmess_json = json.loads(decoded_str)
                # 尝试从不同可能的字段获取名称
                return vmess_json.get('ps') or vmess_json.get('name') or vmess_json.get('remarks')
            except json.JSONDecodeError:
                logging.warning(f"Vmess链接解码后的内容不是有效的JSON: {vmess_link[:30]}...")
    except Exception as e:
        logging.debug(f"解析Vmess名称失败: {vmess_link[:30]}...: {e}")  # 使用debug级别减少日志噪音
        
    return None

def get_ssr_name(ssr_link):
    """从SSR链接中提取配置名称"""
    if not ssr_link or not ssr_link.startswith("ssr://"):
        return None
        
    try:
        b64_part = ssr_link[6:]  # 移除"ssr://"前缀
        decoded_str = decode_base64(b64_part)
        
        if not decoded_str:
            return None
            
        # SSR链接格式: server:port:protocol:method:obfs:password/?params
        parts = decoded_str.split('/?')
        if len(parts) < 2:
            return None
            
        params_str = parts[1]
        try:
            params = parse_qs(params_str)
            if 'remarks' in params and params['remarks']:
                remarks_b64 = params['remarks'][0]
                # SSR的remarks参数本身也是base64编码的
                return decode_base64(remarks_b64)
        except Exception as e:
            logging.debug(f"解析SSR参数失败: {e}")
            
    except Exception as e:
        logging.debug(f"解析SSR名称失败: {ssr_link[:30]}...: {e}")  # 使用debug级别减少日志噪音
        
    return None

# --- New Filter Function ---
def should_filter_config(config):
    """根据特定规则过滤无效或低质量的配置"""
    if not config or not isinstance(config, str):
        return True
    
    # 检查是否包含过滤短语
    if FILTERED_PHRASE in config.lower():
        return True
    
    # 检查过度URL编码
    percent25_count = config.count('%25')
    if percent25_count >= MIN_PERCENT25_COUNT or '%2525' in config:
        return True
    
    # 检查配置长度
    if len(config) >= MAX_CONFIG_LENGTH:
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
        
    # 只初始化有模式的类别，节省内存
    matches = {}
    
    for category, patterns in categories_data.items():
        # 只处理非空的模式列表
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
                    
                    if found:
                        # 清理并去重匹配结果
                        for item in found:
                            if item and isinstance(item, str):
                                cleaned_item = item.strip()
                                if cleaned_item:
                                    category_matches.add(cleaned_item)
                                    # 如果匹配项数量过大，限制以避免内存问题
                                    if len(category_matches) > 10000:
                                        logging.warning(f"类别 {category} 的匹配项超过10000，可能会导致内存问题")
                                        break
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
def generate_simple_readme(protocol_counts, country_counts, all_keywords_data, github_repo_path="miladtahanian/V2RayScrapeByCountry", github_branch="main"):
    """生成README.md文件，展示抓取结果统计信息"""
    tz = pytz.timezone('Asia/Shanghai')
    now = datetime.now(tz)
    timestamp = now.strftime("%Y-%m-%d %H:%M:%S %Z")
    
    # 计算统计信息
    total_protocol_configs = sum(protocol_counts.values())
    total_country_configs = sum(country_counts.values())
    countries_with_data = len(country_counts)
    protocols_with_data = len(protocol_counts)

    raw_github_base_url = f"https://raw.githubusercontent.com/{github_repo_path}/refs/heads/{github_branch}/{OUTPUT_DIR}"

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

    md_content += "## 📁 协议文件\n\n"
    if protocol_counts:
        md_content += "| 协议 | 总数 | 链接 |\n"
        md_content += "|---|---|---|\n"
        for category_name, count in sorted(protocol_counts.items()):
            file_link = f"{raw_github_base_url}/{category_name}.txt"
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
            foreign_name_str = "" # 外语名称
            iso_code_original_case = "" # 用于保存来自JSON文件的原始大小写ISO代码

            if country_category_name in all_keywords_data:
                keywords_list = all_keywords_data[country_category_name]
                if keywords_list and isinstance(keywords_list, list):
                    # 1. 查找国家的两字母ISO代码用于旗帜图像URL
                    iso_code_lowercase_for_url = ""
                    for item in keywords_list:
                        if isinstance(item, str) and len(item) == 2 and item.isupper() and item.isalpha():
                            iso_code_lowercase_for_url = item.lower()
                            iso_code_original_case = item # 保存原始大小写的代码
                            break 
                    
                    if iso_code_lowercase_for_url:
                        # 使用flagcdn.com，宽度为20像素
                        flag_image_url = f"https://flagcdn.com/w20/{iso_code_lowercase_for_url}.png"
                        flag_image_markdown = f'<img src="{flag_image_url}" width="20" alt="{country_category_name} flag">'
                    
                    # 2. 提取外语名称
                    for item in keywords_list:
                        if isinstance(item, str):
                            # 忽略ISO代码(用于旗帜的那个)
                            if iso_code_original_case and item == iso_code_original_case:
                                continue
                            # 忽略国家的原始名称(JSON键)
                            if item.lower() == country_category_name.lower() and not is_non_english_text(item):
                                continue
                            # 忽略其他未被选为ISO代码的大写两或三字母代码
                            if len(item) in [2,3] and item.isupper() and item.isalpha() and item != iso_code_original_case:
                                continue
                            
                            # 如果是非英语文本
                            if is_non_english_text(item):
                                foreign_name_str = item
                                break 
            
            # 3. 为"国家"列构建最终文本
            display_parts = []
            # 如果旗帜图像标签已创建
            if flag_image_markdown:
                display_parts.append(flag_image_markdown)
            
            display_parts.append(country_category_name) # 原始名称 (键)

            if foreign_name_str:
                display_parts.append(f"({foreign_name_str})")
            
            country_display_text = " ".join(display_parts)
            
            file_link = f"{raw_github_base_url}/{country_category_name}.txt"
            link_text = f"{country_category_name}.txt"
            md_content += f"| {country_display_text} | {count} | [`{link_text}`]({file_link}) |\n"
    else:
        md_content += "没有找到与国家相关的配置。\n"
    md_content += "\n"

    try:
        with open(README_FILE, 'w', encoding='utf-8') as f:
            f.write(md_content)
        logging.info(f"Successfully generated {README_FILE}")
    except Exception as e:
        logging.error(f"Failed to write {README_FILE}: {e}")

# main函数和其他函数实现
async def main():
    """主函数，协调整个抓取和处理流程"""
    # 检查必要的输入文件是否存在
    if not os.path.exists(URLS_FILE) or not os.path.exists(KEYWORDS_FILE):
        logging.critical("未找到输入文件。")
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
                # 3. 可以扩展支持更多协议格式的名称提取
                # 例如trojan, vless等

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
                    
                # 准备此国家的文本关键词
                text_keywords_for_country = []
                try:
                    for kw in keywords_for_country_list:
                        if isinstance(kw, str):
                            # 过滤条件：不是非字母数字的短代码（可能是表情符号）
                            is_potential_emoji_or_short_code = (1 <= len(kw) <= 7) and not kw.isalnum()
                            if not is_potential_emoji_or_short_code:
                                # 只添加非外语字符串，或与国家名相同的字符串
                                if not is_non_english_text(kw) or kw.lower() == country_name_key.lower():
                                    if kw not in text_keywords_for_country:
                                        text_keywords_for_country.append(kw)
                except Exception as e:
                    logging.debug(f"处理国家关键词时出错 {country_name_key}: {e}")
                
                # 检查是否匹配任何关键词
                match_found = False
                current_name_lower = current_name_to_check_str.lower()
                
                for keyword in text_keywords_for_country:
                    if not isinstance(keyword, str):
                        continue
                        
                    # 对缩写使用单词边界匹配，对普通词使用包含匹配
                    is_abbr = (len(keyword) in [2, 3]) and keyword.isupper() and keyword.isalpha()
                    
                    if is_abbr:
                        # 对于缩写，使用单词边界确保精确匹配
                        try:
                            pattern = r'\b' + re.escape(keyword) + r'\b'
                            if re.search(pattern, current_name_to_check_str, re.IGNORECASE):
                                match_found = True
                                break
                        except Exception as e:
                            logging.debug(f"正则表达式匹配失败 {keyword}: {e}")
                    else:
                        # 对于普通关键词，使用不区分大小写的包含检查（已预先计算小写版本提高性能）
                        if keyword.lower() in current_name_lower:
                            match_found = True
                            break
                
                if match_found:
                    final_configs_by_country[country_name_key].add(config)
                    country_matched = True
                    break  # 一个配置只关联到一个国家
                
            if country_matched:
                break

    # 统计信息日志
    logging.info(f"成功处理 {processed_pages}/{len(fetched_pages)} 个页面，找到 {found_configs} 个有效配置，过滤掉 {filtered_out_configs} 个无效配置")
    
    # 准备输出目录
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
    
    # 确保输出目录存在
    try:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        logging.info(f"正在保存文件到目录: {OUTPUT_DIR}")
    except (PermissionError, OSError) as e:
        logging.critical(f"无法创建输出目录 {OUTPUT_DIR}: {e}")
        return

    # 保存协议配置文件
    protocol_counts = {}
    for category, items in final_all_protocols.items():
        if items:  # 只保存非空集合
            saved, count = save_to_file(OUTPUT_DIR, category, items)
            if saved:
                protocol_counts[category] = count
    
    # 保存国家配置文件
    country_counts = {}
    countries_with_configs = 0
    total_country_configs = 0
    
    for category, items in final_configs_by_country.items():
        if items:  # 只保存非空集合
            saved, count = save_to_file(OUTPUT_DIR, category, items)
            if saved:
                country_counts[category] = count
                countries_with_configs += 1
                total_country_configs += count
    
    # 生成README文件
    try:
        generate_simple_readme(protocol_counts, country_counts, categories_data,
                               github_repo_path="miladtahanian/V2RayScrapeByCountry",
                               github_branch="main")
    except Exception as e:
        logging.error(f"生成README文件时出错: {e}")
        # 继续执行，不中断程序
    
    # 输出完成信息
    logging.info(f"=== 抓取完成 ===")
    logging.info(f"找到并保存的协议配置: {sum(protocol_counts.values())}")
    logging.info(f"有配置的国家数量: {countries_with_configs}")
    logging.info(f"国家相关配置总数: {total_country_configs}")
    logging.info(f"输出目录: {OUTPUT_DIR}")
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
