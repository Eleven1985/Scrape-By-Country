"""把分享链接（vmess/vless/trojan/ss/ssr/hysteria2/tuic）转换成 Clash 与 sing-box 配置。

只做纯函数转换，不依赖网络。无法解析的链接统一返回 None，由调用方统计丢弃。
"""

import base64
import json
import re
from urllib.parse import parse_qs, unquote, urlsplit

# 合法的 Shadowsocks 加密方式；不在表内的凭据说明这条 ss:// 是错标的
SS_CIPHERS = {
    'aes-128-gcm', 'aes-192-gcm', 'aes-256-gcm',
    'aes-128-cfb', 'aes-192-cfb', 'aes-256-cfb',
    'aes-128-ctr', 'aes-192-ctr', 'aes-256-ctr',
    'chacha20-ietf-poly1305', 'xchacha20-ietf-poly1305', 'chacha20-poly1305',
    'chacha20', 'chacha20-ietf', 'xchacha20',
    'rc4-md5', 'rc4', 'bf-cfb', 'camellia-128-cfb', 'camellia-192-cfb',
    'camellia-256-cfb', 'salsa20', 'none', 'plain',
    '2022-blake3-aes-128-gcm', '2022-blake3-aes-256-gcm',
    '2022-blake3-chacha20-poly1305',
}

SUPPORTED_TYPES = ('vmess', 'vless', 'trojan', 'ss', 'ssr', 'hysteria2', 'tuic')


def _b64decode(data):
    if not data:
        return ''
    data = re.sub(r'\s+', '', data)
    data = data.replace('-', '+').replace('_', '/')
    data += '=' * (-len(data) % 4)
    try:
        return base64.b64decode(data, validate=False).decode('utf-8', 'ignore')
    except Exception:
        return ''


def _as_int(value, default=0):
    try:
        number = int(str(value).strip())
    except (TypeError, ValueError):
        return default
    return number if 0 < number < 65536 else default


def _first(mapping, *keys, default=''):
    for key in keys:
        value = mapping.get(key)
        if isinstance(value, list):
            value = value[0] if value else ''
        if value:
            return str(value).strip()
    return default


def _alpn_list(value):
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    if isinstance(value, str) and value.strip():
        return [v.strip() for v in value.split(',') if v.strip()]
    return []


def _truthy(value):
    return str(value).strip().lower() in ('1', 'true', 'yes', 'on')


def _name_from_fragment(fragment):
    return unquote(fragment or '').strip()


def _query_dict(parts):
    return {k: v[0] for k, v in parse_qs(parts.query).items()}


def _base_node(node_type, name, server, port):
    if not server or not port:
        return None
    return {'type': node_type, 'name': name, 'server': server, 'port': port,
            'tls': False, 'network': 'tcp', 'insecure': False}


# --------------------------------------------------------------------------
# 各协议解析
# --------------------------------------------------------------------------

def _parse_vmess(uri):
    body = uri[len('vmess://'):]
    fragment = ''
    if '#' in body:
        body, fragment = body.split('#', 1)
    raw = _b64decode(body)
    if not raw:
        return None
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(data, dict):
        return None

    server = str(data.get('add') or '').strip().strip('[]')
    port = _as_int(data.get('port'))
    node = _base_node('vmess', str(data.get('ps') or _name_from_fragment(fragment)).strip(),
                      server, port)
    if not node or not data.get('id'):
        return None

    node['uuid'] = str(data.get('id')).strip()
    node['alter_id'] = _as_int(data.get('aid'), 0)
    node['cipher'] = str(data.get('scy') or 'auto').strip() or 'auto'
    node['network'] = str(data.get('net') or 'tcp').strip().lower() or 'tcp'
    node['path'] = str(data.get('path') or '').strip()
    node['host'] = str(data.get('host') or '').strip()
    node['service_name'] = node['path'] if node['network'] == 'grpc' else ''
    node['tls'] = str(data.get('tls') or '').strip().lower() in ('tls', 'reality', 'true')
    node['sni'] = str(data.get('sni') or node['host'] or '').strip()
    node['alpn'] = _alpn_list(data.get('alpn'))
    node['fp'] = str(data.get('fp') or '').strip()
    return node


def _parse_vless(uri):
    parts = urlsplit(uri)
    node = _base_node('vless', _name_from_fragment(parts.fragment),
                      (parts.hostname or '').strip('[]'), _as_int(parts.port))
    if not node or not parts.username:
        return None

    query = _query_dict(parts)
    node['uuid'] = unquote(parts.username).strip()
    node['flow'] = _first(query, 'flow')
    node['network'] = (_first(query, 'type') or 'tcp').lower()
    node['path'] = _first(query, 'path') or '/'
    node['host'] = _first(query, 'host')
    node['service_name'] = _first(query, 'serviceName', 'servicename') or (
        node['path'] if node['network'] == 'grpc' else '')
    node['sni'] = _first(query, 'sni', 'peer') or node['host']
    node['fp'] = _first(query, 'fp')
    node['alpn'] = _alpn_list(_first(query, 'alpn'))
    node['insecure'] = _truthy(_first(query, 'allowInsecure', 'insecure'))
    security = (_first(query, 'security') or 'none').lower()
    node['tls'] = security in ('tls', 'reality', 'xtls')
    node['reality_pbk'] = _first(query, 'pbk')
    node['reality_sid'] = _first(query, 'sid')
    if node['reality_pbk']:
        node['tls'] = True
    return node


def _parse_trojan(uri):
    parts = urlsplit(uri)
    node = _base_node('trojan', _name_from_fragment(parts.fragment),
                      (parts.hostname or '').strip('[]'), _as_int(parts.port))
    if not node or not parts.username:
        return None

    query = _query_dict(parts)
    # 密码里可能含冒号，urlsplit 会把它拆成 username/password，这里拼回去
    password = unquote(parts.username)
    if parts.password:
        password = f"{password}:{unquote(parts.password)}"
    node['password'] = password.strip()
    node['network'] = (_first(query, 'type') or 'tcp').lower()
    node['path'] = _first(query, 'path') or '/'
    node['host'] = _first(query, 'host')
    node['service_name'] = _first(query, 'serviceName') or (
        node['path'] if node['network'] == 'grpc' else '')
    node['sni'] = _first(query, 'sni', 'peer') or node['host']
    node['fp'] = _first(query, 'fp')
    node['alpn'] = _alpn_list(_first(query, 'alpn'))
    node['insecure'] = _truthy(_first(query, 'allowInsecure', 'insecure'))
    node['tls'] = (_first(query, 'security') or 'tls').lower() != 'none'
    return node


def _parse_hysteria2(uri):
    if uri.startswith('hy2://'):
        uri = 'hysteria2://' + uri[len('hy2://'):]
    parts = urlsplit(uri)
    node = _base_node('hysteria2', _name_from_fragment(parts.fragment),
                      (parts.hostname or '').strip('[]'), _as_int(parts.port))
    if not node:
        return None

    query = _query_dict(parts)
    password = unquote(parts.username or '')
    if parts.password:
        password = f"{password}:{unquote(parts.password)}"
    if not password:
        return None

    node['password'] = password
    node['sni'] = _first(query, 'sni', 'peer')
    node['insecure'] = _truthy(_first(query, 'insecure', 'allowInsecure'))
    node['obfs'] = _first(query, 'obfs')
    node['obfs_password'] = _first(query, 'obfs-password', 'obfs_password')
    node['alpn'] = _alpn_list(_first(query, 'alpn'))
    node['tls'] = True
    return node


def _parse_tuic(uri):
    parts = urlsplit(uri)
    node = _base_node('tuic', _name_from_fragment(parts.fragment),
                      (parts.hostname or '').strip('[]'), _as_int(parts.port))
    if not node or not parts.username:
        return None

    query = _query_dict(parts)
    userinfo = unquote(parts.username)
    if ':' in userinfo:
        node['uuid'], node['password'] = userinfo.split(':', 1)
    else:
        node['uuid'] = userinfo
        node['password'] = unquote(parts.password or '')
    node['sni'] = _first(query, 'sni', 'peer')
    node['alpn'] = _alpn_list(_first(query, 'alpn'))
    node['insecure'] = _truthy(_first(query, 'insecure', 'allowInsecure'))
    node['congestion_control'] = _first(query, 'congestion_control', 'congestion-controller')
    node['udp_relay_mode'] = _first(query, 'udp_relay_mode', 'udp-relay-mode')
    node['tls'] = True
    return node


def _parse_ss(uri):
    body = uri[len('ss://'):]
    name = ''
    if '#' in body:
        body, fragment = body.split('#', 1)
        name = _name_from_fragment(fragment)

    query = {}
    if '?' in body:
        body, query_string = body.split('?', 1)
        query = {k: v[0] for k, v in parse_qs(query_string).items()}

    if '@' in body:
        userinfo, hostport = body.rsplit('@', 1)
        if ':' in userinfo:
            method, password = userinfo.split(':', 1)
            method, password = unquote(method), unquote(password)
        else:
            decoded = _b64decode(userinfo)
            if ':' not in decoded:
                return None
            method, password = decoded.split(':', 1)
    else:
        decoded = _b64decode(body)
        if '@' not in decoded:
            return None
        userinfo, hostport = decoded.rsplit('@', 1)
        if ':' not in userinfo:
            return None
        method, password = userinfo.split(':', 1)

    if ':' not in hostport:
        return None
    server, port_text = hostport.rsplit(':', 1)
    server = server.strip().strip('[]')
    port = _as_int(port_text)
    node = _base_node('ss', name, server, port)
    if not node:
        return None

    method = method.strip().lower()
    if method not in SS_CIPHERS:
        return None

    node['method'] = method
    node['password'] = password
    node['plugin'] = query.get('plugin', '')
    node['plugin_opts'] = query.get('plugin_opts') or query.get('plugin-opts') or ''
    return node


def _parse_ssr(uri):
    raw = _b64decode(uri[len('ssr://'):])
    if not raw:
        return None

    main, _, query_string = raw.partition('/?')
    tail = main.rsplit(':', 5)
    if len(tail) != 6:
        return None
    server, port_text, protocol, method, obfs, password_b64 = tail

    port = _as_int(port_text)
    server = server.strip().strip('[]')
    node = _base_node('ssr', '', server, port)
    if not node:
        return None

    method = method.strip().lower()
    if method not in SS_CIPHERS:
        return None

    query = {}
    if query_string:
        query = {k: _b64decode(v[0]) for k, v in parse_qs(query_string).items()}

    node['method'] = method
    node['password'] = _b64decode(password_b64)
    node['protocol'] = protocol.strip() or 'origin'
    node['protocol_param'] = query.get('protoparam', '')
    node['obfs'] = obfs.strip() or 'plain'
    node['obfs_param'] = query.get('obfsparam', '')
    node['name'] = query.get('remarks', '')
    return node


_PARSERS = {
    'vmess': _parse_vmess,
    'vless': _parse_vless,
    'trojan': _parse_trojan,
    'ss': _parse_ss,
    'ssr': _parse_ssr,
    'hysteria2': _parse_hysteria2,
    'tuic': _parse_tuic,
}


def parse_uri(uri):
    """把一条分享链接解析成统一的节点字典；无法解析返回 None。"""
    if not isinstance(uri, str):
        return None
    uri = uri.strip()
    scheme = uri.split('://', 1)[0].lower() if '://' in uri else ''
    if scheme == 'hy2':
        scheme = 'hysteria2'
    elif scheme == 'shadowsocks':
        scheme = 'ss'
    parser = _PARSERS.get(scheme)
    if not parser:
        return None
    try:
        node = parser(uri)
    except Exception:
        return None
    if not node or node['type'] not in SUPPORTED_TYPES:
        return None
    if not node.get('name'):
        node['name'] = f"{node['server']}:{node['port']}"
    return node


def is_valid_ss(uri):
    """判断一条 ss:// 是否真的是可用的 Shadowsocks 链接。"""
    node = _parse_ss(uri) if isinstance(uri, str) and uri.startswith('ss://') else None
    return node is not None


# --------------------------------------------------------------------------
# Clash（mihomo / Clash.Meta）输出
# --------------------------------------------------------------------------

def _clash_transport(proxy, node):
    network = node.get('network', 'tcp')
    if network in ('ws', 'httpupgrade'):
        proxy['network'] = 'ws'
        opts = {'path': node.get('path') or '/'}
        if node.get('host'):
            opts['headers'] = {'Host': node['host']}
        proxy['ws-opts'] = opts
    elif network == 'grpc':
        proxy['network'] = 'grpc'
        proxy['grpc-opts'] = {'grpc-service-name': node.get('service_name') or ''}
    elif network in ('h2', 'http'):
        proxy['network'] = 'h2'
        opts = {'path': node.get('path') or '/'}
        if node.get('host'):
            opts['host'] = [node['host']]
        proxy['h2-opts'] = opts


def _clash_tls(proxy, node):
    if not node.get('tls'):
        return
    proxy['tls'] = True
    if node.get('sni'):
        proxy['servername'] = node['sni']
    if node.get('insecure'):
        proxy['skip-cert-verify'] = True
    if node.get('fp'):
        proxy['client-fingerprint'] = node['fp']
    if node.get('alpn'):
        proxy['alpn'] = node['alpn']
    if node.get('reality_pbk'):
        reality = {'public-key': node['reality_pbk']}
        if node.get('reality_sid'):
            reality['short-id'] = node['reality_sid']
        proxy['reality-opts'] = reality


def clash_proxy(node):
    """节点字典 -> Clash proxy 条目。"""
    name = node['name']
    proxy = {'name': name, 'type': node['type'],
             'server': node['server'], 'port': node['port']}
    node_type = node['type']

    if node_type == 'vmess':
        proxy['uuid'] = node['uuid']
        proxy['alterId'] = node.get('alter_id', 0)
        proxy['cipher'] = node.get('cipher') or 'auto'
        proxy['udp'] = True
        _clash_tls(proxy, node)
        _clash_transport(proxy, node)

    elif node_type == 'vless':
        proxy['uuid'] = node['uuid']
        proxy['udp'] = True
        if node.get('flow'):
            proxy['flow'] = node['flow']
        _clash_tls(proxy, node)
        _clash_transport(proxy, node)

    elif node_type == 'trojan':
        proxy['password'] = node['password']
        proxy['udp'] = True
        _clash_tls(proxy, node)
        _clash_transport(proxy, node)

    elif node_type == 'ss':
        proxy['cipher'] = node['method']
        proxy['password'] = node['password']
        proxy['udp'] = True
        plugin = (node.get('plugin') or '').lower()
        if plugin.startswith('obfs') or plugin.startswith('simple-obfs'):
            options = {}
            for item in (node.get('plugin_opts') or '').split(';'):
                if '=' in item:
                    key, value = item.split('=', 1)
                    options[key.strip()] = value.strip()
            proxy['plugin'] = 'obfs'
            proxy['plugin-opts'] = {'mode': options.get('obfs', 'http')}
            if options.get('obfs-host'):
                proxy['plugin-opts']['host'] = options['obfs-host']

    elif node_type == 'ssr':
        proxy['cipher'] = node['method']
        proxy['password'] = node['password']
        proxy['protocol'] = node.get('protocol') or 'origin'
        proxy['obfs'] = node.get('obfs') or 'plain'
        if node.get('protocol_param'):
            proxy['protocol-param'] = node['protocol_param']
        if node.get('obfs_param'):
            proxy['obfs-param'] = node['obfs_param']
        proxy['udp'] = True

    elif node_type == 'hysteria2':
        proxy['password'] = node['password']
        if node.get('sni'):
            proxy['sni'] = node['sni']
        if node.get('insecure'):
            proxy['skip-cert-verify'] = True
        if node.get('obfs'):
            proxy['obfs'] = node['obfs']
            proxy['obfs-password'] = node.get('obfs_password', '')

    elif node_type == 'tuic':
        proxy['uuid'] = node['uuid']
        proxy['password'] = node['password']
        if node.get('sni'):
            proxy['sni'] = node['sni']
        if node.get('alpn'):
            proxy['alpn'] = node['alpn']
        if node.get('congestion_control'):
            proxy['congestion-controller'] = node['congestion_control']
        if node.get('udp_relay_mode'):
            proxy['udp-relay-mode'] = node['udp_relay_mode']
        if node.get('insecure'):
            proxy['skip-cert-verify'] = True

    return proxy


# --------------------------------------------------------------------------
# sing-box 输出
# --------------------------------------------------------------------------

def _singbox_tls(node):
    if not node.get('tls'):
        return None
    tls = {'enabled': True}
    if node.get('sni'):
        tls['server_name'] = node['sni']
    if node.get('insecure'):
        tls['insecure'] = True
    if node.get('alpn'):
        tls['alpn'] = node['alpn']
    if node.get('fp'):
        tls['utls'] = {'enabled': True, 'fingerprint': node['fp']}
    if node.get('reality_pbk'):
        reality = {'enabled': True, 'public_key': node['reality_pbk']}
        if node.get('reality_sid'):
            reality['short_id'] = node['reality_sid']
        tls['reality'] = reality
    return tls


def _singbox_transport(node):
    network = node.get('network', 'tcp')
    if network in ('ws', 'httpupgrade'):
        transport = {'type': 'ws', 'path': node.get('path') or '/'}
        if node.get('host'):
            transport['headers'] = {'Host': node['host']}
        return transport
    if network == 'grpc':
        return {'type': 'grpc', 'service_name': node.get('service_name') or ''}
    if network in ('h2', 'http'):
        transport = {'type': 'http', 'path': node.get('path') or '/'}
        if node.get('host'):
            transport['host'] = [node['host']]
        return transport
    return None


def singbox_outbound(node):
    """节点字典 -> sing-box outbound。"""
    outbound = {'type': node['type'], 'tag': node['name'],
                'server': node['server'], 'server_port': node['port']}
    node_type = node['type']

    if node_type == 'vmess':
        outbound['uuid'] = node['uuid']
        outbound['alter_id'] = node.get('alter_id', 0)
        outbound['security'] = node.get('cipher') or 'auto'

    elif node_type == 'vless':
        outbound['uuid'] = node['uuid']
        if node.get('flow'):
            outbound['flow'] = node['flow']

    elif node_type == 'trojan':
        outbound['password'] = node['password']

    elif node_type == 'ss':
        outbound['method'] = node['method']
        outbound['password'] = node['password']
        if node.get('plugin'):
            outbound['plugin'] = node['plugin']
            if node.get('plugin_opts'):
                outbound['plugin_opts'] = node['plugin_opts']

    elif node_type == 'ssr':
        outbound['type'] = 'shadowsocksr'
        outbound['method'] = node['method']
        outbound['password'] = node['password']
        outbound['protocol'] = node.get('protocol') or 'origin'
        outbound['obfs'] = node.get('obfs') or 'plain'
        if node.get('protocol_param'):
            outbound['protocol_param'] = node['protocol_param']
        if node.get('obfs_param'):
            outbound['obfs_param'] = node['obfs_param']

    elif node_type == 'hysteria2':
        outbound['password'] = node['password']
        if node.get('obfs'):
            outbound['obfs'] = {'type': node['obfs'],
                                'password': node.get('obfs_password', '')}

    elif node_type == 'tuic':
        outbound['uuid'] = node['uuid']
        outbound['password'] = node['password']
        if node.get('congestion_control'):
            outbound['congestion_control'] = node['congestion_control']
        if node.get('udp_relay_mode'):
            outbound['udp_relay_mode'] = node['udp_relay_mode']

    tls = _singbox_tls(node)
    if tls:
        outbound['tls'] = tls

    if node_type in ('vmess', 'vless', 'trojan'):
        transport = _singbox_transport(node)
        if transport:
            outbound['transport'] = transport

    return outbound
