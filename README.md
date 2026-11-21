# 🧩 Node-Configs

<p align="center">
  <img src="https://img.shields.io/github/license/looomix/Node-Configs?style=flat-square&color=blue" alt="License" />
  <img src="https://img.shields.io/badge/python-3.9%2B-3776AB?style=flat-square&logo=python" alt="Python 3.9+" />
  <img src="https://img.shields.io/github/actions/workflow/status/looomix/Node-Configs/main.yml?style=flat-square" alt="GitHub Workflow Status" />
  <img src="https://img.shields.io/github/last-commit/looomix/Node-Configs?style=flat-square" alt="Last Commit" />
  <br>
  <img src="https://img.shields.io/github/issues/looomix/Node-Configs?style=flat-square" alt="GitHub Issues" />
  <img src="https://img.shields.io/badge/Configs-2381-blue?style=flat-square" alt="Total Configs" />
  <img src="https://img.shields.io/github/stars/looomix/Node-Configs?style=social" alt="GitHub Stars" />
  <img src="https://img.shields.io/badge/status-active-brightgreen?style=flat-square" alt="Project Status" />
  <img src="https://img.shields.io/badge/language-中文%20%26%20English-007EC6?style=flat-square" alt="Language" />
</p>

## 最后更新: 14:51 2026-11-21

---

## 📖 关于项目

自动从多个公开来源抓取网络节点，解析、去重后按协议分类，并同步转换成 **Clash / sing-box 可直接导入的完整配置**。
所有文件每 12 小时由 GitHub Actions 自动更新。

> 节点来源均为互联网公开数据，本项目只做聚合与格式转换，**不承诺任何节点的可用性与安全性**，请自行甄别。

---

## ⚡ 一键订阅

> 复制下面任意一条链接，粘贴到客户端的「订阅 / 远程配置」里即可，无需下载文件。

| 客户端 | 订阅链接 | 说明 |
|:---:|:---|:---|
| **Clash / mihomo**<br>Clash Verge / ClashX Meta / FlClash | [`configs/clash/config.yaml`](https://raw.githubusercontent.com/looomix/Node-Configs/refs/heads/main/configs/clash/config.yaml) | 完整配置：节点 + 策略组 + 规则，导入即用 |
| **sing-box** | [`configs/singbox/config.json`](https://raw.githubusercontent.com/looomix/Node-Configs/refs/heads/main/configs/singbox/config.json) | 完整配置：mixed 入站 + selector/urltest 出站 |
| **v2rayN / v2rayNG / NekoRay / Karing** | [`configs/protocols/AllProtocols.txt`](https://raw.githubusercontent.com/looomix/Node-Configs/refs/heads/main/configs/protocols/AllProtocols.txt) | 原始分享链接，逐条导入 |

补充文件：
- Clash 只要节点列表（可作 proxy-provider）：[`configs/clash/proxies.yaml`](https://raw.githubusercontent.com/looomix/Node-Configs/refs/heads/main/configs/clash/proxies.yaml)
- sing-box 只要出站列表（可合并进已有配置）：[`configs/singbox/outbounds.json`](https://raw.githubusercontent.com/looomix/Node-Configs/refs/heads/main/configs/singbox/outbounds.json)

---

## 📊 概览

| 指标 | 数值 |
|:---|:---:|
| 分享链接总数 | 2381 |
| 无法转换的链接 | 16 |
| 重复节点（已合并） | 509 |
| **可转换唯一节点** | **1856** |

> 无法转换的链接通常是因为该协议暂不支持生成 Clash / sing-box 结构（如 WireGuard），原始链接仍在 `configs/protocols/` 中可用。

---

## 📁 协议配置
目前有 2381 个配置可用。

<div align="center">

| 协议 | 数量 | 下载链接 |
|:-------:|:-----:|:------------:|
| **汇总** | **2381** | [`AllProtocols.txt`](https://raw.githubusercontent.com/looomix/Node-Configs/refs/heads/main/configs/protocols/AllProtocols.txt) |
| Hysteria2 | 32 | [`Hysteria2.txt`](https://raw.githubusercontent.com/looomix/Node-Configs/refs/heads/main/configs/protocols/Hysteria2.txt) |
| ShadowSocks | 254 | [`ShadowSocks.txt`](https://raw.githubusercontent.com/looomix/Node-Configs/refs/heads/main/configs/protocols/ShadowSocks.txt) |
| Trojan | 122 | [`Trojan.txt`](https://raw.githubusercontent.com/looomix/Node-Configs/refs/heads/main/configs/protocols/Trojan.txt) |
| Vless | 1849 | [`Vless.txt`](https://raw.githubusercontent.com/looomix/Node-Configs/refs/heads/main/configs/protocols/Vless.txt) |
| Vmess | 124 | [`Vmess.txt`](https://raw.githubusercontent.com/looomix/Node-Configs/refs/heads/main/configs/protocols/Vmess.txt) |
</div>

---

## 🛠️ 使用方法

**Clash / mihomo**
1. 复制上面的 `config.yaml` 订阅链接；
2. Clash Verge / ClashX Meta →「配置」→ 粘贴链接 → 导入；
3. 选择「🚀 节点选择」里的节点即可。

**sing-box**
1. 复制上面的 `config.json` 订阅链接（或用 `outbounds.json` 合并进已有配置）；
2. 启动后本地 `127.0.0.1:2080` 即为 mixed 代理端口（HTTP + SOCKS5）。

**v2rayN / v2rayNG / NekoRay / Karing**
1. 下载 `AllProtocols.txt` 或对应协议文件；
2. 在客户端「从剪贴板导入 / 订阅」中导入。

> **建议**: 为获得最佳性能，请定期更新订阅。

---

## 📢 注意事项
- 本项目仅用于学习和研究目的，所有节点均来自互联网公开来源。
- 请根据您所在国家的法律法规负责任地使用。
- 如遇问题或建议，请使用 [Issues](https://github.com/looomix/Node-Configs/issues)。
