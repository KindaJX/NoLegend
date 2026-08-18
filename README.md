# NoLegend · 不败扫描器

> **NO 不败（No-Lose）套利扫描器** — 针对 Polymarket 温度分区市场，自动识别「买入一组连续档位的 NO 后，无论结算落在哪里都不亏」的无风险机会，并输出可执行的等额买入方案。支持多线程并发轮扫、飞书通知、Taker 返佣核算。

---

## 项目简介

本项目扫描 Polymarket 上满足 **多档互斥分区结构** 的温度市场（例如「某城市某日最高气温落在哪一档」），对每个档位直接读取 **NO 订单簿的卖一价**，枚举所有连续子块组合，找出满足

```
Σ(NO 卖一价) ≤ (k − 1) 美元
```

的组合（k = 买入档位数）。只要该式成立，该组合即为**无条件不败**：

| 结算情形 | 回款 |
|---|---|
| 结算落在所买 k 档之内 | 恰好 1 档 NO=0，其余 k−1 档 NO=1 → 回款 **k−1 美元/份** |
| 结算落在所买 k 档之外 | k 档全部 NO=1 → 回款 **k 美元/份** |

最坏情形（块内结算）回款 k−1 ≥ 投入 Σno，因此**不依赖结算结果，真不败**。

---

## 核心特性

- 🎯 **只做 NO 侧**：不碰 YES，不从 YES 推导 NO 价格，只用订单簿真实卖一价（`asks[-1]`，Polymarket CLOB 返回降序数组，最低卖价在末尾）
- 🧩 **互斥分区校验**：仅处理 `negRisk=True` 的市场（恰好一档结算 $1）。自动剔除累计阈值盘、多结果盘
- 🔍 **温度市场专扫**：只扫描每日最高温/最低温分区市场（t+0/t+1/t+2），降水/飓风/地震等非温度市场已过滤
- ⚡ **事件级并行**：8 线程并发扫描事件，内部档位再 8 并发，**单轮 157 事件仅需 ~26 秒**（串行时 213 秒）
- 🔄 **连续轮扫模式**：`--loop` 常驻运行，默认 30 秒间隔，适合服务器 7×24 部署
- 💰 **手续费 + Taker 返佣核算**：按官方公式计算手续费，并纳入白银阶（8%）返佣计算，输出**含返佣后**的块内/块外真实收益
- 📢 **多端输出**：控制台渲染、飞书机器人卡片通知（仅限返佣后盈利标的）、本地日志（自动滚动 10MB）
- 🏥 **健康报告**：每小时自动推送飞书运行报告（扫描次数/命中数/异常数），API 异常实时告警
- 📦 **单文件部署**：整个项目就是一个 Python 脚本，依赖仅 `requests`

---

## 快速开始

### 环境要求

- Python 3.8+
- 依赖：`requests`

```bash
pip install requests
```

### 运行

```bash
# 单次扫描
python polymarket_no_arbitrage_scanner.py

# 连续轮扫（服务器常驻模式）
python polymarket_no_arbitrage_scanner.py --loop --interval 30 --workers 8

# 显示未命中组合
python polymarket_no_arbitrage_scanner.py --verbose
```

### 配置（脚本头部常量 / 环境变量 / CLI 参数）

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `N_PER_BUCKET` | 10 | 每档固定买入份数 |
| `MAX_RANGE` | 11 | 子块最大跨度（档位数） |
| `TAKER_REBATE_RATE` | 0.08 | 白银阶返佣率（8%），升阶后修改 |
| `CATEGORIES` | `{"weather": "天气（温度）"}` | 扫描标签，仅温度市场 |
| `FEISHU_WEBHOOK` | 内置地址 | 飞书 webhook（环境变量覆盖） |
| `POLY_PROXY` | 未设置 | API 代理地址（环境变量，默认直连） |
| `--loop / -l` | — | 连续轮扫模式 |
| `--interval / -i` | 30 | 轮扫间隔秒数 |
| `--workers / -w` | 8 | 事件级并发数 |
| `--verbose / -v` | — | 显示最接近门槛的未命中组合 |

---

## 数据源（硬性约束）

- **发现市场**：Polymarket 官方 Gamma API（`https://gamma-api.polymarket.com`）
- **取价**：CLOB API（`https://clob.polymarket.com`）订单簿，NO 档位 `asks` 数组**最后一个**元素为最低卖一价
- **费率**：Gamma 市场字段 `feeSchedule.rate`（字段缺失视为零费率）
- **返佣规则**：参考 [Polymarket Taker Rebates](https://docs.polymarket.com/programs/taker-rebates)，白银阶返还手续费的 8%
- 不使用 `outcomePrices` 概率作为价格依据（概率反应慢于订单簿）

---

## 输出示例

```
[天气（温度）] Highest temperature in London on August 18?
  组合: 5 档连续子块, 门槛 = 4 美元 = 400 美分（共 11 档，未买全）
  Σno(块内 NO 卖一价和) = 399.8 美分  <= 门槛 400 美分  ✓
  结算在块内 ROI（保底） = 0.05%
  结算在块外 ROI = 25.06%
  --- 模拟下单: 每档固定买入 10 份 ---
    总投入         = $39.98
    结算在块内回款 = $40.00  净赚 $0.02
    结算在块外回款 = $50.00  净赚 $10.02
    本次交易手续费 = $0.33
    手续费后（块内）收益率 = -0.77%  （收益：$-0.31）
    手续费后（块外）收益率 = 24.25%  （收益：$9.69）
    --- Taker 返佣（白银阶 8%）---
    返佣金额 = $0.03  返佣收益率 = 0.07%
    含返佣后（块内）收益率 = -0.70%  （收益：$-0.28）
    含返佣后（块外）收益率 = 24.30%  （收益：$9.71）
```

---

## 服务器部署

```bash
# 海外服务器直连，后台常驻
nohup python3 polymarket_no_arbitrage_scanner.py --loop --interval 30 --workers 8 > scan.log 2>&1 &

# 使用 systemd 自拉起（推荐）
# [Service]
# ExecStart=/usr/bin/python3 /path/polymarket_no_arbitrage_scanner.py --loop --interval 30
# Restart=always
# RestartSec=10
```

### 飞书通知

代码内置了飞书机器人 webhook。如需更换，设置环境变量：

```bash
export FEISHU_WEBHOOK="https://open.feishu.cn/open-apis/bot/v2/hook/your-webhook"
```

每小时会自动推送健康报告；API 异常和错误会实时告警。

---

## 策略原理与风险提示

### 为什么会出现不败机会

多档分区市场中，各档 NO 卖一价之和通常接近 k−1（市场有效定价）。当市场滞后、错价或流动性不足导致「局部档位 NO 卖一价之和 ≤ k−1」时，出现无风险窗口。

### 重要风险提示

1. **手续费会侵蚀保底收益**：Σno 恰好贴住门槛的组合，扣手续费后块内可能为负。请以「手续费后收益」为准评估。
2. **真子组合的块外收益是小概率事件**：未买档位的结算概率通常极低，块外彩票价值有限。
3. **成交价假设**：卖一价仅保证第一份以该价成交，实际批量买入可能滑点。
4. **务必核对互斥性**：本项目以 `negRisk=True` 过滤非互斥市场；实盘前请再次确认市场结算规则（恰好一档结算 $1）。

---

## 目录结构

```
.
├── polymarket_no_arbitrage_scanner.py    # 主程序（单文件，约 950 行）
├── polymarket-no-arbitrage-scanner-prd.md  # 需求文档
├── scan_history.log                      # 命中记录（自动生成，10MB 自动轮转）
└── scan_runs.log                         # 运行记录（自动生成，10MB 自动轮转）
```

---

## 免责声明

本项目仅供学习与研究，不构成任何投资建议。加密货币预测市场存在高风险，请自行评估风险并承担相应后果。