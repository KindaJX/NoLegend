#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Polymarket 分区标的不败套利扫描器（NO 侧专用）

核心逻辑：
  对一个"多档互斥分区"市场（如日温度落在哪一档，含最高/最低温），直接读取每个档位 NO 订单簿的
  卖一价(bestAsk)，枚举所有连续子块，若某块内各档 NO 卖一价之和 Σno ≤ (k-1) 美元
  （k = 块内档数），则该块无条件不败：结算落在块内回款 k-1，落在块外回款 k，
  最坏情形不亏。每个标的只选 ROI 最高的一种组合，按每档固定买入 10 份模拟下单。

数据源（硬性约束）：
  - 只用 Polymarket 官方 Gamma API 发现市场与档位。
  - NO 卖一价一律取自 CLOB 订单簿的卖一价。注意 CLOB /book 返回的 asks 数组是
    【降序】排列（asks[0] 是最高卖单，asks[-1] 才是最低卖一价），因此取 asks[-1].price，
    不用 outcomePrices 概率，也不从 YES 侧推导 NO 价格，保证按该价可真实成交。
"""

import json
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

# Windows 控制台默认 GBK，强制 UTF-8 输出避免中文乱码
if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

import requests

GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"

# 飞书自定义机器人 webhook（可用环境变量 FEISHU_WEBHOOK 覆盖）
LARK_WEBHOOK = os.environ.get(
    "FEISHU_WEBHOOK",
    "https://open.feishu.cn/open-apis/bot/v2/hook/9256a704-79a9-4af2-949d-f4d526bbd1b3",
)
# 脚本所在目录下的本地日志文件
LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scan_history.log")

# 每档固定买入份数
N_PER_BUCKET = 10

# Taker 返佣率（按用户当前阶位设置）
# 白银（Silver）：8%，参考 https://docs.polymarket.com/programs/taker-rebates
TAKER_REBATE_RATE = 0.08
# 子块最大跨度（档位个数）。
# 实测 weather tag 下温度盘（日最高/最低温）几乎全部为 11 档
# （如上海 8/14: 24及以下,25,26,27,28,29,30,31,32,33,34及以上），
# 因此放宽到 11 以覆盖全仓；少量 12/13 档事件如需全覆盖可再上调。
MAX_RANGE = 11

# 按 tag 单独配置子块跨度上限（覆盖该 tag 下实测最高档位数，保证可枚举全仓）。
# 经济类（cpi/gdp/fed/interest-rates/unemployment/inflation）统一用 21，
# 以覆盖 fed 的 21 档事件；选举最高 23 档、宏观指标最高 21 档、地震 9 档。
# 未列出的 tag 走默认 MAX_RANGE=11。
# 注意：对档位数低于上限的事件，枚举自动以 n 为界，不产生额外开销。
MAX_RANGE_BY_TAG = {
    "cpi": 21,
    "gdp": 21,
    "fed": 21,
    "interest-rates": 21,
    "unemployment": 21,
    "inflation": 21,
    "macro-indicators": 21,
    "election": 23,
    "earthquake": 9,
}

# 至少 2 档才构成一个子块
MIN_RANGE = 2

# 默认扫描的标的类型：tag_slug -> 名称
# 当前仅扫描温度相关市场（最高温/最低温），
# 在 discover_events 中用 _weather_kind 过滤，只保留"温度最高"/"温度最低"，
# 降水、飓风、地震等非温度事件被排除。
CATEGORIES = {
    "weather": "天气（温度）",
}

_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
_session = requests.Session()
_session.headers.update(_HEADERS)

# 多线程打印锁 + 运行状态追踪（用于每小时健康报告 + 错误通知）
_print_lock = threading.Lock()
_scan_stats = {
    "scan_count": 0, "event_count": 0, "opportunities": 0,
    "errors": 0, "api_errors": 0, "last_hour_ts": 0.0,
}
# 可通过环境变量 POLY_PROXY 配置代理（如 http://127.0.0.1:7897），默认不代理
_proxy = os.environ.get("POLY_PROXY")
if _proxy:
    _session.proxies.update({"http": _proxy, "https": _proxy})


def _get_json(url, params=None, retries=3):
    for i in range(retries):
        try:
            r = _session.get(url, params=params, timeout=20)
            if r.status_code == 200:
                return r.json()
        except Exception:
            pass
        time.sleep(1.0 + i)
    return None


# ---------------------------------------------------------------
# 1) 发现事件
# ---------------------------------------------------------------
def discover_events(categories=None):
    """按 tag_slug 发现未结算事件，过滤已过期/无 CLOB 的事件，返回事件列表。

    注意：Gamma /events 单次返回有上限（实测约 100 条），必须分页拉全，
    否则会漏掉同一 tag 下排在后页的市场（如 daily-temperature 下 8/14 的
    纽约/芝加哥/迈阿密等）。这里用 offset 循环直到取空。
    """
    categories = categories or CATEGORIES
    now = datetime.now(timezone.utc)
    events = []
    for tag, name in categories.items():
        found = []
        limit = 100
        for offset in range(0, 5000, limit):
            page = _get_json(f"{GAMMA}/events", {"tag_slug": tag, "closed": "false", "limit": str(limit), "offset": str(offset)})
            if not isinstance(page, list):
                if offset == 0:
                    print(f"[warn] tag_slug={tag} 发现失败")
                break
            found.extend(page)
            if len(page) < limit:
                break
        for ev in found:
            if ev.get("closed"):
                continue
            end = ev.get("endDate")
            if end:
                try:
                    end_dt = datetime.fromisoformat(end.replace("Z", "+00:00"))
                    if end_dt < now:
                        continue  # 已到期，跳过
                except ValueError:
                    pass
            # 不在此按档位数提前过滤：全量纳入（含二元盘），
            # 二元盘是否抛弃交由 scan_event 在扫描阶段判定，保证每次扫最新、不因形态漏扫。
            # 只保留温度相关的天气事件（最高温/最低温）
            kind = _weather_kind(ev)
            if kind not in ("温度最高", "温度最低"):
                continue
            events.append({"event": ev, "category": name, "tag_slug": tag})
    return events


# ---------------------------------------------------------------
# 2) 解析档位标签 -> (lo, hi) 数值区间
# ---------------------------------------------------------------
_NUM_RE = r"-?\d+(?:\.\d+)?"


def parse_group(label):
    """把档位标签解析成数值区间 (lo, hi)，无法解析返回 None。

    兼容单位（°C / °F / % / bps 等）与边界写法：
      '<1.0%' / '1.0% or below'       -> (-inf, 1.0)
      '1.0–1.4%' / 'between 1 and 1.4' -> (1.0, 1.4)
      '4.0%+' / '4.0% or higher'       -> (4.0, inf)
      '25°C'                           -> (25, 25)
    数字提取不把区间分隔符 '-' 误当负号。
    """
    if not label:
        return None
    s = label.strip().lower()

    # 边界：上限（小于）
    if "<" in s or "less than" in s or "under" in s or "below" in s or "or lower" in s:
        nums = re.findall(_NUM_RE, s)
        if nums:
            return (float("-inf"), float(nums[-1]))
    # 边界：下限（大于）
    if "+" in s or "above" in s or "higher" in s or "more" in s or "greater" in s or "over" in s:
        nums = re.findall(_NUM_RE, s)
        if nums:
            return (float(nums[0]), float("inf"))
    # 区间：X--Y / X-Y / X to Y / X and Y（允许单位字符夹在中间）
    m = re.search(
        rf"({_NUM_RE})[\s%°a-z]*?(?:[-–—]|to|and)[\s%°a-z]*?({_NUM_RE})", s
    )
    if m:
        return (float(m.group(1)), float(m.group(2)))
    # 单个精确值
    m = re.search(_NUM_RE, s)
    if m:
        v = float(m.group(0))
        return (v, v)
    return None


# ---------------------------------------------------------------
# 3) 取某档 NO 的订单簿卖一价
# ---------------------------------------------------------------
def fetch_no_best_ask(market):
    """取 NO 档位订单簿的卖一价。

    注意：Polymarket CLOB /book 返回的 asks 数组是【降序】排列——
    卖一（最低卖价）在数组末尾，asks[0] 是最高的卖单。
    例：32°C 档 No asks = [0.99, 0.98, ..., 0.73, 0.72]，
        真实卖一 = asks[-1] = 0.72（页面显示 73 ≈ 买一/中间价附近）。
    因此必须取 asks[-1]，否则会错误取到 0.99 这类高价镜像单。
    无卖单返回 None。
    """
    try:
        tokens = json.loads(market.get("clobTokenIds") or "[]")
    except Exception:
        return None
    if len(tokens) < 2:
        return None
    no_token = tokens[1]
    book = _get_json(f"{CLOB}/book", {"token_id": no_token})
    if not book or not book.get("asks"):
        return None
    # asks 降序：卖一 = 数组最后一个（最低价）
    return float(book["asks"][-1]["price"])


# ---------------------------------------------------------------
# 3.5) 取市场 Taker 手续费率（官方文档: fee = C × feeRate × p × (1-p)）
# ---------------------------------------------------------------
def get_fee_rate(market):
    """从 Gamma market 的 feeSchedule.rate 读取 Taker 费率。

    依据官方手续费文档（https://docs.polymarket.com/cn/trading/fees）：
      fee = C × feeRate × p × (1-p)，其中 C=份额数, p=份额价格, feeRate=类别费率。
    实测：天气/经济/一般类 rate=0.05，政治类 rate=0.04；takerBaseFee=1000 为废弃字段
    与实际费率不符，一律以 feeSchedule.rate 为准。字段缺失/None 视为零费率。
    """
    try:
        fs = market.get("feeSchedule") or {}
        rate = fs.get("rate")
        return float(rate) if rate is not None else 0.0
    except Exception:
        return 0.0


# ---------------------------------------------------------------
# 4) 扫描单个事件
# ---------------------------------------------------------------
def scan_event(ev_info, max_range=MAX_RANGE):
    """扫描一个分区事件。

    不做任何"提前按标签过滤"：事件里所有档位（markets）全部纳入，逐档取 NO
    卖一价后再判定。纯二元盘（档位 < MIN_RANGE）自然被跳过；档位标签能否解析成
    数值只影响排序，不影响"是否扫描"。市场是动态变化的，因此每次必须全扫，
    不因标签形态而漏掉可能出现倒挂的新市场。

    返回 dict：
      bucketed  已排序的档位（label/lo/hi/no_ask_cents）
      best      满足 Σno<=k-1 的最优组合（None 表示无机会）
      nearest   所有子块中"最接近门槛"的组合（即使不满足，供 --verbose 诊断）
    """
    ev = ev_info["event"]
    markets = ev.get("markets") or []
    if len(markets) < MIN_RANGE:
        return None  # 二元盘（1 档）——扫到后直接抛弃

    # 关键前提：NO 不败模型只对"互斥分区"市场成立（恰好一档结算 $1，其余 $0）。
    # 用 Gamma 的 negRisk 字段判别：温度/区间分区盘 negRisk=True；
    # 累计阈值盘（≥X、at least X，如麻疹/风速）、多结果盘（各国是否发生，如
    # 埃博拉/地震）negRisk=False——这些市场可能同时结算多个 YES，多个 NO 同时
    # 归零，回款低于 k-1，"不败"不成立，必须跳过。
    if not all(m.get("negRisk") for m in markets):
        return None  # 非互斥分区市场，跳过

    # 全部档位纳入，不因 parse_group 失败而剔除；解析不了数值的保持原顺序后排
    buckets = []
    for i, m in enumerate(markets):
        label = m.get("groupItemTitle")
        rng = parse_group(label)
        if rng is not None:
            buckets.append({"label": label, "lo": rng[0], "hi": rng[1],
                            "parsable": True, "order": i, "market": m})
        else:
            buckets.append({"label": label, "lo": None, "hi": None,
                            "parsable": False, "order": i, "market": m})
    buckets.sort(key=lambda b: (0 if b["parsable"] else 1,
                                b["lo"] if b["lo"] is not None else 0,
                                b["hi"] if b["hi"] is not None else 0,
                                b["order"]))
    if len(buckets) < MIN_RANGE:
        return None

    # 并发取每档 NO 卖一价
    no_asks = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(fetch_no_best_ask, b["market"]): i for i, b in enumerate(buckets)}
        for f in as_completed(futs):
            i = futs[f]
            no_asks[i] = f.result()

    n = len(buckets)
    best = None
    nearest = None  # (差额, 组合)
    # 组合统计：
    #   combos_possible  = 理论上限：k=2..min(n,max_range) 的连续子块总数
    #   combos_evaluated = 实际真正判定过的子块数（块内每档都有 NO 卖单）
    combos_possible = 0
    for k in range(MIN_RANGE, min(n, max_range) + 1):
        combos_possible += n - k + 1
    combos_evaluated = 0
    # 枚举所有连续子块 [s, e]
    for s in range(n):
        sum_no = 0.0
        for e in range(s, n):
            k = e - s + 1
            if k > max_range:
                break
            if no_asks.get(e) is None:
                break  # 块内出现无卖单档位，无法完整买入，向后该档永不可用
            sum_no += no_asks[e]
            if k < MIN_RANGE:
                continue  # 单档（k=1）不构成子块，不计数
            combos_evaluated += 1
            threshold = (k - 1) * 100.0  # 单位：美分
            sum_no_cents = sum_no * 100.0
            combo = {
                "s": s, "e": e, "k": k,
                "sum_no_cents": sum_no_cents,
                "threshold_cents": threshold,
            }
            if sum_no_cents <= threshold:
                roi = (threshold - sum_no_cents) / sum_no_cents
                combo["roi"] = roi
                if best is None or roi > best["roi"] or (roi == best["roi"] and k > best["k"]):
                    best = combo
            else:
                gap = sum_no_cents - threshold
                if nearest is None or gap < nearest["gap"]:
                    nearest = {"gap": gap, "combo": combo}

    bucketed = [{
        "label": b["label"],
        "no_ask_cents": round(no_asks.get(i, 0) * 100, 2) if no_asks.get(i) is not None else None,
    } for i, b in enumerate(buckets)]

    def _build(combo):
        s, e = combo["s"], combo["e"]
        selected = []
        for i in range(s, e + 1):
            m = buckets[i]["market"]
            selected.append({
                "label": buckets[i]["label"],
                "question": m.get("question"),
                "market_id": str(m.get("id")),
                "slug": m.get("slug"),
                "no_ask_cents": round(no_asks[i] * 100, 2),
                "fee_rate": get_fee_rate(m),  # 该档 Taker 费率（feeSchedule.rate）
            })
        return {
            "event": ev,
            "category": ev_info["category"],
            "selected": selected,
            "k": combo["k"],
            "n_total": len(buckets),  # 市场总档位数（用于判断是否全仓）
            "sum_no_cents": combo["sum_no_cents"],
            "threshold_cents": combo["threshold_cents"],
            "roi": combo.get("roi", (combo["threshold_cents"] - combo["sum_no_cents"]) / combo["sum_no_cents"]),
        }

    result = {"bucketed": bucketed, "best": None, "nearest": None,
              "combos_possible": combos_possible, "combos_evaluated": combos_evaluated}
    if best is not None:
        result["best"] = _build(best)
    if nearest is not None:
        result["nearest"] = _build(nearest["combo"])
        result["nearest"]["gap_cents"] = nearest["gap"]
    return result


# ---------------------------------------------------------------
# 5) 模拟下单（每档固定买入 N 份）
# ---------------------------------------------------------------
def simulate_order(res, N=N_PER_BUCKET):
    """按块内每档固定 N 份、以卖一价买入，返回回款情景。单位统一为美分。

    区分两种情形：
      - 真子组合（k < n_total）：结算在块外时 k 档全 NO=1，回款 k 美元/份。
      - 全仓组合（k == n_total）：所有档位都已买入，不存在"块外"结算分支，
        块外回款/收益/ROI 一律置 0，调用方据此显示"无块外选项"。

    手续费（官方公式 fee = C × feeRate × p × (1-p)，USDC）：
	      - 每档手续费 = N × rate_i × p_i × (1 - p_i)，p_i = 该档 NO 卖一价
	      - 总手续费 = Σ 每档手续费（美分计）
	      - 手续费后收益 = 原净赚 - 总手续费；手续费后收益率 = 手续费后收益 / 总投入
	      - 块内/块外两种情形都算，作为原收益的补充字段（不替代原收益）。

	    Taker 返佣（参考 https://docs.polymarket.com/programs/taker-rebates）：
	      - 返佣金额 = 总手续费 × TAKER_REBATE_RATE（白银阶 = 8%）
	      - 含返佣后收益 = 手续费后收益 + 返佣金额
	      - 含返佣后收益率 = 含返佣后收益 / 总投入
	      - 返佣每日 UTC 零点以 pUSD 发放，此处按理论值计算。
    """
    cost_cents = res["sum_no_cents"] * N
    k = res["k"]
    n_total = res.get("n_total", k)
    is_full = k >= n_total  # 全仓：买了全部档位
    # 块内结算：恰 1 档 NO=0，其余 k-1 档 NO=1 -> 回款 (k-1) 美元/份
    inner_payout = res["threshold_cents"] * N
    if is_full:
        # 无块外分支
        outer_payout = 0.0
    else:
        # 块外结算：k 档全 NO=1 -> 回款 k 美元/份
        outer_payout = k * 100.0 * N
    # 手续费：每档 N 份 × rate × p × (1-p)
    fee_cents = 0.0
    for b in res.get("selected", []):
        p = (b.get("no_ask_cents") or 0.0) / 100.0
        rate = b.get("fee_rate") or 0.0
        if p > 0 and p < 1:
            fee_cents += N * rate * p * (1.0 - p) * 100.0  # 转美分
    inner_profit = inner_payout - cost_cents
    outer_profit = outer_payout - cost_cents
    return {
        "cost_cents": cost_cents,
        "is_full": is_full,
        "inner_payout_cents": inner_payout,
        "outer_payout_cents": outer_payout,
        "inner_profit_cents": inner_profit,
        "outer_profit_cents": outer_profit,
        # 两种情形 ROI：结算在块内（保底）/ 结算在块外
        "inner_roi": inner_profit / cost_cents,
        "outer_roi": outer_profit / cost_cents if outer_payout > 0 else 0.0,
        # ---- 手续费补充字段 ----
        "fee_cents": fee_cents,  # 本次总手续费（美分）
        "inner_profit_after_fee": inner_profit - fee_cents,  # 手续费后块内净赚
        "outer_profit_after_fee": outer_profit - fee_cents,  # 手续费后块外净赚
        "inner_roi_after_fee": (inner_profit - fee_cents) / cost_cents,
        "outer_roi_after_fee": (outer_profit - fee_cents) / cost_cents if outer_payout > 0 else 0.0,
        # ---- Taker 返佣补充字段 ----
        "rebate_cents": fee_cents * TAKER_REBATE_RATE,  # 返佣金额（美分）= 手续费 × 返佣率
        "inner_profit_with_rebate": inner_profit - fee_cents + fee_cents * TAKER_REBATE_RATE,  # 含返佣的块内净赚
        "outer_profit_with_rebate": outer_profit - fee_cents + fee_cents * TAKER_REBATE_RATE,  # 含返佣的块外净赚
        "inner_roi_with_rebate": (inner_profit - fee_cents + fee_cents * TAKER_REBATE_RATE) / cost_cents,
        "outer_roi_with_rebate": (outer_profit - fee_cents + fee_cents * TAKER_REBATE_RATE) / cost_cents if outer_payout > 0 else 0.0,
    }


# ---------------------------------------------------------------
# 6) 展示
# ---------------------------------------------------------------
def fmt_cents(c):
    return f"${c / 100:.2f}"


def render(res, sim):
    ev = res["event"]
    print("-" * 72)
    print(f"[{res['category']}] {ev.get('title')}  (event_id={ev.get('id')}, slug={ev.get('slug')})")
    print(f"  组合: {res['k']} 档连续子块, 门槛 = {res['k']-1} 美元 = {res['threshold_cents']:.0f} 美分"
          + ("（全仓）" if sim['is_full'] else f"（共 {res['n_total']} 档，未买全）"))
    print(f"  Σno(块内 NO 卖一价和) = {res['sum_no_cents']:.1f} 美分  <= 门槛 {res['threshold_cents']:.0f} 美分  ✓")
    print(f"  结算在块内 ROI（保底） = {(sim['inner_roi']*100):.2f}%")
    if not sim['is_full']:
        print(f"  结算在块外 ROI = {(sim['outer_roi']*100):.2f}%")
    print("  --- 所选档位（具体市场）NO 卖一价明细 ---")
    for b in res["selected"]:
        name = b.get("question") or f"{ev.get('title')} - {b['label']}"
        print(f"    {name}")
        print(f"      NO 卖一价 = {b['no_ask_cents']:.1f} 美分")
    print("  --- 模拟下单: 每档固定买入 10 份 ---")
    print(f"    总投入         = {fmt_cents(sim['cost_cents'])}")
    print(f"    结算在块内回款 = {fmt_cents(sim['inner_payout_cents'])}  净赚 {fmt_cents(sim['inner_profit_cents'])}")
    if sim['is_full']:
        print("    块外回款 = $0.00  无块外选项（全仓买入所有档位）")
    else:
        print(f"    结算在块外回款 = {fmt_cents(sim['outer_payout_cents'])}  净赚 {fmt_cents(sim['outer_profit_cents'])}")
    # 手续费补充（追加，不替代原收益）
    print(f"    本次交易手续费 = {fmt_cents(sim['fee_cents'])}")
    print(f"    手续费后（块内）收益率 = {sim['inner_roi_after_fee']*100:.2f}%  （收益：{fmt_cents(sim['inner_profit_after_fee'])}）")
    if not sim['is_full']:
        print(f"    手续费后（块外）收益率 = {sim['outer_roi_after_fee']*100:.2f}%  （收益：{fmt_cents(sim['outer_profit_after_fee'])}）")
    # Taker 返佣（追加）
    print(f"    --- Taker 返佣（白银阶 {TAKER_REBATE_RATE*100:.0f}%）---")
    print(f"    返佣金额 = {fmt_cents(sim['rebate_cents'])}  返佣收益率 = {sim['rebate_cents']/sim['cost_cents']*100:.2f}%")
    print(f"    含返佣后（块内）收益率 = {sim['inner_roi_with_rebate']*100:.2f}%  （收益：{fmt_cents(sim['inner_profit_with_rebate'])}）")
    if not sim['is_full']:
        print(f"    含返佣后（块外）收益率 = {sim['outer_roi_with_rebate']*100:.2f}%  （收益：{fmt_cents(sim['outer_profit_with_rebate'])}）")


# ---------------------------------------------------------------
# 7) 飞书通知 + 本地日志
# ---------------------------------------------------------------
def build_card(res, sim):
    """把扫描结果 + 模拟下单详情拼成飞书交互式卡片 JSON。

    标题行使用绿色背景的大 header，突出"发现 NO 不败套利标的"。
    """
    ev = res["event"]
    sections = []
    sections.append(f"**市场**：{ev.get('title')}  (`event_id: {ev.get('id')}`)")
    sections.append(f"**类型**：{res['category']} ｜ **组合**：{res['k']} 档连续子块，门槛 **{res['k']-1} 美元**"
                    + ("（全仓）" if sim['is_full'] else f"（共 {res['n_total']} 档，未买全）"))
    # 档位明细
    items = []
    for b in res["selected"]:
        name = b.get("question") or f"{ev.get('title')} - {b['label']}"
        items.append(f"- {name}  →  NO 卖一价 **{b['no_ask_cents']:.1f} 美分**")
    sections.append("**选中档位（具体市场）及 NO 卖一价**\n" + "\n".join(items))
    outer_roi_str = (
        f"结算在块外 ROI = **{sim['outer_roi']*100:.2f}%**"
        if not sim['is_full'] else
        "结算在块外：**无（全仓买入所有档位）**"
    )
    sections.append(
        f"Σno 和值 = **{res['sum_no_cents']:.1f} 美分** (≤ 门槛 {res['threshold_cents']:.0f} 美分) ✅\n"
        f"结算在块内 ROI（保底） = **{sim['inner_roi']*100:.2f}%**\n"
        f"{outer_roi_str}"
    )
    outer_pay_str = (
        f"｜ 块外回款 {fmt_cents(sim['outer_payout_cents'])} "
        f"(净赚 **{fmt_cents(sim['outer_profit_cents'])}**)"
        if not sim['is_full'] else
        "｜ 块外回款 **$0.00**（无块外选项）"
    )
    sections.append(
        f"**模拟下单：每档固定买入 10 份**\n"
        f"总投入 = {fmt_cents(sim['cost_cents'])} ｜ 块内回款 {fmt_cents(sim['inner_payout_cents'])} "
        f"(净赚 **{fmt_cents(sim['inner_profit_cents'])}**){outer_pay_str}"
    )
    # 手续费补充（追加，不替代原收益）
    fee_str = (
        f"**手续费**：本次交易手续费 = **{fmt_cents(sim['fee_cents'])}**\n"
        f"手续费后（块内）收益率 = **{sim['inner_roi_after_fee']*100:.2f}%**"
        f"（收益：**{fmt_cents(sim['inner_profit_after_fee'])}**）\n"
    )
    if not sim['is_full']:
        fee_str += (
            f"手续费后（块外）收益率 = **{sim['outer_roi_after_fee']*100:.2f}%**"
            f"（收益：**{fmt_cents(sim['outer_profit_after_fee'])}**）"
        )
    else:
        fee_str += "手续费后（块外）：**无（全仓买入所有档位）**"
    sections.append(fee_str)
    # Taker 返佣（追加）
    rebate_str = (
        f"**Taker 返佣（白银阶 {TAKER_REBATE_RATE*100:.0f}%）**\n"
        f"返佣金额 = **{fmt_cents(sim['rebate_cents'])}**"
        f"（返佣收益率 = **{sim['rebate_cents']/sim['cost_cents']*100:.2f}%**）\n"
        f"含返佣后（块内）收益率 = **{sim['inner_roi_with_rebate']*100:.2f}%**"
        f"（收益：**{fmt_cents(sim['inner_profit_with_rebate'])}**）\n"
    )
    if not sim['is_full']:
        rebate_str += (
            f"含返佣后（块外）收益率 = **{sim['outer_roi_with_rebate']*100:.2f}%**"
            f"（收益：**{fmt_cents(sim['outer_profit_with_rebate'])}**）"
        )
    else:
        rebate_str += "含返佣后（块外）：**无（全仓买入所有档位）**"
    sections.append(rebate_str)
    elements = [{"tag": "markdown", "content": sec} for sec in sections]
    return {
        "msg_type": "interactive",
        "card": {
            "config": {"wide_screen_mode": True},
            "header": {
                "template": "green",
                "title": {"tag": "plain_text", "content": "🎯 发现 NO 不败套利标的"},
            },
            "elements": elements,
        },
    }


def send_lark_card(payload):
    """以交互式卡片发送到飞书机器人，返回成功与否。"""
    if not LARK_WEBHOOK:
        print("[warn] 未配置飞书 webhook，跳过通知")
        return False
    try:
        r = requests.post(LARK_WEBHOOK, json=payload, timeout=15)
        ok = r.status_code == 200 and r.json().get("code") == 0
        if not ok:
            print(f"[warn] 飞书通知发送失败: {r.status_code} {r.text[:300]}")
        return ok
    except Exception as e:
        print(f"[warn] 飞书通知异常: {e}")
        return False


def send_lark(text):
    """发送文本消息到飞书机器人，返回成功与否。"""
    if not LARK_WEBHOOK:
        print("[warn] 未配置飞书 webhook，跳过通知")
        return False
    try:
        r = requests.post(LARK_WEBHOOK, json={"msg_type": "text", "content": {"text": text}}, timeout=15)
        ok = r.status_code == 200 and r.json().get("code") == 0
        if not ok:
            print(f"[warn] 飞书通知发送失败: {r.status_code} {r.text[:300]}")
        return ok
    except Exception as e:
        print(f"[warn] 飞书通知异常: {e}")
        return False


def send_lark_error(msg):
    """发送错误文本到飞书机器人，用于通知 API 限频/超时等异常。"""
    text = f"⚠️ [NO 不败扫描器] 异常告警\n{msg}"
    return send_lark(text)


def send_lark_health_report(stats, elapsed):
    """每小时发送一次运行健康报告。"""
    now_str = datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")
    uptime = stats["scan_count"]
    text = (
        f"✅ [NO 不败扫描器] 运行健康报告\n"
        f"时间：{now_str}\n"
        f"累计扫描轮次：{uptime}\n"
        f"最近一轮扫描事件：{stats['event_count']} 个 ｜ 命中：{stats['opportunities']} 个\n"
        f"最近一轮耗时：{elapsed:.1f} 秒\n"
        f"累计异常：{stats['errors']} 次 ｜ API 错误：{stats['api_errors']} 次\n"
        f"状态：{'正常' if stats['errors'] < 3 else '请关注'}"
    )
    return send_lark(text)


def safe_print(*args, **kwargs):
    """线程安全的 print，避免多线程输出交错。"""
    with _print_lock:
        print(*args, **kwargs)


def _check_hourly_report(scan_elapsed):
    """检查是否到达整点，发送健康报告并重置统计。"""
    now = time.time()
    if now - _scan_stats["last_hour_ts"] >= 3600:
        _scan_stats["last_hour_ts"] = now
        send_lark_health_report(_scan_stats, scan_elapsed)
        _scan_stats["scan_count"] = 0
        _scan_stats["event_count"] = 0
        _scan_stats["opportunities"] = 0
        _scan_stats["errors"] = 0
        _scan_stats["api_errors"] = 0


def _rotate_log(path, max_bytes=10_485_760):
    """日志文件超过 max_bytes（默认 10MB）时轮转，保留最近 5 份。"""
    try:
        if os.path.exists(path) and os.path.getsize(path) > max_bytes:
            dirname = os.path.dirname(path)
            basename = os.path.basename(path)
            for i in range(4, 0, -1):
                old = os.path.join(dirname, f"{basename}.{i}")
                new = os.path.join(dirname, f"{basename}.{i + 1}")
                if os.path.exists(old):
                    os.replace(old, new)
            os.replace(path, os.path.join(dirname, f"{basename}.1"))
    except Exception:
        pass


def write_log(res, sim, notify_ok=None):
    """把本次扫描结果追加到本地日志文件。"""
    ts = datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
    ev = res["event"]
    lines = [
        "=" * 72,
        f"[{ts}] 标的: {ev.get('title')}  (event_id={ev.get('id')}, slug={ev.get('slug')})",
        f"  类型: {res['category']} | 组合: {res['k']} 档 | 门槛: {res['k']-1} 美元",
    ]
    for b in res["selected"]:
        lines.append(f"    {b['label']:<14} NO 卖一价 = {b['no_ask_cents']:.1f} 美分")
    lines.append(f"  Σno = {res['sum_no_cents']:.1f} 美分 | ROI = {res['roi']*100:.2f}%")
    if sim.get("is_full"):
        lines.append(f"  模拟下单(每档{N_PER_BUCKET}份): 投入 {fmt_cents(sim['cost_cents'])} | "
                     f"块内净赚 {fmt_cents(sim['inner_profit_cents'])} | 全仓无块外选项")
    else:
        lines.append(f"  模拟下单(每档{N_PER_BUCKET}份): 投入 {fmt_cents(sim['cost_cents'])} | "
                     f"块内净赚 {fmt_cents(sim['inner_profit_cents'])} | 块外净赚 {fmt_cents(sim['outer_profit_cents'])}")
    lines.append(f"  手续费: {fmt_cents(sim['fee_cents'])} | 手续费后块内净赚 {fmt_cents(sim['inner_profit_after_fee'])}"
                 + ("" if sim.get("is_full") else f" | 手续费后块外净赚 {fmt_cents(sim['outer_profit_after_fee'])}"))
    lines.append(f"  Taker返佣(白银阶{TAKER_REBATE_RATE*100:.0f}%): 返佣金额 {fmt_cents(sim['rebate_cents'])} | "
                 f"返佣收益率 {sim['rebate_cents']/sim['cost_cents']*100:.2f}%"
                 f" | 含返佣块内收益率 {sim['inner_roi_with_rebate']*100:.2f}%"
                 + ("" if sim.get("is_full") else f" | 含返佣块外收益率 {sim['outer_roi_with_rebate']*100:.2f}%"))
    if notify_ok is not None:
        lines.append(f"  飞书通知: {'成功' if notify_ok else '失败'}")
    try:
        _rotate_log(LOG_FILE)
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
    except Exception as e:
        print(f"[warn] 写入本地日志失败: {e}")


# ---------------------------------------------------------------
# main
# ---------------------------------------------------------------
def _event_day(ev):
    """从事件标题提取交易日（'on August N' / '8月14日'），返回 'N' 或 None。"""
    t = ev.get("title") or ""
    m = re.search(r"(?:on\s+)?(?:8月|August)\s*(\d{1,2})", t)
    return m.group(1) if m else None


def _weather_kind(event):
    """粗略归类天气市场的子类型，用于候选统计。"""
    t = (event.get("title") or "")
    tl = t.lower()
    if "highest temperature" in tl or "最高温" in t or "气温最高" in t or "最高气温" in t:
        return "温度最高"
    if "lowest temperature" in tl or "最低温" in t or "气温最低" in t or "最低气温" in t:
        return "温度最低"
    if "precipitation" in tl or "降水量" in t or "降雨量" in t:
        return "降水"
    if tl.startswith("how many") or "多少次" in t or "多少" in t:
        return "数量类"
    if "hurricane" in tl or "飓风" in t or "台风" in t or "tropical cyclone" in tl:
        return "飓风/台风"
    if "tornado" in tl or "龙卷风" in t:
        return "龙卷风"
    if "earthquake" in tl or "地震" in t:
        return "地震"
    if "volcano" in tl or "火山" in t:
        return "火山"
    if "flu" in tl or "流感" in t or "measles" in tl or "病例" in t or "肺炎" in t or "pandemic" in tl:
        return "传染病/大流行"
    return "其他"


def _print_candidate_summary(events):
    """按来源 tag / 子类型分组统计并打印候选事件数量，供每次扫描核对。"""
    from collections import Counter
    by_kind = Counter()
    temp_days = Counter()
    for ev_info in events:
        ev = ev_info["event"]
        if ev_info["category"] != CATEGORIES["weather"]:
            # 非天气类：直接按来源 tag 名称分组
            kind = ev_info["category"]
        else:
            kind = _weather_kind(ev)
        by_kind[kind] += 1
        if kind in ("温度最高", "温度最低"):
            day = _event_day(ev)
            temp_days[(kind, day if day else "unknown")] += 1
    print(f"发现 {len(events)} 个候选事件，按来源/类型统计：")
    for kind in sorted(by_kind, key=lambda x: -by_kind[x]):
        print(f"  {kind}: {by_kind[kind]}")
    if temp_days:
        print("  其中温度盘按交易日统计：")
        for (kind, day) in sorted(temp_days, key=lambda x: (x[0], x[1])):
            print(f"    {kind} t日({day}): {temp_days[(kind, day)]}")
    print()


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Polymarket 温度市场 NO 不败套利扫描器")
    parser.add_argument("--verbose", "-v", action="store_true", help="显示未命中的最接近组合")
    parser.add_argument("--loop", "-l", action="store_true", help="连续轮扫模式")
    parser.add_argument("--interval", "-i", type=int, default=30,
                        help="轮扫间隔秒数（默认 30 秒，仅 --loop 时生效）")
    parser.add_argument("--workers", "-w", type=int, default=8,
                        help="事件级并发数（默认 8）")
    args = parser.parse_args()
    verbose = args.verbose

    EVENT_WORKERS = args.workers

    def run_one_scan():
        t_start = time.perf_counter()
        safe_print("正在发现分区市场事件 ...")
        events = discover_events()
        _print_candidate_summary(events)

        opportunities = 0
        subset_count = 0
        full_count = 0
        total_possible = 0
        total_evaluated = 0
        errors = 0
        api_errors = 0

        # 工具：线程安全地累加计数器
        cnt_lock = threading.Lock()
        def add_stats(**kw):
            nonlocal opportunities, subset_count, full_count, total_possible, total_evaluated, errors, api_errors
            with cnt_lock:
                for k, v in kw.items():
                    if k == "opportunities": opportunities += v
                    elif k == "subset": subset_count += v
                    elif k == "full": full_count += v
                    elif k == "possible": total_possible += v
                    elif k == "evaluated": total_evaluated += v
                    elif k == "errors": errors += v
                    elif k == "api_errors": api_errors += v

        def process_event(ev_info):
            """处理单个事件，返回(命中标记, 组合数统计)或None。"""
            max_range = MAX_RANGE_BY_TAG.get(ev_info.get("tag_slug"), MAX_RANGE)
            try:
                res = scan_event(ev_info, max_range=max_range)
            except Exception as e:
                safe_print(f"[error] 扫描事件异常: {ev_info['event'].get('title')}: {e}")
                return {"error": True}
            if res is None:
                return {"skip": True}
            out = {"possible": res.get("combos_possible", 0), "evaluated": res.get("combos_evaluated", 0)}
            best = res.get("best")
            if best is not None:
                sim = simulate_order(best)
                n_total = len(res.get("bucketed", []))
                is_subset = best["k"] < n_total
                with _print_lock:
                    render(best, sim)
                # 只通知返佣后收益为正的标的
                has_rebate_profit = (
                    sim["inner_profit_with_rebate"] > 0 or
                    sim["outer_profit_with_rebate"] > 0
                )
                if has_rebate_profit:
                    notify_ok = send_lark_card(build_card(best, sim))
                else:
                    notify_ok = False
                write_log(best, sim, notify_ok)
                out.update({"hit": True, "is_subset": is_subset})
            elif verbose and res.get("nearest"):
                near = res["nearest"]
                with _print_lock:
                    print("-" * 72)
                    print(f"[{near['category']}] {near['event'].get('title')}  "
                          f"(slug={near['event'].get('slug')})")
                    print(f"  未满足条件，最接近的组合 k={near['k']}，"
                          f"Σno={near['sum_no_cents']:.1f} 美分 vs 门槛 {near['threshold_cents']:.0f} 美分，"
                          f"还差 {near['gap_cents']:.1f} 美分  ✗")
            return out

        # 事件级并行扫描
        with ThreadPoolExecutor(max_workers=EVENT_WORKERS) as ex:
            futs = {ex.submit(process_event, ev_info): ev_info for ev_info in events}
            for f in as_completed(futs):
                ev_info = futs[f]
                try:
                    result = f.result()
                except Exception as e:
                    safe_print(f"[error] 事件处理异常: {ev_info['event'].get('title')}: {e}")
                    add_stats(errors=1)
                    continue
                if result is None:
                    continue
                if result.get("error"):
                    add_stats(errors=1, api_errors=1)
                    send_lark_error(f"扫描事件异常: {ev_info['event'].get('title')}")
                    continue
                if result.get("skip"):
                    continue
                add_stats(
                    possible=result.get("possible", 0),
                    evaluated=result.get("evaluated", 0),
                )
                if result.get("hit"):
                    add_stats(opportunities=1, subset=1 if result.get("is_subset") else 0,
                              full=0 if result.get("is_subset") else 1)

        if opportunities == 0:
            safe_print("未扫描到满足 NO 不败条件 (Σno <= k-1) 的组合。")
            if verbose:
                safe_print("（已列出各事件中最接近门槛的组合，供观察市场倒挂程度）")
        t_elapsed = time.perf_counter() - t_start
        safe_print(f"\n扫描完成，共发现 {opportunities} 个符合规则的市场标的。")
        subset_pct = (subset_count / opportunities * 100) if opportunities else 0.0
        safe_print(f"  其中 真子组合（未买全档，k<n）: {subset_count} 个，占 {subset_pct:.1f}%")
        safe_print(f"        全仓组合（k=n 买全档）  : {full_count} 个，占 {100.0 - subset_pct:.1f}%")
        safe_print(f"本次扫描耗时：{t_elapsed:.2f} 秒（共扫描 {len(events)} 个事件）")
        safe_print(f"子块组合统计：理论上限 {total_possible} 种，实际完整评估 {total_evaluated} 种"
              f"（含无卖单档位被截断的未评估组合）")
        _write_run_log(events, opportunities, t_elapsed, total_possible, total_evaluated)

        # 更新全局统计（用于每小时健康报告）
        _scan_stats["scan_count"] += 1
        _scan_stats["event_count"] = len(events)
        _scan_stats["opportunities"] = opportunities
        _scan_stats["errors"] += errors
        _scan_stats["api_errors"] += api_errors
        if errors > 0:
            send_lark_error(f"本轮扫描异常 {errors} 次（API 错误 {api_errors} 次）")

        return t_elapsed

    # 单次扫描
    first_elapsed = run_one_scan()
    _scan_stats["last_hour_ts"] = time.time()

    if args.loop:
        safe_print(f"\n进入连续轮扫模式，间隔 {args.interval} 秒，并发 {EVENT_WORKERS} 事件 ...")
        safe_print("=" * 72)
        _check_hourly_report(first_elapsed)
        while True:
            wait = args.interval
            for remaining in range(wait, 0, -1):
                safe_print(f"\r下次扫描倒计时 {remaining} 秒 ...", end="", flush=True)
                time.sleep(1)
            safe_print("\r" + " " * 50 + "\r", end="")
            safe_print(f"\n{'='*72}")
            safe_print(f"轮扫: {datetime.now(timezone.utc).astimezone().strftime('%Y-%m-%d %H:%M:%S')}")
            safe_print("=" * 72)
            try:
                elapsed = run_one_scan()
            except Exception as e:
                safe_print(f"[error] 轮扫异常: {e}")
                send_lark_error(f"轮扫异常: {e}")
                elapsed = 0
            _check_hourly_report(elapsed)


def _write_run_log(events, opportunities, elapsed, combos_possible=0, combos_evaluated=0):
    """把本次扫描的耗时与结果追加写入运行日志 scan_runs.log。"""
    ts = datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")
    runfile = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scan_runs.log")
    line = (f"[{ts}] 扫描事件 {len(events)} 个 | 命中 {opportunities} 个 | "
            f"组合 {combos_evaluated}/{combos_possible} 种评估 | "
            f"耗时 {elapsed:.2f} 秒")
    try:
        _rotate_log(runfile)
        with open(runfile, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception as e:
        print(f"[warn] 写入运行日志失败: {e}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)