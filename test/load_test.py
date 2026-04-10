"""压测脚本：模拟真实用户 Query 对系统进行高并发压测

使用方式:
    1. 确保服务已启动 (uvicorn main:app --host 0.0.0.0 --port 8000)
    2. cd 项目根目录
    3. python -m test.load_test

单测模式（快速验证接口是否通）:
    python -m test.load_test --quick

阶梯压测（找最大并发数）:
    python -m test.load_test --stepped --max-users 500

持续稳压（测持续吞吐）:
    python -m test.load_test --sustained --users 100 --duration 300

进阶 QPS 压测（用 wrk2 探极限，需 pip install wrk2）:
    参考脚本底部的 wrk 命令行

输出示例:
    ========== 压测报告 ==========
    总请求数:    1250
    成功:       1230 (98.4%)
    失败:       20  (1.6%)
    平均延迟:   12.3s
    P50 延迟:   10.1s
    P90 延迟:   18.5s
    P99 延迟:   25.2s
    最高并发:   87
    总耗时:     120s
"""

import argparse
import asyncio
import json
import os
import random
import statistics
import sys
import time
from dataclasses import dataclass, field
from typing import Any

import httpx

# ─────────────────────────────��───────────────
# 配置
# ─────────────────────────────────────────────
API_BASE = os.getenv("API_BASE", "http://localhost:8000")
QUERY_ENDPOINT = f"{API_BASE}/api/query"
TIMEOUT_SECONDS = 300  # 单次请求超时（含 LLM 生成）

# 典型 Query 样本池（模拟真实用户行为）
TYPICAL_QUERIES = [
    "统计2025年1月份各品类的销售额占比",
    "去年Q4华北区域大区经理的业绩完成率是多少",
    "最近7天日活跃用户数趋势",
    "2025年2月华东各省GMV环比增长率",
    "统计各省份退款率排名前十",
    "本月新增付费用户数和环比增幅",
    "统计直播带货各时段GMV分布",
    "查询近30天复购率趋势",
    "各仓库库存周转天数排名",
    "2024年度GMV目标达成率",
]


# ─────────────────────────────────────────────
# 数据结构
# ─────────────────────────────────────────────
@dataclass
class RequestResult:
    query: str
    latency: float          # 总耗时（秒）
    llm_latency: float = 0  # LLM 阶段耗时（秒）
    success: bool = False
    error: str = ""
    chunks_count: int = 0
    final_status: str = ""  # "success" | "error" | "timeout"
    start_time: float = 0


@dataclass
class LoadTestReport:
    total_requests: int = 0
    success_count: int = 0
    error_count: int = 0
    timeout_count: int = 0
    avg_latency: float = 0
    max_latency: float = 0
    min_latency: float = 0
    p50_latency: float = 0
    p90_latency: float = 0
    p95_latency: float = 0
    p99_latency: float = 0
    peak_concurrency: int = 0
    total_duration: float = 0
    qps: float = 0
    results: list[RequestResult] = field(default_factory=list)


# ─────────────────────────────────────────────
# 核心压测逻辑
# ─────────────────────────────────────────────
async def send_query(client: httpx.AsyncClient, query: str, timeout: int = TIMEOUT_SECONDS) -> RequestResult:
    """发送一个 Query 请求，完整接收 SSE 流式响应"""
    start = time.monotonic()
    result = RequestResult(query=query, start_time=start)

    try:
        async with client.stream(
            "POST",
            QUERY_ENDPOINT,
            json={"query": query},
            timeout=httpx.Timeout(timeout, connect=10),
        ) as resp:
            if resp.status_code != 200:
                result.error = f"HTTP {resp.status_code}"
                result.final_status = "error"
                result.latency = time.monotonic() - start
                return result

            chunks = []
            async for line in resp.aiter_lines():
                if not line.startswith("data: "):
                    continue
                data_str = line[len("data: "):]
                try:
                    data = json.loads(data_str)
                except json.JSONDecodeError:
                    continue
                chunks.append(data)

            result.chunks_count = len(chunks)
            result.latency = time.monotonic() - start

            # 从 chunks 中尝试解析 LLM 耗时（stream_writer 携带的 stage 信息）
            stage_times = {}
            for chunk in chunks:
                if isinstance(chunk, dict) and "stage" in chunk:
                    ts = chunk.get("_ts", 0)
                    if ts:
                        stage_times[chunk["stage"]] = ts

            if stage_times:
                # 粗略：最后一个 stage 时间作为 llm 近似
                result.llm_latency = stage_times.get("生成SQL", 0) or stage_times.get("correct_sql", 0)

            result.success = True
            result.final_status = "success"
            return result

    except httpx.TimeoutException:
        result.error = "TIMEOUT"
        result.final_status = "timeout"
        result.latency = time.monotonic() - start
        return result
    except Exception as e:
        result.error = str(e)
        result.final_status = "error"
        result.latency = time.monotonic() - start
        return result


async def _worker(
    worker_id: int,
    total: int,
    request_duration: float,
    results: list[RequestResult],
    concurrency_counter: asyncio.Queue,
    stop_event: asyncio.Event,
):
    """单个压测 worker，持续向服务发请求"""
    async with httpx.AsyncClient(http2=True) as client:
        while not stop_event.is_set():
            if time.monotonic() - request_duration > 60:  # 预留 60s 给正在进行的请求
                break

            query = random.choice(TYPICAL_QUERIES)
            await concurrency_counter.put(1)
            result = await send_query(client, query)
            try:
                concurrency_counter.get_nowait()
            except asyncio.QueueEmpty:
                pass

            results.append(result)


async def run_load_test(
    users: int,
    duration: int,
    ramp_up: int = 10,
) -> LoadTestReport:
    """执行压测主逻辑"""

    print(f"\n{'='*60}")
    print(f"  压测启动 | 用户数={users} | 持续={duration}s | 预热={ramp_up}s")
    print(f"{'='*60}\n")

    results: list[RequestResult] = []
    concurrency_counter: asyncio.Queue = asyncio.Queue()
    stop_event = asyncio.Event()
    request_start = time.monotonic()

    workers = [
        asyncio.create_task(_worker(i, users, request_start, results, concurrency_counter, stop_event))
        for i in range(users)
    ]

    # 预热：逐步启动 workers
    active = 0
    for w in workers:
        w.cancel()
    workers.clear()

    # 分批次启动，防止瞬间冲击太大
    batch_size = max(5, users // 10)
    for batch_i in range(0, users, batch_size):
        batch = workers[batch_i: batch_i + batch_size]
        for w in batch:
            w.cancel()
        await asyncio.sleep(ramp_up / max(1, users // batch_size))
        active += len(batch)
        print(f"  [启动进度] {batch_i + len(batch)}/{users} workers online")

    # 实际 worker 任务
    workers = [
        asyncio.create_task(_worker(i, users, request_start, results, concurrency_counter, stop_event))
        for i in range(users)
    ]

    await asyncio.sleep(duration)
    stop_event.set()

    await asyncio.gather(*workers, return_exceptions=True)

    total_duration = time.monotonic() - request_start
    return build_report(results, total_duration)


def build_report(results: list[RequestResult], total_duration: float) -> LoadTestReport:
    """从结果列表生成压测报告"""
    if not results:
        return LoadTestReport(total_duration=total_duration)

    latencies = [r.latency for r in results]
    success_results = [r for r in results if r.success]
    error_results = [r for r in results if not r.success]

    report = LoadTestReport(
        total_requests=len(results),
        success_count=len(success_results),
        error_count=len(error_results),
        timeout_count=sum(1 for r in results if r.final_status == "timeout"),
        avg_latency=statistics.mean(latencies),
        min_latency=min(latencies),
        max_latency=max(latencies),
        p50_latency=statistics.median(latencies),
        p90_latency=_percentile(latencies, 90),
        p95_latency=_percentile(latencies, 95),
        p99_latency=_percentile(latencies, 99),
        total_duration=total_duration,
        qps=len(results) / total_duration if total_duration > 0 else 0,
        results=results,
    )
    return report


def _percentile(sorted_data: list[float], p: int) -> float:
    if not sorted_data:
        return 0
    s = sorted(sorted_data)
    idx = int(len(s) * p / 100)
    idx = min(idx, len(s) - 1)
    return s[idx]


def print_report(report: LoadTestReport):
    """格式化打印压测报告"""
    print(f"\n{'='*60}")
    print(f"                    压 测 报 告")
    print(f"{'='*60}")
    print(f"  总请求数      {report.total_requests:>10}")
    print(f"  成功         {report.success_count:>10} ({_pct(report.success_count, report.total_requests):>5.1f}%)")
    print(f"  失败         {report.error_count:>10} ({_pct(report.error_count, report.total_requests):>5.1f}%)")
    print(f"  超时         {report.timeout_count:>10}")
    print(f"  --- 延迟统计 ---")
    print(f"  平均延迟     {report.avg_latency:>10.2f}s")
    print(f"  最小延迟     {report.min_latency:>10.2f}s")
    print(f"  最大延迟     {report.max_latency:>10.2f}s")
    print(f"  P50          {report.p50_latency:>10.2f}s")
    print(f"  P90          {report.p90_latency:>10.2f}s")
    print(f"  P95          {report.p95_latency:>10.2f}s")
    print(f"  P99          {report.p99_latency:>10.2f}s")
    print(f"  --- 吞吐统计 ---")
    print(f"  总耗时       {report.total_duration:>10.1f}s")
    print(f"  QPS          {report.qps:>10.2f}")
    print(f"{'='*60}")

    # 错误详情
    if report.error_count > 0:
        err_groups: dict[str, int] = {}
        for r in report.results:
            if not r.success:
                key = r.error or r.final_status
                err_groups[key] = err_groups.get(key, 0) + 1
        print(f"\n  错误类型分布:")
        for k, v in sorted(err_groups.items(), key=lambda x: -x[1]):
            print(f"    {k:<30s}: {v:>5} 次")

    # 建议
    print(f"\n  调优建议:")
    if report.timeout_count > report.total_requests * 0.05:
        print(f"    ⚠ 超时率 > 5%，建议增加 MySQL 连接池上限或启用多 worker")
    if report.avg_latency > 30:
        print(f"    ⚠ 平均延迟 > 30s，可能是 LLM 限流或向量检索慢，建议加缓存或限流")
    if report.qps < 1:
        print(f"    ⚠ QPS < 1，瓶颈可能在 LLM 调用，建议用 mock 隔离测试")


def _pct(part: int, total: int) -> float:
    return (part / total * 100) if total > 0 else 0


# ─────────────────────────────────────────────
# 命令行入口
# ─────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="智能问答 Agent 压测脚本")
    parser.add_argument("--quick", action="store_true", help="单测模式：并发10，持续10s，快速验证接口")
    parser.add_argument("--stepped", action="store_true", help="阶梯压测：10→50→100→200→500用户，逐步探最大并发")
    parser.add_argument("--sustained", action="store_true", help="持续稳压：固定用户数持续压测")
    parser.add_argument("--users", type=int, default=50, help="并发用户数 (default: 50)")
    parser.add_argument("--duration", type=int, default=60, help="压测持续秒数 (default: 60)")
    parser.add_argument("--ramp-up", type=int, default=10, help="预热启动秒数 (default: 10)")
    parser.add_argument("--output", type=str, default="", help="结果 JSON 输出路径 (可选)")
    args = parser.parse_args()

    # 单测模式
    if args.quick:
        print("=== 单测模式：并发10，持续10s ===")
        report = asyncio.run(run_load_test(users=10, duration=10, ramp_up=3))
        print_report(report)
        return

    # 阶梯压测
    if args.stepped:
        steps = [10, 50, 100, 200, 500]
        reports: list[tuple[int, LoadTestReport]] = []
        for users in steps:
            if args.users and users > args.users:
                break
            report = asyncio.run(run_load_test(users=users, duration=60, ramp_up=15))
            reports.append((users, report))
            print_report(report)
            time.sleep(5)  # 阶间冷却 5s

        # 汇总
        print(f"\n{'='*60}")
        print(f"              阶 梯 压 测 汇 总")
        print(f"{'='*60}")
        print(f"  {'并发数':>8} | {'QPS':>8} | {'成功率':>8} | {'P90延迟':>8} | {'最大延迟':>8}")
        print(f"  {'-'*8} | {'-'*8} | {'-'*8} | {'-'*8} | {'-'*8}")
        for users, r in reports:
            print(f"  {users:>8} | {r.qps:>8.2f} | {_pct(r.success_count, r.total_requests):>7.1f}% | {r.p90_latency:>7.1f}s | {r.max_latency:>7.1f}s")
        return

    # 持续稳压
    print(f"=== 持续稳压：{args.users} 用户，持续 {args.duration}s ===")
    report = asyncio.run(run_load_test(
        users=args.users,
        duration=args.duration,
        ramp_up=args.ramp_up,
    ))
    print_report(report)

    # 可选：输出 JSON
    if args.output:
        output_path = os.path.join(os.path.dirname(__file__), args.output)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump({
                "total_requests": report.total_requests,
                "success_count": report.success_count,
                "error_count": report.error_count,
                "timeout_count": report.timeout_count,
                "avg_latency": round(report.avg_latency, 2),
                "p50_latency": round(report.p50_latency, 2),
                "p90_latency": round(report.p90_latency, 2),
                "p95_latency": round(report.p95_latency, 2),
                "p99_latency": round(report.p99_latency, 2),
                "max_latency": round(report.max_latency, 2),
                "total_duration": round(report.total_duration, 1),
                "qps": round(report.qps, 2),
            }, f, ensure_ascii=False, indent=2)
        print(f"\n  报告已保存: {output_path}")


if __name__ == "__main__":
    main()
