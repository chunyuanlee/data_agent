"""压测脚本：Mock 掉 LLM，隔离测试系统吞吐量

用途:
    - 排除 LLM 网络延迟干扰
    - 专注测试: FastAPI + LangGraph + MySQL + Qdrant + ES 的真实并发能力
    - 快速定位数据库连接池、向量检索等底层瓶颈

使用方式:
    1. 先安装 mock-server: pip install fastapi uvicorn
    2. 启动 mock LLM server: python -m test.mock_llm_server
    3. 修改 app/agent/nodes/generate_sql.py 中的 LLM 调用为本机 mock 地址
    4. python -m test.load_test_mock

注意:
    mock_llm_server.py 仅模拟 LLM 返回固定 SQL，延迟 0.5s ~ 2s 随机，
    模拟真实 LLM 的"思考时间"以便测试并发队列饱和场景。
"""

import argparse
import asyncio
import json
import os
import random
import statistics
import time
import httpx

API_BASE = os.getenv("API_BASE", "http://localhost:8000")
QUERY_ENDPOINT = f"{API_BASE}/api/query"
TIMEOUT_SECONDS = 120


async def send_query(client: httpx.AsyncClient, query: str, timeout: int = TIMEOUT_SECONDS):
    start = time.monotonic()
    try:
        async with client.stream(
            "POST",
            QUERY_ENDPOINT,
            json={"query": query},
            timeout=httpx.Timeout(timeout, connect=10),
        ) as resp:
            chunks = 0
            async for line in resp.aiter_lines():
                if line.startswith("data: "):
                    chunks += 1
            return {
                "query": query,
                "latency": time.monotonic() - start,
                "success": resp.status_code == 200,
                "chunks": chunks,
                "error": "" if resp.status_code == 200 else f"HTTP {resp.status_code}",
            }
    except httpx.TimeoutException:
        return {"query": query, "latency": time.monotonic() - start, "success": False, "chunks": 0, "error": "TIMEOUT"}
    except Exception as e:
        return {"query": query, "latency": time.monotonic() - start, "success": False, "chunks": 0, "error": str(e)}


async def worker(worker_id: int, queries: list[str], results: list, stop_event: asyncio.Event):
    async with httpx.AsyncClient(http2=True) as client:
        idx = 0
        while not stop_event.is_set():
            query = queries[idx % len(queries)]
            idx += 1
            r = await send_query(client, query)
            results.append(r)


async def run_mock_test(users: int, duration: int):
    print(f"\n{'='*55}")
    print(f"  Mock 压测 | 并发用户={users} | 持续={duration}s")
    print(f"{'='*55}\n")

    queries = [
        "统计2025年1月份各品类的销售额占比",
        "最近7天日活跃用户数趋势",
        "查询近30天复购率趋势",
        "各省份退款率排名前十",
        "本月新增付费用户数和环比增幅",
    ]

    results: list = []
    stop_event = asyncio.Event()

    tasks = [asyncio.create_task(worker(i, queries, results, stop_event)) for i in range(users)]
    await asyncio.sleep(duration)
    stop_event.set()
    await asyncio.gather(*tasks, return_exceptions=True)

    _print_mock_report(results)


def _pct(part: int, total: int) -> float:
    return (part / total * 100) if total > 0 else 0


def _percentile(sorted_data: list[float], p: int) -> float:
    if not sorted_data:
        return 0
    s = sorted(sorted_data)
    idx = int(len(s) * p / 100)
    idx = min(idx, len(s) - 1)
    return s[idx]


def _print_mock_report(results: list):
    if not results:
        print("无结果")
        return

    total = len(results)
    success = sum(1 for r in results if r["success"])
    error = total - success
    timeouts = sum(1 for r in results if r["error"] == "TIMEOUT")
    latencies = [r["latency"] for r in results]

    duration = max(r["latency"] for r in results) if results else 1
    qps = total / duration

    print(f"\n{'='*55}")
    print(f"               Mock 压 测 报 告")
    print(f"{'='*55}")
    print(f"  总请求数      {total:>10}")
    print(f"  成功         {success:>10} ({_pct(success, total):>5.1f}%)")
    print(f"  失败         {error:>10} ({_pct(error, total):>5.1f}%)")
    print(f"  超时         {timeouts:>10}")
    print(f"  --- 延迟统计 ---")
    print(f"  平均延迟     {statistics.mean(latencies):>10.2f}s")
    print(f"  P50          {statistics.median(latencies):>10.2f}s")
    print(f"  P90          {_percentile(latencies, 90):>10.2f}s")
    print(f"  P95          {_percentile(latencies, 95):>10.2f}s")
    print(f"  P99          {_percentile(latencies, 99):>10.2f}s")
    print(f"  最大延迟     {max(latencies):>10.2f}s")
    print(f"  QPS          {qps:>10.2f}")
    print(f"{'='*55}")

    if error > 0:
        err_groups = {}
        for r in results:
            if not r["success"]:
                err_groups[r["error"]] = err_groups.get(r["error"], 0) + 1
        print(f"\n  错误分布:")
        for k, v in sorted(err_groups.items(), key=lambda x: -x[1]):
            print(f"    {k:<30s}: {v:>5} 次")

    # 简单建议
    avg_lat = statistics.mean(latencies)
    if avg_lat > 5:
        print(f"\n  ⚠ 平均延迟 > 5s，瓶颈可能在 MySQL 查询或向量检索")
    if error > total * 0.1:
        print(f"\n  ⚠ 错误率 > 10%，可能是连接池耗尽，建议检查 MySQL pool_size")
    if qps < 5:
        print(f"\n  ⚠ QPS < 5，瓶颈可能在同步阻塞节点，建议 profile 定位")


def main():
    parser = argparse.ArgumentParser(description="Mock LLM 压测")
    parser.add_argument("--users", type=int, default=50, help="并发用户数")
    parser.add_argument("--duration", type=int, default=60, help="持续秒数")
    args = parser.parse_args()
    asyncio.run(run_mock_test(args.users, args.duration))


if __name__ == "__main__":
    main()
