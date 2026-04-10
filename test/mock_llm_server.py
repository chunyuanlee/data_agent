"""Mock LLM Server：模拟真实 LLM 的流式响应

用途:
    - 替换 DeepSeek 等外部 LLM API
    - 在压测时完全控制响应延迟，排除第三方网络波动干扰
    - 支持流式 SSE 输出，与真实 API 行为一致

启动方式:
    python -m test.mock_llm_server --port 8080

响应行为:
    - 每个请求延迟 0.5s ~ 2.5s（随机，模拟 LLM 思考时间）
    - 流式输出 3~6 个 chunk，模拟真实 token 序列
    - 输出固定 SQL 示例，便于后续节点（validate_sql、execute_sql）正常执行
"""

import argparse
import asyncio
import json
import random
from fastapi import FastAPI
from fastapi.responses import StreamingResponse

app = FastAPI(title="Mock LLM Server")

# 固定 SQL 模板库
SQL_TEMPLATES = [
    "SELECT category, SUM(sales_amount) as total_sales FROM dim_product WHERE dt >= '2025-01-01' GROUP BY category ORDER BY total_sales DESC",
    "SELECT region, SUM(gmv) as total_gmv, AVG(completion_rate) as avg_completion FROM dwd_sales WHERE dt >= '2024-10-01' GROUP BY region",
    "SELECT DATE(visit_time) as dt, COUNT(DISTINCT user_id) as dau FROM dwd_user_action WHERE dt >= DATE_SUB(CURDATE(), INTERVAL 7 DAY) GROUP BY dt",
    "SELECT province, SUM(gmv) as total, LAG(SUM(gmv)) OVER (ORDER BY province) as prev_gmv FROM dwd_gmv WHERE dt >= '2025-02-01' GROUP BY province",
    "SELECT province, SUM(refund_amount) / SUM(gmv) as refund_rate FROM dwd_orders WHERE dt >= '2025-01-01' GROUP BY province ORDER BY refund_rate DESC LIMIT 10",
    "SELECT DATE(join_time) as dt, COUNT(user_id) as new_users FROM dim_user WHERE join_time >= DATE_SUB(CURDATE(), INTERVAL 30 DAY) GROUP BY dt",
    "SELECT hour, SUM(gmv) as hourly_gmv FROM dwd_live WHERE dt >= '2025-01-01' GROUP BY hour ORDER BY hour",
    "SELECT DATE(order_time) as dt, COUNT(DISTINCT user_id) as repurchase_users, COUNT(DISTINCT user_id) OVER () as total_users FROM dwd_orders WHERE dt >= DATE_SUB(CURDATE(), INTERVAL 30 DAY) GROUP BY dt",
    "SELECT warehouse_id, SUM(inventory) / SUM(outbound) as turnover_days FROM dwd_inventory GROUP BY warehouse_id ORDER BY turnover_days DESC",
    "SELECT SUM(gmv) as total_gmv, SUM(target_gmv) as total_target, SUM(gmv) / SUM(target_gmv) as completion_rate FROM dwd_annual WHERE year = 2024",
]

# Mock stage 信息（与真实 stream_writer 格式一致）
STAGES = [
    {"stage": "抽取关键词", "keywords": ["2025", "1月", "品类", "销售额"]},
    {"stage": "检索字段", "columns": ["category", "sales_amount", "dt"]},
    {"stage": "检索指标", "metrics": ["销售额", "GMV", "完成率"]},
    {"stage": "生成SQL", "sql": ""},
]


def _build_chunks(sql: str):
    """生成模拟的 SSE chunk 序列"""
    sql_stage_idx = 3
    chunks = []

    # 1. 基础 stage chunks
    for i, stage in enumerate(STAGES[:sql_stage_idx]):
        import time as _time
        chunk = {
            "stage": stage["stage"],
            "data": {k: v for k, v in stage.items() if k != "stage"},
            "_ts": round(random.uniform(0.05, 0.3), 3),
        }
        chunks.append(json.dumps(chunk, ensure_ascii=False))

    # 2. SQL chunk（分多次流式输出，模拟真实 token 逐字生成）
    tokens = sql.split()
    sql_chunks = []
    batch_size = max(1, len(tokens) // 4)
    for i in range(0, len(tokens), batch_size):
        part = " ".join(tokens[i : i + batch_size])
        sql_chunks.append(part)

    for i, part in enumerate(sql_chunks):
        chunk = {
            "stage": "生成SQL",
            "sql_fragment": part,
            "is_last": i == len(sql_chunks) - 1,
            "_ts": round(random.uniform(0.3, 1.5), 3),
        }
        chunks.append(json.dumps(chunk, ensure_ascii=False))

    # 3. 执行结果 chunks
    exec_chunk = {
        "stage": "execute_sql",
        "result": {
            "columns": ["category", "total_sales"],
            "rows": [
                {"category": "电子产品", "total_sales": 1234567.89},
                {"category": "服装", "total_sales": 987654.32},
            ],
            "row_count": 2,
        },
        "_ts": round(random.uniform(0.1, 0.5), 3),
    }
    chunks.append(json.dumps(exec_chunk, ensure_ascii=False))

    # 4. 最终摘要
    summary_chunk = {
        "stage": "完成",
        "summary": "查询执行成功，共返回 2 条数据",
        "_ts": round(random.uniform(0.01, 0.1), 3),
    }
    chunks.append(json.dumps(summary_chunk, ensure_ascii=False))

    return chunks


@app.post("/v1/chat/completions")
async def chat_completions(body: dict):
    """兼容 OpenAI / LangChain 的 chat completions 接口"""
    model = body.get("model", "mock-llm")
    messages = body.get("messages", [])
    stream = body.get("stream", False)

    if not stream:
        return {
            "model": model,
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "content": random.choice(SQL_TEMPLATES),
                    }
                }
            ],
        }

    # 流式模式：模拟 LLM 逐字输出
    sql = random.choice(SQL_TEMPLATES)
    chunks = _build_chunks(sql)

    async def event_stream():
        for i, chunk_data in enumerate(chunks):
            # 模拟 token 生成间隔（可变延迟，更真实）
            await asyncio.sleep(random.uniform(0.05, 0.25))
            yield f"data: {chunk_data}\n\n"
        yield "data: [DONE]\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@app.get("/health")
async def health():
    return {"status": "ok", "mock": True}


def main():
    parser = argparse.ArgumentParser(description="Mock LLM Server")
    parser.add_argument("--port", type=int, default=8080, help="监听端口 (default: 8080)")
    parser.add_argument("--delay-min", type=float, default=0.5, help="最小延迟秒数")
    parser.add_argument("--delay-max", type=float, default=2.5, help="最大延迟秒数")
    args = parser.parse_args()

    print(f"\n{'='*50}")
    print(f"  Mock LLM Server 启动")
    print(f"  端口: {args.port}")
    print(f"  延迟范围: {args.delay_min}s ~ {args.delay_max}s")
    print(f"{'='*50}\n")

    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
