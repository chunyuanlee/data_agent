# 压测脚本说明

## 快速开始

### 1. 依赖安装

```bash
cd 项目根目录
pip install -r test/requirements.txt
```

### 2. 启动服务

```bash
# 在一个终端启动主服务（单 worker 模式）
uvicorn main:app --host 0.0.0.0 --port 8000

# 生产环境多 worker 模式（参考调优用）
# uvicorn main:app --workers 4 --host 0.0.0.0 --port 8000
```

### 3. 执行压测

#### 模式一：快速验证（接口连通性测试）

```bash
python -m test.load_test --quick
```

**预期输出：** 10 并发用户，持续 10 秒，验证接口正常

---

#### 模式二：阶梯压测（找最大并发数）

```bash
python -m test.load_test --stepped
```

**执行流程：**
```
10用户/60s → 等待5s冷却 → 50用户/60s → 等待5s冷却
→ 100用户/60s → 等待5s冷却 → 200用户/60s → 等待5s冷却
→ 500用户/60s
```

**观察重点：** 每次阶梯成功后，延迟是否急剧上升，若出现大量超时则到达瓶颈

---

#### 模式三：持续稳压（测持续吞吐）

```bash
# 基础稳压
python -m test.load_test --sustained --users 100 --duration 300

# 高并发稳压
python -m test.load_test --sustained --users 300 --duration 300 --ramp-up 30
```

**观察重点：** QPS 是否稳定，平均延迟和 P99 延迟趋势

---

#### 模式四：输出报告

```bash
python -m test.load_test --sustained --users 100 --duration 120 --output load_report.json
# 报告保存至 test/load_report.json
```

---

### 4. Mock 模式压测（隔离 LLM 干扰）

当需要排除 DeepSeek 等第三方 LLM API 的网络延迟影响时，使用 Mock 模式：

**步骤：**

1. 修改 `app/config/app_config.yaml`，将 LLM 配置指向本地 mock：

```yaml
llm:
  model_name: "http://localhost:8080/v1/chat/completions"
  api_key: "mock"
```

2. 启动 Mock LLM Server：

```bash
python -m test.mock_llm_server --port 8080
```

3. 启动主服务后执行压测：

```bash
python -m test.load_test_mock --users 100 --duration 120
```

**用途：** 精准测试 FastAPI + LangGraph + MySQL + Qdrant + ES 的并发吞吐量

---

## 压测报告解读

### 关键指标

| 指标 | 健康值 | 警告值 | 危险值 |
|---|---|---|---|
| QPS | > 10 | 5 ~ 10 | < 5 |
| 平均延迟 | < 5s | 5 ~ 15s | > 15s |
| P99 延迟 | < 15s | 15 ~ 30s | > 30s |
| 超时率 | < 1% | 1% ~ 5% | > 5% |
| 成功率 | > 99% | 95% ~ 99% | < 95% |

### 常见瓶颈定位

#### 1. MySQL 连接池耗尽
**症状：** 大量 `TIMEOUT`，QPS 无法提升，错误集中在 `pool exhausted`
**定位：** 观察压测报告中错误分布，若出现大量 `pool` 相关错误

**调优：**

```python
# app/clients/mysql_client.py
pool_size=50        # 原值 10
max_overflow=100     # 原值 20
```

---

#### 2. LLM API 限流
**症状：** 高并发时延迟激增，成功率下降，`TIMEOUT` 增多
**定位：** 切换 Mock 模式压测，若 Mock 模式正常则说明是 LLM 服务商限流

**调优：**
```python
# app/agent/nodes/generate_sql.py
import asyncio
llm_semaphore = asyncio.Semaphore(10)  # 限制同时最多10个 LLM 调用

async def generate_sql(...):
    async with llm_semaphore:
        result = await chain.ainvoke(...)
```

---

#### 3. Python GIL 瓶颈（单进程）
**症状：** CPU 使用率低，但并发上不去（尤其在纯 CPU 任务时）
**定位：** `top` 或 `ps` 观察 uvicorn 进程数

**调优：**
```bash
# 多 worker 启动
uvicorn main:app --workers 4 --host 0.0.0.0 --port 8000
```

---

## 压测路线图（本地单机）

```
Day 1: 快速验证
  → python -m test.load_test --quick
  → 确认接口正常，无报��

Day 2: 基线压测（单 worker）
  → uvicorn main:app --host 0.0.0.0 --port 8000
  → python -m test.load_test --stepped
  → 记录：最大并发数、平均延迟、QPS 上限

Day 3: 调优 & 复测
  → MySQL 连接池调大
  → LLM 限流保护
  → 再次 --stepped 复测，对比提升效果

Day 4: 高并发稳压
  → uvicorn main:app --workers 4 ...
  → python -m test.load_test --sustained --users 200 --duration 300
  → 观察 5 分钟内系统是否稳定

Day 5: Mock 隔离测试
  → 启动 mock_llm_server
  → 修改配置指向本地
  → python -m test.load_test_mock --users 200 --duration 300
  → 精准评估系统本身的吞吐量上限
```

## 注意事项

- **压测环境隔离**：压测时务必使用测试数据库，不要压生产数据
- **网络因素**：本地压测时注意本机资源（CPU/内存/网络）是否成为瓶颈
- **延迟含义**：本系统延迟 = FastAPI 接收 + LangGraph 链路 + LLM 生成 + MySQL 执行，其中 LLM 生成可能占 80%+
- **流式响应**：本系统使用 SSE 流式输出，压测脚本完整接收了所有 chunks，能真实反映用户体验