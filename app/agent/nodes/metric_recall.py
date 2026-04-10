from langchain_core.output_parsers import JsonOutputParser
from langchain_core.prompts import PromptTemplate
from langgraph.runtime import Runtime

from app.agent.context import DataAgentContext
from app.agent.llm import llm
from app.agent.state import DataAgentState
from app.core.logging import logger
from app.models.qdrant.metric_info_qdrant import MetricInfoQdrant
from app.prompt.prompt_loader import load_prompt
from app.repositories.qdrant.metric_repository_qdrant import MetricQdrantRepository


async def metric_recall(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    """
    召回指标信息
    """
    # 初始化流式写入器，通知前端当前处理阶段
    writer = runtime.stream_writer
    writer({"stage": "召回指标信息"})

    # 从状态中获取初始关键词和查询语句
    keywords = state["keywords"]
    query = state["query"]
    
    # 从运行时上下文中获取嵌入客户端和指标仓库实例
    embedding_client = runtime.context['embedding_client']
    metric_qdrant_repository: MetricQdrantRepository = runtime.context['metric_qdrant_repository']

    try:
        # 加载提示词模板，用于扩展关键词以增强召回效果
        prompt = PromptTemplate(template=load_prompt("extend_keywords_for_metric_recall"), input_variables=["query"])
        output_parser = JsonOutputParser()
        # 构建执行链：提示词 -> 大语言模型 -> JSON 解析器
        chain = prompt | llm | output_parser

        # 调用大模型生成扩展关键词
        result = chain.invoke(input={"query": query})
        # 合并原始关键词与扩展关键词，并去重
        keywords = list(set(keywords + result))

        # 用于存储去重后的指标信息，键为指标 ID
        metrics_map: dict[str, MetricInfoQdrant] = {}
        
        # 遍历所有关键词进行向量检索
        for keyword in keywords:
            # 生成关键词的向量嵌入
            embedding = await embedding_client.aembed_query(keyword)
            # 在向量数据库中搜索相似指标，此处取出来的是向量库里的payload
            metrics = await metric_qdrant_repository.search(embedding, score_threshold=0.6, limit=5)
            # 将搜索结果存入地图，自动根据 ID 去重
            for metric in metrics:
                if metric['id'] not in metrics_map:
                    metrics_map[metric['id']] = metric

        # 提取最终的指标列表
        retrieved_metrics = metrics_map.values()

        # 记录日志，输出召回到的指标 ID
        logger.info(f"召回指标信息：{metrics_map.keys()}")
        return {"retrieved_metrics": retrieved_metrics}
    except Exception as e:
        # 捕获异常并记录错误日志，随后重新抛出
        logger.error(f"召回指标信息失败：{str(e)}")
        raise
