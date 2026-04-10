import argparse
import asyncio
from pathlib import Path

from app.clients.embedding_client import embedding_client_manager
from app.clients.es_client import es_client_manager
from app.clients.mysql_client import dw_client_manager, meta_client_manager
from app.clients.qdrant_client import qdrant_client_manager
from app.repositories.es.value_es_repository import ValueESRepository
from app.repositories.mysql.dw_mysql_repository import DWMySQLRepository
from app.repositories.mysql.meta_mysql_repository import MetaMySQLRepository
from app.repositories.qdrant.column_repository_qdrant import ColumnQdrantRepository
from app.repositories.qdrant.metric_repository_qdrant import MetricQdrantRepository
from app.service.meta_knowledge_service import MetaKnowledgeService


async def build(meta_config: Path):
    # 初始化数据仓库客户端管理器
    dw_client_manager.init()
    # 初始化元数据客户端管理器
    meta_client_manager.init()
    # 初始化嵌入模型客户端管理器
    embedding_client_manager.init()
    # 初始化 Qdrant 向量数据库客户端管理器
    qdrant_client_manager.init()
    # 初始化 Elasticsearch 客户端管理器
    es_client_manager.init()
    # 异步创建数据仓库和元数据的数据库会话，并在使用完毕后自动关闭
    async with dw_client_manager.session_factory() as dw_session, meta_client_manager.session_factory() as meta_session:
        # 实例化数据仓库 MySQL 仓储类
        dw_mysql_repository = DWMySQLRepository(dw_session)
        # 实例化元数据 MySQL 仓储类
        meta_mysql_repository = MetaMySQLRepository(meta_session)

        # 实例化列信息的 Qdrant 仓储类
        column_qdrant_repository = ColumnQdrantRepository(qdrant_client_manager.client)
        # 实例化指标信息的 Qdrant 仓储类
        metric_qdrant_repository = MetricQdrantRepository(qdrant_client_manager.client)
        # 获取嵌入模型客户端实例
        embedding_client = embedding_client_manager.client
        # 实例化值信息的 Elasticsearch 仓储类
        value_es_repository = ValueESRepository(es_client_manager.client)

        # 实例化元知识服务，注入所有必要的依赖组件
        meta_knowledge_service = MetaKnowledgeService(
            dw_mysql_repository=dw_mysql_repository,
            meta_mysql_repository=meta_mysql_repository,
            embedding_client=embedding_client,
            column_qdrant_repository=column_qdrant_repository,
            metric_qdrant_repository=metric_qdrant_repository,
            value_es_repository=value_es_repository
        )
        # 执行构建元知识的核心逻辑
        await meta_knowledge_service.build_meta_knowledge(meta_config)

    # 关闭数据仓库客户端连接
    await dw_client_manager.close()
    # 关闭元数据客户端连接
    await meta_client_manager.close()
    # 关闭 Qdrant 客户端连接
    await qdrant_client_manager.close()
    # 关闭 Elasticsearch 客户端连接
    await es_client_manager.close()


if __name__ == '__main__':
    """解析命令行参数"""
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", required=True)
    args = parser.parse_args()

    asyncio.run(build(Path(args.config)))
