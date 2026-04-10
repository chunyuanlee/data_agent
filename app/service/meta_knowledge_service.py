import uuid

from langchain_huggingface import HuggingFaceEndpointEmbeddings

from app.config.config_loader import load_config
from app.config.meta_config import MetaConfig, TableConfig, MetricConfig
from app.core.logging import logger
from app.models.es.value_info_es import ValueInfoES
from app.models.mysql.column_info_mysql import ColumnInfoMySQL
from app.models.mysql.column_metric_mysql import ColumnMetricMySQL
from app.models.mysql.metric_info_mysql import MetricInfoMySQL
from app.models.mysql.table_info_mysql import TableInfoMySQL
from app.models.qdrant.column_info_qdrant import ColumnInfoQdrant
from app.models.qdrant.metric_info_qdrant import MetricInfoQdrant
from app.repositories.es.value_es_repository import ValueESRepository
from app.repositories.mysql.dw_mysql_repository import DWMySQLRepository
from app.repositories.mysql.meta_mysql_repository import MetaMySQLRepository
from app.repositories.qdrant.column_repository_qdrant import ColumnQdrantRepository
from app.repositories.qdrant.metric_repository_qdrant import MetricQdrantRepository


class MetaKnowledgeService:

    def __init__(self,
                 dw_mysql_repository: DWMySQLRepository,
                 meta_mysql_repository: MetaMySQLRepository,
                 embedding_client: HuggingFaceEndpointEmbeddings,
                 column_qdrant_repository: ColumnQdrantRepository,
                 metric_qdrant_repository: MetricQdrantRepository,
                 value_es_repository: ValueESRepository
                 ):
        self.dw_repository = dw_mysql_repository
        self.meta_repository = meta_mysql_repository
        self.embedding_client = embedding_client
        self.column_qdrant_repository = column_qdrant_repository
        self.metric_qdrant_repository = metric_qdrant_repository
        self.full_text_repository = value_es_repository

    async def _save_tables_to_meta_db(self, tables: list[TableConfig]):
        # 初始化存储表信息的列表
        table_infos: list[TableInfoMySQL] = []
        # 初始化存储列信息的列表
        column_infos: list[ColumnInfoMySQL] = []

        # 遍历每个表配置
        for table in tables:
            # 创建表信息对象
            table_info = TableInfoMySQL(
                id=table.name,
                name=table.name,
                role=table.role,
                description=table.description
            )
            # 将表信息添加到列表中
            table_infos.append(table_info)

            # 获取当前表的所有列类型
            column_types = await self.dw_repository.get_column_types(table.name)
            # 遍历表中的每一列
            for column in table.columns:
                # 获取列的示例值（最多10个）
                column_values = await self.dw_repository.get_column_values(table.name, column.name, 10)

                # 创建列信息对象，里面的example和type需要特殊处理得到值，其他都是column的属性
                column_info = ColumnInfoMySQL(
                    id=f"{table.name}.{column.name}",
                    name=column.name,
                    type=column_types[column.name], # 列类型
                    role=column.role,
                    examples=column_values, # 列示例值
                    description=column.description,
                    alias=column.alias,
                    table_id=table.name
                )
                # 将列信息添加到列表中
                column_infos.append(column_info)

        # 2.保存元数据库信息到meta数据库
        async with self.meta_repository.session.begin():
            # 批量保存表信息
            await self.meta_repository.save_table_infos(table_infos)
            # 批量保存列信息
            await self.meta_repository.save_column_infos(column_infos)

        # 返回保存的表信息和列信息
        return table_infos, column_infos

    def _convert_column_info_from_mysql_to_qdrant(self, column_info: ColumnInfoMySQL) -> ColumnInfoQdrant:
        return ColumnInfoQdrant(
            id=column_info.id,
            name=column_info.name,
            type=column_info.type,
            role=column_info.role,
            examples=column_info.examples,
            description=column_info.description,
            alias=column_info.alias,
            table_id=column_info.table_id
        )

    def _convert_metric_info_from_mysql_to_qdrant(self, metric_info: MetricInfoMySQL) -> MetricInfoQdrant:
        return MetricInfoQdrant(
            id=metric_info.id,
            name=metric_info.name,
            description=metric_info.description,
            relevant_columns=metric_info.relevant_columns,
            alias=metric_info.alias
        )

    async def _sync_columns_to_qdrant(self, columns: list[ColumnInfoMySQL]):
        # 创建 qdrant collection，确保集合存在
        await self.column_qdrant_repository.ensure_collection()

        # 初始化用于存储待插入记录的列表
        records: list[dict] = []
        # 遍历每一个列信息对象
        for column_info in columns:
            # 添加基于列名的记录
            records.append(
                {'id': uuid.uuid4(),
                 'embedding_text': column_info.name,
                 'payload': self._convert_column_info_from_mysql_to_qdrant(column_info)})
            # 添加基于列描述的记录
            records.append(
                {'id': uuid.uuid4(), 'embedding_text': column_info.description,
                 'payload': self._convert_column_info_from_mysql_to_qdrant(column_info)})
            # 遍历列的每一个别名，添加基于别名的记录
            for alias in column_info.alias:
                records.append(
                    {'id': uuid.uuid4(), 'embedding_text': alias,
                     'payload': self._convert_column_info_from_mysql_to_qdrant(column_info)})

        # 为每条记录生成一个新的唯一 ID
        ids = [uuid.uuid4() for _ in records]
        # 初始化存储向量嵌入的列表
        embeddings = []
        # 设置批量处理的大小
        embedding_batch_size = 20
        # 分批处理记录以获取嵌入向量
        for i in range(0, len(records), embedding_batch_size):
            # 获取当前批次的记录
            batch_record = records[i:i + embedding_batch_size]
            # 提取当前批次所有记录的文本内容
            batch_embedding_text = [record['embedding_text'] for record in batch_record]
            # 异步调用嵌入客户端获取批次文本的向量嵌入
            batch_embeddings = await self.embedding_client.aembed_documents(batch_embedding_text)
            # 将批次结果扩展到总列表中
            embeddings.extend(batch_embeddings)
        # 提取所有记录的负载数据（payload）
        payloads = [record['payload'] for record in records]

        # 将生成的 ID、嵌入向量和负载数据批量插入到 Qdrant 中
        await self.column_qdrant_repository.upsert(ids, embeddings, payloads)

    async def _save_metrics_to_meta_db(self, metrics: list[MetricConfig]) -> list[MetricInfoMySQL]:
        # 初始化存储指标信息的列表
        metric_infos: list[MetricInfoMySQL] = []
        # 初始化存储列与指标关联关系的列表
        column_metrics: list[ColumnMetricMySQL] = []
        # 遍历每一个指标配置对象
        for metric in metrics:
            # 创建指标信息对象
            metric_info = MetricInfoMySQL(
                id=metric.name,
                name=metric.name,
                description=metric.description,
                relevant_columns=metric.relevant_columns,
                alias=metric.alias
            )
            # 将指标信息添加到列表中
            metric_infos.append(metric_info)

            # 遍历该指标关联的所有列
            for column in metric.relevant_columns:
                # 创建列与指标的关联对象
                column_metric = ColumnMetricMySQL(
                    column_id=column,
                    metric_id=metric.name
                )
                # 将关联对象添加到列表中
                column_metrics.append(column_metric)
        # 开启数据库事务会话
        async with self.meta_repository.session.begin():
            # 批量保存指标信息到数据库
            await self.meta_repository.save_metric_infos(metric_infos)
            # 批量保存列与指标的关联关系到数据库
            await self.meta_repository.save_column_metrics(column_metrics)
        # 返回保存成功的指标信息列表
        return metric_infos

    async def _sync_values_to_es(self, table_infos: list[TableInfoMySQL],
                                 column_infos: list[ColumnInfoMySQL],
                                 meta_config: MetaConfig):
        await self.full_text_repository.ensure_index()

        values: list[ValueInfoES] = []
        table_id2name = {table_info.id: table_info.name for table_info in table_infos}
        column_id2sync = {}
        for table in meta_config.tables:
            for column in table.columns:
                column_id2sync[f"{table.name}.{column.name}"] = column.sync

        for column_info in column_infos:
            table_name = table_id2name[column_info.table_id]
            column_name = column_info.name
            sync = column_id2sync[column_info.id]
            # 判断配置里，该字段是否需要同步到es中做全文索引，一般是只处理dim表的关键字段，事实表不太需要
            if sync:
                # 基本算取出所有的值了
                column_value = await self.dw_repository.get_column_values(table_name, column_name, 100000)
                # 把值结构化封装
                values.extend([ValueInfoES(
                    id=f"{table_name}.{column_name}.{value}",
                    value=value,
                    type=column_info.type,
                    column_id=column_info.id,
                    column_name=column_info.name,
                    table_id=column_info.table_id,
                    table_name=table_name
                ) for value in column_value])
        await self.full_text_repository.batch_index(values)

    async def _sync_metrics_to_qdrant(self, metric_infos: list[MetricInfoMySQL]):
        # 创建qdrant collection
        await self.metric_qdrant_repository.ensure_collection()

        records: list[dict] = []
        for metric_info in metric_infos:
            records.append(
                {'id': uuid.uuid4(),
                 'embedding_text': metric_info.name,
                 'payload': self._convert_metric_info_from_mysql_to_qdrant(metric_info)})
            records.append(
                {'id': uuid.uuid4(),
                 'embedding_text': metric_info.description,
                 'payload': self._convert_metric_info_from_mysql_to_qdrant(metric_info)})
            for alias in metric_info.alias:
                records.append(
                    {'id': uuid.uuid4(),
                     'embedding_text': alias,
                     'payload': self._convert_metric_info_from_mysql_to_qdrant(metric_info)})
        ids = [uuid.uuid4() for _ in records]
        embeddings = []
        embedding_batch_size = 20
        for i in range(0, len(records), embedding_batch_size):
            batch_record = records[i:i + embedding_batch_size]
            batch_embedding_text = [record['embedding_text'] for record in batch_record]
            batch_embeddings = await self.embedding_client.aembed_documents(batch_embedding_text)
            embeddings.extend(batch_embeddings)
        payloads = [record['payload'] for record in records]

        await self.metric_qdrant_repository.upsert(ids, embeddings, payloads)

    async def build_meta_knowledge(self, config_file):
        # 1.加载配置文件
        meta_config: MetaConfig = load_config(MetaConfig, config_file)
        logger.info('加载元数据配置文件')
        if meta_config.tables:
            # 2.保存表信息到meta数据库
            table_infos, column_infos = await self._save_tables_to_meta_db(meta_config.tables)
            logger.info('保存表信息和字段信息到meta数据库')
            # 3.同步字段信息到qdrant
            await self._sync_columns_to_qdrant(column_infos)
            logger.info('同步字段信息到qdrant')
            # 4.同步字段数据到es
            await self._sync_values_to_es(table_infos, column_infos, meta_config)
            logger.info('同步字段值到es')
        if meta_config.metrics:
            # 3.保存metrics信息到meta数据库
            metric_infos = await self._save_metrics_to_meta_db(meta_config.metrics)
            logger.info('保存metric信息到meta数据库')

            # 6.同步metric信息到qdrant
            await self._sync_metrics_to_qdrant(metric_infos)
            logger.info('同步metric信息到qdrant')
        logger.info('元数据知识库构建完成')
