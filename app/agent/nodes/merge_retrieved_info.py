import yaml
from langgraph.runtime import Runtime

from app.agent.context import DataAgentContext
from app.agent.state import DataAgentState, TableInfoState, ColumnInfoState, MetricInfoState
from app.core.logging import logger
from app.models.mysql.column_info_mysql import ColumnInfoMySQL
from app.models.mysql.table_info_mysql import TableInfoMySQL
from app.models.qdrant.column_info_qdrant import ColumnInfoQdrant


async def merge_retrieved_info(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    writer = runtime.stream_writer
    writer({"stage": "合并召回信息"})
    # 已召回信息
    retrieved_columns = state["retrieved_columns"]
    retrieved_values = state["retrieved_values"]
    retrieved_metrics = state["retrieved_metrics"]
    meta_mysql_repository = runtime.context['meta_mysql_repository']
    # 合并的目标信息
    table_infos: list[TableInfoState] = []
    metric_infos: list[MetricInfoState] = []

    # 一个去重用的字典
    id_to_column_map: dict[str, ColumnInfoQdrant] = {value['id']: value for value in retrieved_columns}

    for retrieved_value in retrieved_values:
        column_id = retrieved_value['column_id']
        value = retrieved_value['value']
        if column_id in id_to_column_map:
            # 判断当前值是否已经在examples里，避免重复,需要确定一下value
            if value not in id_to_column_map[column_id]['examples']:
                id_to_column_map[column_id]['examples'].append(value)
        else:
            # 如果当前列不在列信息里，则去元数据库查出列信息，添加
            column_info: ColumnInfoMySQL = await meta_mysql_repository.get_column_by_id(column_id)
            if value not in column_info.examples:
                column_info.examples.append(value)
            id_to_column_map[column_id] = _convert_column_info_from_mysql_to_qdrant(column_info)

    for retrieved_metric in retrieved_metrics:
        relevant_columns = retrieved_metric['relevant_columns']
        for column_id in relevant_columns:
            if column_id not in id_to_column_map:
                # 如果找回的metrics里的相关列不在整体的列信息里，去元数据库中获取这个列的信息，添加到整体列信息里
                column_info: ColumnInfoMySQL = await meta_mysql_repository.get_column_by_id(column_id)
                id_to_column_map[column_id] = _convert_column_info_from_mysql_to_qdrant(column_info)

    # 最终要整理成的表和列聚合信息
    # 1.拿到table-column的聚合信息
    table_to_columns_map: dict[str, list[ColumnInfoQdrant]] = {}
    for column in id_to_column_map.values(): # column是ColumnInfoQdrant类型
        if column['table_id'] not in table_to_columns_map:
            table_to_columns_map[column['table_id']] = []
        table_to_columns_map[column['table_id']].append(column)

    for table_id, columns in table_to_columns_map.items():
        table_info: TableInfoMySQL = await meta_mysql_repository.get_table_by_id(table_id)
        column_states: list[ColumnInfoState] = [] # 拿到列信息
        column_state_ids: list[str] = [] # 拿到列的id，用来去重
        for column in columns:
            column_state = _convert_column_info_from_qdrant_to_state(column)
            column_state_ids.append(column['id'])
            column_states.append(column_state)
        # 获取表的主外键信息
        key_columns = await meta_mysql_repository.get_key_columns_by_table_id(table_id)
        for key_column in key_columns:
            # 如果主外键不在原本的列信息里，补充进去
            if key_column.id not in column_state_ids:
                key_column_state = _convert_column_info_from_mysql_to_state(key_column)
                column_states.append(key_column_state)

        table_info_state = TableInfoState(
            name=table_info.name,
            role=table_info.role,
            description=table_info.description,
            columns=column_states
        )
        table_infos.append(table_info_state)

    for metric in retrieved_metrics:
        metric_info_state = MetricInfoState(
            name=metric['name'],
            description=metric['description'],
            alias=metric['alias']
        )
        metric_infos.append(metric_info_state)

    logger.info(f"召回信息合并成功")
    return {"table_infos": table_infos, "metric_infos": metric_infos}


def _convert_column_info_from_mysql_to_qdrant(column_info: ColumnInfoMySQL) -> ColumnInfoQdrant:
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


def _convert_column_info_from_qdrant_to_state(column_info: ColumnInfoQdrant) -> ColumnInfoState:
    return ColumnInfoState(
        name=column_info['name'],
        type=column_info['type'],
        role=column_info['role'],
        description=column_info['description'],
        alias=column_info['alias'],
        examples=column_info['examples']
    )


def _convert_column_info_from_mysql_to_state(column_info: ColumnInfoMySQL) -> ColumnInfoState:
    return ColumnInfoState(
        name=column_info.name,
        type=column_info.type,
        role=column_info.role,
        description=column_info.description,
        alias=column_info.alias,
        examples=column_info.examples
    )
