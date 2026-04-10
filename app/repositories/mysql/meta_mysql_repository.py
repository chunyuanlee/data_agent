from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.mysql.column_info_mysql import ColumnInfoMySQL
from app.models.mysql.column_metric_mysql import ColumnMetricMySQL
from app.models.mysql.metric_info_mysql import MetricInfoMySQL
from app.models.mysql.table_info_mysql import TableInfoMySQL


class MetaMySQLRepository:
    # 初始化方法，接收一个异步数据库会话
    def __init__(self, meta_session: AsyncSession):
        self.session = meta_session

    # 批量保存表信息列表到数据库
    async def save_table_infos(self, table_infos: list[TableInfoMySQL]):
        self.session.add_all(table_infos)

    # 批量保存列信息列表到数据库
    async def save_column_infos(self, column_infos: list[ColumnInfoMySQL]):
        self.session.add_all(column_infos)

    # 批量保存指标信息列表到数据库
    async def save_metric_infos(self, metric_infos: list[MetricInfoMySQL]):
        self.session.add_all(metric_infos)

    # 批量保存列指标列表到数据库
    async def save_column_metrics(self, column_metrics: list[ColumnMetricMySQL]):
        self.session.add_all(column_metrics)

    # 根据列 ID 获取单个列信息对象，若不存在则返回 None
    async def get_column_by_id(self, column_id: str) -> ColumnInfoMySQL | None:
        return await self.session.get(ColumnInfoMySQL, column_id)

    # 根据表 ID 获取单个表信息对象，若不存在则返回 None
    async def get_table_by_id(self, table_id) -> TableInfoMySQL | None:
        return await self.session.get(TableInfoMySQL, table_id)

    # 根据表 ID 获取该表的所有主键和外键列信息列表
    async def get_key_columns_by_table_id(self, table_id) -> list[ColumnInfoMySQL]:
        # 定义原生 SQL 查询语句，筛选角色为主键或外键的列
        sql = text("""
                   select *
                   from column_info
                   where table_id = :table_id
                     and role in ('primary_key', 'foreign_key')
                   """)
        # 将原生 SQL 转换为 SQLAlchemy 可执行的查询对象，主要是为了让sqlalchemy替我们把返回结果封装成ColumnInfoMySQL实体类
        # 具体使用方法可以查SQLAlchemy的使用文档https://docs.sqlalchemy.org.cn/en/20/orm/queryguide/select.html#selecting-orm-entities-and-attributes
        query = select(ColumnInfoMySQL).from_statement(sql)

        # 执行查询并传入参数
        '''
        此处如果不加scalars，会返回这样的值，包了一层元组：
        [
            (ColumnInfoMySQL(...),),
            (ColumnInfoMySQL(...),)
        ]，
        用了scalars，会把最外层元组去掉：
        [
            ColumnInfoMySQL(...),  # 第一行
            ColumnInfoMySQL(...),  # 第二行
        ]
        '''
        result = await self.session.execute(query, {"table_id": table_id})
        # 返回所有匹配的结果标量值列表
        return result.scalars().all()
