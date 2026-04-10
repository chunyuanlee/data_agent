import yaml
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.prompts import PromptTemplate
from langgraph.runtime import Runtime

from app.agent.context import DataAgentContext
from app.agent.llm import llm
from app.agent.state import DataAgentState
from app.core.logging import logger
from app.prompt.prompt_loader import load_prompt


async def filter_table_info(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    writer = runtime.stream_writer
    writer({"stage": "筛选表信息"})

    table_infos = state["table_infos"]
    query = state["query"]

    try:
        prompt = PromptTemplate(template=load_prompt("filter_table_info"), input_variables=["query", "table_infos"])
        output_parser = JsonOutputParser()
        chain = prompt | llm | output_parser
        # 注意此处将table_infos转换为yaml格式的字符串
        result = await chain.ainvoke({"query": query, "table_infos": yaml.dump(table_infos, allow_unicode=True,
                                                                               sort_keys=False)})
        '''
        result的结果格式为：
        {
            "表名1":["字段1", "字段2", "..."],
            "表名2":["字段1", "字段2", "..."]
        }
        '''
        # 下面根据模型筛选出的表和字段信息，对原始的table_info列表进行删除。
        # 注意：不能遍历某个列表的同时删除列表元素，会混乱，此处拷贝一个副本列表table_infos[:]去遍历，然后删除table_infos的元素
        for table_info in table_infos[:]:
            if table_info['name'] not in result:
                table_infos.remove(table_info)
            else:
                for column in table_info['columns'][:]:
                    if column['name'] not in result[table_info['name']]:
                        table_info['columns'].remove(column)

        logger.info(f"表格筛选结果: {[table_info['name'] for table_info in table_infos]}")
        return {"table_infos": table_infos}
    except Exception as e:
        logger.error(f"表格筛选失败: {str(e)}")
        raise
