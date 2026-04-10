import json

from fastapi import APIRouter, Depends
from starlette.responses import StreamingResponse

from app.api.deps import get_chat_service
from app.schemas.chat import QuerySchema

chat_router = APIRouter(prefix="/api")


@chat_router.post("/query")
async def date_query(query: QuerySchema, chat_service=Depends(get_chat_service)):
    # 生成器方法
    async def event_stream():
        try:
            async for chunk in chat_service.stream_chat(query.query):
                # 转为json，并根据sse协议添加\n\n，返回结果
                yield f"data: {json.dumps(chunk, ensure_ascii=False, default=str)}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)}, ensure_ascii=False, default=str)}\n\n"
    # 流式返回结果
    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
    )
