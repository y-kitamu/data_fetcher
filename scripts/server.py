"""server.py"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

import data_fetcher

app = FastAPI()
app.include_router(data_fetcher.server.router)
origins = [
    "http://localhost:3000",
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    # 認証情報のアクセスを許可(今回は必要ない)
    allow_credentials=True,
    # 全てのリクエストメソッドを許可(["GET", "POST"]など個別指定も可能)
    allow_methods=["*"],
    # アクセス可能なレスポンスヘッダーを設定（今回は必要ない）
    allow_headers=["*"],
)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("server:app", port=5000, log_level="info", host="0.0.0.0", reload=True)
