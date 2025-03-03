"""server.py"""

from fastapi import FastAPI

import data_fetcher

app = FastAPI()
app.include_router(data_fetcher.server.router)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("server:app", port=5000, log_level="info", host="0.0.0.0", reload=True)
