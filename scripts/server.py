"""server.py"""
from pathlib import Path
import subprocess

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

import data_fetcher

app = FastAPI()
app.include_router(data_fetcher.server.router)
origins = [
    "http://crypto_server:3000",
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

def convert_org_to_md(src_org_dir: Path, dst_md_dir: Path):
    dst_md_dir.mkdir(exist_ok=True)
    for org_file in src_org_dir.rglob("*.org"):
        dst_path = dst_md_dir / org_file.with_suffix(".md").name
        command = [
            "emacs",
            "--batch",
            org_file.as_posix(),
            "-l",
            "~/dotfiles/.emacs.d/straight/repos/dash.el/dash.el",
            "-l",
            "~/dotfiles/.emacs.d/straight/repos/yk_elisp/yk-util.el",
            f"--eval='(yk/org-export-to-markdown \"{dst_path.as_posix()}\")'"
        ]
        print(" ".join(command))
        subprocess.run(" ".join(command), shell=True, check=True)
        data_fetcher.logger.debug(f"Converted {org_file} to {dst_path}")



if __name__ == "__main__":
    import uvicorn

    uvicorn.run("server:app", port=5000, log_level="info", host="0.0.0.0", reload=True)
    # convert_org_to_md(Path.home() / Path("dotfiles/.emacs.d/documents/junk"), Path.home() / Path("work/tmp"))
