from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from dijon.routers import root_servers


app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
app.include_router(root_servers.router)
