from fastapi import APIRouter, FastAPI

from app.routers import agent, auth, calls, campaigns, dnc, livekit

app = FastAPI(title="Cold Caller API")

api = APIRouter(prefix="/api")


@api.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


api.include_router(auth.router)
api.include_router(campaigns.router)
api.include_router(calls.router)
api.include_router(dnc.router)
api.include_router(agent.router)
api.include_router(livekit.router)

app.include_router(api)
