from fastapi import APIRouter, FastAPI

app = FastAPI(title="Cold Caller API")

api = APIRouter(prefix="/api")


@api.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


app.include_router(api)
