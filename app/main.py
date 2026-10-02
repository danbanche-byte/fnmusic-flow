from fastapi import FastAPI, File, HTTPException, UploadFile

from .models import SourceDefinition, TrackQuery
from .sources import SourceRegistry

app = FastAPI(title="fnmusic-flow", version="0.1.0")
registry = SourceRegistry()


@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "service": "fnmusic-flow", "sources": await registry.health()}


@app.get("/api/sources", response_model=list[SourceDefinition])
async def list_sources() -> list[SourceDefinition]:
    return registry.list()


@app.post("/api/sources/import", response_model=SourceDefinition, status_code=201)
async def import_source(file: UploadFile = File(...), name: str | None = None) -> SourceDefinition:
    if not file.filename or not file.filename.lower().endswith(".js"):
        raise HTTPException(status_code=400, detail="请上传 .js 落雪音源文件")
    try:
        content = (await file.read()).decode("utf-8")
        return registry.register_lx_script(content, name=name)
    except (UnicodeDecodeError, ValueError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@app.post("/api/sources/{name}/resolve")
async def resolve(name: str, query: TrackQuery) -> dict:
    try:
        url = await registry.resolve(name, query)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="音源不存在") from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    return {"url": url}

