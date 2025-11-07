from fastapi import Request
from motor.motor_asyncio import AsyncIOMotorDatabase

def get_db(request: Request) -> AsyncIOMotorDatabase:
    """
    Resolve the Mongo database from FastAPI app state.
    """
    db = getattr(request.app.state, "db", None)
    if db is None:
        raise RuntimeError("Database is not initialized in app.state.db")
    return db