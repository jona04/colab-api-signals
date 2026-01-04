from fastapi import Request
from motor.motor_asyncio import AsyncIOMotorDatabase


def get_db(request: Request) -> AsyncIOMotorDatabase:
    db = getattr(request.app.state, "mongo_db", None)
    if db is None:
        raise RuntimeError("MongoDB database not initialized. Check app lifespan startup.")
    return db
