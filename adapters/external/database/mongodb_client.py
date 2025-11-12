"""
MongoDB client factory for api-signals.

Provides a shared, properly configured AsyncIOMotorClient.
"""

from motor.motor_asyncio import AsyncIOMotorClient

from config.settings import settings



def get_mongo_client() -> AsyncIOMotorClient:
    """
    Create a configured AsyncIOMotorClient.

    Centralizes timeouts, pool size and options so all callers share the same behavior.
    """
    client = AsyncIOMotorClient(
        settings.MONGODB_URI,
        uuidRepresentation="standard",
        maxPoolSize=settings.MONGODB_MAX_POOL_SIZE,
        serverSelectionTimeoutMS=settings.MONGODB_SERVER_SELECTION_TIMEOUT_MS,
        connectTimeoutMS=settings.MONGODB_CONNECT_TIMEOUT_MS,
        socketTimeoutMS=settings.MONGODB_SOCKET_TIMEOUT_MS,
    )
    return client
