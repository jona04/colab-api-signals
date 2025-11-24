# core/domain/entities/base_entity.py
from typing import Any, Optional, TypeVar, Type
from pydantic import BaseModel, ConfigDict

E = TypeVar("E", bound="MongoEntity")

class MongoEntity(BaseModel):
    """
    Base entity for Mongo-backed documents.
    Maps Mongo's `_id` to `id` (string) and keeps timestamps.
    """
    id: Optional[str] = None  # maps _id
    created_at: Optional[int] = None
    created_at_iso: Optional[str] = None
    updated_at: Optional[int] = None
    updated_at_iso: Optional[str] = None
    
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="allow",      # se o doc tiver campos a mais, não quebra
        use_enum_values=True,
    )

    @classmethod
    def from_mongo(cls: Type[E], doc: Optional[dict[str, Any]]) -> Optional[E]:
        if not doc:
            return None
        data = dict(doc)
        if "_id" in data:
            data["id"] = str(data.pop("_id"))
        return cls.model_validate(data)

    def to_mongo(self) -> dict[str, Any]:
        data = self.model_dump(mode="python", exclude_none=True)
        if "id" in data:
            data["_id"] = data.pop("id")
        return data
