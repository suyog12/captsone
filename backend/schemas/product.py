from __future__ import annotations

from typing import Optional

from pydantic import BaseModel


class ProductRow(BaseModel):
    item_id: int
    description: Optional[str] = None
    family: Optional[str] = None
    category: Optional[str] = None
    supplier: Optional[str] = None
    is_private_brand: bool = False
    unit_price: Optional[float] = None
    pack_size: Optional[str] = None
    image_url: Optional[str] = None
    units_in_stock: int = 0

    model_config = {"from_attributes": True}


class ProductBrowseResponse(BaseModel):
    items: list[ProductRow]
    total: int
    limit: int
    offset: int


class ProductFiltersResponse(BaseModel):
    families: list[str]
    categories: list[str]
    suppliers: list[str]