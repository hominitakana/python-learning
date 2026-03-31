from fastapi import FastAPI
from typing import Optional, List
from pydantic import BaseModel, Field

app = FastAPI()

class ShopInfo(BaseModel):
    name: str
    location: str


#リクエストボディ このような形式のデータが入ってくる
class Item(BaseModel):
    name: str = Field(min_length=4, max_length=12)
    description: Optional[str] = None
    price: int
    tax: Optional[float] = None


class Data(BaseModel):
    shop_info: Optional[ShopInfo]
    items: List[Item]



# postはデータ構造をクラスで作成してから作成する。
@app.post("/item/")
async def create_item(item: Item):
    return {"message": f"{item.name}は税込み{int(item.price * item.tax)}円です"}

@app.post("/")
async def post_root(data: Data):
    return data

@app.get("/")
async def get_root():
    return {"message" : "You can Access!"}