from fastapi import FastAPI
from typing import Optional
from pydantic import BaseModel

app = FastAPI()


#リクエストボディ このような形式のデータが入ってくる
class Item(BaseModel):
    name: str
    description: Optional[str] = None
    price: int
    tax: Optional[float] = None


# postはデータ構造をクラスで作成してから作成する。
@app.post("/item/")
async def create_item(item: Item):
    return {"message": f"{item.name}は税込み{int(item.price * item.tax)}円です"}

@app.get("/")
async def read_root():
    return {"message" : "You can Access!"}