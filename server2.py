from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Type, Optional
from datamodel_code_generator.parser.jsonschema import JsonSchemaParser
from docarray.index import InMemoryExactNNIndex
import os
import sys
import tempfile
import importlib.util
from sentence_transformers import SentenceTransformer

transformer = SentenceTransformer('all-MiniLM-L6-v2')
def encode(text):
    return transformer.encode(text)

app = FastAPI()
class Database:
    def __init__(self):
        self.db = None

database = Database()

def get_db() -> InMemoryExactNNIndex:
    return database.db

def generate_and_get_data_model(json_schema: str, base_class: str) -> Type[Any]:
    try:
        parser = JsonSchemaParser(json_schema, base_class=base_class)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to initialize JsonSchemaParser: {e}")

    try:
        generated_code = parser.parse()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to parse schema: {e}")

    temp_dir = tempfile.mkdtemp()
    model_path = os.path.join(temp_dir, "dynamic_model.py")

    with open(model_path, "w") as f:
        f.write(generated_code)

    if temp_dir not in sys.path:
        sys.path.append(temp_dir)

    dynamic_model_module = importlib.import_module("dynamic_model")

    return getattr(dynamic_model_module, 'DataModel')

class IndexDataRequest(BaseModel):
    json_schema: str
    base_class: str
    data: str

@app.post("/index_data", response_model=bool)
def index_data(request: IndexDataRequest):
    DataModel = generate_and_get_data_model(request.json_schema, request.base_class)

    if DataModel is None:
        raise HTTPException(status_code=500, detail="Failed to generate or load the data model.")
    
    try:
        database.db = InMemoryExactNNIndex[DataModel]()
        data = DataModel.parse_raw(request.data)
        data.embedding = encode(data.text)
        database.db.index(data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to index the data: {e}")

    return True

class SearchRequest(BaseModel):
    json_schema: str
    base_class: str
    query: str
    top_k: int


@app.post("/search")
def search(request: SearchRequest, db: InMemoryExactNNIndex = Depends(get_db)):
    DataModel = generate_and_get_data_model(request.json_schema, request.base_class)

    if DataModel is None:
        raise HTTPException(status_code=500, detail="Failed to generate or load the data model.")
    query_model = DataModel(text=request.query, embedding=encode(request.query))
    # print(query_model.text)
    if db is None:
        raise HTTPException(status_code=500, detail="Database not initialized.")
    
    try:
        # print(encode(request.query))
        results, scores = db.find(query_model, search_field="embedding", limit=request.top_k)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve context: {e}")
    print(results.text)
    return results[0].json()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
