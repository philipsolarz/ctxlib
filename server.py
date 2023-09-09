from fastapi import FastAPI, HTTPException, Depends
import os
from pathlib import Path
from typing import List, Optional, Dict, Any, Type
from pydantic import BaseModel
from datamodel_code_generator import DataModelType, PythonVersion
from datamodel_code_generator.model import get_data_model_types
from datamodel_code_generator.parser.jsonschema import JsonSchemaParser
import json
import importlib
from docarray.index import InMemoryExactNNIndex
import sys
from sqlalchemy import Column, String, Integer, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import DeclarativeBase
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import Session
DATABASE_URL = "postgresql://username:password@localhost/dbname"  # Ideally read this from an environment variable
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = DeclarativeBase()


class Namespace(Base):
    __tablename__ = 'namespaces'
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True)
    workspaces = relationship("Workspace", back_populates="namespace")

class Workspace(Base):
    __tablename__ = 'workspaces'
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True)
    namespace_id = Column(Integer, ForeignKey('namespaces.id'))
    namespace = relationship("Namespace", back_populates="workspaces")
    repositories = relationship("Repository", back_populates="workspace")

class Repository(Base):
    __tablename__ = 'repositories'
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True)
    workspace_id = Column(Integer, ForeignKey('workspaces.id'))
    workspace = relationship("Workspace", back_populates="repositories")

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

app = FastAPI()


class DirectoryManager:

    @staticmethod
    def make_dir(path: str) -> bool:
        try:
            Path(path).mkdir(parents=True, exist_ok=True)
            return True
        except Exception as e:
            print(f"Error: {e}")
            return False

    @staticmethod
    def remove_dir(path: str) -> bool:
        try:
            Path(path).rmdir()
            return True
        except Exception as e:
            print(f"Error: {e}")
            return False

    @staticmethod
    def rename_dir(old_path: str, new_path: str) -> bool:
        try:
            Path(old_path).rename(new_path)
            return True
        except Exception as e:
            print(f"Error: {e}")
            return False


@app.post("/namespaces/{namespace}", response_model=bool)
def create_namespace(namespace: str, db: Session = Depends(get_db)):
    new_namespace = Namespace(name=namespace)
    db.add(new_namespace)
    db.commit()
    success = DirectoryManager.make_dir(f"namespaces/{namespace}")
    if not success:
        raise HTTPException(status_code=500,
                            detail="Failed to create namespace")
    return success


@app.put("/namespaces/{old_namespace}/{new_namespace}", response_model=bool)
def update_namespace(old_namespace: str, new_namespace: str):
    success = DirectoryManager.rename_dir(f"namespaces/{old_namespace}",
                                          f"namespaces/{new_namespace}")
    if not success:
        raise HTTPException(status_code=500,
                            detail="Failed to update namespace")
    return success


@app.delete("/namespaces/{namespace}", response_model=bool)
def delete_namespace(namespace: str):
    success = DirectoryManager.remove_dir(f"namespaces/{namespace}")
    if not success:
        raise HTTPException(status_code=500,
                            detail="Failed to delete namespace")
    return success


@app.post("/namespaces/{namespace}/workspaces/{workspace}",
          response_model=bool)
def create_workspace(workspace: str, namespace: str):
    success = DirectoryManager.make_dir(
        f"namespaces/{namespace}/workspaces/{workspace}")
    if not success:
        raise HTTPException(status_code=500,
                            detail="Failed to create workspace")
    return success


@app.put("/namespaces/{namespace}/workspaces/{old_workspace}/{new_workspace}",
         response_model=bool)
def update_workspace(old_workspace: str, new_workspace: str, namespace: str):
    success = DirectoryManager.rename_dir(
        f"namespaces/{namespace}/workspaces/{old_workspace}",
        f"namespaces/{namespace}/workspaces/{new_workspace}")
    if not success:
        raise HTTPException(status_code=500,
                            detail="Failed to update workspace")
    return success


@app.delete("/namespaces/{namespace}/workspaces/{workspace}",
            response_model=bool)
def delete_workspace(workspace: str, namespace: str):
    success = DirectoryManager.remove_dir(
        f"namespaces/{namespace}/workspaces/{workspace}")
    if not success:
        raise HTTPException(status_code=500,
                            detail="Failed to delete workspace")
    return success


@app.post(
    "/namespaces/{namespace}/workspaces/{workspace}/repositories/{repository}",
    response_model=bool)
def create_repository(repository: str, workspace: str, namespace: str):
    # Creating directory for the repository
    dir_path = f"namespaces/{namespace}/workspaces/{workspace}/repositories/{repository}"
    success = DirectoryManager.make_dir(dir_path)

    if not success:
        raise HTTPException(status_code=500,
                            detail="Failed to create repository")

    return success


@app.put(
    "/namespaces/{namespace}/workspaces/{workspace}/repositories/{old_repository}/{new_repository}",
    response_model=bool)
def update_repository(old_repository: str, new_repository: str, workspace: str,
                      namespace: str):
    success = DirectoryManager.rename_dir(
        f"namespaces/{namespace}/workspaces/{workspace}/repositories/{old_repository}",
        f"namespaces/{namespace}/workspaces/{workspace}/repositories/{new_repository}"
    )
    if not success:
        raise HTTPException(status_code=500,
                            detail="Failed to update repository")
    return success


@app.delete(
    "/namespaces/{namespace}/workspaces/{workspace}/repositories/{repository}",
    response_model=bool)
def delete_repository(repository: str, workspace: str, namespace: str):
    success = DirectoryManager.remove_dir(
        f"namespaces/{namespace}/workspaces/{workspace}/repositories/{repository}"
    )
    if not success:
        raise HTTPException(status_code=500,
                            detail="Failed to delete repository")
    return success


def generate_model_from_schema(schema: str, base_class: str,
                               output_file_path: str) -> Any:
    try:
        parser = JsonSchemaParser(schema, base_class=base_class)
    except Exception as e:
        raise RuntimeError(f"Failed to initialize JsonSchemaParser: {e}")

    try:
        result = parser.parse()
    except Exception as e:
        raise RuntimeError(f"Failed to parse schema: {e}")

    try:
        # Assuming result is a string containing the generated code
        with open(output_file_path, 'w') as f:
            f.write(result)
    except Exception as e:
        raise RuntimeError(f"Failed to write to output file: {e}")

    return result

class CreateModelRequest(BaseModel):
    json_schema: str
    base_class: str

@app.post(
    "/namespaces/{namespace}/workspaces/{workspace}/repositories/{repository}/models/{model}",
    response_model=bool)
def create_model(model: str, repository: str, workspace: str, namespace: str, request: CreateModelRequest):
    json_schema = request.json_schema
    base_class = request.base_class
    # Creating directory for the repository if it doesn't exist
    dir_path = f"namespaces/{namespace}/workspaces/{workspace}/repositories/{repository}/models"
    success = DirectoryManager.make_dir(dir_path)
    if not success:
        raise HTTPException(status_code=500,
                            detail="Failed to create models directory")

    # Creating model.py file in the models directory
    output_file_path = f"{dir_path}/{model}.py"
    result = generate_model_from_schema(json_schema, base_class, output_file_path)
    if not result:
        raise HTTPException(status_code=500,
                            detail=f"Failed to generate model.py file")
    return success

def load_data_model(namespace: str, workspace: str, repository: str, model: str) -> Type[Any]:
    base_dir = os.path.abspath("namespaces")
    if base_dir not in sys.path:
        sys.path.append(base_dir)
    try:
        model_module = importlib.import_module(
            f"namespaces.{namespace}.workspaces.{workspace}.repositories.{repository}.models.{model}"
        )
        return getattr(model_module, model)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to import the model: {e}")


@app.post("/namespaces/{namespace}/workspaces/{workspace}/repositories/{repository}/models/{model}/index", response_model=bool)
def index_data(model: str, repository: str, workspace: str, namespace: str, data: str, DataModel: Type[Any] = Depends(load_data_model)):
    db = InMemoryExactNNIndex[DataModel]()
    db.index(data)
    return True



@app.get("/namespaces/{namespace}/workspaces/{workspace}/repositories/{repository}/models/{model}/data",response_model=bool)
def get_model_data(data_id: str, model: str, repository: str, workspace: str, namespace: str):
    base_dir = os.path.abspath("namespaces")
    if base_dir not in sys.path:
        sys.path.append(base_dir)
    try:
        model_module = importlib.import_module(f"namespaces.{namespace}.workspaces.{workspace}.repositories.{repository}.models.{model}")
        DataModel = getattr(model_module, model)
        index = InMemoryExactNNIndex[DataModel]()
        return index[data_id]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to import the model: {e}")
    return False



if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001, reload=True)