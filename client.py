import requests
from typing import List, Optional, Any, Dict, Type, Tuple
from pydantic import BaseModel
from docarray import BaseDoc


def generate_schema_from_datamodel(
        DataModel: Type[BaseDoc]) -> Tuple[str, str]:
    try:
        schema = DataModel.schema_json()
    except AttributeError:
        raise ValueError(
            "Provided DataModel class must have a 'schema_json' method.")

    base_class = None

    for cls in DataModel.__bases__:
        if issubclass(cls, BaseDoc):
            base_class = f"{cls.__module__}.{cls.__name__}"
            break

    if base_class is None:
        raise ValueError("Provided DataModel class must inherit from BaseDoc.")

    return schema, base_class


class ContextDatabaseClient:

    def __init__(self, base_url: str = "http://localhost:8000") -> None:
        self.base_url = base_url
        self.current_namespace = ""
        self.current_workspace = ""
        self.current_repository = ""
        self.current_model = None

    def _send_request(self,
                      method: str,
                      endpoint: str,
                      data: dict = None) -> Dict[str, Any]:
        url = f"{self.base_url}/{endpoint}"
        result = {'success': False, 'data': None}
        try:
            if method == 'GET':
                response = requests.get(url, params=data)
            elif method == 'POST':
                response = requests.post(url, json=data)
            elif method == 'PUT':
                response = requests.put(url, json=data)
            elif method == 'DELETE':
                response = requests.delete(url, json=data)
            else:
                return result

            if response.status_code == 200:
                result['success'] = True
                result['data'] = response.json()
                return result
            else:
                response.raise_for_status()
        except requests.RequestException as e:
            print(f"An error occurred: {e}")
            return result

    def create_namespace(self, namespace: str) -> bool:
        result = self._send_request('POST', f'namespaces/{namespace}/')
        return result['success']

    def update_namespace(self, old_namespace: str, new_namespace: str) -> bool:
        result = self._send_request(
            'PUT', f'namespaces/{old_namespace}/{new_namespace}/')
        return result['success']

    def delete_namespace(self, namespace: str) -> bool:
        result = self._send_request('DELETE', f'namespaces/{namespace}/')
        return result['success']

    def list_namespaces(self) -> List[str]:
        result = self._send_request('GET', 'namespaces/')
        return result['data'] if result['success'] else []

    def get_current_namespace(self) -> str:
        return self.current_namespace

    def set_current_namespace(self, namespace: str) -> bool:
        self.current_namespace = namespace
        return True

    def create_workspace(self,
                         workspace: str,
                         namespace: Optional[str] = None) -> bool:
        namespace = namespace or self.current_namespace
        result = self._send_request(
            'POST', f'namespaces/{namespace}/workspaces/{workspace}/')
        return result['success']

    def update_workspace(self,
                         old_workspace: str,
                         new_workspace: str,
                         namespace: Optional[str] = None) -> bool:
        namespace = namespace or self.current_namespace
        result = self._send_request(
            'PUT',
            f'namespaces/{namespace}/workspaces/{old_workspace}/{new_workspace}/'
        )
        return result['success']

    def delete_workspace(self,
                         workspace: str,
                         namespace: Optional[str] = None) -> bool:
        namespace = namespace or self.current_namespace
        result = self._send_request(
            'DELETE', f'namespaces/{namespace}/workspaces/{workspace}/')
        return result['success']

    def list_workspaces(self, namespace: Optional[str] = None) -> List[str]:
        namespace = namespace or self.current_namespace
        result = self._send_request('GET',
                                    f'namespaces/{namespace}/workspaces/')
        return result['data'] if result['success'] else []

    def get_current_workspace(self) -> str:
        return self.current_workspace

    def set_current_workspace(self, workspace: str) -> bool:
        self.current_workspace = workspace
        return True

    def create_repository(self,
                          repository: str,
                          workspace: Optional[str] = None,
                          namespace: Optional[str] = None) -> bool:
        workspace = workspace or self.current_workspace
        namespace = namespace or self.current_namespace
        result = self._send_request(
            'POST',
            f'namespaces/{namespace}/workspaces/{workspace}/repositories/{repository}/')
        return result['success']

    def update_repository(self,
                          old_repository: str,
                          new_repository: str,
                          workspace: Optional[str] = None,
                          namespace: Optional[str] = None) -> bool:
        workspace = workspace or self.current_workspace
        namespace = namespace or self.current_namespace
        result = self._send_request(
            'PUT',
            f'namespaces/{namespace}/workspaces/{workspace}/repositories/{old_repository}/{new_repository}/'
        )
        return result['success']

    def delete_repository(self,
                          repository: str,
                          workspace: Optional[str] = None,
                          namespace: Optional[str] = None) -> bool:
        workspace = workspace or self.current_workspace
        namespace = namespace or self.current_namespace
        result = self._send_request(
            'DELETE',
            f'namespaces/{namespace}/workspaces/{workspace}/repositories/{repository}/'
        )
        return result['success']

    def list_repositories(self,
                          workspace: Optional[str] = None,
                          namespace: Optional[str] = None) -> List[str]:
        workspace = workspace or self.current_workspace
        namespace = namespace or self.current_namespace
        result = self._send_request(
            'GET',
            f'namespaces/{namespace}/workspaces/{workspace}/repositories/')
        return result['data'] if result['success'] else []

    def get_current_repository(self) -> str:
        return self.current_repository

    def set_current_repository(self, repository: str) -> bool:
        self.current_repository = repository
        return True

    def create_model(self,
                     model: Type[BaseDoc],
                     repository: Optional[str] = None,
                     workspace: Optional[str] = None,
                     namespace: Optional[str] = None) -> bool:
        repository = repository or self.current_repository
        workspace = workspace or self.current_workspace
        namespace = namespace or self.current_namespace
        json_schema, base_class = generate_schema_from_datamodel(model)
        # print(schema, base_class)
        result = self._send_request(
            'POST',
            f'namespaces/{namespace}/workspaces/{workspace}/repositories/{repository}/models/{model.__name__}',
            {
                "json_schema": json_schema,
                "base_class": base_class
            })
        return result['success']
    
    def update_model(self,
                     old_model: str,
                     new_model: Type[BaseDoc],
                     repository: Optional[str] = None,
                     workspace: Optional[str] = None,
                     namespace: Optional[str] = None) -> bool:
        repository = repository or self.current_repository
        workspace = workspace or self.current_workspace
        namespace = namespace or self.current_namespace
        json_schema, base_class = generate_schema_from_datamodel(new_model)
        result = self._send_request(
            'PUT',
            f'namespaces/{namespace}/workspaces/{workspace}/repositories/{repository}/models/{old_model}/{new_model}',
            {
                "json_schema": json_schema,
                "base_class": base_class
            })
        return result['success']
    
    def delete_model(self,
                     model: str,
                     repository: Optional[str] = None,
                     workspace: Optional[str] = None,
                     namespace: Optional[str] = None) -> bool:
        repository = repository or self.current_repository
        workspace = workspace or self.current_workspace
        namespace = namespace or self.current_namespace
        result = self._send_request(
            'DELETE',
            f'namespaces/{namespace}/workspaces/{workspace}/repositories/{repository}/models/{model}')
        return result['success']
    
    def list_models(self,
                    repository: Optional[str] = None,
                    workspace: Optional[str] = None,
                    namespace: Optional[str] = None) -> List[str]:
        repository = repository or self.current_repository
        workspace = workspace or self.current_workspace
        namespace = namespace or self.current_namespace
        result = self._send_request(
            'GET',
            f'namespaces/{namespace}/workspaces/{workspace}/repositories/{repository}/models/')
        return result['data'] if result['success'] else []
    
    def get_current_model(self) -> Type[BaseDoc]:
        return self.current_model
    
    def set_current_model(self, model: Type[BaseDoc]) -> bool:
        self.current_model = model
        return True
    
    def index_data(self,
                   data: BaseDoc,
                   model: Optional[Type[BaseDoc]] = None,
                   repository: Optional[str] = None,
                   workspace: Optional[str] = None,
                   namespace: Optional[str] = None) -> bool:
        model = model.__name__ if model else self.current_model.__name__
        # model = model.__name__ or self.current_model.__name__
        repository = repository or self.current_repository
        workspace = workspace or self.current_workspace
        namespace = namespace or self.current_namespace

        result = self._send_request(
            'POST',
            f'namespaces/{namespace}/workspaces/{workspace}/repositories/{repository}/models/{model}/index/',
            {"data": data.json()})
        return result['success']


# Example usage
client = ContextDatabaseClient("http://127.0.0.1:8001")

from docarray.documents import TextDoc
from docarray.typing import TextUrl, AnyEmbedding


class DataModel(BaseDoc):
    text: Optional[str]
    url: Optional[TextUrl]
    embedding: Optional[AnyEmbedding]
    bytes_: Optional[bytes]

client.create_namespace("root")
client.set_current_namespace("root")
client.create_workspace("default")
client.set_current_workspace("default")
client.create_repository("main")
client.set_current_repository("main")
client.create_model(DataModel)
client.set_current_model(DataModel)
data = DataModel(text="Hello World!")
# client.load_data(data)
client.index_data(data)