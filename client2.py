import requests
from typing import List, Optional, Any, Dict, Type, Tuple
from pydantic import BaseModel
from docarray import BaseDoc


def generate_schema_from_datamodel(DataModel: Type[BaseDoc]) -> Tuple[str, str]:
    try:
        schema = DataModel.schema_json()
    except AttributeError:
        raise ValueError("Provided DataModel class must have a 'schema_json' method.")

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

    def _send_request(self, method: str, endpoint: str, data: dict = None) -> Dict[str, Any]:
        url = f"{self.base_url}/{endpoint}"
        result = {'success': False, 'data': None}
        try:
            if method == 'POST':
                response = requests.post(url, json=data)

            if response.status_code == 200:
                result['success'] = True
                result['data'] = response.json()
                return result
            else:
                response.raise_for_status()
        except requests.RequestException as e:
            print(f"An error occurred: {e}")
            return result

    def index_data(self, data: BaseDoc, model: Type[BaseDoc]) -> bool:
        json_schema, base_class = generate_schema_from_datamodel(model)
        request_data = {
            "json_schema": json_schema,
            "base_class": base_class,
            "model_name": model.__name__,
            "data": data.json()
        }
        result = self._send_request('POST', 'index_data', request_data)
        return result['success']

    def search(self, query: str, DataModel: Type[BaseDoc], top_k: int = 10) -> List[BaseDoc]:
        json_schema, base_class = generate_schema_from_datamodel(DataModel)
        request_data = {
            "json_schema": json_schema,
            "base_class": base_class,
            # "model_name": model.__name__,
            "query": query,
            "top_k": top_k
        }
        result = self._send_request('POST', 'search', request_data)
        if result['success']:
            return DataModel.parse_raw(result['data'])
            return [model.parse_raw(item) for item in result['data']]
        else:
            return []

# Example usage
client = ContextDatabaseClient("http://127.0.0.1:8000")
from docarray.typing import TextUrl as _TextUrl
from docarray.typing import AnyEmbedding as _AnyEmbedding
class TextUrl(_TextUrl):

    @classmethod
    def __modify_schema__(cls, field_schema: Dict[str, Any]) -> None:
        super().__modify_schema__(field_schema)
        field_schema['customTypePath'] = "docarray.typing.TextUrl"


class AnyEmbedding(_AnyEmbedding):

    @classmethod
    def __modify_schema__(cls, field_schema: Dict[str, Any]) -> None:
        super().__modify_schema__(field_schema)
        field_schema['customTypePath'] = "docarray.typing.AnyEmbedding"
class DataModel(BaseDoc):
    text: Optional[str]
    url: Optional[TextUrl]
    embedding: Optional[AnyEmbedding]
    bytes_: Optional[bytes]
data = DataModel(text="Hello World!")
success = client.index_data(data, DataModel)
print("Indexing successful:", success)

query = "Hello"
success = client.search(query, DataModel)
print("Search successful:", success.text)