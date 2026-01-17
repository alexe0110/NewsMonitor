import io
import json

from minio import Minio


def read_json(client: Minio, bucket: str, filename: str) -> list:
    """Чтение JSON файла из MinIO."""
    response = client.get_object(bucket, filename)
    try:
        return json.loads(response.read().decode('utf-8'))
    finally:
        response.close()
        response.release_conn()


def save_json(client: Minio, bucket: str, filename: str, data: list) -> None:
    """Сохранение JSON данных в MinIO."""
    json_bytes = json.dumps(data, indent=2, ensure_ascii=False).encode('utf-8')
    client.put_object(
        bucket_name=bucket,
        object_name=filename,
        data=io.BytesIO(json_bytes),
        length=len(json_bytes),
        content_type='application/json',
    )
