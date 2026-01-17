import httpx
from minio import Minio
import json
from datetime import datetime
import os

# Вспомогательные функции
def get_minio_client():
    # Создание MinIO клиента из env vars
    pass

def save_to_minio(client, bucket, filename, data):
    # Сохранение JSON в MinIO
    pass

# Основные функции
def fetch_hackernews(**context):
    # 1. Получить top 30 IDs
    # 2. Для каждого получить детали
    # 3. Сохранить в MinIO
    pass

def fetch_devto(**context):
    # 1. GET /api/articles
    # 2. Сохранить в MinIO
    pass