import io
import json
import logging
import os
from datetime import datetime

import httpx
from minio import Minio

logger = logging.getLogger(__name__)


def get_minio_client():
    """Создание MinIO клиента из environment variables"""
    return Minio(
        endpoint=os.getenv('MINIO_ENDPOINT', 'minio:9000'),
        access_key=os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
        secret_key=os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
        secure=False,
    )


def save_to_minio(client, bucket, filename, data):
    """Сохранение JSON данных в MinIO"""
    json_data = json.dumps(data, indent=2)
    json_bytes = json_data.encode('utf-8')

    client.put_object(
        bucket_name=bucket,
        object_name=filename,
        data=io.BytesIO(json_bytes),
        length=len(json_bytes),
        content_type='application/json',
    )


def fetch_hackernews(**context):
    """
    Загрузка топ-30 новостей из Hacker News API
    Сохраняет результат в MinIO bucket raw-news
    """
    logger.info('Fetching Hacker News top stories...')

    # Получить список top story IDs
    try:
        response = httpx.get('https://hacker-news.firebaseio.com/v0/topstories.json', timeout=10.0)
        response.raise_for_status()
        story_ids = response.json()[:30]
    except httpx.HTTPError as e:
        logger.error(f'Failed to fetch story IDs: {e}')
        raise

    # Получить детали для каждой новости
    news_items = []
    for story_id in story_ids:
        try:
            response = httpx.get(
                f'https://hacker-news.firebaseio.com/v0/item/{story_id}.json',
                timeout=10.0,
            )
            response.raise_for_status()
            item = response.json()

            # Добавляем только если есть title и url
            if item and item.get('title') and item.get('url'):
                news_items.append(
                    {
                        'id': item.get('id'),
                        'title': item.get('title'),
                        'url': item.get('url'),
                        'score': item.get('score', 0),
                        'time': item.get('time'),
                        'by': item.get('by'),
                        'source': 'hackernews',
                    }
                )
        except httpx.HTTPError as e:
            logger.warning(f'Failed to fetch story {story_id}: {e}')
            continue

    logger.info(f'Fetched {len(news_items)} items from Hacker News')

    # Сохранить в MinIO
    minio_client = get_minio_client()
    filename = f'{datetime.now().strftime("%Y-%m-%d_%H%M")}_hackernews.json'

    save_to_minio(minio_client, 'raw-news', filename, news_items)
    logger.info(f'Saved to MinIO: raw-news/{filename}')

    return len(news_items)


def fetch_devto(**context):
    """
    Загрузка последних 30 статей из Dev.to API
    Сохраняет результат в MinIO bucket raw-news
    """
    logger.info('Fetching Dev.to articles...')

    # Получить статьи
    try:
        response = httpx.get('https://dev.to/api/articles', params={'per_page': 30}, timeout=10.0)
        response.raise_for_status()
        articles = response.json()
    except httpx.HTTPError as e:
        logger.error(f'Failed to fetch Dev.to articles: {e}')
        raise

    # Форматировать данные
    news_items = []
    for article in articles:
        news_items.append(
            {
                'id': article.get('id'),
                'title': article.get('title'),
                'description': article.get('description'),
                'url': article.get('url'),
                'published_at': article.get('published_at'),
                'tags': article.get('tag_list', []),
                'user': article.get('user', {}).get('username'),
                'source': 'devto',
            }
        )

    logger.info(f'Fetched {len(news_items)} items from Dev.to')

    # Сохранить в MinIO
    minio_client = get_minio_client()
    filename = f'{datetime.now().strftime("%Y-%m-%d_%H%M")}_devto.json'

    save_to_minio(minio_client, 'raw-news', filename, news_items)
    logger.info(f'Saved to MinIO: raw-news/{filename}')

    return len(news_items)
