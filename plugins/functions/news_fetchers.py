import logging
from datetime import UTC, datetime

import httpx

from config.settings import processing_settings, storage_settings
from plugins.common.clients import get_minio_client
from plugins.common.minio_utils import save_json

logger = logging.getLogger(__name__)


def _fetch_with_retry(url: str, params: dict | None = None) -> dict | list:
    response = httpx.get(url, params=params, timeout=10.0)
    response.raise_for_status()
    return response.json()


def fetch_hackernews(**context) -> int:
    """Загрузка топ новостей из Hacker News API."""
    logger.info('Fetching Hacker News top stories...')

    story_ids = _fetch_with_retry('https://hacker-news.firebaseio.com/v0/topstories.json')[
        : processing_settings.fetch_limit
    ]

    news_items = []
    for story_id in story_ids:
        try:
            item = _fetch_with_retry(f'https://hacker-news.firebaseio.com/v0/item/{story_id}.json')
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
            logger.warning('Failed to fetch story %s: %s', story_id, e)

    logger.info('Fetched %d items from Hacker News', len(news_items))

    filename = f'{datetime.now(tz=UTC).strftime("%Y-%m-%d_%H%M")}_hackernews.json'
    save_json(get_minio_client(), storage_settings.raw_bucket, filename, news_items)
    logger.info('Saved to MinIO: %s/%s', storage_settings.raw_bucket, filename)

    return len(news_items)


def fetch_devto(**context) -> int:
    """Загрузка последних статей из Dev.to API."""
    logger.info('Fetching Dev.to articles...')

    articles = _fetch_with_retry(
        'https://dev.to/api/articles',
        params={'per_page': processing_settings.fetch_limit},
    )

    news_items = [
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
        for article in articles
    ]

    logger.info('Fetched %d items from Dev.to', len(news_items))

    filename = f'{datetime.now(tz=UTC).strftime("%Y-%m-%d_%H%M")}_devto.json'
    save_json(get_minio_client(), storage_settings.raw_bucket, filename, news_items)
    logger.info('Saved to MinIO: %s/%s', storage_settings.raw_bucket, filename)

    return len(news_items)
