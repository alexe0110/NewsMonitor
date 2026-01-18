from collections.abc import Callable
from datetime import UTC, datetime

from pydantic import BaseModel


class NormalizedNews(BaseModel):
    """Нормализованная новость для классификации."""

    id: str
    text: str
    source: str
    url: str | None
    original_title: str | None
    published_at: str | None


def _truncate(text: str, max_length: int) -> str:
    """Обрезает текст до максимальной длины."""
    if len(text) > max_length:
        return text[: max_length - 3] + '...'
    return text


def _build_devto_text(item: dict) -> str:
    """Собирает текст из Dev.to статьи."""
    title = item.get('title', '')
    description = item.get('description', '')
    return f'{title}. {description}' if description else title


def normalize_hackernews(item: dict, max_len: int) -> NormalizedNews:
    """Нормализация элемента HackerNews."""
    published_at = None
    if item.get('time'):
        published_at = datetime.fromtimestamp(item['time'], tz=UTC).isoformat()

    return NormalizedNews(
        id=f'hn_{item.get("id")}',
        text=_truncate(item.get('title', ''), max_len),
        source='hackernews',
        url=item.get('url'),
        original_title=item.get('title'),
        published_at=published_at,
    )


def normalize_devto(item: dict, max_len: int) -> NormalizedNews:
    """Нормализация элемента Dev.to."""
    return NormalizedNews(
        id=f'devto_{item.get("id")}',
        text=_truncate(_build_devto_text(item), max_len),
        source='devto',
        url=item.get('url'),
        original_title=item.get('title'),
        published_at=item.get('published_at'),
    )


Normalizer = Callable[[dict, int], NormalizedNews]

NORMALIZERS: dict[str, Normalizer] = {
    'hackernews': normalize_hackernews,
    'devto': normalize_devto,
}
