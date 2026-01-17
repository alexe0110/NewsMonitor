"""Функции для сбора и обработки новостей."""

from plugins.functions.news_fetchers import fetch_devto, fetch_hackernews
from plugins.functions.processing_utils import find_unprocessed_files

__all__ = ['fetch_hackernews', 'fetch_devto', 'find_unprocessed_files']
