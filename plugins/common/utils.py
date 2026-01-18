from datetime import UTC, datetime


def parse_datetime(value: str | None) -> datetime:
    """Парсинг даты из строки ISO формата.

    Args:
        value: Дата в ISO формате или None.

    Returns:
        datetime объект с timezone UTC.
    """
    if value is None:
        return datetime.now(tz=UTC)
    try:
        return datetime.fromisoformat(value.replace('Z', '+00:00'))
    except (ValueError, AttributeError) as e:
        raise ValueError(f'Invalid ISO datetime: {value}') from e
