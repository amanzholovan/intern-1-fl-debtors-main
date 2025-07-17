import pytest
from unittest.mock import MagicMock
from sqlalchemy.orm import Session
from main import MainParser  # Убедитесь, что импортируется из правильного файла


@pytest.fixture
def mock_session():
    """Мокаем сессию базы данных для тестов."""
    session = MagicMock(spec=Session)
    return session


def test_shadow_swap_tables(mock_session):
    """Тестируем метод shadow_swap_tables с мок-сессией."""

    # Мокаем данные для теста
    debtor_data = [
        {'identifier': '12345', 'fio': 'John Doe', 'app_num': 'A12345', 'debt_sum': 1000, 'hash_value': 'hash123'},
        {'identifier': '67890', 'fio': 'Jane Smith', 'app_num': 'B67890', 'debt_sum': 1500, 'hash_value': 'hash456'},
    ]

    # Создаем экземпляр MainParser с мокаемой сессией
    parser = MainParser(debt_type=1, db_session=mock_session)

    # Мокаем функцию parse_page, чтобы она возвращала данные
    parser.parse_page = MagicMock(return_value=debtor_data)

    # Вызываем метод, который тестируем
    parser.shadow_swap_tables()

    # Проверяем, что был вызван execute с нужным запросом
    mock_session.execute.assert_any_call(
        'INSERT INTO public.debtor_shadow (column1, column2) VALUES (%s, %s)',
        ['12345', 'John Doe']
    )

    # Дополнительно: проверяем, что запрос был вызван хотя бы один раз
    assert mock_session.execute.call_count > 0
