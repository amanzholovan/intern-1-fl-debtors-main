import requests
from bs4 import BeautifulSoup
from collections import Counter

# URL страницы
url = 'https://tazalau.qoldau.kz/ru/list/debtor?p=5'

# Загрузка страницы
response = requests.get(url)
response.raise_for_status()  # Проверка успешности запроса

# Парсинг HTML
soup = BeautifulSoup(response.text, 'html.parser')

# Извлечение всех номеров заявлений (по вашему примеру, это 'Входящий номер заявления')
app_nums = []
for row in soup.find_all('tr'):
    # Предположим, что номер заявления в 4-й ячейке таблицы (td[3])
    td_elements = row.find_all('td')
    if len(td_elements) > 3:
        app_num = td_elements[3].text.strip()  # Входящий номер заявления
        app_nums.append(app_num)

# Проверка на уникальность
app_num_counts = Counter(app_nums)
duplicates = {num: count for num, count in app_num_counts.items() if count > 1}

# Выводим информацию о дублирующихся номерах заявлений
if duplicates:
    print("Найдены дубликаты номеров заявлений:")
    for num, count in duplicates.items():
        print(f"Номер заявления {num} встречается {count} раз(а)")
else:
    print("Дубликатов номеров заявлений не найдено.")
