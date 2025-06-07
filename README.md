 pyspark-product-category-join

Пример решения задачи на PySpark DataFrame API.  
Реализован метод, который по трем DataFrame (`products`, `categories`, `product_category`) строит итоговый DataFrame:

✅ Пары "Имя продукта — Имя категории" (все существующие связи)  
✅ Продукты без категорий (в этом случае "Имя категории" — пустое поле)

---

## Описание задачи

Исходные DataFrame:

- `products`:
    - `product_id` (int)
    - `product_name` (string)
- `categories`:
    - `category_id` (int)
    - `category_name` (string)
- `product_category`:
    - `product_id` (int)
    - `category_id` (int)

Каждому продукту может соответствовать несколько категорий или ни одной.  
Каждой категории может соответствовать несколько продуктов или ни одного.

---

## Ожидаемый результат

Итоговый DataFrame с двумя колонками:

- `product_name`
- `category_name` (может быть пустым, если у продукта нет категории)

---

## Алгоритм

1️⃣ LEFT JOIN `products` ← `product_category` по `product_id`  
2️⃣ LEFT JOIN результат → `categories` по `category_id`  
3️⃣ Выборка колонок `product_name` и `category_name`

---

## Пример использования

См. файл `pyspark_product_category_join.py`.  
В файле показан пример создания тестовых DataFrame и вызова метода.

---

## Загрузка и установка зависимостей

1️⃣ Склонируйте проект:

```bash
git clone https://github.com/ВАШ_ЮЗЕРНЕЙМ/pyspark-product-category-join.git
cd pyspark-product-category-join
```

2️⃣ Создайте виртуальное окружение (рекомендуется):

```bash
python -m venv venv
source venv/bin/activate    # Linux / macOS
venv\Scripts\activate       # Windows
```

3️⃣ Установите зависимости:

```bash
pip install pyspark
```

---

## Запуск примера

```bash
python pyspark_product_category_join.py
```

---

## Лицензия

MIT License.
