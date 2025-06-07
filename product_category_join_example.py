from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def build_product_category_df(products_df, categories_df, product_category_df):
    # Джойним продукты с таблицей связей (left join, чтобы сохранить все продукты)
    joined = products_df.join(product_category_df, on='product_id', how='left')
    # Джойним с категориями (left join, чтобы сохранить продукты без категорий)
    result = joined.join(categories_df, on='category_id', how='left')
    # Оставляем только нужные колонки и переименовываем
    result = result.select(
        F.col('product_name'),
        F.col('category_name')
    )
    return result

# Пример использования:
if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("ProductCategoryJoin").getOrCreate()

    # Тестовые данные
    products_data = [
        (1, "Яблоко"),
        (2, "Банан"),
        (3, "Молоко"),
        (4, "Хлеб")
    ]
    categories_data = [
        (10, "Фрукты"),
        (20, "Молочные продукты"),
        (30, "Выпечка")
    ]
    product_category_data = [
        (1, 10),
        (2, 10),
        (3, 20),
        (4, 30),
        (4, 20)  # Хлеб также в молочных продуктах (пример множественной категории)
    ]

    products_df = spark.createDataFrame(products_data, ["product_id", "product_name"])
    categories_df = spark.createDataFrame(categories_data, ["category_id", "category_name"])
    product_category_df = spark.createDataFrame(product_category_data, ["product_id", "category_id"])

    result_df = build_product_category_df(products_df, categories_df, product_category_df)
    result_df.show(truncate=False)

    spark.stop() 