# SparkTask

### Java 8, Scala 2.12.5, Spark 3.3.1, Maven

### Запуск
```
1. git clone
2. mvn clean package
3. ./bin/spark-submit --class ru.shemich.App SparkTask-1.0-jar-with-dependencies.jar
```
### Данные
Две таблицы
1. Транзакции между компаниями.
````
   COMPANY_FROM: string - Имя компании отправителя

   COMPANY_TO: string - Имя компании получателя

   AMOUNT: long - Сумма транзакции

   TRANSACTION_ID long - Идентификатор транзакции
````
2. Справочник регионов, в которых зарегистрированы компании
```
   REGION: string - Имя региона

   COMPANIES: array<string> - Список компаний зарегистрированных в этом регионе
```
Ожидаемый результат:
1. Итоговая таблица

   По каждому региону собрать зарегистрированные в нем компании, их баланс, количество транзакций

   Отсортировать по региону и по убывающей прибыли
```
   REGION: string - Имя региона

   COMPANY: string - Имя компании

   BALANCE: long - Баланс компании

   COUNT: long - Количество транзакций
```
2. Код

   Как минимум набор команд для shell

   Как максимум компилируемый проект со сборкой jar
