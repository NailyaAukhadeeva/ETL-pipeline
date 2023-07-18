**Create ETL-pipeline**<br/>
---
Работа выполнена: 06.2023
### Описание задачи<br/>
Результатом этого задания должна быть таблица в ClickHouse, в которой записаны значения метрик, фиксирующие уровень активности пользователей в приложении. Таблица ежедневно должна наполняться данными за прошлый день.
Структура финальной таблицы:
- Дата - event_date
- Название среза - dimension
- Значение среза - dimension_value
- Число просмотров - views
- Число лайков - likes
- Число полученных сообщений - messages_received
- Число отправленных сообщений - messages_sent
- От скольких пользователей получили сообщения - users_received
- Скольким пользователям отправили сообщение - users_sent
- Срез — это os, gender и age
### Инструменты<br/>
- jupyter notebook;
- airflow

### План работ<br/>  
1. Выбрать из таблицы feed_actions для каждого юзера число просмотров и лайков. Из таблицы message_actions выбрать статистику обмена сообщениями (отправленные, полученные). Полученные таблицы объединить;
2. Посчитать метрики в срезах: пол, возраст и ос;
3. Финальные данные со всеми метриками записываем в таблицу в ClickHouse.
4. Обернуть в даг с несколькими тасками
