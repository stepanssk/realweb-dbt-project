# dbt проект Реалвеб

## Почему dbt?

* Контроль версии SQL моделей
* Написание тестов на схемы данных и на качество данных
* Отправка алёртов по результам тестов
* Автоматическая документация
* Оптимизация SQL за счёт переиспользования моделей
* Более гибкий аналог Scheduled Queries в BigQuery
* [и многое другое](https://docs.getdbt.com/docs/introduction)

## Документация

Красивого сайта у нас ещё нет, но вы всегда можете посмотреть на ваш проект, выполнив последовательно
```sh
dbt docs generate
dbt docs serve
``` 

## Стратегия изменений в репозитории проекта

Придерживаемся стратегии [GitHub Flow](https://docs.github.com/en/get-started/quickstart/github-flow). Для любых изменений **создаём отдельную ветку**, проверяем что всё работает (как минимум `dbt run`), отправляем pull request в master ветку, ответственный за master делает код ревью и после мерджит с master.

![image](https://user-images.githubusercontent.com/43750521/170499387-7873e660-1654-469e-a940-69b70f432189.png)

## Как начать работать с проектом?

### Вариант 1 - с использованием Anaconda

Спасибо [@nirakon](https://github.com/nirakon) за подробную инструкцию.

1. [Устанавливаем Miniconda](https://docs.conda.io/en/latest/miniconda.html). Если уже стоит Anaconda, то не нужно.
2. [Устанавливаем Visual Studio Code](https://code.visualstudio.com/download)
3. [Устанавливаем Git](https://git-scm.com/download)
4. Запускаем Anaconda Prompt (Miniconda3). Создаём новое пространство для dbt `conda create --name dbt-env pip`.
Если у Вас установлен python 3.10 (проверить - `python --version`), необходимо вручную прописать версию поменьше, например так: `conda create --name dbt-env python=3.9.0 pip`
5. Переходим в него `conda activate dbt-env` и устанавливаем dbt  `pip install dbt-bigquery`
6. Проверяем установку `dbt --version`. 
7. Устанавливаем расширение `ms-python.python` в VSCode (поиск расширений - CTRL + SHIFT + X).
8. В VSCode назначаем интерпетатор для Python в созданном пространстве dbt-env (Ctrl+Shift+P -> Python: Select Interpreter -> выбираем наше пространство dbt-env)
9. Подключаемся к GitHub в VSCode и [скачиваем нужный репозиторий](https://code.visualstudio.com/docs/editor/versioncontrol#_cloning-a-repository) (https://github.com/realweb-msk/realweb-dbt-project). Перед этим на всякий случай сохраните в надежном месте открытые в VSCode файлы. Клонировать репозиторий рекомендую в корневую папку (`C:/Users/Username/`)
10. В корневой папке создаем папку `.dbt`,  а в ней файл `profiles.yml` для подключения к ClickHouse. Этот файл должен находиться за пределами вашего проекта dbt, чтобы избежать передачи конфиденциальных учетных данных в git. DBT будет искать этот файл именно по этому адресу: `C:/Users/Username/.dbt/profiles.yml`. В `profiles.yml` копируем и сохраняем следующий код:

 ```yml
# Пример profiles.yml. Обычно используется две среды dev (development) и prod (production)

realweb:
  outputs:
    dev:
      dataset: dbt_username # ! не забудь поменять на своё имя!
      fixed_retries: 1
      keyfile: /Users/Username/secrets/dbt_runner_for_realweb.json #! не забудь поменять на адрес до своего json-ключа!
      method: service-account
      priority: interactive
      project: realweb-152714
      threads: 4
      timeout_seconds: 300
      location: US
      type: bigquery
    prod:
      dataset: dbt_production
      keyfile: /Users/Username/secrets/dbt_runner_for_realweb.json #! не забудь поменять на адрес до своего json-ключа!
      method: service-account
      priority: interactive
      project: realweb-152714
      threads: 4
      timeout_seconds: 300
      location: US
      type: bigquery
  target: dev
 ```
Также необходимо получить json-ключ в GCP *(или попросить его у меня)* и положить его в надёжное место (например,в папку `secrets`)

11. Выполняем в консоли `dbt debug`. Если всё хорошо, можно начать пользоваться dbt.
12. Открыть консоль можно и в самом VSCode. Нажмите на "Терминал" , затем - "создать терминал", а потом в открывшемся окне справа выберите (˅) нужную вам консоль (н-р command prompt) 

### Вариант 2 - без использования Anaconda

Основаная суть - вместо установки Miniconda устанавливаем пакет для виртуальных сред (см. https://packaging.python.org/en/latest/guides/installing-using-pip-and-virtual-environments/)

1. Устанавливаем virtual env: `py -m pip install --user virtualenv`
2. Создаем виртуальную среду: `py -m venv dbt-env`
3. Активируем созданную среду: `.\dbt-env\Scripts\activate`
4. Далее действуем аналогично варианту с cond-ой - переходим к инструкции выше во вторую часть пункта 5 (устанавливаем dbt `pip install dbt-bigquery` ...)

## Если я хочу создать свой проект?

1. [Укрепиться в решении - Вводный вебинар от OWOX про dbt](https://www.youtube.com/watch?v=eLDV_y0Chow)
2. [Пройти небольшой бесплатный курс по dbt](https://courses.getdbt.com/)
3. [Первые шаги](https://docs.getdbt.com/dbt-cli/install/overview)
4. [Запускаем dbt в продакшн на Google Cloud Platform](https://github.com/realweb-msk/realweb-dbt)

## Задание по dbt

NB: `username` пишите, пожалуйста, в формате "первая буква имени" + "фамилия" - так мы сможем найти вашу таблицу и ваш датасет. Примеры: `rsultanov`,`mpopkov`,`dlazuta`

Представьте: Вы пришли на работу в Риалвеб. Ваш коллега заболел, и вам предстоит закончить начатую им задачу: подготовить данные для дашборда (его вам предстоить сделать во время следующего спринта). В дашборде вам предстоит отразить количество установок приложения, показов рекламы, кликов по объявлениям и расходов по кампаниям в группировке по дате, названию кампании, группе объявлений, платформе (ios, android), рекламному кабинету. На данном этапе вам следует написать модель `username_af_installs` и материализовать ее как таблицу в датасете `dbt_production`.
К счастью, ваш коллега уже настроил работу потоков данных с помощью Garpun Feeds и написал [stg_](https://youtu.be/qOx8l_QFz9I?t=21) модели для установок и данных из рекламных кабинетов.

1. Пройдите указанным выше путем (**Как начать работать с проектом**). Если не будет хватать каких-то разрешений - выдадим.
2. Создайте свою ветку для работы - в левом нижнем углу в VSCode нажмите на **main**, затем *Создание новой ветви из...*, затем введите имя ветви (например, своё имя), и затем *origin/main*
3. Выполните в консоли `dbt run` - так все модели в проекте попадут в ваш датасет **dbt_username**, по умолчанию в виде view.
4. Выполните в консоли `dbt test -m stg_af_intalls -t prod` - после этого к таблице `stg_af_intalls` (мы выбираем ее с помощью флага `-m`), находящейся в датасете `dbt_production` (это наш production-датасет, мы выбираем его с помощью `-t prod`), будут применены тесты, описанные для этой модели в файле `schema.yml` (папка `models`)
5. Теперь - к работе! Напишите модель `username_af_installs` и материализуйте её как таблицу в датасете `dbt_production`. Модель представляет собой SQL-код, по результатам работы которого должна получиться целевая таблица: клики, показы, установки, расходы в разбивке по дате, кампании, группе объявления, платформе и источнику (рекламному кабинету). В таблице должны быть только кампании Риалвеба (`is_realweb=TRUE`) и только User Acquisition (`is_ret_campaign=FALSE`). Обращайтесь к существующим моделям с помощью функции ref, например: 
```sql
SELECT * FROM {{ ref('stg_yandex') }}
WHERE is_realweb AND NOT is_ret_campaign
```
В процессе работы помогут команды  `dbt run -m username_af_installs` - материализация данной модели в вашем датасете (чтобы проверить код на ошибки в SQL),
`dbt run -m username_af_installs -t prod` - материализация модели в `dbt_production`.
Дополнительно можете добавить в модель [партицирование по дате](https://docs.getdbt.com/reference/resource-configs/bigquery-configs#using-table-partitioning-and-clustering).

6. Ваш заказчик требователен. В дашборде не должно быть случайно попавших туда NULL-компаний, а в столбце "платформа" не должно быть ничего кроме "ios" и "android". В файле `schema.yml` задайте для вашей модели `username_af_installs` два теста: один для названия кампании, а другой для платформы (вдохновиться примерами можно в этом же файле). Протестируйте вашу модель, выполнив `dbt test -m username_af_installs -t prod`. В норме *один из тестов должен упасть*.

7. В системе управления версиями (CTRL + SHIFT + G) зафиксируйте ваши изменения (символ ☑️) , затем - "опубликовать ветвь", а потом  нажмите на символ справа от галочки (CREATE PULL REQUEST). Так ваши изменения попадут на код-ревью. Если все хорошо, то мы добавим их в ветку `main` :)
