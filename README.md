# data.gov.ru
Скачивание наборов данных с сайта data.gov.ru

Для работы необходимо зарегистрироваться на data.gov.ru и получить Ключ к API (access_token).<br/>

Данный ключ необходимо либо напрямую прописать в переменную access_token, <br/>
либо объявить в переменной среды окружения data_gov_access_token.

Отбор\фильтрация наборов данных ведется по полю organization_name. <br/>
Ищется вхождение подстроки поиска, без учета регистра, в organization_name.

Строка посика передается в качестве аргумента при вызове скрипта.<br/><br/>

python3 data_gov_ru.py --s 'Москв'    # Отбираем данные по Москве<br/>
python3 data_gov_ru.py                # сформирует файл organization.json<br/>
python3 data_gov_ru.py --t data       # сформирует файл datasets.json<br/>



Справка по использованию скрипта

usage: data_gov_ru.py [-h] [--s S] [--t {data,org}] [--console {yes,no}]

Справка по аргументам!

optional arguments:<br/>
  -h, --help          show this help message and exit<br/>
  --s S               Строка поиска<br/>
  --t {data,org}      Алгоритм поиска, по наборам данных "data", по <br/>
                      организациям "org". По умолчанию "org" <br/>
  --console {yes,no}  Вывод лога в консоль, по умолчанию "yes" <br/>


Результатом работы скрипта являются:
 - файл organizations.json (содержит список организаций) или файл datasets.json (содержит список наборов данных)
 - папки с наборами данных, названия папок соответствуют названиям организаций
