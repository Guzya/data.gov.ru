# Необходимо заполнить access_token 
# (сделал ч\з переменную окружения "data_gov_access_token")
# для работы с порталом data.gov.ru
# Выдается при регистрации на data.gov.ru
#
#


from urllib.request import urlopen
from urllib.error import URLError, HTTPError
import ssl
import json
from json import JSONDecodeError
import datetime
import time
import random
import os
import sys
import csv
import logging

def getDatasets(access_token, ctx):
    """Перечень наборов данных"""

    try:        
        html = urlopen('https://data.gov.ru/api/json/dataset/?access_token=' + access_token, context=ctx)
        return json.loads(html.read(), strict=False)
    logger.error HTTPError as e:
        print(e)
        return None
    logger.error URLError as e:
        print(e)
        return None
    logger.error JSONDecodeError as e:
        print(e)
        return None


def getDatasetVersion(dataset, access_token, ctx=None):
    """Достаем информацию по конкретному набору данных (НЕ сами данные)"""

    try:		
        logger.info('https://data.gov.ru/api/json/dataset/' + dataset + '?access_token=' + access_token)
        html = urlopen('https://data.gov.ru/api/json/dataset/' + dataset + '?access_token=' + access_token, context=ctx)
        return json.loads(html.read(), strict=False)
    except HTTPError as e:
        logger.error(e)
        return None
    except URLError as e:
        logger.error(e)
        return None
    except JSONDecodeError as e:
        with open(os.path.dirname(sys.argv[0]) + '/err_json.txt','w') as f:
            f.write(html.read())
        logger.error(e)
        return None
    except Exception as e:
        with open(os.path.dirname(sys.argv[0]) + '/err.txt','w') as f:
            f.write(html.read())
        logger.error(e)
        logger.error(dataset)
        return None
        
        
def getDatasetData(dataset, access_token, ctx=None):
    """Скачиваем набор данных (сами данные)"""

    try:
        
        # Создаем папку для организации, если ее нет
        creatorDir = dataset['creator'].replace('/','_')
        if not os.path.exists(creatorDir):
            os.makedirs(creatorDir)
        
        if dataset['format'] is not None:
            fileName = creatorDir + "/" + dataset['identifier'] + '.' + dataset['format']
            fileNameContent = creatorDir + "/" + dataset['identifier'] + "_structure." + dataset['format']
        else:
            fileName = creatorDir + "/" + dataset['identifier'] + '.HZ'
            fileNameContent = creatorDir + "/" + dataset['identifier'] + "_structure.HZ"
            logger.info('DataSet  {} : {}'.format(dataset['identifier'], dataset['format']))

        # Получаем временную отметку для следующего запроса
        html = urlopen('https://data.gov.ru/api/json/dataset/' + dataset[
            'identifier'] + '/version/' + '?access_token=' + access_token, context=ctx)
        logger.info('Url Version: ' + html.url)
        datasetCreated = json.loads(html.read())

        # Получаем ссылки на файлы с данными
        html = urlopen(
            'https://data.gov.ru/api/json/dataset/' + dataset['identifier'] + '/version/' + datasetCreated[0][
                'created'] + '/?access_token=' + access_token, context=ctx)
        logger.info('Url Sources: ' + html.url)

        datasetSource = json.loads(html.read())

        # Ищем последнюю модификацию по полю updated
        updateDate = datetime.datetime(1900, 1, 1, 0, 0, 0)
        index = 0
        for line in datasetSource:
            if updateDate < datetime.datetime.strptime(line['updated'], '%Y%m%dT%H%M%S'):
                updateDate = datetime.datetime.strptime(line['updated'], '%Y%m%dT%H%M%S')
                index = datasetSource.index(line)

        logger.info('updateDate: {} ,Index: {}'.format(updateDate, index))


       # Качаем файл с данными 
        if dataset['format'] == 'csv':
            dataFile = urlopen(
            'https://data.gov.ru/api/json/dataset/' + dataset['identifier'] + '/version/' + datasetCreated[0][
                'created'] + '/content/?access_token=' + access_token, context=ctx)
            logger.info('URL dataset file: ' + 'https://data.gov.ru/api/json/dataset/' + dataset['identifier'] + '/version/' + datasetCreated[0][
                'created'] + '/content/?access_token=' + access_token)
            
            with open(fileName, 'w') as f:
                dataLines = json.loads(dataFile.read())
                w = csv.DictWriter(f, dataLines[0].keys(),delimiter=';')
                w.writeheader()
                for line in dataLines:
                    w.writerow(line)
            
        else:        
            dataFile = urlopen(datasetSource[index]['source'], context=ctx)
            logger.info('URL dataset file: ' + datasetSource[index]['source'])
            with open(fileName, 'bw') as f:
                f.write(dataFile.read())

        html = urlopen(
            'https://data.gov.ru/api/json/dataset/' + dataset['identifier'] + '/version/' + datasetCreated[0][
                'created'] + '/structure' + '/?access_token=' + access_token, context=ctx)
        
        logger.info('URL structure file: ' + 'https://data.gov.ru/api/json/dataset/' + dataset['identifier'] + '/version/' + datasetCreated[0][
                'created'] + '/structure' + '/?access_token=' + access_token)
        
        datasetStructure = json.loads(html.read())

        dataFileStructure = urlopen(datasetStructure['source'], context=ctx)

        with open(fileNameContent, 'bw') as f:
            f.write(dataFileStructure.read())

    except HTTPError as e:
        logger.error(e)
        return None
    except URLError as e:
        logger.error(e)
        return None
    except JSONDecodeError as e:
        logger.error(e)
        return None        
    except Exception as e:
        logger.error('Ошибка -------:')
        logger.error(e)
        logger.error(dataset)
        return None



def getDataSetsFiltered(datasets, filterString):
    """Наборы данных по фильтру (наименование организации)"""

    datasetsFiltered = []
    for line in datasets:
        if (line['organization_name'] is None)or(filterString not in line['organization_name'].lower()):
            continue
        datasetsFiltered.append(line)
    return datasetsFiltered


def main(access_token, search_string=None, ctx=None):
    """Основная функция"""

    datasets = getDatasets(access_token, ctx)
    if datasets is None:
        logger.error('Не смогли получить перечень наборов данных')
        exit(1)
    logger.info('Всего наборов данных: {}'.format(len(datasets)))
    
    with open('datasets.json','w', encoding='utf-8') as f:
        json.dump(datasets, f, indent=4)
		
    if len(sys.argv) == 1:
        exit(0)
		
    datasets_data = getDataSetsFiltered(datasets, search_string.lower())        # Отбираем данные по Строке поиска
    logger.info('По критерию поиска подходит наборов: {}'.format(len(datasets_data)))
    
    datasets_version = []
    i = 0
    for dataset in datasets_data:
        i = i + 1
        logger.info('getDatasetVersion: ' + str(i))
        time.sleep(random.randint(0, 6))
        datasets_version.append(getDatasetVersion(dataset['identifier'], access_token, ctx))
    
    i = 0
    for dataset in datasets_version:
        i = i + 1
        logger.info('getDatasetData: ' + str(i))
        time.sleep(random.randint(0, 6))
        getDatasetData(dataset, access_token, ctx)


if __name__ == '__main__':
	
	
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
	
    formatLogger = logging.Formatter('%(asctime)s: %(name)-12s: %(funcName)-12s: %(levelname)-8s: %(message)s')
    formatConsole = logging.Formatter('%(asctime)s: %(levelname)-6s: %(message)s')
	
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(formatConsole)
	
    filehandler = logging.FileHandler('Data.gov.ru.log')
    filehandler.setLevel(logging.INFO)
    filehandler.setFormatter(formatLogger)
	
    logger.addHandler(console)
    logger.addHandler(filehandler)
	
    logger.info('Начало работы ------------------------------------- ')	
    startTime = datetime.datetime.now()
    
    ctx = ssl.create_default_context(capath='/etc/ssl/certs')
    
    access_token = os.environ['data_gov_access_token']  # Выдается при регистрации на data.gov.ru   
    if len(sys.argv) == 1:
        main(access_token, ctx=ctx)
    elif len(sys.argv) == 2:
        logger.info('Строка поиска: \'{}\''.format(sys.argv[1]))
        main(access_token, search_string=sys.argv[1], ctx=ctx)
    else:
        logger.error('Аргументов должно быть не больше одного.')
        logger.error('Пример: python3 data_gov_ru.py \'Москв\'')
        exit(1)
        
    stopTime = datetime.datetime.now()    
    logger.info('Времы выполнения скрипта: ' + str(stopTime - startTime))
    logger.info('Окончание работы ------------------------------------- ' )
