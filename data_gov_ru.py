# Необходимо заполнить access_token 
# для работы с порталом data.gov.ru
# Выдается при регистрации на data.gov.ru
#
#


from urllib.request import urlopen
from urllib.error import URLError, HTTPError
import ssl
import json
import datetime
import time
import random
import os


def getDatasets(access_token, ctx):
    """Перечень наборов данных"""

    try:
        html = urlopen('https://data.gov.ru/api/json/dataset/?access_token=' + access_token, context=ctx)
        return json.loads(html.read())
    except HTTPError as e:
        print(e)
        return None
    except URLError as e:
        print(e)
        return None


def getDatasetVersion(dataset, access_token, ctx=None):
    """Достаем информацию по конкретному набору данных (НЕ сами данные)"""

    try:
        html = urlopen('https://data.gov.ru/api/json/dataset/' + dataset + '?access_token=' + access_token, context=ctx)
        return json.loads(html.read())
    except HTTPError as e:
        print(e)
        return None
    except URLError as e:
        print(e)
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
            print('DataSet  {} : {}'.format(dataset['identifier'], dataset['format']))

        # Получаем временную отметку для следующего запроса
        html = urlopen('https://data.gov.ru/api/json/dataset/' + dataset[
            'identifier'] + '/version/' + '?access_token=' + access_token, context=ctx)
        print('Url Version: ' + html.url)
        datasetCreated = json.loads(html.read())

        # Получаем ссылки на файлы с данными
        html = urlopen(
            'https://data.gov.ru/api/json/dataset/' + dataset['identifier'] + '/version/' + datasetCreated[0][
                'created'] + '/?access_token=' + access_token, context=ctx)
        print('Url Sources: ' + html.url)

        datasetSource = json.loads(html.read())

        # Ищем последнюю модификацию по полю updated
        updateDate = datetime.datetime(1900, 1, 1, 0, 0, 0)
        index = 0
        for line in datasetSource:
            if updateDate < datetime.datetime.strptime(line['updated'], '%Y%m%dT%H%M%S'):
                updateDate = datetime.datetime.strptime(line['updated'], '%Y%m%dT%H%M%S')
                index = datasetSource.index(line)

        print('updateDate: {} ,Index: {}'.format(updateDate, index))

        # Качаем файл с данными
        dataFile = urlopen(datasetSource[index]['source'], context=ctx)
        print('URL source: ' + datasetSource[index]['source'])
                	        
        with open(fileName, 'bw') as f:
            f.write(dataFile.read())

        html = urlopen(
            'https://data.gov.ru/api/json/dataset/' + dataset['identifier'] + '/version/' + datasetCreated[0][
                'created'] + '/structure' + '/?access_token=' + access_token, context=ctx)

        datasetStructure = json.loads(html.read())

        dataFileStructure = urlopen(datasetStructure['source'], context=ctx)

        with open(fileNameContent, 'bw') as f:
            f.write(dataFileStructure.read())

    except HTTPError as e:
        print(e)
        return None
    except URLError as e:
        print(e)
        return None
    except Exception as e:
        print(e)
        print(dataset)
        return None



def getDataSetsFiltered(datasets, filterString):
    """Наборы данных по фильтру (наименование организации)"""

    datasetsFiltered = []
    for line in datasets:
        if filterString not in line['organization_name']:
            continue
        datasetsFiltered.append(line)
    return datasetsFiltered


def main(access_token, ctx=None):
    """Основная функция"""

    datasets = getDatasets(access_token, ctx)
    KBR = getDataSetsFiltered(datasets,'Москв')    # Отбираем данные по Москве

    KBRVersion = []
    i = 0
    for dataset in KBR:
        i = i + 1
        print('getDatasetVersion: ' + str(i))
        time.sleep(random.randint(0, 6))
        KBRVersion.append(getDatasetVersion(dataset['identifier'], access_token, ctx))
    
    i = 0
    for dataset in KBRVersion:
        i = i + 1
        print('getDatasetData: ' + str(i))
        time.sleep(random.randint(0, 6))
        getDatasetData(dataset, access_token, ctx)


if __name__ == '__main__':

    ctx = ssl.create_default_context(capath='/etc/ssl/certs')
    
    access_token = ''  # Выдается при регистрации на data.gov.ru

    main(access_token, ctx)
