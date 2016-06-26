#!/usr/bin/env python
# encoding: utf8

import re
import os
import os.path
import cjson
from collections import namedtuple, defaultdict, Counter
from hashlib import sha1
from time import sleep
from random import sample
from math import ceil
from datetime import datetime 

import requests as requests
requests.packages.urllib3.disable_warnings()

import pandas as pd
from bs4 import BeautifulSoup


DATA_DIR = 'data'
JSON_DIR = os.path.join(DATA_DIR, 'json')
JSON_LIST = os.path.join(JSON_DIR, 'list.txt')
HTML_DIR = os.path.join(DATA_DIR, 'html')
HTML_LIST = os.path.join(HTML_DIR, 'list.txt')

OKOGU1 = 1318010
OKOGU1_SIZE = 2189
OKOGU2 = 13173
OKOGU2_SIZE = 288

ATLAS = 'http://www.fsin-atlas.ru'
ATLAS_REGIONS = 'http://www.fsin-atlas.ru/catalog/regions/'
ATLAS_PAGES = os.path.join(DATA_DIR, 'atlas.json')

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2049.0 Safari/537.36'
}

ZAKUPKI_ATLAS_JOIN_CHECK = os.path.join(DATA_DIR, 'zakupki_atlas_join_check.xlsx')
BUS_REPORTS = os.path.join(DATA_DIR, 'bus_reports.xlsx')

OKPD_NAMES = os.path.join(DATA_DIR, 'okpd_names.xls')
CONTRACT_PRODUCTS = os.path.join(DATA_DIR, 'contract_products.xlsx')


ZakupkiCustomerRecord = namedtuple(
    'ZakupkiCustomerRecord',
    ['name', 'address', 'inn']
)
AtlasRecord = namedtuple(
    'AtlasRecord',
    ['title', 'address', 'description', 'size']
)
Coordinates = namedtuple('Coordinates', ['latitude', 'longitude'])
Address = namedtuple('Address', ['description', 'coordinates'])
ZakupkiAtlastRecord = namedtuple(
    'ZakupkiAtlastRecord',
    ['title', 'address', 'description', 'size', 'inn']
)

BusSearchRecord = namedtuple(
    'BusSearchRecord',
    ['inn', 'id', 'url', 'name', 'other']
)
BusReportYears = namedtuple(
    'BusReportYears',
    ['id', 'year_2013', 'year_2014', 'year_2015']
)
BusReportRecord = namedtuple(
    'BusReportRecord',
    ['id', 'year', 'incomings', 'expenses', 'operating']
)
BusReportIncomingsRecord = namedtuple(
    'BusReportIncomingsRecord',
    ['total', 'paid', 'force']
)
BusReportExpensesRecord = namedtuple(
    'BusReportExpensesRecord',
    ['total', 'salaries', 'gkh']
)
BusReportOperatingRecord = namedtuple(
    'BusReportOperatingRecord',
    ['total']
)

ContractSupplier = namedtuple(
    'ContractSupplier',
    ['name', 'inn', 'address']
)
ContractCustomer = namedtuple(
    'ContractCustomer',
    ['name', 'inn', 'address']
)
Code = namedtuple('Code', ['id', 'name'])
ContractProduct = namedtuple(
    'ContractProduct',
    ['name', 'money', 'okpd', 'okved']
)
Contract = namedtuple(
    'Contract',
    ['id', 'date', 'lot', 'purchase', 'suppliers',
     'customer', 'products']
)
OkpdNameRecord = namedtuple(
    'OkpdNameRecord',
    ['id', 'name']
)


def log_progress(sequence, every=None, size=None):
    from ipywidgets import IntProgress, HTML, VBox
    from IPython.display import display

    is_iterator = False
    if size is None:
        try:
            size = len(sequence)
        except TypeError:
            is_iterator = True
    if size is not None:
        if every is None:
            if size <= 200:
                every = 1
            else:
                every = size / 200     # every 0.5%
    else:
        assert every is not None, 'sequence is iterator, set every'

    if is_iterator:
        progress = IntProgress(min=0, max=1, value=1)
        progress.bar_style = 'info'
    else:
        progress = IntProgress(min=0, max=size, value=0)
    label = HTML()
    box = VBox(children=[label, progress])
    display(box)

    index = 0
    try:
        for index, record in enumerate(sequence, 1):
            if index == 1 or index % every == 0:
                if is_iterator:
                    label.value = '{index} / ?'.format(index=index)
                else:
                    progress.value = index
                    label.value = u'{index} / {size}'.format(
                        index=index,
                        size=size
                    )
            yield record
    except:
        progress.bar_style = 'danger'
        raise
    else:
        progress.bar_style = 'success'
        progress.value = index
        label.value = str(index or '?')


def jobs_manager():
    from IPython.lib.backgroundjobs import BackgroundJobManager
    from IPython.core.magic import register_line_magic
    from IPython import get_ipython
    
    jobs = BackgroundJobManager()

    @register_line_magic
    def job(line):
        ip = get_ipython()
        jobs.new(line, ip.user_global_ns)

    return jobs


def kill_thread(thread):
    import ctypes
    
    id = thread.ident
    code = ctypes.pythonapi.PyThreadState_SetAsyncExc(
        ctypes.c_long(id),
        ctypes.py_object(SystemError)
    )
    if code == 0:
        raise ValueError('invalid thread id')
    elif code != 1:
        ctypes.pythonapi.PyThreadState_SetAsyncExc(
            ctypes.c_long(id),
            ctypes.c_long(0)
        )
        raise SystemError('PyThreadState_SetAsyncExc failed')


def hash_item(item):
    return sha1(item.encode('utf8')).hexdigest()


hash_url = hash_item


def get_json_filename(url):
    return '{hash}.json'.format(
        hash=hash_url(url)
    )


def get_json_path(url):
    return os.path.join(
        JSON_DIR,
        get_json_filename(url)
    )


def load_items_cache(path):
    with open(path) as file:
        for line in file:
            line = line.decode('utf8').rstrip('\n')
            if '\t' in line:
                # several lines in cache got currepted
                hash, item = line.split('\t', 1)
                yield item


def list_json_cache():
    return load_items_cache(JSON_LIST)


def update_items_cache(item, path):
    with open(path, 'a') as file:
        hash = hash_item(item)
        file.write('{hash}\t{item}\n'.format(
            hash=hash,
            item=item.encode('utf8')
        ))
        

def update_json_cache(url):
    update_items_cache(url, JSON_LIST)


def dump_json(path, data):
    with open(path, 'w') as file:
        file.write(cjson.encode(data))


def load_raw_json(path):
    with open(path) as file:
        return cjson.decode(file.read())


def download_json(url):
    response = requests.get(
        url,
        headers=HEADERS
    )
    try:
        return response.json()
    except ValueError:
        return


def fetch_json(url):
    path = get_json_path(url)
    data = download_json(url)
    dump_json(path, data)
    update_json_cache(url)


def fetch_jsons(urls):
    for url in urls:
        fetch_json(url)


def load_json(url):
    path = get_json_path(url)
    return load_raw_json(path)


def get_clearspending_select_okogu_url(okogu, page=1, perpage=50):
    return ('http://openapi.clearspending.ru/'
            'restapi/v3/customers/select/?okogu={okogu}'
            '&page={page}&perpage={perpage}'.format(
                okogu=okogu,
                page=page,
                perpage=perpage
            ))


def get_pages_count(total, page, start):
    pages = int(ceil(float(total) / page))
    for index in xrange(pages):
        yield index + start


def get_html_filename(url):
    return '{hash}.html'.format(
        hash=hash_url(url)
    )


def get_html_path(url):
    return os.path.join(
        HTML_DIR,
        get_html_filename(url)
    )


def list_html_cache():
    return load_items_cache(HTML_LIST)


def update_html_cache(url):
    update_items_cache(url, HTML_LIST)


def dump_text(path, text):
    with open(path, 'w') as file:
        file.write(text.encode('utf8'))


def load_text(path):
    with open(path) as file:
        return file.read().decode('utf8')


def download_html(url):
    response = requests.get(url)
    return response.text


def fetch_html(url):
    path = get_html_path(url)
    html = download_html(url)
    dump_text(path, html)
    update_html_cache(url)


def fetch_htmls(urls):
    for url in urls:
        fetch_html(url)


def load_html(url):
    path = get_html_path(url)
    return load_text(path)


def get_soup(html):
    return BeautifulSoup(html, 'lxml')


def load_atlas_region_urls():
    html = load_html(ATLAS_REGIONS)
    soup = get_soup(html)
    for link in soup.find_all('a'):
        href = link['href']
        if href.startswith('/catalog/region'):
            yield ATLAS + href


def load_atlas_urls(url):
    html = load_html(url)
    soup = get_soup(html)
    for link in soup.find_all('a'):
        attrs = link.attrs
        if 'href' in attrs:
            href = link['href']
            if href.startswith('/catalog/object'):
                yield ATLAS + href


def load_atlas_page(url):
    html = load_html(url)
    soup = get_soup(html)
    title = soup.find('h1').text
    data = {}
    for item in soup.find('ul', class_='options').find_all('li'):
        key = item.find('span', class_='label').text
        value = item.find('span', class_='content').text
        data[key] = value
    address = data.get(u'Полный адрес:')
    description = data.get(u'Описание:')
    size = data.get(u'Содержится:')
    return AtlasRecord(
        title,
        address,
        description,
        size
    )


def dump_atlas_pages(atlas_pages):
    dump_json(ATLAS_PAGES, atlas_pages)


def load_atlas_pages():
    data = load_raw_json(ATLAS_PAGES)
    for item in data:
        yield AtlasRecord(*item)


def load_zakupki_customers(urls):
    for url in urls:
        data = load_json(url)
        for item in data['customers']['data']:
            name = item['shortName']
            address = item['factualAddress']['addressLine']
            inn = item['inn']
            yield ZakupkiCustomerRecord(name, address, inn)


def get_geocode_url(address):
    return u'http://geocode-maps.yandex.ru/1.x/?format=json&geocode={address}'.format(
        address=address
    )


def parse_geocode_data(data):
    response = data['response']['GeoObjectCollection']
    data = response['featureMember']
    if data:
        item = data[0]['GeoObject']
        longitude, latitude = item['Point']['pos'].split(' ')
        longitude = float(longitude)
        latitude = float(latitude)
        return Coordinates(
            latitude,
            longitude
        )
        
        
def load_coordinates(address):
    url = get_geocode_url(address)
    data = load_json(url)
    return parse_geocode_data(data)


def get_distance(a, b):
    return (a.latitude - b.latitude)**2 + (a.longitude - b.longitude)**2


def dump_zakupki_atlas_join_check(zakupki_customer_records, atlas_records):
    mapping = {
        _.description: _.coordinates
        for _ in address_coordinates
    }

    zakupti_coordinates = defaultdict(list)
    for record in zakupki_customer_records:
        coordinates = mapping[record.address]
        zakupti_coordinates[coordinates].append(record)
        
    data = []
    for record in atlas_records:
        address = record.address
        if address:
            atlas_coordinates = mapping[address]
            coordinates = min(
                zakupti_coordinates,
                key=lambda _: get_distance(_, atlas_coordinates)
            )
            customers = zakupti_coordinates[coordinates]
            distance = get_distance(atlas_coordinates, coordinates)
            for customer in customers:
                correct = None
                if len(customers) == 1 and distance == 0:
                    correct = '+'
                data.append((
                    correct, distance, record.address,
                    customer.address, record.title, customer.name,
                    record.description, record.size, customer.inn
                ))
    table = pd.DataFrame(
        data,
        columns=[
            'correct', 'distance',
            'atlas_address', 'zakupki_address',
            'atlas_name', 'zakupki_name',
            'description', 'size', 'inn'
        ]
    )
    table.to_excel(ZAKUPKI_ATLAS_JOIN_CHECK, index=False)


def read_excel(path):
    table = pd.read_excel(path)
    return table.where(pd.notnull(table), None)


def load_zakupki_atlas_join_check():
    table = read_excel(ZAKUPKI_ATLAS_JOIN_CHECK)
    for _, row in table.iterrows():
        if row.correct == '+':
            yield ZakupkiAtlastRecord(
                row.atlas_name,
                row.zakupki_address,
                row.description,
                row['size'],
                str(row.inn)
            )


def get_bus_search_url(inn):
    return ('http://bus.gov.ru/public/agency/extendedSearchAgencyNew.json?action=&agency={inn}'
            '&documentTypes=A&okatoSubElements=false&orderAttributeName=rank&orderDirectionASC=false'
            '&page=1&pageSize=10&ppoSubElements=false&primaryActivitySubElements=false&searchTermCondition=and'
            '&secondaryActivitySubElements=false&vguSubElements=true&withBranches=true').format(
        inn=inn
    )


def parse_bus_search_record(inn, data):
    agencies = data['agencies']
    id = None
    url = None
    name = None
    other = None
    if agencies:
        other = len(agencies) - 1
        # assume first result is best
        agency = agencies[0]
        id = agency['agencyId']
        name = agency['shortName']
        url = agency['website']
    return BusSearchRecord(inn, id, url, name, other)


def load_bus_search_record(inn):
    url = get_bus_search_url(inn)
    data = load_json(url)
    return parse_bus_search_record(inn, data)


def get_bus_latest_report_url(id):
    return ('http://bus.gov.ru/public/agency/last-annual-balance-F0503121-info.json'
            '?agencyId={id}'.format(id=id))


def parse_bus_report_years(id, data):
    years = {
        _['financialYear']: _['id']
        for _ in data['formationPeriods']
    }
    return BusReportYears(
        id,
        year_2013=years.get(2013),
        year_2014=years.get(2014),
        year_2015=years.get(2015)
    )


def load_bus_report_years(id):
    url = get_bus_latest_report_url(id)
    data = load_json(url)
    return parse_bus_report_years(id, data)


def get_bus_report_url(id, year):
    return ('http://bus.gov.ru/public/agency/annual-balance-F0503121-info.json'
            '?agencyId={id}&annualBalanceId={year}'.format(
            id=id,
            year=year
        ))


def parse_bus_float(value):
    if value:
        # 554,404,818.06
        return float(value.replace(',', ''))

    
def parse_bus_report(id, year, data):
    balance = data['annualBalance']
    sections = {}
    for record in balance['incomings']:
        section = record['lineCode']
        value = parse_bus_float(record['totalEndYear'])
        sections[section] = value
    incomings = BusReportIncomingsRecord(
        total=sections['010'],
        paid=sections['040'],
        force=sections['050']
    )
    sections = {}
    for record in balance['expenses']:
        section = record['lineCode']
        value = parse_bus_float(record['totalEndYear'])
        sections[section] = value
    expenses = BusReportExpensesRecord(
        total=sections['150'],
        salaries=sections['160'],
        gkh=sections['173']
    )
    sections = {}
    for record in balance['netOperatingResults']:
        section = record['lineCode']
        value = parse_bus_float(record['totalEndYear'])
        sections[section] = value
    operating = BusReportOperatingRecord(
        total=sections['290'],
    )
    return BusReportRecord(id, year, incomings, expenses, operating)
    
    
def load_bus_report(id, year):
    url = get_bus_report_url(id, year)
    data = load_json(url)
    return parse_bus_report(id, year, data)


def dump_bus_reports(bus_reports, bus_search_records):
    mapping = {_.id: _ for _ in bus_search_records}
    data = []
    for record in bus_reports:
        search_record = mapping[record.id]
        incomings = record.incomings
        expenses = record.expenses
        url = 'http://bus.gov.ru/pub/agency/{id}/annual-balances/{year}'.format(
            id=record.id,
            year=record.year
        )
        data.append((
            search_record.name,
            search_record.inn,
            url,
            incomings.total,
            incomings.paid,
            expenses.total,
            expenses.salaries,
            record.operating.total
        ))
    table = pd.DataFrame(
        data,
        columns=[
            'name',
            'inn',
            'url',
            'incomings_total',
            'incomings_paid',
            'expenses_total',
            'expenses_salaries',
            'operating_total'
        ]
    )
    table.to_excel(BUS_REPORTS, index=False)


def get_clearspending_select_inn_url(inn, page=1, perpage=50):
    return ('http://openapi.clearspending.ru/'
            'restapi/v3/contracts/select/?customerinn={inn}'
            '&page={page}&perpage={perpage}'.format(
                inn=inn,
                page=page,
                perpage=perpage
            ))


def parse_contract_suppliers(data):
    if not data:
        return
    for item in data:
        inn = item.get('inn')
        name = item.get('organizationName')
        address = item.get('factualAddress')
        yield ContractSupplier(name, inn, address)


def parse_contract_customer(data):
    if not data:
        return
    name = data['fullName']
    inn = data['inn']
    address = data.get('postalAddress')
    return ContractCustomer(name, inn, address)


def parse_contract_products(data):
    if not data:
        return
    for item in data:
        name = item.get('name')
        money = item.get('sum')
        okpd = None
        if 'OKPD' in item:
            okpd = item['OKPD']
            okpd = Code(okpd['code'], okpd.get('name'))
        okved = None
        if 'OKVED' in item:
            okved = item['OKVED']
            okved = Code(okved['code'], okved.get('name'))
        yield ContractProduct(name, money, okpd, okved)


def parse_contract_date(item):
    date = item.get('publishDate')
    if date:
        return datetime.strptime(date[:10], '%Y-%m-%d')
        
        
def parse_contracts(data):
    for item in data['contracts']['data']:
        id = item.get('regNum')
        date = parse_contract_date(item)
        lot = None
        if 'lot' in item:
            lot = item['lot']['subject']
        purchase = None
        if 'purchaseInfo' in item:
            purchase = item['purchaseInfo'].get('name')
        suppliers = list(parse_contract_suppliers(item.get('suppliers')))
        customer = parse_contract_customer(item.get('customer'))
        products = list(parse_contract_products(item.get('products')))
        yield Contract(id, date, lot, purchase, suppliers, customer, products)

        
def load_contracts(urls):
    for url in urls:
        data = load_json(url)
        if data:
            for record in parse_contracts(data):
                yield record


def okpd_names():
    table = read_excel(OKPD_NAMES)
    for _, row in table.iterrows():
        okpd = str(row[0])[:4]
        name = row[1]
        yield OkpdNameRecord(okpd, name)
        

def dump_contract_products(contracts, okpd_names):
    counts = Counter()
    money = Counter()
    mapping = {_.id: _.name for _ in okpd_names}
    for contract in contracts:
        for product in contract.products:
            okpd = product.okpd
            if okpd:
                id = okpd.id
                if '.' in id:
                    # id = id.split('.')
                    # id = '.'.join(id[:2])
                    id = id[:4]
                    counts[id] += 1
                    money[id] += product.money or 0
    data = []
    for id, count in counts.most_common():
        name = mapping.get(id)
        price = money[id]
        data.append((id, name, count, price))
    table = pd.DataFrame(
        data,
        columns=['id', 'name', 'count', 'price']
    )
    table.to_excel(CONTRACT_PRODUCTS, index=False)
