import re
import os
import requests
import multiprocessing as mp
import queue
import json

from time import sleep
from bs4 import BeautifulSoup


def save_page(html, id):
    with open('./pages/{id}'.format(id=id), 'w') as file:
        file.write(html)


def get_text(html):
    if html == '':
        return None

    return BeautifulSoup(html, 'html.parser').text.strip()


def get_author_list(soup):
    return [html.text.strip() for html in soup.find_all('a')]


def extract_book_info(html):
    ret = dict()
    soup = BeautifulSoup(html, 'html.parser')

    detail_section = soup.find('section', {'id': 'details'})
    category = set([el.text.strip() for el in detail_section.find_all('a')])
    ret['Kategórie'] = [x for x in category]

    # Details
    detail_titles = [el.text.strip() for el in detail_section.find_all('dt')]
    detail_values = [el.text.strip() for el in detail_section.find_all('dd')]

    zips = list(zip(detail_titles, detail_values))

    # cut at index of '' due to details DOM structure of Martinus
    val = dict((x, y) for x, y in zips[:detail_titles.index('')])
    ret['Detaily'] = val

    # Price (Discounted and Full)
    price = soup.find('div', {'class': 'tabs'}).find('section', {'id': 'web'})

    full = price.find('span', {'class': 'text-strikethrough'}).text.strip()
    if full is None:
        full = price.find('div', {'class': 'price-box__price'}).text.strip()[:-2]
        disc = 'Bez zľavy'
    else:
        disc = price.find('div', {'class': 'price-box__price'}).text.strip()[:-2]
        full = full[:-2]

    ret['Plná cena'] = full
    ret['Zľavnená cena'] = disc

    # Name
    title = soup.find('h1', {'class': 'product-detail__title'}).text.strip()
    ret['Názov'] = title

    # Author
    author_soup = soup.find('ul', {'class': 'product-detail__author'})
    authors = get_author_list(author_soup)
    ret['Autori'] = authors


    # Rating NOTE: can be None
    rating = soup.find('div', {'class': 'rating-text__value'})
    if rating is not None:
        rating = soup.find('div', {'class': 'rating-text__value'}).find('span', {'class': 'text-bold'}).text.strip()
    ret['Hodnotenie'] = rating

    # Web availability
    if soup.find('a', {'id': 'web-label'}).find('span', {'class': 'status'}) is not None:
        web_availability = soup.find('a', {'id': 'web-label'}).find('span', {'class': 'status'}).text.strip()
    else:
        web_availability = 'Vypredané'
    ret['Dostupnosť'] = web_availability

    # Annotation
    annotation = soup.find('section', {'id': 'description'}).find('div', {'class': 'cms-article'}).text.strip()
    annotation = annotation.replace('\n', '')
    ret['Anotácia'] = annotation

    return json.dumps(ret, sort_keys=True, indent=2)


def trim(link):
    return link[2:] if link.startswith('//') else link


def append_http(link):
    return link if link.startswith('http') else 'https://{link}'.format(link=link)


def filter_links(links, explored=[]):
    if len(explored):
        links = [link for link in links if link not in explored]

    product_link_regex = re.compile("martinus\.sk/\?uItem=[0-9]+$")
    book_subpage_link_regex = re.compile("martinus\.sk/knihy")
    productlinks = set([append_http(trim(link)) for link in links if product_link_regex.search(link)])
    misc_links = set([append_http(trim(link)) for link in links if book_subpage_link_regex.search(link)])
    productlinks.update(misc_links)
    return productlinks


def find_links(html):
    soup = BeautifulSoup(html, 'html.parser')
    return [a['href'] for a in soup.main.find_all('a', href=re.compile("martinus\.sk"))]


def is_book(soup):
    return re.search("Knihy", soup.text)


def is_slovak(soup):
    return re.search("slovenský", soup.text)


def is_slovak_book(html):
    soup = BeautifulSoup(html, 'html.parser')
    breadcrumbs = soup.find("section", {"class": "section--breadcrumbs"})
    categories = soup.find("section", {"id": "details"})
    return breadcrumbs is not None and categories is not None and is_book(breadcrumbs) is not None and is_slovak(categories) is not None


def saved_files():
    file_names = os.listdir('./pages/')
    return ['https://www.martinus.sk/?uItem={name}'.format(name=name) for name in file_names]


def crawl():
    try:
        while end.value is 0:
            job_queue_lock.acquire()
            try:
                link = job_queue.get(block=False)
            except queue.Empty:
                job_queue_lock.release()
                print('Worker: empty job queue, sleeping...')
                sleep(2)
                continue
            job_queue_lock.release()

            html = requests.get(link).text
            boolean = is_slovak_book(html)

            if boolean:
                save_page(html, link.split('=')[1])
                with count.get_lock():
                    count.value += 1
                    print('Worker: Updated count to', count.value)
            else:
                print('Worker: Link miss', link)

            new_links = find_links(html)
            link_queue.put(new_links)
    except Exception as e:
        print('Slave Error:', e)

    return 0


if __name__ == '__main__':
    url = 'https://www.martinus.sk/'
    source = requests.get(url).text
    explored = saved_files()
    product_links = filter_links(find_links(source), explored=explored)
    limit = 10000

    # multiprocessing
    end = mp.Value('b', False)
    count = mp.Value('i', 1400)
    job_queue = mp.Queue()
    link_queue = mp.Queue()
    [job_queue.put(link) for link in product_links]
    job_queue_lock = mp.Lock()

    process_pool = mp.Pool(7)
    [process_pool.apply_async(crawl) for x in range(7)]

    while 1:
        with count.get_lock():
            if count.value > limit:
                break
        if link_queue.empty():
            print('Master: Empty queue, sleeping...')
            sleep(4)
            continue

        links = filter_links(link_queue.get(), explored)
        explored.extend(links)
        print('Master: Explored total of {link_count} links'.format(link_count=len(explored)))

        job_queue_lock.acquire()
        print('Master: Creating jobs')
        [job_queue.put(link) for link in links]
        job_queue_lock.release()
        print('Master: Job creation done')

    end.value = True
    process_pool.terminate()
