import happybase
from typing import List

from .models import *

host = '223.2.22.112'


def retrieve_word(word: str) -> List[Word]:
    """ Retrieve for pages which contain the specific word.

    :param str word: the retrieved word
    :return: a list of Word objects
    :rtype: List[Word]
    """
    connection = happybase.Connection(host=host, port=9090)
    table = connection.table('NNU_WORDS')
    row = table.row(word)
    connection.close()

    words_list = []
    if len(row) == 0:
        return words_list

    urls = row[b'segment:URLs'].decode()
    urls = [url for url in urls.split(' ') if url != '']
    for url in urls:
        w = Word(word)
        w.from_page_offset(url)
        if w.url is not None:
            words_list.append(w)

    return words_list


def retrieve_pages(urls: List[str]) -> List[Page]:
    """ Retrieve information for the specific list of URLs.

    Note: non-existing URLs will be filtered.

    :param List[str] urls: a list of URLs
    :return: a list of Page objects
    :rtype: List[Page]
    """
    connection = happybase.Connection(host=host, port=9090)
    table = connection.table('NNU_PAGES')
    urls = list(set([url.encode() for url in urls]))
    rows = table.rows(urls)
    connection.close()

    pages_list = []
    if len(rows) == 0:
        return pages_list

    for row in rows:
        url = row[0].decode()
        title = row[1][b'crawl:title'].decode()
        body = row[1][b'crawl:body'].decode()
        pagerank = float(row[1][b'pagerank:value'])
        page = Page(url, title, body, pagerank)
        pages_list.append(page)

    return pages_list


def get_top_k_urls(words_list: List[Word], k: int) -> List[str]:
    """ Get the most k frequent URLs.

    Note: A keyword occurs in the title count for 10 times.

    :param List[Word] words_list: a list of Word objects
    :param int k: the number of URLs
    :return: a list of k URLs
    :rtype: List[str]
    """
    count_dict = {}
    for word in words_list:
        times = 10 if word.is_title else 1
        if word.url in count_dict.keys():
            count_dict[word.url] += times
        else:
            count_dict[word.url] = times

    count_list = [(url, count) for url, count in count_dict.items()]
    count_list.sort(key=lambda item: item[1], reverse=True)
    k_urls_list = [item[0] for item in count_list[:k]]
    return k_urls_list


def add_words_into_pages(pages: List[Page], words: List[Word]) -> List[Page]:
    """ Add words to their corresponding pages.

    :param List[Page] pages: a list of Page objects
    :param List[Word] words: as list of Word objects
    :return: a list of Page objects
    :rtype: List[Page]
    """
    pages_dict = {page.url: page for page in pages}

    for word in words:
        if word.url in pages_dict.keys():
            pages_dict[word.url].add_word(word)

    pages_list = list(pages_dict.values())
    return pages_list
