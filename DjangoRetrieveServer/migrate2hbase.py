import os
import random
import argparse

import happybase
from typing import Dict


def read_page_file(file_path: str) -> Dict[str, Dict[bytes, bytes]]:
    with open(file_path, mode='r') as file:
        lines = file.readlines()

    pages_dict = {}
    for line in lines:
        try:
            _, _, url, _, next_urls, _, title, _, body = line.split('\t')
            pages_dict[url] = {
                b'crawl:next_urls': next_urls.strip().encode(),
                b'crawl:title': title.strip().encode(),
                b'crawl:body': body.strip().encode(),
                b'pagerank:value': str(random.random())[:12].encode(),
            }
        except ValueError:
            print(f"Error occurred when parsing {line}.")

    return pages_dict


def migrate_page_file(host: str, port: int, file_path: str) -> None:
    print(f'Migrating file "{file_path}" ...')
    pages_dict = read_page_file(file_path)

    connection = happybase.Connection(host=host, port=port)
    table = connection.table('NNU_PAGES')

    for i, (url, info) in enumerate(pages_dict.items()):
        if i % 100 == 0:
            print(f"Migrating [{i}] ...")
        table.put(url, info)


def migrate_pages(host: str, port: int, input_path: str) -> None:
    for file in os.listdir(input_path):
        file_path = os.path.join(input_path, file)
        migrate_page_file(host, port, file_path)


def read_word_file(file_path: str) -> Dict[str, Dict[bytes, bytes]]:
    with open(file_path, mode='r') as file:
        lines = file.readlines()

    words_dict = {}
    for line in lines:
        try:
            k = line.find("\t")
            word = line[:k]
            urls = line[k + 1:]
            words_dict[word] = {
                b'segment:URLs': urls.strip().encode(),
            }
        except ValueError:
            print(f"Error occurred when parsing {line}.")

    return words_dict


def migrate_word_file(host: str, port: int, file_path: str) -> None:
    print(f'Migrating file "{file_path}" ...')
    words_dict = read_word_file(file_path)

    connection = happybase.Connection(host=host, port=port)
    table = connection.table('NNU_WORDS')

    for i, (word, urls) in enumerate(words_dict.items()):
        if i % 100 == 0:
            print(f"Migrating [{i}] ...")
        table.put(word, urls)


def migrate_words(host: str, port: int, input_path: str) -> None:
    for file in os.listdir(input_path):
        file_path = os.path.join(input_path, file)
        migrate_word_file(host, port, file_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('host')
    parser.add_argument('port')
    parser.add_argument('table_name')
    parser.add_argument('input_path')
    args = parser.parse_args()
    Host = args.host
    Port = int(args.port)
    Table_Name = args.table_name
    Input_Path = args.input_path

    if Table_Name == "NNU_PAGES":
        migrate_pages(Host, Port, Input_Path)
    elif Table_Name == "NNU_WORDS":
        migrate_words(Host, Port, Input_Path)
