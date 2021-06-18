import re

from typing import List


class Word:
    def __init__(self, word: str):
        self.word = word
        self.url = None
        self.is_title = None
        self.start = None
        self.stop = None

    def from_page_offset(self, page_offset: str) -> None:
        """
        Page-offset format explanation:
        'www.1.com@@1-2': the 1-2 characters of its title contain the given word.
        'www.1.com@3-4': the 3-4 characters of its body contain the given word.
        """
        k = page_offset.rfind("@")
        position = page_offset[k + 1:]

        if not re.match('^[0-9]+-[0-9]+$', position):
            return

        if k >= 1 and page_offset[k - 1] == '@':
            self.url = page_offset[:k - 1]
            self.is_title = True
        else:
            self.url = page_offset[:k]
            self.is_title = False

        self.start = int(position.split('-')[0])
        self.stop = int(position.split('-')[1])


class Page:
    def __init__(self, url: str, title: str, body: str, pagerank: float):
        self.url = url
        self.title = title
        self.body = body
        self.pagerank = pagerank
        self.words_list = []

    def add_word(self, word: Word) -> None:
        self.words_list.append(word)

    @staticmethod
    def __highlight_content(content: str, words_list: List[Word]):
        positions_list = [[word.start, word.stop] for word in words_list]
        positions_list.sort(key=lambda pos: pos[0], reverse=False)
        i = 0
        while i < len(positions_list):
            if i == 0:
                i += 1
                continue
            if positions_list[i][0] <= positions_list[i - 1][1]:
                positions_list[i - 1] = [positions_list[i - 1][0], positions_list[i][1]]
                positions_list.pop(i)
                continue
            i += 1

        positions_list.sort(key=lambda pos: pos[0], reverse=True)

        for s, t in positions_list:
            content = content[:s] + '<span class="text-danger">' + content[s:t] + '</span>' + content[t:]
        return content

    @property
    def highlight_html_title(self) -> str:
        words_list = []
        for word in self.words_list:
            if word.is_title:
                words_list.append(word)
        return self.__highlight_content(self.title, words_list)

    @property
    def highlight_html_body(self) -> str:
        words_list = []
        for word in self.words_list:
            if not word.is_title:
                words_list.append(word)
        return self.__highlight_content(self.body, words_list)

    @property
    def simplified_highlight_html_body(self) -> str:
        body = self.highlight_html_body
        s = max(0, body.find('<span class="text-danger">') - 20)
        body = body[s:s + 300]
        return body
