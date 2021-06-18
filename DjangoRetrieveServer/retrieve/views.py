import jieba
from django.shortcuts import render
from django.http import JsonResponse

from . import utils


def view_index(request):
    context = {}
    return render(request, 'retrieve/index.html', context)


def view_search(request):
    context = {}
    return render(request, 'retrieve/search.html', context)


def api_search_by_keyword(request):
    keyword = request.GET.get('keyword', '')

    keywords_list = jieba.cut(keyword, cut_all=False)

    words_list = []
    for keyword in keywords_list:
        words_list.extend(utils.retrieve_word(keyword))

    urls_list = utils.get_top_k_urls(words_list, 100)
    pages_list = utils.retrieve_pages(urls_list)
    pages_list = utils.add_words_into_pages(pages_list, words_list)

    data = {
        'pages': [
            # {'title': 'title-1', 'body': 'body-1', 'URL': 'www.1.com'},
            # {'title': 'title-2', 'body': 'body-2', 'URL': 'www.2.com'},
            # {'title': 'title-3', 'body': 'body-3', 'URL': 'www.3.com'},
        ],
    }

    for page in pages_list:
        data['pages'].append({
            'title': page.highlight_html_title,
            'body': page.simplified_highlight_html_body,
            'URL': page.url,
        })

    return JsonResponse(data)
