from django.shortcuts import render


# Create your views here.

def view_index(request):
    context = {}
    return render(request, 'retrieve/index.html', context)


def view_result(request):
    context = {}
    return render(request, 'retrieve/result.html', context)
