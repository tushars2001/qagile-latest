from django.shortcuts import render

# Create your views here.


def ignite_home(request):
    print(str(request))
    return render(request, 'ignite_home.html', {
        'include_html': str(request).split("'")[1].replace("/ignite-tracker/", "")})


def ignite_home2(request):
    print(request.resolver_match.url_name)
    assert False
    return render(request, 'ignite_home.html', {'include_html': str(request).split("/")[2].replace("'>", "")})
