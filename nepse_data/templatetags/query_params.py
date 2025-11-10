from django import template

register = template.Library()

@register.simple_tag
def set_query_param(request, param, value):
    """
    Takes the current request, copies its GET parameters,
    and sets or adds a new parameter, returning the new query string.
    """
    params = request.GET.copy()
    params[param] = value
    return params.urlencode()