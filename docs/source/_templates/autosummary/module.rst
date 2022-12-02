{{ fullname | escape | underline}}

{% block modules %}
{% if modules %}
.. rubric:: Modules

.. autosummary::
   :toctree: {{ name }}
   :recursive:

{% for item in modules %}
   {{ item }}
{%- endfor %}
{% endif %}
{% endblock %}

.. automodule:: {{ fullname }}