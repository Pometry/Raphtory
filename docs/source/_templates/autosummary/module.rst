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

{% block classes %}
{% set actual_members = [] %}
{% for m in members %}
{% if not m.startswith("_") %}
{% set _ = actual_members.append(m) %}
{% endif %}
{% endfor %}
{% if actual_members %}
   .. rubric:: Members

   .. autosummary::
      :nosignatures:

{% for cls in actual_members %}
      {{ fullname }}.{{ cls }}
{%- endfor %}
{% endif %}
{% endblock %}


