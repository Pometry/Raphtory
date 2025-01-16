{{ name | escape | underline }}

.. rubric:: Description

.. automodule:: {{ fullname }}

.. currentmodule:: {{ fullname }}


{% if modules %}
.. rubric:: Modules

.. autosummary::
    :toctree: {{ name }}
    :recursive:
    {% for module in modules %}
    {{ module | replace(fullname + ".", "") }}
    {% endfor %}
{% endif %}

{% if attributes %}
.. rubric:: Attributes
    {% for attribute in members %}
.. autoattribute:: {{ attribute }}
    {% endfor %}
{% endif %}


{% if classes %}
.. rubric:: Classes

.. autosummary::
    :toctree: {{ name }}
    {% for class in classes %}
    {{ class }}
    {% endfor %}

{% endif %}

{% if functions %}
.. rubric:: Functions

.. autosummary::
    :toctree: {{ name }}
    {% for function in functions %}
    {{ function }}
    {% endfor %}

{% endif %}


