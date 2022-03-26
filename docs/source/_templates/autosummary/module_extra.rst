{% extends "!autosummary/module.rst" %}
{% block classes %}

{% set types = [] %}
{% for item in members %}
   {% if not item.startswith('_') and not (item in functions or item in attributes or item in exceptions) %}
      {% set _ = types.append(item) %}
   {% endif %}
{%- endfor %}

{% if types %}
.. rubric:: {{ _('Classes') }}

   .. autosummary::
      :toctree:
      :nosignatures:
   {% for item in types %}
      {{ item }}
   {%- endfor %}

{% endif %}
{% endblock %}

