{% macro format_number(value, cifre, decimali, segno) %}
    {%- set value_str = value | string -%}
    {%- set result = value_str -%}

    {%- if value < 0 -%}
        {%- set value_str = (value * -1) | string -%}
    {%- endif -%}

    {%- set parts = value_str.split('.') -%}
    {%- set integer_part = parts[0] -%}
    {%- set decimal_part = parts[1] if parts | length > 1 else '' -%}

    -- Gestione decimali
    {%- if decimal_part | length > decimali -%}
        {%- set decimal_part = decimal_part[0:decimali] -%}
    {%- else -%}
        {%- for i in range(decimali - (decimal_part | length)) -%}
            {%- set decimal_part = decimal_part + '0' -%}
        {%- endfor -%}
    {%- endif -%}

    {%- set formatted_value = integer_part ~ '.' ~ decimal_part -%}

    -- Gestione cifre totali (padding)
    {%- if formatted_value | length < cifre -%}
        {%- set padding = '0' * (cifre - (formatted_value | length)) -%}
        {%- set formatted_value = padding ~ formatted_value -%}
    {%- endif -%}

    -- Gestione segno
    {%- if segno > 0 -%}
        {%- if value < 0 -%}
            {%- set result = '-' ~ formatted_value -%}
        {%- else -%}
            {%- set result = ' ' ~ formatted_value -%}
        {%- endif -%}
    {%- else -%}
        {%- set result = formatted_value -%}
    {%- endif -%}

    {{ result }}
{% endmacro %}
