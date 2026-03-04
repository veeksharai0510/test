def _normalize_raw_value(raw_value):
    """Normalize raw_value to always be a list."""
    if isinstance(raw_value, dict):
        return [raw_value]
    elif not isinstance(raw_value, list):
        return [raw_value]
    return raw_value


def _extract_attr_values(sub_attr):
    """Extract attribute name-value pairs from sub_attr."""
    vals = []
    raw_value = sub_attr.get("value", [])
    raw_value = _normalize_raw_value(raw_value)
