def update_user_modified_entities(modified_values, extract_output):
    """Update extracted output with user modifications."""
    try:
        chan = _extract_changes_from_modified_values(modified_values)
        audit_data, found_keys, wrong_old_value_keys, wrong_old_value = (
            _apply_changes_to_output(extract_output, chan)
        )
        
        missing_keys = _find_missing_keys(chan, found_keys)
        
        if len(audit_data) <= 0:
            audit_data = None
        
        logger.info(f'wrong values keys are {list(wrong_old_value_keys)}')
        logger.info(f'wrong values are {list(wrong_old_value)}')
        logger.info(f'missing_keys are {missing_keys}')

        return (extract_output, audit_data, missing_keys, 
                list(wrong_old_value_keys), list(wrong_old_value))
    except Exception as e:
        logger.exception(f"Error in update_user_modified_entities: {e}")


def _extract_changes_from_modified_values(modified_values):
    """Extract changes from modified values."""
    chan = []
    for item in modified_values:
        if isinstance(item, dict):
            _replace_new(item, chan)
    return chan


def _replace_new(node, chan):
    """Recursively replace and collect new values."""
    if "sub_attr_output" in node:
        for item in node["sub_attr_output"]:
            _replace_new(item, chan)
    
    if "value" in node:
        for value_item in node["value"]:
            logger.info(f'value_item is {value_item}')
            if "editable_data" in value_item:
                for edit in value_item["editable_data"]:
                    chan.append({
                        value_item["name"]: {
                            "name": edit["attr_name"],
                            "old_value": edit["attr_value"],
                            "new_value": edit["modified_attr_value"]
                        }
                    })
    return chan


def _apply_changes_to_output(extract_output, chan):
    """Apply collected changes to the output."""
    audit_data = []
    found_keys = set()
    wrong_old_value_keys = set()
    wrong_old_value = set()
    
    for key, val_list in extract_output.items():
        for item in val_list:
            if item.get("group_header"):
                _process_changes_for_item(
                    item, chan, audit_data, found_keys,
                    wrong_old_value_keys, wrong_old_value
                )
    
    return audit_data, found_keys, wrong_old_value_keys, wrong_old_value


def _process_changes_for_item(item, chan, audit_data, found_keys, 
                             wrong_old_value_keys, wrong_old_value):
    """Process changes for a single item."""
    for sub_attr_key, sub_attr_list in item.get("sub_attr_output", {}).items():
        for sub_attr in sub_attr_list:
            if sub_attr.get("group_header"):
                _process_group_header(sub_attr, chan, audit_data)
            else:
                _process_sub_attribute(
                    sub_attr, chan, audit_data, found_keys,
                    wrong_old_value_keys, wrong_old_value
                )


def _process_group_header(sub_attr, chan, audit):
    """Process group header in sub-attribute."""
    for sub_attr_key, sub_attr_list in sub_attr.get("sub_attr_output", {}).items():
        for sub_attr_item in sub_attr_list:
            _process_sub_attribute(sub_attr_item, chan, audit, set(), set(), set())


def _process_sub_attribute(sub_attr, modi_chan, audit, found_keys,
                          wrong_old_value_keys, wrong_old_value):
    """Process a sub-attribute."""
    attr_name = sub_attr.get("value", [{}])[0].get("attr_name", "")
    attr_value = sub_attr.get("value", [{}])[0].get("attr_value", "")
    
    if isinstance(attr_value, str):
        _handle_string_value(
            sub_attr, attr_name, attr_value, modi_chan, audit,
            found_keys, wrong_old_value_keys, wrong_old_value
        )
    elif isinstance(attr_value, dict):
        _handle_dict_value(
            attr_name, attr_value, modi_chan, audit, found_keys,
            wrong_old_value_keys, wrong_old_value
        )


def _handle_string_value(sub_attr, attr_name, attr_value, modi_chan, audit,
                        found_keys, wrong_old_value_keys, wrong_old_value):
    """Handle string value in sub-attribute."""
    for dic in modi_chan:
        for dic_key, dic_value in dic.items():
            if dic_key == attr_name:
                found_keys.add(dic_key)
                old_values = [
                    _normalize_value(v)
                    for v in dic_value["old_value"].split(",")
                ]
                current_values = [
                    _normalize_value(v)
                    for v in attr_value.split(",")
                ]
                if old_values == current_values:
                    sub_attr["value"][0]["attr_value"] = dic_value["new_value"]
                    audit.append({
                        "attr_name": attr_name,
                        "old_value": dic_value["old_value"],
                        "new_value": dic_value["new_value"]
                    })
                else:
                    wrong_old_value_keys.add(dic_key)
                    wrong_old_value.add(dic_value["old_value"])

def _normalize_value(value):
    value = value.lower()
    value = re.sub(r"[^\w\s]", "", value) 
    value = " ".join(value.split())       
    return value



def _handle_dict_value(attr_name, attr_value, modi_chan, audit, found_keys,
                      wrong_old_value_keys, wrong_old_value):
    """Handle dictionary value in sub-attribute."""
    for dic in modi_chan:
        if attr_name in dic:
            found_keys.add(attr_name)
            attr_details = dic[attr_name]
            _process_attr_items(attr_details, attr_value, audit, attr_name, 
                              wrong_old_value_keys, wrong_old_value)


def _process_attr_items(attr_details, attr_value, audit, attr_name, 
                       wrong_old_value_keys, wrong_old_value):
    """Process each attribute item in the dictionary."""
    for key, value in attr_value.items():
        if attr_details.get("name") == key:
            _process_attr_values(attr_details, key, value, attr_value, audit, 
                               attr_name, wrong_old_value_keys, wrong_old_value)


def _process_attr_values(attr_details, key, value, attr_value, audit, attr_name, 
                        wrong_old_value_keys, wrong_old_value):
    """Update attribute value or track wrong old value."""
    old_value = attr_details.get("old_value")
    
    if old_value == value:
        _update_attr_value(attr_details, key, value, attr_value, audit)
    else:
        _track_wrong_value(attr_name, old_value, wrong_old_value_keys, wrong_old_value)


def _update_attr_value(attr_details, key, value, attr_value, audit):
    """Update attribute value and add to audit log."""
    new_value = attr_details.get("new_value")
    attr_value[key] = new_value
    audit.append({
        "attr_name": attr_details.get("name"),
        "old_value": attr_details.get("old_value"),
        "new_value": new_value
    })


def _track_wrong_value(attr_name, old_value, wrong_old_value_keys, wrong_old_value):
    """Track incorrect old values."""
    wrong_old_value_keys.add(attr_name)
    wrong_old_value.add(old_value)

def _find_missing_keys(chan, found_keys):
    """Find keys that were in changes but not found in output."""
    modi_keys = {list(d.keys())[0] for d in chan}
    return list(modi_keys - found_keys)
