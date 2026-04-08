        if isinstance(field_val, list) and field_val and all(isinstance(i, dict) for i in field_val):
            section_output = self.set_attribute_output(field_name, value=None, group_header=True)
            for idx, child_item in enumerate(field_val, start=1):
                child_page = child_item.get("page", page)
                child_page_content = child_item.get("page_content", page_content) or ""
                for child_field_name, child_field_val in child_item.items():
                    if child_field_name in ("page", "page_content", "highlight"):
                        continue
                    indexed_field_name = f"{child_field_name} - {idx}"
                    val, score, highlight = self._extract_val_score_highlight_from_sub_meta(
                        child_field_val, child_page, child_page_content, child_item
                    )
                    if isinstance(val, list) and len(val) > 1:
                        nested_section = self.set_attribute_output(indexed_field_name, value=None, group_header=True)
                        for sub_idx, single_val in enumerate(val, start=1):
                            sub_field_name = f"{indexed_field_name}.{sub_idx}"
                            sub_attr = self.set_attribute_output(
                                attribute_name=sub_field_name,
                                value=single_val,
                                score=score,
                                highlight=highlight,
                                group_header=False
                            )
                            nested_section.set_sub_attr_output(sub_field_name, sub_attr)
                        section_output.set_sub_attr_output(indexed_field_name, nested_section)
                    elif isinstance(val, list) and len(val) == 1:
                        attr_output = self.set_attribute_output(
                            attribute_name=indexed_field_name,
                            value=val[0],
                            score=score,
                            highlight=highlight,
                            group_header=False
                        )
                        section_output.set_sub_attr_output(indexed_field_name, attr_output)
                    else:
                        attr_output = self.set_attribute_output(
                            attribute_name=indexed_field_name,
                            value=val,
                            score=score,
                            highlight=highlight,
                            group_header=False
                        )
                        section_output.set_sub_attr_output(indexed_field_name, attr_output)
            if section_output.sub_attr_output:
                entities.set_sub_attr_output(field_name, section_output)
            return


data =  {
        "extraction_details": {
            "extraction_id": 0,
            "document_id": 0,
            "output": {}
        }
    }

def _safe_display_prop(item, key, default=None):
    return (item.get("display_properties") or {}).get(key, default)
def _normalize_raw_value(raw_value):
    if isinstance(raw_value, dict):
        return [raw_value]
    if not isinstance(raw_value, list):
        return [raw_value]
    return raw_value

def _extract_attr_name(sub_attr):
    return sub_attr.get("display_properties", {}).get("display_name") or sub_attr.get("name")

def _extract_vals_from_dict_entry(v, sub_attr):
    vals = []
    if "attr_name" in v and "attr_value" in v:
        if v["attr_value"] is not None and v["attr_value"] != "":
            vals.append({"attr_name": v.get("attr_name"), "attr_value": v.get("attr_value")})
    elif "attr_value" in v:
        av = v["attr_value"]
        if isinstance(av, dict):
            for k, vv in av.items():
                vals.append({"attr_name": k, "attr_value": vv})
        elif av is not None and av != "":
            vals.append({"attr_name": _extract_attr_name(sub_attr), "attr_value": av})
    return vals

def _extract_attr_values(sub_attr):
    vals = []
    raw_value = _normalize_raw_value(sub_attr.get("value", []))
    for v in raw_value:
        if isinstance(v, dict):
            vals.extend(_extract_vals_from_dict_entry(v, sub_attr))
        else:
            if v is not None and v != "":
                vals.append({"attr_name": _extract_attr_name(sub_attr), "attr_value": v})
    return vals

def _build_value_entry(v, item, name, high_light):
    entry = {
        "editable_data": [{"attr_name": v["attr_name"], "attr_value": v["attr_value"]}],
        "score": item.get("score"),
        "display_name": name
    }
    if high_light:
        entry["highlight"] = _safe_display_prop(item, "highlight")
    return entry

def _build_empty_value_entry(item, name, high_light):
    entry = {
        "editable_data": [],
        "score": item.get("score"),
        "display_name": name
    }
    if high_light:
        entry["highlight"] = _safe_display_prop(item, "highlight")
    return entry

def _collect_subattr_iterator(sub_attr_output):
    iterator = []
    if isinstance(sub_attr_output, dict):
        for _, lst in sub_attr_output.items():
            if isinstance(lst, list):
                for sub in lst:
                    iterator.append(sub)
            elif isinstance(lst, dict):
                iterator.append(lst)
    elif isinstance(sub_attr_output, list):
        iterator = sub_attr_output
    return iterator

def process_universal(item, high_light):
    name = _safe_display_prop(item, "display_name") or item.get("name")
    display_style = _safe_display_prop(item, "display_style", "key_value_pair")
    entity = {
        "name": name,
        "sub_attr_output": [],
        "value": [],               # start empty; only add real values
        "group_header": item.get("group_header", False),
        "display_properties": {"display_style": display_style}
    }
    top_vals = _extract_attr_values(item)
    if top_vals:
        for v in top_vals:
            entity["value"].append(_build_value_entry(v, item, name, high_light))

    sub_attr_output = item.get("sub_attr_output") or []
    iterator = _collect_subattr_iterator(sub_attr_output)
    # Always process each sub as its own nested entity and append to sub_attr_output.
    for sub in iterator:
        nested = process_universal(sub, high_light)
        entity["sub_attr_output"].append(nested)

    if not entity["value"]:
        entity["value"].append(_build_empty_value_entry(item, name, high_light))
    return entity

def _is_str_keyed_list_valued_dict(item):
    return isinstance(item, dict) and all(isinstance(k, str) and isinstance(v, list) for k, v in item.items())

def _collect_from_str_keyed_dict(item, high_light):
    final = []
    for _, lst in item.items():
        for ent in lst:
            final.append(process_universal(ent, high_light))
    return final

def _collect_from_list_input(input_data, high_light):
    final = []
    for item in input_data:
        if _is_str_keyed_list_valued_dict(item):
            final.extend(_collect_from_str_keyed_dict(item, high_light))
        else:
            final.append(process_universal(item, high_light))
    return final

def _collect_from_dict_input(input_data, high_light):
    final = []
    for _, lst in input_data.items():
        if isinstance(lst, list):
            for ent in lst:
                final.append(process_universal(ent, high_light))
    return final

def _collect_entities_from_input(input_data, high_light):
    if isinstance(input_data, list):
        return _collect_from_list_input(input_data, high_light)
    if isinstance(input_data, dict):
        return _collect_from_dict_input(input_data, high_light)
    return []

def final_preprocess(input_data, high_light, extraction_id, document_id, extract_name, created_on, filename, annotation):
    final = _collect_entities_from_input(input_data, high_light)
    data["extraction_details"]["extraction_id"] = extraction_id
    data["extraction_details"]["document_id"] = document_id
    data["extraction_details"]["output"]["entity_data"] = final
    extraction_details = {
        "ext_extraction_name": extract_name,
        "ext_started_on": created_on,
    }
    document_list = [
        {
            "dex_document_name": filename,
            "dex_created_on": created_on
        }
    ]
    data["extraction_details"]["extraction_details"] = extraction_details

    data["extraction_details"]["document_list"] = document_list
    data["extraction_details"]["annotation"] = annotation
    data["extraction_details"]["error_message"] = ""
    return data
