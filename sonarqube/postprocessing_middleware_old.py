import json
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



class ComponentOutput:
    def __init__(self, display_name=None, value=None, highlight=None, score=0,
                    group_header=False, component=None, name=None, index=None,
                    display_style='key_value_pair', columns=None, page_header=None):
        self.score = round(score, 2)
        self.value = value
        self.display_properties = {
            'display_name': display_name,
            'highlight': json.dumps(highlight),
            'display_style': display_style
        }
        self.group_header = group_header
        self.component = component
        self.name = name
        self.index = index
        self.subattr_output = {}
        self.page_header = page_header
    def set_sub_attr_output(self, sub_attr_name, sub_attr_out):
        try:
            if sub_attr_name in self.subattr_output:
                self.subattr_output[sub_attr_name].append(sub_attr_out.__dict__)
            else:
                self.subattr_output[sub_attr_name] = [sub_attr_out.__dict__]
        except Exception as e:
            logger.exception(f"Set Component Output :{e}")
            

META_KEYS = {"value", "page", "page_content", "highlight", "score"}
def looks_like_value_dict(d):
   """Return True if d looks like a metadata/value wrapper (not a grouped section)."""
   return isinstance(d, dict) and set(d.keys()).issubset(META_KEYS)
def looks_like_group_dict(d):
   """Return True if d looks like a grouped section: has any non-metadata key."""
   return isinstance(d, dict) and any(k not in META_KEYS for k in d.keys())
class CAOutputBuilder:
    def set_attribute_output(self, attribute_name, value="", score=0, highlight={}, group_header=False):
        v = [{"attrName": attribute_name, "attrValue": value}]
        comp_out = ComponentOutput(display_name=attribute_name,
                                    value=v,
                                    highlight=highlight if highlight is not None else {},
                                    score=score if score is not None else 0.0,
                                    group_header=group_header,
                                    component="CA",
                                    name=attribute_name,
                                    display_style='key_value_pair',
                                    page_header=False)
        return comp_out
    
    def _build_highlight_from_meta(self, meta, fallback_page, fallback_page_content, fallback_highlight_src):
        if isinstance(meta, dict) and ('page' in meta or 'page_content' in meta or 'highlight' in meta):
            hv = meta.get('highlight', {}) or {}
            return [{
                "text": meta.get('page_content', "") or fallback_page_content,
                "page_num": meta.get('page', fallback_page),
                "top": hv.get('top'),
                "left": hv.get('left'),
                "height": hv.get('height'),
                "width": hv.get('width')
            }]
        iv = fallback_highlight_src.get("highlight", {}) or {}
        if fallback_page or fallback_page_content:
            return [{
                "text": fallback_page_content,
                "page_num": fallback_page,
                "top": iv.get('top'),
                "left": iv.get('left'),
                "height": iv.get('height'),
                "width": iv.get('width')
            }]
        return {}

    def _extract_val_score_highlight_from_sub_meta(self, sub_meta, page, page_content, item):
        if isinstance(sub_meta, dict) and 'value' in sub_meta:
            val = sub_meta.get('value')
            score = sub_meta.get('score', 0.0) or 0.0
            if 'page' in sub_meta or 'page_content' in sub_meta or 'highlight' in sub_meta:
                hv = sub_meta.get('highlight', {}) or {}
                highlight = [{
                    "text": sub_meta.get('page_content', "") or page_content,
                    "page_num": sub_meta.get('page', page),
                    "top": hv.get('top'),
                    "left": hv.get('left'),
                    "height": hv.get('height'),
                    "width": hv.get('width')
                }]
            else:
                highlight = [{}]
        else:
            val = sub_meta
            score = 0.0
            highlight = self._build_highlight_from_meta({}, page, page_content, item)
        return val, score, highlight

    def _extract_val_score_highlight_from_sub_sub_meta(self, sub_sub_meta, page, page_content, item):
        if isinstance(sub_sub_meta, dict) and ('page' in sub_sub_meta or 'page_content' in sub_sub_meta or 'highlight' in sub_sub_meta):
            hv = sub_sub_meta.get('highlight', {}) or {}
            highlight = [{
                "text": sub_sub_meta.get('page_content', "") or page_content,
                "page_num": sub_sub_meta.get('page', page),
                "top": hv.get('top'),
                "left": hv.get('left'),
                "height": hv.get('height'),
                "width": hv.get('width')
            }]
        else:
            highlight = self._build_highlight_from_meta({}, page, page_content, item)
        if isinstance(sub_sub_meta, dict) and 'value' in sub_sub_meta:
            val = sub_sub_meta.get('value')
            score = sub_sub_meta.get('score', 0.0) or 0.0
        else:
            val = sub_sub_meta
            score = 0.0
        return val, score, highlight

    def _process_nested_group_section(self, sub_name, sub_meta, page, page_content, item):
        nested_section = self.set_attribute_output(sub_name, value=None, group_header=True)
        for sub_sub_name, sub_sub_meta in sub_meta.items():
            val, score, highlight = self._extract_val_score_highlight_from_sub_sub_meta(sub_sub_meta, page, page_content, item)
            if val is None:
                continue
            attr_output = self.set_attribute_output(
                attribute_name=sub_sub_name,
                value=val,
                score=score,
                highlight=highlight,
                group_header=False
            )
            nested_section.set_sub_attr_output(sub_sub_name, attr_output)
        return nested_section

    def _process_group_dict_field(self, field_name, field_val, page, page_content, item, entities):
        section_output = self.set_attribute_output(field_name, value=None, group_header=True)
        for sub_name, sub_meta in field_val.items():
            if looks_like_group_dict(sub_meta):
                nested_section = self._process_nested_group_section(sub_name, sub_meta, page, page_content, item)
                if nested_section.subattr_output:
                    section_output.set_sub_attr_output(nested_section.name or nested_section.display_properties.get('display_name'), nested_section)
            else:
                val, score, highlight = self._extract_val_score_highlight_from_sub_meta(sub_meta, page, page_content, item)
                if val is None:
                    continue
                attr_output = self.set_attribute_output(
                    attribute_name=sub_name,
                    value=val,
                    score=score,
                    highlight=highlight,
                    group_header=False
                )
                section_output.set_sub_attr_output(sub_name, attr_output)
        if section_output.subattr_output:
            entities.set_sub_attr_output(section_output.name or section_output.display_properties.get('display_name'), section_output)

    def _extract_val_score_highlight_from_field_val(self, field_val, page, page_content, item):
        if looks_like_value_dict(field_val):
            val = field_val.get('value')
            score = field_val.get('score', 0.0)
            if 'page' in field_val or 'page_content' in field_val or 'highlight' in field_val:
                hv = field_val.get('highlight', {}) or {}
                highlight = [{
                    "text": field_val.get('page_content', "") or page_content,
                    "page_num": field_val.get('page', page),
                    "top": hv.get('top'),
                    "left": hv.get('left'),
                    "height": hv.get('height'),
                    "width": hv.get('width')
                }]
            else:
                highlight = {}
            val_to_use = val if val is not None else field_val
            val = val_to_use
        else:
            # fallback: unexpected dict shape — treat as flat attribute but keep item-level meta
            val = field_val
            score = 0.0
            highlight = self._build_highlight_from_meta({}, page, page_content, item)
        return val, score, highlight

    def _process_list_item_field(self, field_name, field_val, page, page_content, item, entities):
        # If the field value is a dict, create a grouped section (same logic as top-level)
        if not isinstance(field_val, dict):
            return
        # If it is a grouped dict (contains keys that are NOT metadata), make it a group
        if looks_like_group_dict(field_val):
            self._process_group_dict_field(field_name, field_val, page, page_content, item, entities)
        else:
            # field_val is a value-wrapper (only metadata keys) or a primitive: treat as flat attribute
            val, score, highlight = self._extract_val_score_highlight_from_field_val(field_val, page, page_content, item)
            if val is None:
                return
            attr_output = self.set_attribute_output(
                attribute_name=field_name,
                value=val,
                score=score,
                highlight=highlight,
                group_header=False
            )
            entities.set_sub_attr_output(attr_output.name or attr_output.display_properties.get('display_name'), attr_output)

    def _process_list_item(self, item, entities):
        if not isinstance(item, dict):
            return
        # each item is expected to be a dict with one or more keys; typical case is one key-value plus page/page_content
        page = item.get("page")
        page_content = item.get("page_content", "") or ""
        # For each key in item that is not 'page'/'page_content', treat as field
        for field_name, field_val in item.items():
            if field_name in ("page", "page_content", "highlight"):
                continue
            self._process_list_item_field(field_name, field_val, page, page_content, item, entities)

    def _process_list_record(self, record, entities):
        for item in record:
            self._process_list_item(item, entities)

    def _extract_field_meta_val_score_highlight(self, field_meta):
        if isinstance(field_meta, dict) and 'value' in field_meta:
            val = field_meta.get('value')
            score = field_meta.get('score', 0.0) or 0.0
            if 'page' in field_meta or 'page_content' in field_meta or 'highlight' in field_meta:
                hv = field_meta.get('highlight', {}) or {}
                highlight = [{
                    "text": field_meta.get('page_content', "") or "",
                    "page_num": field_meta.get('page'),
                    "top": hv.get('top'),
                    "left": hv.get('left'),
                    "height": hv.get('height'),
                    "width": hv.get('width')
                }]
            else:
                highlight = field_meta.get('highlight', {}) or {}
        else:
            val = field_meta
            score = 0.0
            highlight = [{}]
        return val, score, highlight

    def _process_grouped_top_val(self, top_key, top_val, entities):
        # grouped section
        section_output = self.set_attribute_output(top_key, value=None, group_header=True)
        for field_name, field_meta in top_val.items():
            # treat field_meta as value-wrapper or primitive (same logic)
            val, score, highlight = self._extract_field_meta_val_score_highlight(field_meta)
            if val is None:
                continue
            attr_output = self.set_attribute_output(
                attribute_name=field_name,
                value=val,
                score=score,
                highlight=highlight,
                group_header=False
            )
            section_output.set_sub_attr_output(field_name, attr_output)
        if section_output.subattr_output:
            entities.set_sub_attr_output(section_output.name or section_output.display_properties.get('display_name'), section_output)

    def _extract_top_val_score_highlight(self, top_val):
        # flat attribute or value-wrapper
        if looks_like_value_dict(top_val):
            val = top_val.get('value')
            score = top_val.get('score', 0.0) or 0.0
            hv = top_val.get('highlight', {}) or {}
            highlight = [{
                "text": top_val.get('page_content', "") or "",
                "page_num": top_val.get('page'),
                "top": hv.get('top'),
                "left": hv.get('left'),
                "height": hv.get('height'),
                "width": hv.get('width')
            }]
            val_to_use = val if val is not None else top_val
            val = val_to_use
        else:
            val = top_val
            score = 0.0
            highlight = {}
        return val, score, highlight

    def _process_dict_record(self, record, entities):
        # For each top-level entry in the record:
        for top_key, top_val in record.items():
            if isinstance(top_val, dict) and looks_like_group_dict(top_val):
                self._process_grouped_top_val(top_key, top_val, entities)
            else:
                val, score, highlight = self._extract_top_val_score_highlight(top_val)
                if val is None:
                    continue
                attr_output = self.set_attribute_output(
                    attribute_name=top_key,
                    value=val,
                    score=score,
                    highlight=highlight,
                    group_header=False
                )
                entities.set_sub_attr_output(attr_output.name or attr_output.display_properties.get('display_name'), attr_output)

    def set_output_dynamic(self, input_json):
        entities = self.set_attribute_output("Entities",value=None,group_header=True)
        for record in input_json:
            if isinstance(record, list):
                # ADDED: handle each inner dict as a flat top-level attribute entry
                self._process_list_record(record, entities)
                continue

            # For each top-level entry in the record:
            self._process_dict_record(record, entities)
        
        # return Entities only if it contains anything, else return empty list
        return [entities.__dict__] if entities.subattr_output else []


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
    if "attrName" in v and "attrValue" in v:
        if v["attrValue"] is not None and v["attrValue"] != "":
            vals.append({"attrName": v.get("attrName"), "attrValue": v.get("attrValue")})
    elif "attrValue" in v:
        av = v["attrValue"]
        if isinstance(av, dict):
            for k, vv in av.items():
                vals.append({"attrName": k, "attrValue": vv})
        elif av is not None and av != "":
            vals.append({"attrName": _extract_attr_name(sub_attr), "attrValue": av})
    return vals

def _extract_attr_values(sub_attr):
    vals = []
    raw_value = _normalize_raw_value(sub_attr.get("value", []))
    for v in raw_value:
        if isinstance(v, dict):
            vals.extend(_extract_vals_from_dict_entry(v, sub_attr))
        else:
            if v is not None and v != "":
                vals.append({"attrName": _extract_attr_name(sub_attr), "attrValue": v})
    return vals

def _build_value_entry(v, item, name, high_light):
    entry = {
        "editable_data": [{"attrName": v["attrName"], "attrValue": v["attrValue"]}],
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

def _collect_subattr_iterator(subattr_output):
    iterator = []
    if isinstance(subattr_output, dict):
        for _, lst in subattr_output.items():
            if isinstance(lst, list):
                for sub in lst:
                    iterator.append(sub)
            elif isinstance(lst, dict):
                iterator.append(lst)
    elif isinstance(subattr_output, list):
        iterator = subattr_output
    return iterator

def process_universal(item, high_light):
    name = _safe_display_prop(item, "display_name") or item.get("name")
    display_style = _safe_display_prop(item, "display_style", "key_value_pair")
    entity = {
        "name": name,
        "subattr_output": [],
        "value": [],               # start empty; only add real values
        "group_header": item.get("group_header", False),
        "display_properties": {"display_style": display_style}
    }
    top_vals = _extract_attr_values(item)
    if top_vals:
        for v in top_vals:
            entity["value"].append(_build_value_entry(v, item, name, high_light))

    subattr_output = item.get("subattr_output") or []
    iterator = _collect_subattr_iterator(subattr_output)
    # Always process each sub as its own nested entity and append to subattr_output.
    for sub in iterator:
        nested = process_universal(sub, high_light)
        entity["subattr_output"].append(nested)

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
