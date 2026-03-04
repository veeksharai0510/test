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
    
    def set_output_dynamic(self, input_json):
        entities = self.set_attribute_output("Entities",value=None,group_header=True)
        for record in input_json:
            if isinstance(record, list):
                # ADDED: handle each inner dict as a flat top-level attribute entry
                for item in record:
                    if not isinstance(item, dict):
                        continue
                    # each item is expected to be a dict with one or more keys; typical case is one key-value plus page/page_content
                    page = item.get("page")
                    page_content = item.get("page_content", "") or ""
                    # For each key in item that is not 'page'/'page_content', treat as field
                    for field_name, field_val in item.items():
                        if field_name in ("page", "page_content", "highlight"):
                            continue
                        # If the field value is a dict, create a grouped section (same logic as top-level)
                        if isinstance(field_val, dict):
                            # If it is a grouped dict (contains keys that are NOT metadata), make it a group
                            if looks_like_group_dict(field_val):
                                section_output = self.set_attribute_output(field_name, value=None, group_header=True)
                                for sub_name, sub_meta in field_val.items():
                                    # If the sub_meta itself is a grouped dict (rare), treat recursively as nested section
                                    if looks_like_group_dict(sub_meta):
                                        nested_section = self.set_attribute_output(sub_name, value=None, group_header=True)
                                        for sub_sub_name, sub_sub_meta in sub_meta.items():
                                            # prefer per-item meta, else fall back to parent item-level meta
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
                                                iv = item.get("highlight", {}) or {}
                                                highlight = [{
                                                    "text": page_content,
                                                    "page_num": page,
                                                    "top": iv.get('top'),
                                                    "left": iv.get('left'),
                                                    "height": iv.get('height'),
                                                    "width": iv.get('width')
                                                }] if (page or page_content) else {}
                                            if isinstance(sub_sub_meta, dict) and 'value' in sub_sub_meta:
                                                val = sub_sub_meta.get('value')
                                                score = sub_sub_meta.get('score', 0.0) or 0.0
                                            else:
                                                val = sub_sub_meta
                                                score = 0.0
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
                                        if nested_section.subattr_output:
                                            section_output.set_sub_attr_output(nested_section.name or nested_section.display_properties.get('display_name'), nested_section)
                                    else:
                                        # sub_meta is a value-wrapper or primitive: create flat subattribute
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
                                            iv = item.get("highlight", {}) or {}
                                            highlight = [{
                                                "text": page_content,
                                                "page_num": page,
                                                "top": iv.get('top'),
                                                "left": iv.get('left'),
                                                "height": iv.get('height'),
                                                "width": iv.get('width')
                                            }] if (page or page_content) else {}
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
                            else:
                                # field_val is a value-wrapper (only metadata keys) or a primitive: treat as flat attribute
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
                                    iv = item.get("highlight", {}) or {}
                                    highlight = [{
                                        "text": page_content,
                                        "page_num": page,
                                        "top": iv.get('top'),
                                        "left": iv.get('left'),
                                        "height": iv.get('height'),
                                        "width": iv.get('width')
                                    }] if (page or page_content) else {}
                                if val is None:
                                    continue
                                attr_output = self.set_attribute_output(
                                    attribute_name=field_name,
                                    value=val,
                                    score=score,
                                    highlight=highlight,
                                    group_header=False
                                )
                                entities.set_sub_attr_output(attr_output.name or attr_output.display_properties.get('display_name'), attr_output)
                            
                continue

            # For each top-level entry in the record:
            for top_key, top_val in record.items():
                if isinstance(top_val, dict) and looks_like_group_dict(top_val):
                    # grouped section
                    section_output = self.set_attribute_output(top_key, value=None, group_header=True)
                    for field_name, field_meta in top_val.items():
                        # treat field_meta as value-wrapper or primitive (same logic)
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
                else:
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
def _extract_attr_values(sub_attr):
    vals = []
    raw_value = sub_attr.get("value", [])
    if isinstance(raw_value, dict):
        raw_value = [raw_value]
    if not isinstance(raw_value, list):
        raw_value = [raw_value]
    for v in raw_value:
        if isinstance(v, dict):
            if "attrName" in v and "attrValue" in v:
                if v["attrValue"] is not None and v["attrValue"] != "":
                    vals.append({"attrName": v.get("attrName"), "attrValue": v.get("attrValue")})
            elif "attrValue" in v:
                av = v["attrValue"]
                if isinstance(av, dict):
                    for k, vv in av.items():
                        vals.append({"attrName": k, "attrValue": vv})
                elif av is not None and av != "":
                    vals.append({"attrName": sub_attr.get("display_properties", {}).get("display_name") or sub_attr.get("name"),
                                    "attrValue": av})
        else:
            if v is not None and v != "":
                vals.append({"attrName": sub_attr.get("display_properties", {}).get("display_name") or sub_attr.get("name"),
                                "attrValue": v})
    return vals

def process_universal(item,high_light):
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
            if high_light:
                entity["value"].append({
                    "editable_data": [{"attrName": v["attrName"], "attrValue": v["attrValue"]}],
                    "score": item.get("score"),
                    "highlight": _safe_display_prop(item, "highlight"),
                    "display_name": name
                })
            else:
                entity["value"].append({
                    "editable_data": [{"attrName": v["attrName"], "attrValue": v["attrValue"]}],
                    "score": item.get("score"),
                    "display_name": name
                })


    subattr_output = item.get("subattr_output") or []
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
    # Always process each sub as its own nested entity and append to subattr_output.
    for sub in iterator:
        nested = process_universal(sub,high_light)
        entity["subattr_output"].append(nested)


    if not entity["value"]:
        if high_light:
            entity["value"].append({
                "editable_data": [],   
                "score": item.get("score"),
                "highlight": _safe_display_prop(item, "highlight"),
                "display_name": name
            })
        else:
            entity["value"].append({
                    "editable_data": [],   
                    "score": item.get("score"),
                    "display_name": name
                })
    return entity

def final_preprocess(input_data,high_light,extraction_id,document_id,extract_name,created_on,filename,annotation):
    final = []
    if isinstance(input_data, list):
        for item in input_data:
            if isinstance(item, dict) and all(isinstance(k, str) and isinstance(v, list) for k, v in item.items()):
                for _, lst in item.items():
                    for ent in lst:
                        final.append(process_universal(ent,high_light))
            else:
                final.append(process_universal(item,high_light))
    elif isinstance(input_data, dict):
        for _, lst in input_data.items():
            if isinstance(lst, list):
                for ent in lst:
                    final.append(process_universal(ent,high_light))
    else:
        if isinstance(input_data, dict):
            final.append(process_universal(input_data, high_light))
    data["extraction_details"]["extraction_id"]=extraction_id
    data["extraction_details"]["document_id"]=document_id
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
    data["extraction_details"]["extraction_details"]=extraction_details

    data["extraction_details"]["document_list"]=document_list
    data["extraction_details"]["annotation"]= annotation
    data["extraction_details"]["error_message"]=""
    return data
