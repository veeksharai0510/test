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
        """Set subattribute output for the component."""
        try:
            if sub_attr_name in self.subattr_output:
                self.subattr_output[sub_attr_name].append(sub_attr_out.__dict__)
            else:
                self.subattr_output[sub_attr_name] = [sub_attr_out.__dict__]
        except Exception as e:
            logger.exception(f"Set Component Output: {e}")


META_KEYS = {"value", "page", "page_content", "highlight", "score"}


def looks_like_value_dict(d):
    """Return True if d looks like a metadata/value wrapper (not a grouped section)."""
    return isinstance(d, dict) and set(d.keys()).issubset(META_KEYS)


def looks_like_group_dict(d):
    """Return True if d looks like a grouped section: has any non-metadata key."""
    return isinstance(d, dict) and any(k not in META_KEYS for k in d.keys())


# ============================================================================
# HELPER FUNCTIONS FOR METADATA/HIGHLIGHT EXTRACTION
# ============================================================================

def _build_highlight_entry(text, page_num, highlight_dict=None):
    """Build a single highlight entry with provided metadata."""
    hv = highlight_dict or {}
    return {
        'text': text,
        'page_num': page_num,
        'top': hv.get('top'),
        'left': hv.get('left'),
        'height': hv.get('height'),
        'width': hv.get('width')
    }


def _has_highlight_metadata(meta_dict):
    """Check if metadata dict contains highlight-related keys."""
    return isinstance(meta_dict, dict) and any(
        key in meta_dict for key in ('page', 'page_content', 'highlight')
    )


def _get_highlight_text_and_page(meta_dict, fallback_text, fallback_page):
    """Extract highlight text and page from metadata with fallback."""
    if isinstance(meta_dict, dict):
        text = meta_dict.get('page_content', '') or fallback_text
        page = meta_dict.get('page', fallback_page)
        hv = meta_dict.get('highlight', {}) or {}
    else:
        text = fallback_text
        page = fallback_page
        hv = {}
    return text, page, hv


def _resolve_highlight_from_meta(meta_dict, item_page, item_page_content):
    """Resolve highlight metadata, preferring per-item meta over parent meta."""
    if not _has_highlight_metadata(meta_dict):
        # No highlight metadata found
        if item_page or item_page_content:
            return [_build_highlight_entry(item_page_content, item_page)]
        return {}
    
    # Extract from meta_dict with fallbacks
    text, page, hv = _get_highlight_text_and_page(meta_dict, item_page_content, item_page)
    return [_build_highlight_entry(text, page, hv)]


def _extract_value_and_score(meta_dict):
    """Extract value and score from metadata or primitive."""
    if isinstance(meta_dict, dict) and 'value' in meta_dict:
        return meta_dict.get('value'), meta_dict.get('score', 0.0) or 0.0
    return meta_dict, 0.0


# ============================================================================
# HELPER FUNCTIONS FOR ATTRIBUTE OUTPUT CREATION
# ============================================================================

class CAOutputBuilder:
    """Builder for creating component attribute outputs."""

    def set_attribute_output(self, attribute_name, value="", score=0, highlight={}, group_header=False):
        """Create a ComponentOutput for a single attribute."""
        v = [{"attr_name": attribute_name, "attr_value": value}]
        comp_out = ComponentOutput(
            display_name=attribute_name,
            value=v,
            highlight=highlight if highlight is not None else {},
            score=score if score is not None else 0.0,
            group_header=group_header,
            component="CA",
            name=attribute_name,
            display_style='key_value_pair',
            page_header=False
        )
        return comp_out

    def _build_flat_attribute(self, attribute_name, meta_dict, item_page, item_page_content):
        """Build a flat attribute output from metadata or primitive."""
        val, score = _extract_value_and_score(meta_dict)
        
        if val is None:
            return None
        
        highlight = _resolve_highlight_from_meta(meta_dict, item_page, item_page_content)
        return self.set_attribute_output(
            attribute_name=attribute_name,
            value=val,
            score=score,
            highlight=highlight,
            group_header=False
        )

    def _build_grouped_section(self, section_name, section_dict, item_page, item_page_content):
        """Build a grouped section output."""
        section_output = self.set_attribute_output(section_name, value=None, group_header=True)
        
        for sub_name, sub_meta in section_dict.items():
            attr_out = self._process_nested_section(sub_name, sub_meta, item_page, item_page_content)
            if attr_out and self._has_valid_value(attr_out):
                section_output.set_sub_attr_output(sub_name, attr_out)
        
        return section_output

    def _process_nested_section(self, sub_name, sub_meta, item_page, item_page_content):
        """Process a nested section with hierarchical structure."""
        if looks_like_group_dict(sub_meta):
            return self._build_grouped_section(sub_name, sub_meta, item_page, item_page_content)
        return self._build_flat_attribute(sub_name, sub_meta, item_page, item_page_content)

    @staticmethod
    def _has_valid_value(attr_out):
        """Check if attribute output has valid value data."""
        return attr_out.value and attr_out.value[0].get('editable_data')

    def _add_section_to_entities(self, section_output, entities):
        """Add a grouped section to entities if it has content."""
        if section_output.subattr_output:
            section_key = section_output.name or section_output.display_properties.get('display_name')
            entities.set_sub_attr_output(section_key, section_output)

    def _add_attribute_to_entities(self, attr_output, entities):
        """Add a flat attribute to entities."""
        if attr_output:
            attr_key = attr_output.name or attr_output.display_properties.get('display_name')
            entities.set_sub_attr_output(attr_key, attr_output)

    def _process_field_value(self, field_name, field_val, item_page, item_page_content, entities):
        """Process a field value, handling both dict and primitive types.
        
        This unified method handles the identical branch logic that was previously
        duplicated in _process_field_in_item and _process_record_field.
        """
        if isinstance(field_val, dict):
            if looks_like_group_dict(field_val):
                section_output = self._build_grouped_section(field_name, field_val, item_page, item_page_content)
                self._add_section_to_entities(section_output, entities)
            else:
                attr_output = self._build_flat_attribute(field_name, field_val, item_page, item_page_content)
                self._add_attribute_to_entities(attr_output, entities)
        else:
            attr_output = self.set_attribute_output(
                attribute_name=field_name,
                value=field_val,
                score=0.0,
                highlight={},
                group_header=False
            )
            entities.set_sub_attr_output(field_name, attr_output)

    def _process_list_item(self, item, entities):
        """Process a single item from a list in input_json."""
        if not isinstance(item, dict):
            return
        
        page = item.get("page")
        page_content = item.get("page_content", "") or ""
        
        for field_name, field_val in item.items():
            if field_name not in ("page", "page_content", "highlight"):
                self._process_field_value(field_name, field_val, page, page_content, entities)

    def _process_record(self, record, entities):
        """Process a single record from input_json."""
        for top_key, top_val in record.items():
            self._process_field_value(top_key, top_val, None, "", entities)

    def set_output_dynamic(self, input_json):
        """Convert dynamic input JSON to component output structure."""
        entities = self.set_attribute_output("Entities", value=None, group_header=True)
        
        for record in input_json:
            if isinstance(record, list):
                for item in record:
                    self._process_list_item(item, entities)
            else:
                self._process_record(record, entities)
        
        return [entities.__dict__] if entities.subattr_output else []


# ============================================================================
# HELPER FUNCTIONS FOR UNIVERSAL PROCESSING
# ============================================================================

def _safe_display_prop(item, key, default=None):
    """Safely extract a display property from an item."""
    return (item.get("display_properties") or {}).get(key, default)


def _is_valid_attr_entry(attr_value):
    """Check if attribute value is valid (not None, not empty string)."""
    return attr_value is not None and attr_value != ""


def _get_display_name(sub_attr):
    """Get the display name from sub_attr, with fallback to 'name'."""
    return _safe_display_prop(sub_attr, "display_name") or sub_attr.get("name")


def _append_attr_value(vals, attr_name, attr_value):
    """Append a single attribute name-value pair to vals list."""
    if _is_valid_attr_entry(attr_value):
        vals.append({"attr_name": attr_name, "attr_value": attr_value})


def _add_dict_values(v, sub_attr, vals):
    """Add dictionary values to vals list, handling multiple formats."""
    if "attr_name" in v and "attr_value" in v:
        _append_attr_value(vals, v.get("attr_name"), v.get("attr_value"))
    elif "attr_value" in v:
        _process_attr_value(v.get("attr_value"), sub_attr, vals)


def _process_attr_value(attr_value, sub_attr, vals):
    """Process an attribute value which may be dict or primitive."""
    if isinstance(attr_value, dict):
        for k, vv in attr_value.items():
            _append_attr_value(vals, k, vv)
    else:
        display_name = _get_display_name(sub_attr)
        _append_attr_value(vals, display_name, attr_value)


def _to_list(value):
    """Convert a value to a list. If already a list, return as-is."""
    return value if isinstance(value, list) else [value]


def _extract_attr_values(sub_attr):
    """Extract attribute name-value pairs from sub_attr."""
    vals = []
    raw_value = sub_attr.get("value", [])
    raw_value = _to_list(raw_value)
    
    for v in raw_value:
        if isinstance(v, dict):
            _add_dict_values(v, sub_attr, vals)
        else:
            display_name = _get_display_name(sub_attr)
            _append_attr_value(vals, display_name, v)
    
    return vals


def _build_value_entry(attr_name, attr_value, score, highlight, display_name, include_highlight=True):
    """Build a single value entry."""
    entry = {
        "editable_data": [{"attr_name": attr_name, "attr_value": attr_value}],
        "score": score,
        "display_name": display_name
    }
    if include_highlight:
        entry["highlight"] = highlight
    return entry


def _process_entity_values(item, entity, high_light):
    """Process and add top-level values to an entity."""
    top_vals = _extract_attr_values(item)
    name = entity["name"]
    
    for v in top_vals:
        entry = _build_value_entry(
            v["attr_name"], v["attr_value"], item.get("score"),
            _safe_display_prop(item, "highlight"), name, high_light
        )
        entity["value"].append(entry)


def _flatten_subattr_output(subattr_output):
    """Flatten subattr_output into a single iterable."""
    iterator = []
    if isinstance(subattr_output, dict):
        for _, lst in subattr_output.items():
            if isinstance(lst, list):
                iterator.extend(lst)
            elif isinstance(lst, dict):
                iterator.append(lst)
    elif isinstance(subattr_output, list):
        iterator = subattr_output
    return iterator


def _process_entity_subattributes(item, entity, high_light):
    """Process and add subattributes to an entity."""
    subattr_output = item.get("subattr_output") or []
    iterator = _flatten_subattr_output(subattr_output)
    
    for sub in iterator:
        nested = process_universal(sub, high_light)
        entity["subattr_output"].append(nested)


def _add_empty_value_entry(entity, item, high_light):
    """Add an empty value entry if no values exist."""
    entry = {
        "editable_data": [],
        "score": item.get("score"),
        "display_name": entity["name"]
    }
    if high_light:
        entry["highlight"] = _safe_display_prop(item, "highlight")
    entity["value"].append(entry)


def process_universal(item, high_light):
    """Process a single item into a universal entity structure."""
    name = _safe_display_prop(item, "display_name") or item.get("name")
    display_style = _safe_display_prop(item, "display_style", "key_value_pair")
    
    entity = {
        "name": name,
        "subattr_output": [],
        "value": [],
        "group_header": item.get("group_header", False),
        "display_properties": {"display_style": display_style}
    }
    
    _process_entity_values(item, entity, high_light)
    _process_entity_subattributes(item, entity, high_light)
    
    if not entity["value"]:
        _add_empty_value_entry(entity, item, high_light)
    
    return entity


# ============================================================================
# HELPER FUNCTIONS FOR FINAL PREPROCESSING
# ============================================================================

def _is_dict_of_lists(item):
    """Check if item is a dict where all values are lists."""
    return (isinstance(item, dict) and 
            all(isinstance(k, str) and isinstance(v, list) for k, v in item.items()))


def _flatten_list_input(input_list, high_light, final):
    """Flatten list input data into entities."""
    for item in input_list:
        if _is_dict_of_lists(item):
            for _, lst in item.items():
                for ent in lst:
                    final.append(process_universal(ent, high_light))
        else:
            final.append(process_universal(item, high_light))


def _flatten_dict_input(input_dict, high_light, final):
    """Flatten dict input data into entities."""
    for _, lst in input_dict.items():
        if isinstance(lst, list):
            for ent in lst:
                final.append(process_universal(ent, high_light))


def _flatten_input_to_entities(input_data, high_light):
    """Flatten input data structure into a list of processed entities."""
    final = []
    
    if isinstance(input_data, list):
        _flatten_list_input(input_data, high_light, final)
    elif isinstance(input_data, dict):
        _flatten_dict_input(input_data, high_light, final)
    
    return final


def _build_extraction_metadata(extract_name, created_on, filename):
    """Build extraction metadata dictionaries."""
    extraction_details = {
        "ext_extraction_name": extract_name,
        "ext_started_on": created_on,
    }
    document_list = [{
        "dex_document_name": filename,
        "dex_created_on": created_on
    }]
    return extraction_details, document_list


def final_preprocess(input_data, high_light, extraction_id, document_id, extract_name, created_on, filename, annotation):
    """Preprocess input data and build final extraction structure."""
    final = _flatten_input_to_entities(input_data, high_light)
    extraction_details, document_list = _build_extraction_metadata(extract_name, created_on, filename)
    
    data = {
        "extraction_details": {
            "extraction_id": extraction_id,
            "document_id": document_id,
            "extraction_details": extraction_details,
            "document_list": document_list,
            "annotation": annotation,
            "error_message": "",
            "output": {
                "entity_data": final
            }
        }
    }
    
    return data
