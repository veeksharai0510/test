import logging
import sys

# Configure the root logger to write to 'debug.log'
logging.basicConfig(
    filename='debug.log',  # Specify the log file name
    level=logging.DEBUG,   # Set the minimum level to DEBUG (logs DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', # Define the log message format
    filemode='w'           # Use 'w' to overwrite the log file on each run, or 'a' to append (default)
)

# Get a logger instance (conventionally using __name__)
logger = logging.getLogger(__name__)


import json
# from setup_logger import setup_logger
from copy import deepcopy
from typing import Any
from collections import OrderedDict
# logger = setup_logger(__name__)
NOT_FOUND = "NOT_FOUND"
GUARD_FAILED = "SKIPPED_BY_GUARD"
COLLAPSE_SINGLE_VALUE_LISTS = True


def fetch_value_recursive(source, key):
    """
    Recursively search for the key in a nested dictionary/list.
    Returns the first match found, or None if not found.
    """
    logger.info(f"DEBUG: Fetching value for key '{key}' from source: {source}")
    if source is None:
        return None
    target = key.strip().lower()
    if isinstance(source, dict):
        return _fetch_from_dict(source, key, target)
    if isinstance(source, list):
        return _fetch_from_list(source, key)
    return None


def _fetch_from_dict(source, key, target):
    if key in source:
        return source[key]
    for k, v in source.items():
        if isinstance(k, str) and k.strip().lower() == target:
            return v
    for v in source.values():
        found = fetch_value_recursive(v, key)
        if found is not None:
            return found
    return None


def _fetch_from_list(source, key):
    for item in source:
        found = fetch_value_recursive(item, key)
        if found is not None:
            return found
    return None


def _handle_primitive_type(raw, is_multiple):
    if raw is None:
        return [NOT_FOUND] if is_multiple else NOT_FOUND
    if is_multiple:
        return raw if isinstance(raw, list) else [raw]
    if isinstance(raw, list):
        return raw[-1] if raw else NOT_FOUND
    return raw


def _build_child_dict_from_source(children, src, root_schema):
    return OrderedDict(
        (child["name"], build_from_schema(child, src, root_schema))
        for child in children
    )


def _scoped_plus_parent(src, data_source):
    if isinstance(src, list):
        dicts = [d for d in src if isinstance(d, dict)]
        return dicts + [data_source] if dicts else data_source
    if isinstance(src, dict):
        return [src, data_source]
    return data_source


def _handle_object_multiple(raw, children, data_source, root_schema):
    logger.info(f"DEBUG: Handling multiple objects. Raw data: {raw}")
    def build(src):
        return _build_child_dict_from_source(children, _scoped_plus_parent(src, data_source), root_schema)

    if isinstance(raw, list):
        dict_items = [item for item in raw if isinstance(item, dict)]
        if dict_items:
            # Merge dictionaries if raw contains multiple dicts
            merged_dict = OrderedDict()
            for item in dict_items:
                logger.info(f"DEBUG: Merging item into merged_dict. Item: {item}, Merged Dict: {merged_dict}")
                deep_merge_dict(merged_dict, item)
            return [build(merged_dict) if merged_dict else build(data_source)]
    if isinstance(raw, dict):
        return [build(raw)]
    return [build(data_source)]


def _handle_object_single(raw, children, data_source, root_schema):
    def build(src):
        return _build_child_dict_from_source(children, _scoped_plus_parent(src, data_source), root_schema)

    if isinstance(raw, dict):
        return build(raw)
    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, dict):
                return build(item)
        return build(data_source)
    return build(data_source)


def _handle_object_type(raw, is_multiple, children, data_source, root_schema):
    if is_multiple:
        return _handle_object_multiple(raw, children, data_source, root_schema)
    return _handle_object_single(raw, children, data_source, root_schema)


def build_from_schema(schema, data_source, root_schema=None):
    dtype = schema.get("datatype", schema.get("dataType", "")).lower()
    is_multiple = schema.get("isMultiple", False)
    children = schema.get("child", []) or schema.get("children", [])
    raw = fetch_value_recursive(data_source, schema["name"])

    if raw is None:
        logger.info(f"DEBUG: [MISS] key '{schema['name']}' NOT FOUND in this data_source")
    else:
        logger.info(f"DEBUG: [HIT] key '{schema['name']}' -> {raw}")

    if dtype == "object":
        return _handle_object_type(raw, is_multiple, children, data_source, root_schema)
    return _handle_primitive_type(raw, is_multiple)


def _merge_list_of_dicts(target_list, source_list):
    for entry in source_list:
        deep_merge_dict(target_list[0], entry)


def deep_merge_dict(target, source):
    for k, v in source.items():
        logger.info(f"DEBUG: Merging key '{k}': Target={target.get(k)}, Source={v}")
        if k not in target:
            target[k] = deepcopy(v)
        else:
            _merge_existing_key(target, k, v)


def _merge_existing_key(target, k, v):
    tv = target[k]
    if isinstance(tv, dict) and isinstance(v, dict):
        deep_merge_dict(tv, v)
    elif isinstance(tv, list) and isinstance(v, list) and all(isinstance(i, dict) for i in tv + v):
        _merge_list_of_dicts(tv, v)
    elif isinstance(tv, list) and isinstance(v, dict):
        for entry in tv:
            deep_merge_dict(entry, v)
    elif isinstance(tv, dict) and isinstance(v, list):
        for entry in v:
            deep_merge_dict(tv, entry)
    else:
        _merge_scalar_conflict(target, k, v)


def _merge_scalar_conflict(target, k, v):
   """
   OLD behavior in postprocessor_old: accumulate into list.
   But this causes duplication when multiple nodes produce the same field.
   The combined_data_source should use LAST-WRITER-WINS for scalar conflicts
   across nodes — only accumulate if it's genuinely multi-valued within one node.
   """
   # Instead of appending, just overwrite with the latest value
   target[k] = v

def _find_node_for_field(fname, nodeid_to_output):
    for nid, out in nodeid_to_output.items():
        if out is None:
            continue
        if fetch_value_recursive(out, fname) is not None:
            return nid
    return None


def _is_missing(v):
    if v is None or v == NOT_FOUND:
        return True
    if isinstance(v, list):
        return all(_is_missing(i) for i in v)
    if isinstance(v, dict):
        return not v or all(_is_missing(x) for x in v.values())
    return False


def _normalize_input1(inp):
    if inp is None:
        return []
    items = inp if isinstance(inp, list) else [inp]
    result = []
    for it in items:
        if isinstance(it, dict) and (("node_id" in it) or ("output" in it)):
            result.append(it)
        else:
            result.append({"node_id": None, "output": it})
    return result


def _flatten_schema(schema, schema_entries):
    if isinstance(schema, list):
        for s in schema:
            _flatten_schema(s, schema_entries)
    elif isinstance(schema, dict):
        schema_entries.append(schema)


def _as_dict(obj):
    if isinstance(obj, list):
        acc = OrderedDict()
        for item in obj:
            if isinstance(item, dict):
                deep_merge_dict(acc, item)
        return acc
    return obj if isinstance(obj, dict) else OrderedDict()


def _check_guard_failed_for_node(n):
    logs = n.get("logs") or {}
    gi = logs.get("guardrails_execution_info") or {}
    ig = gi.get("input_guards") or {}
    evals = ig.get("evaluation") if isinstance(ig, dict) else None
    if not isinstance(evals, list):
        return False
    for block in evals:
        if not isinstance(block, dict):
            continue
        for role in ("system", "user"):
            lst = block.get(role)
            if isinstance(lst, list) and lst and lst[0].get("status") == "FAILED":
                return True
    return False


def _collect_skip_node_ids(data):
    skip_node_ids = set()
    for n in data.get("nodes_executed", []):
        nid = n.get("node_id")
        if nid is None:
            continue
        if _check_guard_failed_for_node(n):
            skip_node_ids.add(str(nid))
    return skip_node_ids


def _build_combined_data_source(input1_nodes, skip_node_ids):
   combined = OrderedDict()
   for node in input1_nodes:
       nid = node.get("node_id")
       nid_str = str(nid) if nid is not None else None
       if nid_str in skip_node_ids:
           continue
       out = node.get("output", node)
       out_dict = _as_dict(out)
       for k, v in out_dict.items():
           combined.setdefault(k, v)  # ← first node wins, no accumulation
   return combined


def _field_from_skipped_node(name, data, input1_nodes, skip_node_ids):
    for exec_node in data.get("nodes_executed", []):
        nid_str = str(exec_node.get("node_id"))
        if nid_str not in skip_node_ids:
            continue
        matching = next((n for n in input1_nodes if str(n.get("node_id")) == nid_str), None)
        if matching is None:
            continue
        out = matching.get("output", {})
        if isinstance(out, dict) and name in out:
            return True
        if isinstance(out, list) and any(isinstance(i, dict) and name in i for i in out):
            return True
    return False


def _present_in_active_nodes(name, input1_nodes, skip_node_ids):
    for n in input1_nodes:
        nid = n.get("node_id")
        nid_str = str(nid) if nid is not None else None
        if nid_str in skip_node_ids:
            continue
        out = n.get("output")
        if isinstance(out, dict) and name in out:
            return True
        if isinstance(out, list) and any(isinstance(i, dict) and name in i for i in out):
            return True
    return False


def _should_skip_non_diet_val(val, name, input1_nodes, skip_node_ids):
    present = _present_in_active_nodes(name, input1_nodes, skip_node_ids)
    if val is None:
        return True
    if val == NOT_FOUND and not present:
        return True
    if isinstance(val, list) and all((v == NOT_FOUND or v is None) for v in val) and not present:
        return True
    return False


def _build_non_diet_output(schema_entries, combined_data_source, root_schema, data, input1_nodes, skip_node_ids):
    merged_output = OrderedDict()
    for entry in schema_entries:
        name = entry.get("name")
        if not name:
            continue
        val = build_from_schema(entry, combined_data_source, root_schema)
        if _field_from_skipped_node(name, data, input1_nodes, skip_node_ids):
            continue
        if _should_skip_non_diet_val(val, name, input1_nodes, skip_node_ids):
            continue
        deep_merge_dict(merged_output, {name: val})
    return [merged_output]


def _register_top_level_keys(out, nid_str, field_to_node):
    if isinstance(out, dict):
        for k in out.keys():
            field_to_node.setdefault(k, nid_str)
    elif isinstance(out, list):
        for item in out:
            if isinstance(item, dict):
                for k in item.keys():
                    field_to_node.setdefault(k, nid_str)


def _build_field_to_node(input1_nodes, skip_node_ids):
    field_to_node = {}
    nodeid_to_output = {}
    for node_entry in input1_nodes:
        nid = node_entry.get("node_id")
        nid_str = str(nid) if nid is not None else None
        if nid_str in skip_node_ids:
            logger.info(f"DEBUG: Skipping node {nid_str} in diet mapping due to guard FAILED")
            continue
        out = node_entry.get("output", {})
        nodeid_to_output[nid_str] = out
        _register_top_level_keys(out, nid_str, field_to_node)
    return field_to_node, nodeid_to_output


def _get_guard_statuses(evals):
    system_entry = [{}]
    user_entry = [{}]
    if isinstance(evals, list):
        system_entry = next((d.get("system") for d in evals if isinstance(d, dict) and "system" in d), [{}])
        user_entry = next((d.get("user") for d in evals if isinstance(d, dict) and "user" in d), [{}])
    sys_status = system_entry[0].get("status") if isinstance(system_entry, list) and system_entry else None
    usr_status = user_entry[0].get("status") if isinstance(user_entry, list) and user_entry else None
    return sys_status, usr_status


def _build_nodeid_to_search_context(data, skip_node_ids):
    nodeid_to_search_context = {}
    for n in data.get("nodes_executed", []):
        nid = n.get("node_id")
        nid_str = str(nid) if nid is not None else None
        if nid_str in skip_node_ids:
            logger.info(f'Nodeid: {nid}-> skipped (guard FAILED)')
            continue

        sc = ((n.get("search_context") or n.get("searchContext") or {}).get("search") or [{}])[0]

        gi = n.get("logs", {}).get("guardrails_execution_info", {})
        ig = gi.get("input_guards")
        if isinstance(ig, dict):
            evals = ig.get("evaluation")
        elif isinstance(ig, list):
            evals = ig
        else:
            evals = None

        sys_status, usr_status = _get_guard_statuses(evals)
        if sys_status == "FAILED" or usr_status == "FAILED":
            logger.info(f'Nodeid: {nid}-> Guard Failed')
            continue

        nodeid_to_search_context[nid_str] = sc

    return nodeid_to_search_context

def _split_score_value_list(v):
    inner_values = [it.get("value") for it in v]
    inner = inner_values[0] if (COLLAPSE_SINGLE_VALUE_LISTS and len(inner_values) == 1) else inner_values
    scores = [it.get("score") for it in v if it.get("score") is not None]
    score = None
    if scores:
        uniq = set(scores)
        score = scores[0] if len(uniq) == 1 else max(uniq)
    return inner, score


def _split_score_value(v):
    if isinstance(v, dict) and "value" in v:
        return v.get("value"), v.get("score")
    if isinstance(v, list) and v and all(isinstance(it, dict) and "value" in it for it in v):
        return _split_score_value_list(v)
    return v, None

def _get_sc_for_field(field_key, field_to_node, nodeid_to_output, nodeid_to_search_context):
    nid = _find_node_for_field(field_key, nodeid_to_output) or field_to_node.get(field_key)
    sc = nodeid_to_search_context.get(nid) if nid else None
    return sc if isinstance(sc, dict) else {}


def _get_highlight_from_sc(sc):
    if not isinstance(sc, dict):
        return {}
    return sc.get("highlight") or {}


def _get_page_content_from_sc(sc):
    if not isinstance(sc, dict):
        return ""
    return sc.get("page_content") or sc.get("pageContent") or ""


def _wrap_field(sc, highlight_info, inner_value, score):
    logger.info(f"DEBUG: Wrapping inner_value: {inner_value}")
    wrapped = {
        "value": inner_value,
        "page": sc.get("page", NOT_FOUND) if isinstance(sc, dict) else NOT_FOUND,
        "page_content": _get_page_content_from_sc(sc),
        "highlight": {
            "top": highlight_info.get("top"),
            "height": highlight_info.get("height"),
            "width": highlight_info.get("width"),
            "left": highlight_info.get("left"),
        },
    }
    if score is not None:
        wrapped["score"] = score
    return wrapped


def _make_wrap_for(fname, field_to_node, nodeid_to_output, nodeid_to_search_context):
    def wrap_for(field_key, v):
        key = field_key if field_key is not None else fname
        sc = _get_sc_for_field(key, field_to_node, nodeid_to_output, nodeid_to_search_context)
        highlight_info = _get_highlight_from_sc(sc)
        inner_value, score = _split_score_value(v)
        return _wrap_field(sc, highlight_info, inner_value, score)

    return wrap_for

def _to_nested_mapping_list_of_dicts(obj, fname, wrap_for):
    if all("value" in i for i in obj):
        return wrap_for(fname, obj)

    merged_inner = OrderedDict()
    for item in obj:
        if isinstance(item, dict):
            for k, v in item.items():
                if k in merged_inner and isinstance(merged_inner[k], dict) and isinstance(v, dict):
                    deep_merge_dict(merged_inner[k], v)
                else:
                    merged_inner[k] = wrap_for(k, v)
    return merged_inner


def _to_nested_mapping_list_value(k, v, wrap_for):
    if v and all(isinstance(i, dict) and "value" in i for i in v):
        return wrap_for(k, v)
    if v and all(isinstance(i, dict) for i in v):
        tmp = OrderedDict()
        for item in v:
            for kk, vv in item.items():
                tmp[kk] = wrap_for(kk, vv)
        return tmp
    return wrap_for(k, v)


def _to_nested_mapping_dict_value(k, v, wrap_for, to_nested_mapping):
    if isinstance(v, dict) and not ("value" in v and len(v) <= 4):
        return to_nested_mapping(v)
    if isinstance(v, list):
        return _to_nested_mapping_list_value(k, v, wrap_for)
    return wrap_for(k, v)


def _make_to_nested_mapping(fname, wrap_for):
    def to_nested_mapping(obj):
        if isinstance(obj, dict):
            inner = OrderedDict()
            for k, v in obj.items():
                inner[k] = _to_nested_mapping_dict_value(k, v, wrap_for, to_nested_mapping)
            return inner
        if isinstance(obj, list) and all(isinstance(i, dict) for i in obj):
            return _to_nested_mapping_list_of_dicts(obj, fname, wrap_for)
        return wrap_for(fname, obj)
    return to_nested_mapping


def _is_value_score_container(v):
    if not isinstance(v, dict):
        return False
    keys = set(v.keys())
    return "value" in keys and keys.issubset({"value", "score"})


def _is_object_like(value, dtype):
    if dtype == "object":
        return True
    if isinstance(value, dict) and not _is_value_score_container(value):
        return True
    if (isinstance(value, list) and all(isinstance(i, dict) for i in value)
            and not all(_is_value_score_container(i) for i in value)):
        return True
    return False


def _to_native(obj):
    if isinstance(obj, (OrderedDict, dict)):
        return {k: _to_native(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_to_native(i) for i in obj]
    return obj


def _field_in_any_output(fname, nodeid_to_output):
    for out in nodeid_to_output.values():
        if isinstance(out, dict) and fname in out:
            return True
        if isinstance(out, list) and any(isinstance(i, dict) and fname in i for i in out):
            return True
    return False


def _build_diet_result(schema_entries, field_to_node, nodeid_to_output, nodeid_to_search_context,
                       base_global, root_schema):
    result_list = []
    for entry in schema_entries:
        fname = entry.get("name")
        if not fname:
            continue

        node_id_str = field_to_node.get(fname)
        node_output_src = nodeid_to_output.get(node_id_str) if node_id_str else None
        data_source_for_entry = [node_output_src, base_global] if node_output_src is not None else base_global

        value = build_from_schema(entry, data_source_for_entry, root_schema)

        if _is_missing(value) and not _field_in_any_output(fname, nodeid_to_output):
            continue

        wrap_for = _make_wrap_for(fname, field_to_node, nodeid_to_output, nodeid_to_search_context)
        to_nested_mapping = _make_to_nested_mapping(fname, wrap_for)

        dtype = entry.get("datatype", entry.get("dataType", "")).lower()
        if _is_object_like(value, dtype):
            result_list.append({fname: to_nested_mapping(value)})
        else:
            result_list.append({fname: wrap_for(fname, value)})

    return [_to_native(result_list)]


def build_output(input1, input2, data, is_diet=False):
    input1_nodes = _normalize_input1(input1)
    schema_entries = []
    _flatten_schema(input2, schema_entries)
    root_schema = input2

    skip_node_ids = _collect_skip_node_ids(data)
    combined_data_source = _build_combined_data_source(input1_nodes, skip_node_ids)

    if not is_diet:
        return _build_non_diet_output(schema_entries, combined_data_source, root_schema,
                                      data, input1_nodes, skip_node_ids)

    base_global = combined_data_source if combined_data_source else data
    field_to_node, nodeid_to_output = _build_field_to_node(input1_nodes, skip_node_ids)
    nodeid_to_search_context = _build_nodeid_to_search_context(data, skip_node_ids)

    return _build_diet_result(schema_entries, field_to_node, nodeid_to_output, nodeid_to_search_context,
                              base_global, root_schema)


def flatten_object_and_wrap_once(obj, wrap):
    flat = {}
    for k, v in obj.items():
        flat[k] = v.get("value", v) if isinstance(v, dict) else v
    wrapped = wrap(None)
    wrapped.pop("value", None)
    wrapped.update(flat)
    return wrapped

with open('jsons\orc-output3.json') as f:
    data = json.load(f)
input1 = data['output']

with open('jsons\output_schema3.json','r') as f:
    input2 = json.load(f)
    
res = build_output(input1,input2,data, True)
with open('jsons\input-style3.json','w',encoding='utf-8') as wf:
	wf.write(json.dumps(res, indent=2))
