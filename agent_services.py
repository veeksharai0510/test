from app.utils.log_config import setup_logger
from copy import deepcopy
from typing import Any, Dict, List
from collections import OrderedDict
logger = setup_logger(**name**)
NOT_FOUND = “NOT_FOUND”
GUARD_FAILED = “SKIPPED_BY_GUARD”

def fetch_value_recursive(source, key):
“””
Recursively search for the key in a nested dictionary/list.
Returns the first match found, or None if not found.
Minimal change: keep fast exact-key match, add case-insensitive trimmed fallback
“””
if source is None:
return None
target = key.strip().lower()
if isinstance(source, dict):
if key in source:
return source[key]
for k, v in source.items():
if isinstance(k, str) and k.strip().lower() == target:
return v
for v in source.values():
found = fetch_value_recursive(v, key)
if found is not None:
return found
elif isinstance(source, list):
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

def _handle_object_type(raw, is_multiple, children, data_source, root_schema):
def _build_child_dict_from_source(src):
return OrderedDict((child[“name”], build_from_schema(child, src, root_schema)) for child in children)

```
def _scoped_plus_parent(src):
    if isinstance(src, list):
        dicts = [d for d in src if isinstance(d, dict)]
        return dicts + [data_source] if dicts else data_source
    if isinstance(src, dict):
        return [src, data_source]
    return data_source

if is_multiple:
    if isinstance(raw, list):
        dict_items = [item for item in raw if isinstance(item, dict)]
        if dict_items:
            return [_build_child_dict_from_source(_scoped_plus_parent(item)) for item in dict_items]
    if isinstance(raw, dict):
        return [_build_child_dict_from_source(_scoped_plus_parent(raw))]
    return [_build_child_dict_from_source(data_source)]

if isinstance(raw, dict):
    return _build_child_dict_from_source(_scoped_plus_parent(raw))
if isinstance(raw, list):
    for item in raw:
        if isinstance(item, dict):
            return _build_child_dict_from_source(_scoped_plus_parent(item))
    return _build_child_dict_from_source(data_source)
return _build_child_dict_from_source(data_source)
```

def build_from_schema(schema, data_source, root_schema=None):
dtype = schema.get(“datatype”, schema.get(“dataType”, “”)).lower()
is_multiple = schema.get(“isMultiple”, False)
children = schema.get(“child”, []) or schema.get(“children”, [])
raw = fetch_value_recursive(data_source, schema[“name”])
if raw is None:
logger.info(f”[MISS] key ‘{schema[‘name’]}’ NOT FOUND in this data_source”)
else:
logger.info(f”[HIT] key ‘{schema[‘name’]}’ -> {raw}”)

```
if dtype == "object":
    return _handle_object_type(raw, is_multiple, children, data_source, root_schema)
return _handle_primitive_type(raw, is_multiple)
```

def deep_merge_dict(target, source):
for k, v in source.items():
if k not in target:
target[k] = deepcopy(v)
elif isinstance(target[k], dict) and isinstance(v, dict):
deep_merge_dict(target[k], v)
elif isinstance(target[k], list) and isinstance(v, list) and all(isinstance(i, dict) for i in target[k] + v):
for entry in v:
deep_merge_dict(target[k][0], entry)
elif isinstance(target[k], list) and isinstance(v, dict):
for entry in target[k]:
deep_merge_dict(entry, v)
elif isinstance(target[k], dict) and isinstance(v, list):
for entry in v:
deep_merge_dict(target[k], entry)
else:
if not isinstance(target[k], list):
target[k] = [target[k]]
if isinstance(v, list):
target[k].extend(v)
else:
target[k].append(v)

def _find_node_for_field(fname, nodeid_to_output):
for nid, out in nodeid_to_output.items():
if out is None:
continue
if fetch_value_recursive(out, fname) is not None:
return nid
return None

def _is_missing(v):
if v is None:
return True
if v == NOT_FOUND:
return True
if isinstance(v, list):
return all(_is_missing(i) for i in v)
if isinstance(v, dict):
if not v:
return True
return all(_is_missing(x) for x in v.values())
return False

def _normalize_input1(inp):
norm = []
if inp is None:
return norm
items = inp if isinstance(inp, list) else [inp]
for it in items:
if isinstance(it, dict):
if (“node_id” in it) or (“output” in it):
norm.append(it)
else:
norm.append({“node_id”: None, “output”: it})
else:
norm.append({“node_id”: None, “output”: it})
return norm

def _flatten_schema(schema):
entries = []
if isinstance(schema, list):
for s in schema:
entries.extend(_flatten_schema(s))
elif isinstance(schema, dict):
entries.append(schema)
return entries

def _as_dict(obj):
if isinstance(obj, list):
acc = OrderedDict()
for item in obj:
if isinstance(item, dict):
deep_merge_dict(acc, item)
return acc
return obj if isinstance(obj, dict) else OrderedDict()

def _collect_skip_node_ids(data):
skip_node_ids = set()
for n in data.get(“nodes_executed”, []):
nid = n.get(“node_id”)
if nid is None:
continue
logs = n.get(“logs”) or {}
gi = logs.get(“guardrails_execution_info”) or {}
ig = gi.get(“input_guards”) or {}
evals = ig.get(“evaluation”) if isinstance(ig, dict) else None
failed = _check_guard_failed(evals)
if failed:
skip_node_ids.add(str(nid))
return skip_node_ids

def _check_guard_failed(evals):
if not isinstance(evals, list):
return False
for block in evals:
if not isinstance(block, dict):
continue
for role in (“system”, “user”):
lst = block.get(role)
if isinstance(lst, list) and lst and lst[0].get(“status”) == “FAILED”:
return True
return False

def _build_combined_data_source(input1_nodes, skip_node_ids):
combined = OrderedDict()
for node in input1_nodes:
nid = node.get(“node_id”)
nid_str = str(nid) if nid is not None else None
if nid_str in skip_node_ids:
logger.info(f”[SKIP] node {nid_str} skipped due to guard FAILED”)
continue
out = node.get(“output”, node)
deep_merge_dict(combined, _as_dict(out))
return combined

def _is_field_from_skipped_node(name, input1_nodes, data, skip_node_ids):
for exec_node in data.get(“nodes_executed”, []):
nid = exec_node.get(“node_id”)
nid_str = str(nid) if nid is not None else None
if nid_str not in skip_node_ids:
continue
matching = next((n for n in input1_nodes if str(n.get(“node_id”)) == nid_str), None)
if matching is None:
continue
out = matching.get(“output”, {})
if isinstance(out, dict) and name in out:
return True
if isinstance(out, list) and any(isinstance(i, dict) and name in i for i in out):
return True
return False

def _is_present_in_active_nodes(name, input1_nodes, skip_node_ids):
for n in input1_nodes:
nid_str = str(n.get(“node_id”)) if n.get(“node_id”) is not None else None
if nid_str in skip_node_ids:
continue
out = n.get(“output”)
if isinstance(out, dict) and name in out:
return True
if isinstance(out, list) and any(isinstance(i, dict) and name in i for i in out):
return True
return False

def _should_skip_non_diet_entry(name, val, input1_nodes, data, skip_node_ids):
if _is_field_from_skipped_node(name, input1_nodes, data, skip_node_ids):
return True
present = _is_present_in_active_nodes(name, input1_nodes, skip_node_ids)
if val is None:
return True
if val == NOT_FOUND and not present:
return True
if isinstance(val, list) and all(v == NOT_FOUND or v is None for v in val) and not present:
return True
return False

def _build_non_diet_output(schema_entries, combined_data_source, input1_nodes, data, skip_node_ids, root_schema):
merged_output = OrderedDict()
for entry in schema_entries:
name = entry.get(“name”)
if not name:
continue
val = build_from_schema(entry, combined_data_source, root_schema)
if _should_skip_non_diet_entry(name, val, input1_nodes, data, skip_node_ids):
continue
deep_merge_dict(merged_output, {name: val})
return [merged_output]

def _build_field_to_node_and_outputs(input1_nodes, skip_node_ids):
field_to_node = {}
nodeid_to_output = {}
for node_entry in input1_nodes:
nid = node_entry.get(“node_id”)
nid_str = str(nid) if nid is not None else None
if nid_str in skip_node_ids:
logger.info(f”Skipping node {nid_str} in diet mapping due to guard FAILED”)
continue
out = node_entry.get(“output”, {})
nodeid_to_output[nid_str] = out
keys = []
if isinstance(out, dict):
keys = list(out.keys())
elif isinstance(out, list):
for item in out:
if isinstance(item, dict):
keys.extend(item.keys())
for k in keys:
field_to_node.setdefault(k, nid_str)
return field_to_node, nodeid_to_output

def _build_nodeid_to_search_context(data, skip_node_ids):
nodeid_to_search_context = {}
for n in data.get(“nodes_executed”, []):
nid = n.get(“node_id”)
nid_str = str(nid) if nid is not None else None
if nid_str in skip_node_ids:
logger.info(f’Nodeid: {nid}-> skipped (guard FAILED)’)
continue
sc = ((n.get(“search_context”) or n.get(“searchContext”) or {}).get(“search”) or [{}])[0]
gi = n.get(“logs”, {}).get(“guardrails_execution_info”, {})
ig = gi.get(“input_guards”)
evals = ig.get(“evaluation”) if isinstance(ig, dict) else (ig if isinstance(ig, list) else None)
system_entry = next((d.get(“system”) for d in evals if isinstance(d, dict) and “system” in d), [{}]) if isinstance(evals, list) else [{}]
user_entry = next((d.get(“user”) for d in evals if isinstance(d, dict) and “user” in d), [{}]) if isinstance(evals, list) else [{}]
system_status = system_entry[0].get(“status”) if isinstance(system_entry, list) and system_entry else None
user_status = user_entry[0].get(“status”) if isinstance(user_entry, list) and user_entry else None
if system_status == “FAILED” or user_status == “FAILED”:
logger.info(f’Nodeid: {nid}-> Guard Failed’)
continue
nodeid_to_search_context[nid_str] = sc
return nodeid_to_search_context

COLLAPSE_SINGLE_VALUE_LISTS = True

def _split_score_value(v):
if isinstance(v, dict) and “value” in v:
return v.get(“value”), v.get(“score”)
if isinstance(v, list) and v and all(isinstance(it, dict) and “value” in it for it in v):
inner_values = [it.get(“value”) for it in v]
inner = inner_values[0] if (COLLAPSE_SINGLE_VALUE_LISTS and len(inner_values) == 1) else inner_values
scores = [it.get(“score”) for it in v if it.get(“score”) is not None]
score = None
if scores:
uniq = set(scores)
score = scores[0] if len(uniq) == 1 else max(uniq)
return inner, score
return v, None

def _make_wrap_for(fname, nodeid_to_output, nodeid_to_search_context, field_to_node):
def _get_sc_for_field(field_key):
nid = _find_node_for_field(field_key, nodeid_to_output) or field_to_node.get(field_key)
sc = nodeid_to_search_context.get(nid) if nid else None
return sc if isinstance(sc, dict) else {}

```
def wrap_for(field_key, v):
    key = field_key if field_key is not None else fname
    sc = _get_sc_for_field(key)
    hl = (sc.get("highlight") if isinstance(sc, dict) else None) or {}
    inner_value, score = _split_score_value(v)
    wrapped = {
        "value": inner_value,
        "page": sc.get("page", NOT_FOUND) if isinstance(sc, dict) else NOT_FOUND,
        "page_content": (sc.get("page_content") if isinstance(sc, dict) else None)
                        or (sc.get("pageContent") if isinstance(sc, dict) else None)
                        or "",
        "highlight": {
            "top": hl.get("top"),
            "height": hl.get("height"),
            "width": hl.get("width"),
            "left": hl.get("left"),
        }
    }
    if score is not None:
        wrapped["score"] = score
    return wrapped

return wrap_for
```

def _make_to_nested_mapping(fname, wrap_for):
def to_nested_mapping(obj):
if isinstance(obj, dict):
inner = OrderedDict()
for k, v in obj.items():
if isinstance(v, dict) and not (“value” in v and len(v) <= 4):
inner[k] = to_nested_mapping(v)
elif isinstance(v, list):
if v and all(isinstance(i, dict) and “value” in i for i in v):
inner[k] = wrap_for(k, v)
elif v and all(isinstance(i, dict) for i in v):
tmp = OrderedDict()
for item in v:
for kk, vv in item.items():
tmp[kk] = wrap_for(kk, vv)
inner[k] = tmp
else:
inner[k] = wrap_for(k, v)
else:
inner[k] = wrap_for(k, v)
return inner
if isinstance(obj, list) and all(isinstance(i, dict) for i in obj):
if all(“value” in i for i in obj):
return wrap_for(fname, obj)
inner = OrderedDict()
for item in obj:
for k, v in item.items():
inner[k] = wrap_for(k, v)
return inner
return wrap_for(fname, obj)

```
return to_nested_mapping
```

def _is_value_score_container(v):
if not isinstance(v, dict):
return False
keys = set(v.keys())
return “value” in keys and keys.issubset({“value”, “score”})

def _is_object_like(dtype, value):
if dtype == “object”:
return True
if isinstance(value, dict) and not _is_value_score_container(value):
return True
if (isinstance(value, list) and all(isinstance(i, dict) for i in value)
and not all(_is_value_score_container(i) for i in value)):
return True
return False

def _field_present_in_any_output(fname, nodeid_to_output):
for out in nodeid_to_output.values():
if isinstance(out, dict) and fname in out:
return True
if isinstance(out, list) and any(isinstance(i, dict) and fname in i for i in out):
return True
return False

def _build_diet_output(schema_entries, base_global, field_to_node, nodeid_to_output,
nodeid_to_search_context, data, skip_node_ids, root_schema):
result_list = []
for entry in schema_entries:
fname = entry.get(“name”)
if not fname:
continue

```
    node_id_str = field_to_node.get(fname)
    node_output_src = nodeid_to_output.get(node_id_str) if node_id_str else None
    data_source_for_entry = [node_output_src, base_global] if node_output_src is not None else base_global

    value = build_from_schema(entry, data_source_for_entry, root_schema)

    if _is_missing(value) and not _field_present_in_any_output(fname, nodeid_to_output):
        continue

    wrap_for = _make_wrap_for(fname, nodeid_to_output, nodeid_to_search_context, field_to_node)
    to_nested_mapping = _make_to_nested_mapping(fname, wrap_for)

    dtype = entry.get("datatype", entry.get("dataType", "")).lower()
    if _is_object_like(dtype, value):
        result_list.append({fname: to_nested_mapping(value)})
    else:
        result_list.append({fname: wrap_for(fname, value)})

return result_list
```

def _to_native(obj):
if isinstance(obj, (OrderedDict, dict)):
return {k: _to_native(v) for k, v in obj.items()}
if isinstance(obj, list):
return [_to_native(i) for i in obj]
return obj

def build_output(input1, input2, data, is_diet=False):
input1_nodes = _normalize_input1(input1)
schema_entries = _flatten_schema(input2)
root_schema = input2

```
skip_node_ids = _collect_skip_node_ids(data)
combined_data_source = _build_combined_data_source(input1_nodes, skip_node_ids)

if not is_diet:
    return _build_non_diet_output(schema_entries, combined_data_source, input1_nodes, data, skip_node_ids, root_schema)

base_global = combined_data_source if combined_data_source else data
field_to_node, nodeid_to_output = _build_field_to_node_and_outputs(input1_nodes, skip_node_ids)
nodeid_to_search_context = _build_nodeid_to_search_context(data, skip_node_ids)

result_list = _build_diet_output(
    schema_entries, base_global, field_to_node, nodeid_to_output,
    nodeid_to_search_context, data, skip_node_ids, root_schema
)

return [_to_native(result_list)]
```

def flatten_object_and_wrap_once(obj, wrap):
flat = {}
for k, v in obj.items():
if isinstance(v, dict):
flat[k] = v.get(“value”, v)
else:
flat[k] = v
wrapped = wrap(None)
wrapped.pop(“value”, None)
wrapped.update(flat)
return wrapped
