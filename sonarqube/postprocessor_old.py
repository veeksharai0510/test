
import logging

# Configure the root logger to write to 'debug.log'
logging.basicConfig(
    filename='debug.log', 
    level=logging.DEBUG,  
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', # Define the log message format
    filemode='w'    
)

# Get a logger instance (conventionally using __name__)
logger = logging.getLogger(__name__)


import json
# from app.utils.log_config import setup_logger
from copy import deepcopy
from typing import Any, Dict, List
from collections import OrderedDict
# logger = setup_logger(__name__)
NOT_FOUND = "NOT_FOUND"
GUARD_FAILED = "SKIPPED_BY_GUARD"


def fetch_value_recursive(source: any, key: str) -> any:
    """
    Recursively search for the key in a nested dictionary/list.
    Returns the first match found, or None if not found.
    Minimal change: keep fast exact-key match, add case-insensitive trimmed fallback
    """
    if source is None:
        return None
    target = key.strip().lower()
    if isinstance(source, dict):
        # fast exact-key match first (preserves existing behavior)
        if key in source:
            return source[key]
        # fall back to case-insensitive trimmed match for robustness
        for k, v in source.items():
            if isinstance(k, str) and k.strip().lower() == target:
                return v
        # otherwise recurse into values
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

# Handle primitive datatypes.
def _handle_primitive_type(raw, is_multiple):
    if raw is None:
        return [NOT_FOUND] if is_multiple else NOT_FOUND
    if is_multiple:
        if isinstance(raw, list):
            return raw
        else:
            return [raw]
    else:
        if isinstance(raw, list):
            return raw[-1] if raw else NOT_FOUND
        else:
            return raw


# Handle object datatype.
# Behavior:
#   - If is_multiple=True: return a list of child objects (preserve previous behavior).
#   - If is_multiple=False and raw is a dict with multiple children present:
#       return a list of single-child OrderedDicts (one per child). This enables
#       attaching page/page_content separately to each child in diet mode.
#   - Otherwise return a single OrderedDict of children.

def _handle_object_type(raw, is_multiple, children, data_source, root_schema): # passing whole input schema(input2)
    def _build_child_dict_from_source(src):
        return OrderedDict((child["name"], build_from_schema(child, src, root_schema))for child in children)

    # Prefer scoped dict(s) but also fallback to parent data_source
    def _scoped_plus_parent(src):
        if isinstance(src, list):
            dicts = [d for d in src if isinstance(d, dict)]
            return dicts + [data_source] if dicts else data_source
        if isinstance(src, dict):
            return [src, data_source]
        return data_source

    if is_multiple:
        result = []
        if isinstance(raw, list):
            dict_items = [item for item in raw if isinstance(item, dict)]
            if dict_items:
                for item in dict_items:
                    result.append(_build_child_dict_from_source(_scoped_plus_parent(item)))
                return result
        if isinstance(raw, dict):
            return [_build_child_dict_from_source(_scoped_plus_parent(raw))]
        return [_build_child_dict_from_source(data_source)]

    # Single object expected
    if isinstance(raw, dict):
        return _build_child_dict_from_source(_scoped_plus_parent(raw))

    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, dict):
                return _build_child_dict_from_source(_scoped_plus_parent(item))
        return _build_child_dict_from_source(data_source)

    # raw is None or primitive → fallback
    return _build_child_dict_from_source(data_source)


def build_from_schema(schema, data_source, root_schema=None):
    dtype = schema.get("datatype", schema.get("dataType", "")).lower()
    is_multiple = schema.get("isMultiple", False)
    children = schema.get("child", []) or schema.get("children", [])
    # Fetch value from data_source recursively (this will be None if not found)
    raw = fetch_value_recursive(data_source, schema["name"])
    # debugging
    if raw is None:
        logger.info(f"[MISS] key '{schema['name']}' NOT FOUND in this data_source")
    else:
        logger.info(f"[HIT] key '{schema['name']}' -> {raw}")

    if dtype == "object":
        return _handle_object_type(raw, is_multiple, children, data_source, root_schema)
    else:
        return _handle_primitive_type(raw, is_multiple)

# Deeply merge source into target recursively, handling dicts and lists.
def deep_merge_dict(target, source):
    for k, v in source.items():
        if k not in target:
            target[k] = deepcopy(v)
        else:
            if isinstance(target[k], dict) and isinstance(v, dict):
                deep_merge_dict(target[k], v)
            elif isinstance(target[k], list) and isinstance(v, list) and all(isinstance(i, dict) for i in target[k] + v):
                # merge dicts inside lists by deep-merging into first element
                for entry in v:
                    deep_merge_dict(target[k][0], entry)
            elif isinstance(target[k], list) and isinstance(v, dict):
                for entry in target[k]:
                    deep_merge_dict(entry, v)
            elif isinstance(target[k], dict) and isinstance(v, list):
                for entry in v:
                    deep_merge_dict(target[k], entry)
            else:
                # convert to list when scalar conflict and append new values
                if not isinstance(target[k], list):
                    target[k] = [target[k]]
                if isinstance(v, list):
                    target[k].extend(v)
                else:
                    target[k].append(v)

def _find_node_for_field(fname, nodeid_to_output):
    """Return the first node_id whose output actually contains the field (any depth)."""
    for nid, out in nodeid_to_output.items():
        if out is None:
            continue
        if fetch_value_recursive(out, fname) is not None:
            return nid
    return None

def _is_missing(v):
    """Return True when v is effectively missing/NOT_FOUND (any nesting)."""
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

# Robust build_output: flattens schema, merges when non-diet, and when is_diet=True
# attaches page/page_content into child objects for object types (one child per object).
def build_output(input1, input2, data, is_diet=False):
    def _normalize_input1(inp):
        norm = []
        if inp is None:
            return norm

        items = inp if isinstance(inp, list) else [inp]
        for it in items:
            if isinstance(it, dict):
                # If it already looks like a node, keep it. Else, wrap as payload.
                if ("node_id" in it) or ("output" in it):
                    norm.append(it)
                else:
                    norm.append({"node_id": None, "output": it})
            else:
                # primitive, list of primitives, etc. → wrap
                norm.append({"node_id": None, "output": it})
        return norm

    input1_nodes = _normalize_input1(input1)
    # Flatten schema entries (input2 could be nested lists)
    schema_entries = []
    def _flatten_schema(schema):
        if isinstance(schema, list):
            for s in schema:
                _flatten_schema(s)
        elif isinstance(schema, dict):
            schema_entries.append(schema)
    _flatten_schema(input2)
    root_schema = input2  # keep the full schema handy
    # ---------- Non-diet ----------
    # Build combined source from input1 so values are found even outside `data`
    def _as_dict(obj):
        if isinstance(obj, list):
            acc = OrderedDict()
            for item in obj:
                if isinstance(item, dict):
                    deep_merge_dict(acc, item)
            return acc
        return obj if isinstance(obj, dict) else OrderedDict()

    # Determine nodes to skip based on guardrail failures in `data.nodes_executed`.
    skip_node_ids = set()
    for n in data.get("nodes_executed", []):
        nid = n.get("node_id")
        if nid is None:
            continue
        logs = n.get("logs") or {}
        gi = logs.get("guardrails_execution_info") or {}
        ig = gi.get("input_guards") or {}
        evals = ig.get("evaluation") if isinstance(ig, dict) else None
        failed = False
        if isinstance(evals, list):
            for block in evals:
                if not isinstance(block, dict):
                    continue
                for role in ("system", "user"):
                    lst = block.get(role)
                    if isinstance(lst, list) and lst:
                        status = lst[0].get("status")
                        if status == "FAILED":
                            failed = True
                            break
                if failed:
                    break
        if failed:
            # preserve original casing by converting to string as-is
            skip_node_ids.add(str(nid))

    combined_data_source = OrderedDict()
    for node in input1_nodes:
        nid = node.get("node_id")
        nid_str = str(nid) if nid is not None else None
        if nid_str in skip_node_ids:
            logger.info(f"[SKIP] node {nid_str} skipped due to guard FAILED")
            continue
        out = node.get("output", node)
        deep_merge_dict(combined_data_source, _as_dict(out))
    if not is_diet:
        merged_output = OrderedDict()
        for entry in schema_entries:
            name = entry.get("name")
            if not name:
                continue
            val = build_from_schema(entry, combined_data_source, root_schema)  # use combined source

            # If value missing because the node that would have provided it was skipped due to guard failure,
            # do not include it in the merged output.
            field_from_skipped = False
            for exec_node in data.get("nodes_executed", []):
                nid = exec_node.get("node_id")
                nid_str = str(nid) if nid is not None else None
                if nid_str not in skip_node_ids:
                    continue
                # Check the original input1 for this node's output
                matching = next((n for n in input1_nodes if str(n.get("node_id")) == nid_str), None)
                if matching is not None:
                    out = matching.get("output", {})
                    if isinstance(out, dict) and name in out:
                        field_from_skipped = True
                        break
                    if isinstance(out, list):
                        for item in out:
                            if isinstance(item, dict) and name in item:
                                field_from_skipped = True
                                break
                        if field_from_skipped:
                            break

            if field_from_skipped:
                continue

            # treat NOT_FOUND / None / empty-string as absent
            present_in_input = any(
                (isinstance(n.get("output"), dict) and name in n.get("output"))
                or (isinstance(n.get("output"), list) and any(isinstance(i, dict) and name in i for i in n.get("output")))
                for n in input1_nodes
                if (str(n.get("node_id")) if n.get("node_id") is not None else None) not in skip_node_ids
            )
            if val is None or (val == NOT_FOUND and not present_in_input) or (isinstance(val, list) and all((v == NOT_FOUND or v is None) for v in val) and not present_in_input):
                continue

            deep_merge_dict(merged_output, {name: val})
        return [merged_output]

    # ---------- diet mode ----------
    base_global = combined_data_source if combined_data_source else data

    # 2) Map fields to nodes (top-level keys is sufficient; context will be resolved per-leaf later)
    field_to_node = {}
    nodeid_to_output = {}
    for node_entry in input1_nodes:
        nid = node_entry.get("node_id")
        nid_str = str(nid) if nid is not None else None
        # skip nodes known to have failed guardrails
        if nid_str in skip_node_ids:
            logger.info(f"Skipping node {nid_str} in diet mapping due to guard FAILED")
            continue
        out = node_entry.get("output", {})
        nodeid_to_output[nid_str] = out

        # register ONLY top-level keys for this node (avoid stealing nested keys)
        if isinstance(out, dict):
            for k in out.keys():
                field_to_node.setdefault(k, nid_str)
        elif isinstance(out, list):
            for item in out:
                if isinstance(item, dict):
                    for k in item.keys():
                        field_to_node.setdefault(k, nid_str)

    # Build nodeid->search_context map (accepts alt names)
    nodeid_to_search_context = {}
    for n in data.get("nodes_executed", []):
        nid = n.get("node_id")
        nid_str = str(nid) if nid is not None else None
        # Skip nodes that were detected as guard-failed
        if nid_str in skip_node_ids:
            logger.info(f'Nodeid: {nid}-> skipped (guard FAILED)')
            continue
        # sc = n.get("search_context") if n.get("search_context") is not None else n.get("searchContext")
        sc = ((n.get("search_context") or n.get("searchContext") or {}).get("search") or [{}])[0]
        # If search_context exists, also double-check guard status in logs and skip if FAILED
        gi = n.get("logs", {}).get("guardrails_execution_info", {})
        ig = gi.get("input_guards")
        evals = ig.get("evaluation") if isinstance(ig, dict) else (ig if isinstance(ig, list) else None)
        system_entry = next((d.get("system") for d in evals if isinstance(d, dict) and "system" in d), [{}]) if isinstance(evals, list) else [{}]
        user_entry = next((d.get("user") for d in evals if isinstance(d, dict) and "user" in d), [{}]) if isinstance(evals, list) else [{}]
        system_status = system_entry[0].get("status") if isinstance(system_entry, list) and system_entry else None
        user_status = user_entry[0].get("status") if isinstance(user_entry, list) and user_entry else None
        if system_status == "FAILED" or user_status == "FAILED":
            logger.info(f'Nodeid: {nid}-> Guard Failed')
            continue
        nodeid_to_search_context[nid_str] = sc

    # ---- Diet mode ----
    result_list = []
    for entry in schema_entries:
        fname = entry.get("name")
        if not fname:
            continue

        # If this field would have been produced by a node that failed guardrails, skip it (your existing logic)
        skip_field_due_to_guard = False
        for exec_node in data.get("nodes_executed", []):
            if str(exec_node.get("node_id")) not in skip_node_ids:
                continue
            # (keep whatever you need here; currently you don't set skip true)
        if skip_field_due_to_guard:
            continue

        # Figure out which node owns this field (top-level ownership)
        node_id_str = field_to_node.get(fname)
        node_output_src = nodeid_to_output.get(node_id_str) if node_id_str else None
        if node_output_src is not None:
            data_source_for_entry = [node_output_src, base_global]
        else:
            data_source_for_entry = base_global

        # >>> IMPORTANT: compute `value` BEFORE using it <<<
        value = build_from_schema(entry, data_source_for_entry, root_schema)

        # Skip effectively missing values
        if _is_missing(value) and not any(
            (isinstance(out, dict) and fname in out) or
            (isinstance(out, list) and any(isinstance(i, dict) and fname in i for i in out))
            for out in nodeid_to_output.values()
        ):
            continue

        # ---- Per-leaf context helpers ----

        # Optional: collapse a single [{score,value}] → just value
        COLLAPSE_SINGLE_VALUE_LISTS = True

        def _get_sc_for_field(field_key):
            nid = _find_node_for_field(field_key, nodeid_to_output) or field_to_node.get(field_key)
            sc = nodeid_to_search_context.get(nid) if nid else None
            return sc if isinstance(sc, dict) else {}

        def _split_score_value(v):
            """
            Flatten common 'score/value' containers, including lists of such containers.
            Returns (flattened_value, score_or_None).
            """
            # Case 1: direct dict container
            if isinstance(v, dict) and "value" in v:
                return v.get("value"), v.get("score")

            # Case 2: list of dict containers [{score?, value}, ...]
            if isinstance(v, list) and v and all(isinstance(it, dict) and "value" in it for it in v):
                inner_values = [it.get("value") for it in v]
                # Collapse [single] to scalar if enabled
                inner = inner_values[0] if (COLLAPSE_SINGLE_VALUE_LISTS and len(inner_values) == 1) else inner_values
                # Aggregate score: if all equal → that, else pick max (deterministic)
                scores = [it.get("score") for it in v if it.get("score") is not None]
                score = None
                if scores:
                    uniq = set(scores)
                    score = scores[0] if len(uniq) == 1 else max(uniq)
                return inner, score

            # No change
            return v, None

        def wrap_for(field_key, v):
            sc = _get_sc_for_field(field_key)
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


        def to_nested_mapping(obj):
            # Recurse and wrap leaves with their own context
            if isinstance(obj, dict):
                inner = OrderedDict()
                for k, v in obj.items():
                    if isinstance(v, dict) and not ("value" in v and len(v) <= 4):
                        inner[k] = to_nested_mapping(v)

                    elif isinstance(v, list):
                        # If it's a list of score/value containers → let wrap_for flatten it
                        if v and all(isinstance(i, dict) and "value" in i for i in v):
                            inner[k] = wrap_for(k, v)
                        # If it's a list of nested dicts (true child objects) → flatten into mapping
                        elif v and all(isinstance(i, dict) for i in v):
                            tmp = OrderedDict()
                            for item in v:
                                for kk, vv in item.items():
                                    tmp[kk] = wrap_for(kk, vv)
                            inner[k] = tmp
                        else:
                            # primitives or mixed → wrap as-is
                            inner[k] = wrap_for(k, v)
                    else:
                        # Primitive or score/value dict
                        inner[k] = wrap_for(k, v)
                return inner
            # If obj is a list of dicts: flatten into mapping
            if isinstance(obj, list) and all(isinstance(i, dict) for i in obj):
                # If it's actually a list of score/value containers, let wrap_for flatten at once
                if all("value" in i for i in obj):
                    return wrap_for(fname, obj)
                inner = OrderedDict()
                for item in obj:
                    for k, v in item.items():
                        inner[k] = wrap_for(k, v)
                return inner

            # Primitive directly under the parent field: use the parent's key for context
            return wrap_for(fname, obj)

        dtype = entry.get("datatype", entry.get("dataType", "")).lower()
        # is_object_like = (
        #     dtype == "object" or
        #     isinstance(value, dict) or
        #     (isinstance(value, list) and all(isinstance(i, dict) for i in value))
        # )

        def _is_value_score_container(v):
            """
            Check if v is a {value, score?} container from LLM output.
            These should be treated as primitives with metadata, not as nested objects.
            """
            if not isinstance(v, dict):
                return False
            keys = set(v.keys())
            return "value" in keys and keys.issubset({"value", "score"})

        is_object_like = (
            dtype == "object" or
            (isinstance(value, dict) and not _is_value_score_container(value)) or
            (isinstance(value, list) and all(isinstance(i, dict) for i in value)
             and not all(_is_value_score_container(i) for i in value))
        )

        if is_object_like:
            # Let to_nested_mapping detect score/value leaves and flatten them
            result_list.append({fname: to_nested_mapping(value)})
            continue

        # Primitive / non-object branch (top-level)
        result_list.append({fname: wrap_for(fname, value)})

    def _to_native(obj):
        if isinstance(obj, OrderedDict):
            return {k: _to_native(v) for k, v in obj.items()}
        if isinstance(obj, dict):
            return {k: _to_native(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [_to_native(i) for i in obj]
        return obj
    native_result = _to_native(result_list)
    return [native_result]


def flatten_object_and_wrap_once(obj, wrap):
    flat = {}
    for k, v in obj.items():
        # child objects are NOT expected here for outlook
        if isinstance(v, dict):
            flat[k] = v.get("value", v)
        else:
            flat[k] = v
    wrapped = wrap(None)
    wrapped.pop("value", None)
    wrapped.update(flat)
    return wrapped



with open('jsons\orc-output1.json') as f:
    data = json.load(f)
input1 = data['output']

with open('jsons\output_schema1.json','r') as f:
    input2 = json.load(f)
    
res = build_output(input1,input2,data, True)
with open('jsons\input-style1.json','w',encoding='utf-8') as wf:
	wf.write(json.dumps(res, indent=2))
