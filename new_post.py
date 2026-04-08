def _fetch_from_dict(source, key, target):
    """Helper: search for key in a dict, with exact then case-insensitive match, then recurse."""
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
    """Helper: search for key by recursing into each list item."""
    for item in source:
        found = fetch_value_recursive(item, key)
        if found is not None:
            return found
    return None


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
        return _fetch_from_dict(source, key, target)
    if isinstance(source, list):
        return _fetch_from_list(source, key)
    return None

# Handle primitive datatypes.
def _handle_primitive_type(raw, is_multiple):
    if raw is None:
        return NOT_FOUND
    if is_multiple:
        if isinstance(raw, list):
            return raw
        else:
            return [raw]
    else:
        if isinstance(raw, list):
            # If it's multiple score/value containers, preserve the full list
            # so _map_list_value can expand them into FY-1, FY-2, etc.
            if len(raw) > 1 and all(isinstance(i, dict) and "value" in i for i in raw):
                return raw  # ← preserve list for expansion
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

def _build_child_dict(children, src, root_schema):
    return OrderedDict((child["name"], build_from_schema(child, src, root_schema)) for child in children)


def _scoped_plus_parent(src, data_source):
    """Prefer scoped dict(s) but also fallback to parent data_source."""
    if isinstance(src, list):
        dicts = [d for d in src if isinstance(d, dict)]
        return dicts + [data_source] if dicts else data_source
    if isinstance(src, dict):
        return [src, data_source]
    return data_source


def _handle_object_multiple(raw, children, data_source, root_schema):
    if isinstance(raw, list):
        dict_items = [item for item in raw if isinstance(item, dict)]
        if dict_items:
            return [
                _build_child_dict(children, _scoped_plus_parent(item, data_source), root_schema)
                for item in dict_items
            ]
    if isinstance(raw, dict):
        return [_build_child_dict(children, _scoped_plus_parent(raw, data_source), root_schema)]
    # raw is None — parent not found, return single empty child dict
    return [_build_empty_child_dict(children)]


def _handle_object_single(raw, children, data_source, root_schema):
    if isinstance(raw, dict):
        return _build_child_dict(children, _scoped_plus_parent(raw, data_source), root_schema)
    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, dict):
                return _build_child_dict(children, _scoped_plus_parent(item, data_source), root_schema)
        # List was present but had no dict items — children are absent
        return _build_empty_child_dict(children)
    # raw is None — parent not found, so children are also absent, don't search globally
    return _build_empty_child_dict(children)

def _handle_object_type(raw, is_multiple, children, data_source, root_schema): # passing whole input schema(input2)
    if is_multiple:
        return _handle_object_multiple(raw, children, data_source, root_schema)
    return _handle_object_single(raw, children, data_source, root_schema)


def build_from_schema(schema, data_source, root_schema=None):
    dtype = schema.get("datatype", schema.get("dataType", "")).lower()
    is_multiple = schema.get("isMultiple", False)
    children = schema.get("child", []) or schema.get("children", [])
    # Fetch value from data_source recursively (this will be None if not found)
    raw = fetch_value_recursive(data_source, schema["name"])
    # debugging
    if raw is None:
        print(f"[MISS] key '{schema['name']}' NOT FOUND in this data_source")
    else:
        print(f"[HIT] key '{schema['name']}' -> {raw}")

    if dtype == "object":
        return _handle_object_type(raw, is_multiple, children, data_source, root_schema)
    else:
        return _handle_primitive_type(raw, is_multiple)

# Deeply merge source into target recursively, handling dicts and lists.
def _merge_lists(target_list, source_list):
    """Merge source_list into target_list without duplicating values."""
    existing_items = set(map(json.dumps, target_list))
    new_items = [item for item in source_list if json.dumps(item) not in existing_items]
    target_list.extend(new_items)


def _merge_value(target, k, v):
    """Merge a single key-value pair from source into target."""
    if isinstance(target[k], dict) and isinstance(v, dict):
        deep_merge_dict(target[k], v)
    elif isinstance(target[k], list) and isinstance(v, list):
        _merge_lists(target[k], v)
    elif isinstance(target[k], list):
        # Avoid appending duplicates for scalar conflicts
        if v not in target[k]:
            target[k].append(v)
    elif isinstance(v, list):
        # Replace scalar with list if source is a list
        target[k] = v
    else:
        # Replace scalar with scalar
        target[k] = v


def deep_merge_dict(target, source):
    for k, v in source.items():
        if k not in target:
            target[k] = deepcopy(v)
        else:
            _merge_value(target, k, v)

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

def _normalize_input1(inp, node_schema_map=None):
    norm = []
    if inp is None:
        return norm
    items = inp if isinstance(inp, list) else [inp]
    for it in items:
        if isinstance(it, dict):
            node = dict(it)  # shallow copy

            nid = node.get("node_id")
            if node_schema_map and nid is not None:
                # response_format is a LIST (possibly nested list)
                node["response_format"] = node_schema_map.get(str(nid), [])

            norm.append(node)
        else:
            # primitive, list of primitives, etc. → wrap
            norm.append({"node_id": None, "output": it})
    return norm


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


def _node_guard_failed(n):
    """Return True if this node's guardrail evaluation has a FAILED status."""
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


def _build_skip_node_ids(data):
    """Determine nodes to skip based on guardrail failures in data.nodes_executed."""
    skip_node_ids = set()
    for n in data.get("nodes_executed", []):
        nid = n.get("node_id")
        if nid is None:
            continue
        if _node_guard_failed(n):
            # preserve original casing by converting to string as-is
            skip_node_ids.add(str(nid))
    return skip_node_ids


def _build_combined_data_source(input1_nodes, skip_node_ids):
    combined_data_source = OrderedDict()
    for node in input1_nodes:
        nid = node.get("node_id")
        nid_str = str(nid) if nid is not None else None
        if nid_str in skip_node_ids:
            print(f"[SKIP] node {nid_str} skipped due to guard FAILED")
            continue
        out = node.get("output", node)
        deep_merge_dict(combined_data_source, _as_dict(out))
    return combined_data_source


def _name_in_output(name, out):
    """Return True if 'name' appears as a top-level key in a dict or list-of-dicts output."""
    if isinstance(out, dict):
        return name in out
    if isinstance(out, list):
        return any(isinstance(item, dict) and name in item for item in out)
    return False


def _field_from_skipped_node(name, data, input1_nodes, skip_node_ids):
    """Return True if the field 'name' originates from a guard-failed (skipped) node."""
    for exec_node in data.get("nodes_executed", []):
        nid = exec_node.get("node_id")
        nid_str = str(nid) if nid is not None else None
        if nid_str not in skip_node_ids:
            continue
        # Check the original input1 for this node's output
        matching = next((n for n in input1_nodes if str(n.get("node_id")) == nid_str), None)
        if matching is None:
            continue
        if _name_in_output(name, matching.get("output", {})):
            return True
    return False


def _is_present_in_input(name, input1_nodes, skip_node_ids):
    """Return True if 'name' appears in any non-skipped node's output."""
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


def _val_should_skip(val, present_in_input):
    """Return True if val is effectively absent and not present in input."""
    if val is None:
        return True
    if val == NOT_FOUND and not present_in_input:
        return True
    if isinstance(val, list) and all((v == NOT_FOUND or v is None) for v in val) and not present_in_input:
        return True
    return False


def _build_non_diet_output(schema_entries, combined_data_source, root_schema,
                            data, input1_nodes, skip_node_ids):
    merged_output = OrderedDict()
    for entry in schema_entries:
        name = entry.get("name")
        if not name:
            continue

        val = build_from_schema(entry, combined_data_source, root_schema)

        # Guard-failed node fields → still drop entirely
        if _field_from_skipped_node(name, data, input1_nodes, skip_node_ids):
            continue

        # Normalize None → NOT_FOUND first, then replace NOT_FOUND → None for output
        if val is None:
            val = NOT_FOUND

        # Replace NOT_FOUND sentinel with None before writing to result
        val = _replace_not_found(val)

        deep_merge_dict(merged_output, {name: val})
    return [merged_output]

def _register_node_keys(out, nid_str, field_to_node):
    """Register top-level keys from a node output into field_to_node."""
    if isinstance(out, dict):
        for k in out.keys():
            field_to_node.setdefault(k, nid_str)
    elif isinstance(out, list):
        for item in out:
            if isinstance(item, dict):
                for k in item.keys():
                    field_to_node.setdefault(k, nid_str)


def _build_field_to_node_maps(input1_nodes, skip_node_ids):
    """Map fields to nodes and build nodeid->output dict."""
    field_to_node = {}
    nodeid_to_output = {}
    for node_entry in input1_nodes:
        nid = node_entry.get("node_id")
        nid_str = str(nid) if nid is not None else None
        # skip nodes known to have failed guardrails
        if nid_str in skip_node_ids:
            print(f"Skipping node {nid_str} in diet mapping due to guard FAILED")
            continue
        out = node_entry.get("output", {})
        nodeid_to_output[nid_str] = out
        # register ONLY top-level keys for this node (avoid stealing nested keys)
        _register_node_keys(out, nid_str, field_to_node)
    return field_to_node, nodeid_to_output


def _get_evals_from_node(n):
    """Extract the evaluations list from a node's guardrails info."""
    gi = n.get("logs", {}).get("guardrails_execution_info", {})
    ig = gi.get("input_guards")
    if isinstance(ig, dict):
        return ig.get("evaluation")
    if isinstance(ig, list):
        return ig
    return None


def _get_guard_entry(evals, role):
    """Return the first entry for the given role from the evals list, or [{}]."""
    if not isinstance(evals, list):
        return [{}]
    return next((d.get(role) for d in evals if isinstance(d, dict) and role in d), [{}])


def _get_entry_status(entry):
    """Return the status string from a guard entry list, or None."""
    if isinstance(entry, list) and entry:
        return entry[0].get("status")
    return None


def _build_nodeid_to_search_context(data, skip_node_ids):
    """Build nodeid->search_context map (accepts alt names)."""
    nodeid_to_search_context = {}
    for n in data.get("nodes_executed", []):
        nid = n.get("node_id")
        nid_str = str(nid) if nid is not None else None
        # Skip nodes that were detected as guard-failed
        if nid_str in skip_node_ids:
            print(f'Nodeid: {nid}-> skipped (guard FAILED)')
            continue
        sc = ((n.get("search_context") or n.get("searchContext") or {}).get("search") or [{}])[0]
        # If search_context exists, also double-check guard status in logs and skip if FAILED
        evals = _get_evals_from_node(n)
        system_entry = _get_guard_entry(evals, "system")
        user_entry = _get_guard_entry(evals, "user")
        system_status = _get_entry_status(system_entry)
        user_status = _get_entry_status(user_entry)
        if system_status == "FAILED" or user_status == "FAILED":
            print(f'Nodeid: {nid}-> Guard Failed')
            continue
        nodeid_to_search_context[nid_str] = sc
    return nodeid_to_search_context


def _fname_in_any_output(fname, nodeid_to_output):
    """Return True if fname appears as a key in any node output."""
    for out in nodeid_to_output.values():
        if isinstance(out, dict) and fname in out:
            return True
        if isinstance(out, list) and any(isinstance(i, dict) and fname in i for i in out):
            return True
    return False


def _get_sc_for_field(field_key, nodeid_to_output, field_to_node, nodeid_to_search_context):
    nid = _find_node_for_field(field_key, nodeid_to_output) or field_to_node.get(field_key)
    sc = nodeid_to_search_context.get(nid) if nid else None
    return sc if isinstance(sc, dict) else {}


def _aggregate_score(score_list):
    """Aggregate a list of scores: return the single value if uniform, else max."""
    if not score_list:
        return None
    uniq = set(score_list)
    return score_list[0] if len(uniq) == 1 else max(uniq)


def _split_score_value(v):
    """
    Flatten common 'score/value' containers, including lists of such containers.
    Returns (flattened_value, score_or_None).
    """
    # Case 1: direct dict container
    if isinstance(v, dict) and "value" in v:
        return v.get("value"), v.get("confidence_score")

    # Case 2: list of dict containers [{score?, value}, ...]
    if isinstance(v, list) and v and all(isinstance(it, dict) and "value" in it for it in v):
        inner_values = [it.get("value") for it in v]
        # Collapse [single] to scalar if enabled
        inner = inner_values[0] if len(inner_values) == 1 else inner_values
        # Aggregate score: if all equal → that, else pick max (deterministic)
        scores = [it.get("confidence_score") for it in v if it.get("confidence_score") is not None]
        return inner, _aggregate_score(scores)

    # No change
    return v, None


def _wrap_for(field_key, v, nodeid_to_output, field_to_node, nodeid_to_search_context):
    # If value is absent, return null structure — do NOT fetch search context
    # This prevents same-named sub-attributes from stealing sibling's page/highlight
    inner_value, score = _split_score_value(v)
    is_absent = inner_value is None or inner_value == NOT_FOUND

    if is_absent:
        return {
            "value": None,
            "page": None,
            "page_content": None,
            "highlight": {
                "top": None,
                "height": None,
                "width": None,
                "left": None,
            }
        }

    # Value is present — fetch search context normally
    sc = _get_sc_for_field(field_key, nodeid_to_output, field_to_node, nodeid_to_search_context)
    hl = (sc.get("highlight") if isinstance(sc, dict) else None) or {}
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


def _map_dict_list_to_mapping(v, wrap):
    """Flatten a list of nested dicts into a single OrderedDict via wrap."""
    tmp = OrderedDict()
    for item in v:
        for kk, vv in item.items():
            tmp[kk] = wrap(kk, vv)
    return tmp


def _map_list_value(k, v, wrap):
    # List of score/value containers with MULTIPLE items → expand into k-1, k-2, ...
    if v and all(isinstance(i, dict) and "value" in i for i in v):
        if len(v) > 1:
            return {f"{k}-{idx + 1}": wrap(k, item) for idx, item in enumerate(v)}
        return wrap(k, v)  # single item → keep as-is
    # List of child objects → returns a list
    if v and all(isinstance(i, dict) for i in v):
        return _map_list_of_dicts(v, k, wrap)
    # Primitives or mixed → wrap as-is
    return wrap(k, v)

def _map_dict_obj(obj, fname, wrap):
    """Handle the dict branch of _to_nested_mapping."""
    inner = OrderedDict()
    for k, v in obj.items():
        if isinstance(v, dict) and not ("value" in v and len(v) <= 4):
            inner[k] = _to_nested_mapping(v, fname, wrap)
        elif isinstance(v, list):
            inner[k] = _map_list_value(k, v, wrap)
        else:
            # Primitive or score/value dict
            inner[k] = wrap(k, v)
    return inner


def _map_list_of_dicts(obj, fname, wrap):
    # List of score/value containers with MULTIPLE items → expand into fname-1, fname-2, ...
    if all("value" in i for i in obj):
        if len(obj) > 1:
            return {f"{fname}-{idx + 1}": wrap(fname, item) for idx, item in enumerate(obj)}
        return wrap(fname, obj)  # single item → keep as-is
    # List of child objects → preserve as a list, wrapping each item separately
    result = []
    for item in obj:
        wrapped_item = OrderedDict()
        for k, v in item.items():
            if isinstance(v, list) and v and all(isinstance(i, dict) and "value" in i for i in v):
                # ← THIS is the fix for Expense-Val inside each child object
                if len(v) > 1:
                    wrapped_item[k] = {f"{k}-{idx + 1}": wrap(k, item) for idx, item in enumerate(v)}
                else:
                    wrapped_item[k] = wrap(k, v)
            else:
                wrapped_item[k] = wrap(k, v)
        result.append(wrapped_item)
    return result

def _to_nested_mapping(obj, fname, wrap):
    # Recurse and wrap leaves with their own context
    if isinstance(obj, dict):
        return _map_dict_obj(obj, fname, wrap)
    # If obj is a list of dicts: flatten into mapping
    if isinstance(obj, list) and all(isinstance(i, dict) for i in obj):
        return _map_list_of_dicts(obj, fname, wrap)
    # Primitive directly under the parent field: use the parent's key for context
    return wrap(fname, obj)


def _is_value_score_container(v):
    """
    Check if v is a {value, score?} container from LLM output.
    These should be treated as primitives with metadata, not as nested objects.
    """
    if not isinstance(v, dict):
        return False
    keys = set(v.keys())
    return "value" in keys and keys.issubset({"value", "confidence_score"})


def _is_object_like(value, dtype):
    """Return True if the value should be treated as an object/nested structure."""
    return (
        dtype == "object" or
        (isinstance(value, dict) and not _is_value_score_container(value)) or
        (isinstance(value, list) and all(isinstance(i, dict) for i in value)
         and not all(_is_value_score_container(i) for i in value))
    )


def _to_native(obj):
    if isinstance(obj, OrderedDict):
        return {k: _to_native(v) for k, v in obj.items()}
    if isinstance(obj, dict):
        return {k: _to_native(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_to_native(i) for i in obj]
    return obj


def _get_data_source_for_entry(fname, field_to_node, nodeid_to_output, base_global,
                                rf_field_to_node=None):  # ← add param
    node_id_str = field_to_node.get(fname)
    # Fallback: use the node that declared this field in its response_format
    if node_id_str is None and rf_field_to_node:
        node_id_str = rf_field_to_node.get(fname)
    node_output_src = nodeid_to_output.get(node_id_str) if node_id_str else None
    if node_output_src is not None:
        return [node_output_src, base_global]
    return base_global


def _build_diet_entry(entry, fname, base_global, field_to_node, nodeid_to_output,
                       root_schema, wrap, rf_field_to_node=None,
                       skip_node_ids=None, data=None, input1_nodes=None):  # ← add params
    
    # Guard-failed node fields → drop entirely (fixes the no-op loop in diet mode)
    if (skip_node_ids and data and input1_nodes and
            _field_from_skipped_node(fname, data, input1_nodes, skip_node_ids)):
        return None

    data_source_for_entry = _get_data_source_for_entry(
        fname, field_to_node, nodeid_to_output, base_global, rf_field_to_node
    )
    value = build_from_schema(entry, data_source_for_entry, root_schema)

    # Normalize None → NOT_FOUND, then replace NOT_FOUND → None for output
    if value is None:
        value = NOT_FOUND
    value = _replace_not_found(value)

    dtype = entry.get("datatype", entry.get("dataType", "")).lower()
    if _is_object_like(value, dtype):
        result = {fname: _to_nested_mapping(value, fname, wrap)}
    else:
        result = {fname: wrap(fname, value)}

    # Replace NOT_FOUND → None in the fully wrapped structure
    return _replace_not_found(result)


def _build_diet_result_list(schema_entries, base_global, field_to_node, nodeid_to_output,
                             nodeid_to_search_context, root_schema, data, skip_node_ids,
                             rf_field_to_node=None, input1_nodes=None):  # ← add param
    def wrap(field_key, v):
        return _wrap_for(field_key, v, nodeid_to_output, field_to_node, nodeid_to_search_context)

    result_list = []
    for entry in schema_entries:
        fname = entry.get("name")
        if not fname:
            continue
        result = _build_diet_entry(
            entry, fname, base_global, field_to_node, nodeid_to_output,
            root_schema, wrap, rf_field_to_node, input1_nodes=input1_nodes # ← pass it
        )
        if result is not None:
            result_list.append(result)

    return result_list


# Robust build_output: flattens schema, merges when non-diet, and when is_diet=True
# attaches page/page_content into child objects for object types (one child per object).
def _node_matches_raw(n, raw_input1):
    """Return a matched node dict if any field in raw_input1 is found in this node's output, else None."""
    out = n.get('output') or {}
    if not isinstance(out, dict):
        return None
    for k, v in raw_input1.items():
        if k in out and out.get(k) == v:
            return {"node_id": n.get('node_id'), "output": out}
    return None


def _match_dict_against_nodes(raw_input1, data):
    """Try to match dict fields against nodes_executed outputs. Returns matched list or empty list."""
    nodes = data.get('nodes_executed', [])
    if not isinstance(nodes, list):
        return []
    matched = []
    for n in nodes:
        result = _node_matches_raw(n, raw_input1)
        if result is not None:
            matched.append(result)
    return matched


def _resolve_input1(data):
    """
    Prepare input1 from data for build_output:
    - If `nodes_executed` is present, prefer that list.
    - If top-level `output` is a dict, try to find which node(s) in `nodes_executed`
      contain matching key/value pairs and use those nodes (so we can pick their search_context).
    - Otherwise fall back to the original `output` value.
    """
    raw_input1 = data.get('nodes_executed') or data.get('output')
    if isinstance(raw_input1, list):
        return raw_input1
    if isinstance(raw_input1, dict):
        # Try to match dict fields against nodes_executed outputs
        matched = _match_dict_against_nodes(raw_input1, data)
        if matched:
            return matched
        # fallback to using the dict directly (old behaviour)
        return raw_input1
    return raw_input1


def build_output(input2, data, nodeid_to_schema, is_diet=False):
    # Convert list → map ONCE
    node_schema_map = _node_schema_list_to_map(nodeid_to_schema)
    input1 = _resolve_input1(data)
    input1_nodes = _normalize_input1(input1, node_schema_map)
    # Flatten schema entries (input2 could be nested lists)
    schema_entries = _build_effective_schema(input2, input1_nodes)
    root_schema = input2  # keep the full schema handy

    # Determine nodes to skip based on guardrail failures in `data.nodes_executed`.
    skip_node_ids = _build_skip_node_ids(data)

    combined_data_source = _build_combined_data_source(input1_nodes, skip_node_ids)

    if not is_diet:
        return _build_non_diet_output(
            schema_entries, combined_data_source, root_schema, data, input1_nodes, skip_node_ids
        )

    # ---------- diet mode ----------
    base_global = combined_data_source if combined_data_source else data

    # 2) Map fields to nodes (top-level keys is sufficient; context will be resolved per-leaf later)
    field_to_node, nodeid_to_output = _build_field_to_node_maps(input1_nodes, skip_node_ids)
    rf_field_to_node = _build_rf_field_to_node(input1_nodes)  # ← NEW

    # Build nodeid->search_context map (accepts alt names)
    nodeid_to_search_context = _build_nodeid_to_search_context(data, skip_node_ids)

    # ---- Diet mode ----
    result_list = _build_diet_result_list(
        schema_entries, base_global, field_to_node, nodeid_to_output,
        nodeid_to_search_context, root_schema, data, skip_node_ids,
        rf_field_to_node,
        input1_nodes=input1_nodes  # ← add this
    )

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

def _node_schema_list_to_map(nodeid_to_schema):
    """
    Convert:
      [{node_id, response_format}, ...]
    into:
      { "4": response_format, "5": response_format }
    """
    m = {}
    if not isinstance(nodeid_to_schema, list):
        return m

    for entry in nodeid_to_schema:
        if not isinstance(entry, dict):
            continue
        nid = entry.get("node_id")
        if nid is None:
            continue
        m[str(nid)] = entry.get("response_format") or []

    return m


def _build_effective_schema(input2, input1_nodes):
    """
    Effective schema = global schema + node-level response_format fields
    """
    # 1. Flatten global schema
    global_entries = []
    _flatten_schema(input2, global_entries)

    schema_map = OrderedDict()
    for s in global_entries:
        name = s.get("name")
        if name:
            schema_map[name] = s

    # 2. Extend using node-level response_format
    for node in input1_nodes:
        rf = node.get("response_format")
        if not rf:
            continue

        rf_entries = []
        _flatten_schema(rf, rf_entries)

        for s in rf_entries:
            name = s.get("name")
            if name and name not in schema_map:
                schema_map[name] = s

    return list(schema_map.values())


def _build_rf_field_to_node(input1_nodes):
    """
    Map: field_name -> node_id, derived from each node's response_format declarations.
    This ensures response_format-only fields know which node to pull data from,
    even if the field doesn't appear as a top-level key in any output.
    """
    rf_field_to_node = {}
    for node in input1_nodes:
        nid = node.get("node_id")
        nid_str = str(nid) if nid is not None else None
        rf = node.get("response_format")
        if not rf:
            continue
        rf_entries = []
        _flatten_schema(rf, rf_entries)
        for s in rf_entries:
            name = s.get("name")
            if name and name not in rf_field_to_node:
                rf_field_to_node[name] = nid_str
    return rf_field_to_node
    
def _replace_not_found(val):
    """
    Recursively replace NOT_FOUND sentinel with None in the final output.
    Called just before writing a value into the result dict.
    """
    if val is NOT_FOUND:
        return None
    if isinstance(val, list):
        return [_replace_not_found(i) for i in val]
    if isinstance(val, (dict, OrderedDict)):
        return type(val)((k, _replace_not_found(v)) for k, v in val.items())
    return val

def _build_empty_child_dict(children):
    """
    When a parent attribute is absent from output, all its children
    should be NOT_FOUND — do NOT search global data source (prevents
    same-named sub-attribute collision across different parent attributes).
    """
    result = OrderedDict()
    for child in children:
        dtype = child.get("datatype", child.get("dataType", "")).lower()
        is_multiple = child.get("isMultiple", False)
        grand_children = child.get("child", []) or child.get("children", [])
        
        if dtype == "object":
            # Recursively build empty structure for nested objects too
            if is_multiple:
                result[child["name"]] = [_build_empty_child_dict(grand_children)]
            else:
                result[child["name"]] = _build_empty_child_dict(grand_children)
        else:
            # Primitive — same logic as _handle_primitive_type with raw=None
            result[child["name"]] = NOT_FOUND  # → None after _replace_not_found
    return result
