from typing import Any, Callable, Dict, Tuple, List, Union, TypeVar

T = TypeVar('T')

def _recursive_update_inplace(
    obj: Any,
    transform: Callable[[Any], Any],
    *,
    handle_dict: bool = True,
    handle_list: bool = False,
    handle_str: bool = False,
    handle_tuple: bool = False
) -> Any:
    """
    Internal helper to recursively update `obj` in place (for mutable containers)
    or return transformed objects (for immutable ones).
    """
    # Dictionaries
    if handle_dict and isinstance(obj, dict):
        for k, v in obj.items():
            obj[k] = _recursive_update_inplace(v, transform,
                                                handle_dict=handle_dict,
                                                handle_list=handle_list,
                                                handle_str=handle_str,
                                                handle_tuple=handle_tuple)
        return obj

    # Lists
    if handle_list and isinstance(obj, list):
        for i, v in enumerate(obj):
            obj[i] = _recursive_update_inplace(v, transform,
                                               handle_dict=handle_dict,
                                               handle_list=handle_list,
                                               handle_str=handle_str,
                                               handle_tuple=handle_tuple)
        return obj

    # Tuples (immutable: leave unchanged or rebuild if desired)
    if handle_tuple and isinstance(obj, tuple):
        # leave tuple as-is
        return obj

    # Strings
    if handle_str and isinstance(obj, str):
        return transform(obj)

    # Leaf or other type
    if not any([
        handle_dict and isinstance(obj, dict),
        handle_list and isinstance(obj, list),
        handle_tuple and isinstance(obj, tuple),
        handle_str and isinstance(obj, str)
    ]):
        return transform(obj)

    return obj


def _recursive_update(
    obj: Any,
    transform: Callable[[Any], Any],
    *,
    handle_dict: bool = True,
    handle_list: bool = False,
    handle_str: bool = False,
    handle_tuple: bool = False
) -> Any:
    """
    Internal helper to recursively produce a new object with transforms applied.
    Original `obj` is not mutated.
    """
    # Dictionaries
    if handle_dict and isinstance(obj, dict):
        return {
            k: _recursive_update(v, transform,
                                  handle_dict=handle_dict,
                                  handle_list=handle_list,
                                  handle_str=handle_str,
                                  handle_tuple=handle_tuple)
            for k, v in obj.items()
        }

    # Lists
    if handle_list and isinstance(obj, list):
        return [
            _recursive_update(v, transform,
                              handle_dict=handle_dict,
                              handle_list=handle_list,
                              handle_str=handle_str,
                              handle_tuple=handle_tuple)
            for v in obj
        ]

    # Tuples
    if handle_tuple and isinstance(obj, tuple):
        return obj  # leave unchanged

    # Strings
    if handle_str and isinstance(obj, str):
        return transform(obj)

    # Leaf or other type
    if not any([
        handle_dict and isinstance(obj, dict),
        handle_list and isinstance(obj, list),
        handle_tuple and isinstance(obj, tuple),
        handle_str and isinstance(obj, str)
    ]):
        return transform(obj)

    return obj


# Q1: In-place update of leaves in a dict d,
# applying e.g. lambda x: x*2 to every leaf value.
def update_leaves_inplace(d: Dict[str, Any]) -> None:
    """
    Modify `d` in-place so that every leaf value v is replaced with v*2.
    Assumes leaf values support the `*2` operation (e.g. strings, numbers).
    """
    _recursive_update_inplace(d, lambda v: v * 2,
                              handle_dict=True,
                              handle_list=False,
                              handle_str=True,
                              handle_tuple=False)


# Q2.2: Non-mutating version
def update_leaves(d: Dict[str, Any]) -> Dict[str, Any]:
    """
    Return a new dictionary where every leaf value v in `d` is replaced with v*2.
    Does not modify the original `d`.
    """
    return _recursive_update(d, lambda v: v * 2,
                             handle_dict=True,
                             handle_list=False,
                             handle_str=True,
                             handle_tuple=False)


# Q3.1: In-place generalization for dicts that may contain lists/tuples/strings
def update_all_inplace(d: Dict[Any, Any]) -> None:
    """
    Modify `d` in-place:
      - dicts are recursed into
      - lists are doubled in-place (i.e. list += list)
      - strings are doubled ('a' -> 'aa')
      - tuples are left unchanged
      - other leaf values are doubled via v*2
    """
    def transform(v: Any) -> Any:
        # For list doubling we handle in the recursion
        if not isinstance(v, (dict, list, tuple, str)):
            return v * 2
        return v

    _recursive_update_inplace(d, transform,
                              handle_dict=True,
                              handle_list=True,
                              handle_str=True,
                              handle_tuple=True)


# Q3.2: Non-mutating version
def update_all(d: Dict[Any, Any]) -> Dict[Any, Any]:
    """
    Return a new dictionary (no mutation of `d`) with the same scheme as `update_all_inplace`.
    """
    def transform(v: Any) -> Any:
        if not isinstance(v, (dict, list, tuple, str)):
            return v * 2
        return v

    return _recursive_update(d, transform,
                             handle_dict=True,
                             handle_list=True,
                             handle_str=True,
                             handle_tuple=True)


# Example usage:
if __name__ == "__main__":
    d = {
        'key11': {'key21': 'a', 'key22': 'b'},
        'key12': 'c',
        'key13': {
            'key31': 'd',
            'key32': {'key21': 'e', 'key22': 'f'},
        },
    }

    update_leaves_inplace(d)
    print("In-place doubled:", d)
    # → {'key11': {'key21': 'aa', 'key22': 'bb'}, ...}

    # Non-mutating:
    orig = {
        'key11': {'key21': 'a', 'key22': 'b'},
        'key12': 'c',
    }
    new = update_leaves(orig)
    print("Original still:", orig)
    print("New doubled:", new)

    # Q3 with lists and tuples
    d2 = {'k1': [1, 2, 3], 'k2': (4, 5), 'k3': 'x', 'k4': {'k5': 7}}
    update_all_inplace(d2)
    print("Q3 in-place:", d2)
    # → [1,2,3,1,2,3], tuple stays (4,5), 'x'→'xx', 7→14

    d2_orig = {'k1': [1, 2, 3], 'k2': (4, 5), 'k3': 'x', 'k4': {'k5': 7}}
    d2_new = update_all(d2_orig)
    print("Q3 non-mut:", d2_new)
    print("Original Q3 still:", d2_orig)
