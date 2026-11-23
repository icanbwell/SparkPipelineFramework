import json
from typing import Any, Dict, List


def parse_json_texts(texts: List[str]) -> List[Any]:
    """Parses JSON texts so tests compare JSON values, not the exact serialized text."""
    return [json.loads(text) for text in texts]


def parse_result_resources(result: Dict[str, Any]) -> Dict[str, Any]:
    """Returns a copy of a batch result whose resource texts are parsed JSON."""
    return {**result, "resources": parse_json_texts(result["resources"])}
