from typing import Dict, Any, Callable

import fastjsonschema


class SchemaValidator:
    def __init__(self, schema: Dict[str, Any]) -> None:
        self.schema_validator: Callable[[Any], None] = fastjsonschema.compile(schema)

    def validate(self, content: Any) -> bool:
        """
        Checks the content against schema and throws error if validation fails
        :param content:
        """
        self.schema_validator(content)
        return True
