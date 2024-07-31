from typing import Dict

from spark_auto_mapper.type_definitions.defined_types import (
    AutoMapperTextInputType,
    AutoMapperAnyDataType,
)

from library.translators.my_translator.translators_my_translator import (
    TranslatorsMyTranslator,
)


def test_translator() -> None:
    my_dict: Dict[AutoMapperTextInputType, AutoMapperAnyDataType] = (
        TranslatorsMyTranslator().get_mapping()
    )
    print(my_dict)

    assert my_dict == {"foo1": "bar1", "foo2": "bar2"}
