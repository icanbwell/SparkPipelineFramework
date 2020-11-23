from typing import Dict

from library.translators.my_translator.translators_my_translator import TranslatorsMyTranslator


def test_translator() -> None:
    my_dict: Dict[str, str] = TranslatorsMyTranslator().get_mapping()
    print(my_dict)

    assert my_dict == {"foo1": "bar1", "foo2": "bar2"}
