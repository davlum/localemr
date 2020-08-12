from localemr.exec.livy.models import LivyRequestBody


def test_from_snake_to_camel_case():
    assert LivyRequestBody.from_snake_to_camel_case('executor_memory') == 'executorMemory'
    assert LivyRequestBody.from_snake_to_camel_case('a_beautiful_day') == 'aBeautifulDay'
