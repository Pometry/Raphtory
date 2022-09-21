import pytest

from pyraphtory._docstring import convert_docstring, string_expr, code
from pyraphtory.context import PyRaphtory


@pytest.fixture(scope="module", autouse=True)
def pyraphtory():
    pr = PyRaphtory().open()
    yield pr
    pr.shutdown()



def test_class_name_conversion():
    assert convert_docstring("`ClassName`") == "`ClassName`"


def test_variable_name_conversion():
    assert convert_docstring("`variableNameConvention1`") == "`variable_name_convention1`"


def test_combined_expression():
    assert convert_docstring("`ClassName.variableName(otherName=true) = 1`") == "`ClassName.variable_name(other_name=True) = 1`"


def test_string_conversion():
    assert convert_docstring("`String`") == "`str`"


def test_scaladoc_comment_removal():
    assert convert_docstring("/** * \n * s\n */") == "*\ns"


def test_direct_conversion():
    assert string_expr.parse("String") == "str"
    assert code.parse("`String`") == "`str`"


def test_link_conversion():
    assert convert_docstring("[[Vertex]]") == "`Vertex`"


def test_link_conversion_with_target():
    assert convert_docstring("[[visitor.Vertex Vertex]]") == "`Vertex`"
