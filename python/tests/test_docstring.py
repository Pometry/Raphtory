from pyraphtory.interop._docstring import convert_docstring, string_expr, code


def test_class_name_conversion():
    assert convert_docstring("`ClassName`") == "`ClassName`"


def test_variable_name_conversion():
    assert convert_docstring("`variableNameConvention1`") == "`variable_name_convention1`"


def test_combined_expression():
    assert convert_docstring(
        "`ClassName.variableName(otherName=true) = 1`") == "`ClassName.variable_name(other_name=True) = 1`"


def test_string_conversion():
    assert convert_docstring("`String`") == "`str`"


def test_scaladoc_comment_removal():
    assert convert_docstring("/** * \n * s\n */") == "* \ns"


def test_direct_conversion():
    assert string_expr.parse("String") == "str"
    assert code.parse("`String`") == "`str`"


def test_link_conversion():
    assert convert_docstring("[[Vertex]]") == "`Vertex`"


def test_link_conversion_with_target():
    assert convert_docstring("[[visitor.Vertex Vertex]]") == "`Vertex`"


def test_weird_link():
    assert convert_docstring(
        "see\n*          [[PropertyMergeStrategy]] for predefined options") == "see\n         `PropertyMergeStrategy` for predefined options"


def test_note_parser():
    assert (convert_docstring(
        "/** @note First line\n *     Second line\n *       Indented third line\n * \n * \n *     Normal fourth line\n * Other text */")
            == ".. note::\n   First line\n   Second line\n     Indented third line\n\n\n   Normal fourth line\n\nOther text")


def test_param_list_without_space():
    assert (convert_docstring(
        "/** Some text\n * @param name this is \n *    some long text \n * @param other this is some short text \n * some other non-indented text needs to be separated */")
            == "Some text\n\n:param name: this is \n   some long text \n:param other: this is some short text \n\nsome other non-indented text needs to be separated"
            )


def test_param_list_with_space():
    assert (convert_docstring(
        "/** Some text\n\n\n * @param name this is \n *    some long text \n * @param other this is some short text \n * some other non-indented text needs to be separated */")
            == "Some text\n\n\n:param name: this is \n   some long text \n:param other: this is some short text \n\nsome other non-indented text needs to be separated"
            )

def test_weird_param_list():
    assert (convert_docstring(
"""/**  Execute only the apply step of the algorithm on every perspective and returns a new RaphtoryGraph with the result.
*  @param algorithm algorithm to apply
*/"""
    ) ==
""" Execute only the apply step of the algorithm on every perspective and returns a new RaphtoryGraph with the result.

 :param algorithm: algorithm to apply""")


def test_param_list_from_accumulator():
    assert (convert_docstring(
        """/** Add new value to accumulator and return the accumulator object
*
* @param newValue Value to add
*/"""
    ) ==
            """Add new value to accumulator and return the accumulator object

:param new_value: Value to add""")


def test_codeblock():
    assert (convert_docstring(
"""/** {{{
* {
*   "jobID" : "EdgeCount",
*   "partitionID" : 0,
*   "perspectives" : [ {
*     "timestamp" : 10
*     "rows" : [ [ "id1", 12 ], [ "id2", 13 ], [ "id3", 24 ] ]
*   } ]
* }
* }}}"""
    ) ==
""".. code-block::
   :dedent:

   {
     "jobID" : "EdgeCount",
     "partitionID" : 0,
     "perspectives" : [ {
       "timestamp" : 10
       "rows" : [ [ "id1", 12 ], [ "id2", 13 ], [ "id3", 24 ] ]
     } ]
   }""")
