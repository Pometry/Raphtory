from parsy import string, whitespace, any_char, seq, peek, alt, eof, string_from, Parser, regex, letter, decimal_digit, \
    generate

code_conversions = {}


def camel_to_snake(name):
    from pyraphtory.interop import camel_to_snake as convert
    from pyraphtory._codegen import clean_identifier
    return clean_identifier(convert(name))


def parse_name(name):
    return camel_to_snake(name)


def finalise_param(s):
    return s + ":"


def convert_param(s):
    return ":param"


def convert_returns(s):
    return ":returns:"


def join_tokens(tokens):
    return "".join(tokens)


def ignore(s):
    return ""


def capitalise(s: str):
    return s.capitalize()


def report_unparsed(s: str):
    print(f"unparsed: {s}")
    return s


def with_optional_whitespace(parser: Parser):
    return whitespace.optional("") + parser + whitespace.optional("")


def linestart_clean(s: str):
    return "\n"


def as_code(s: str):
    return "`" + s + "`"


non_newline_whitespace = regex(r"[^\S\r\n]+")
space = string(" ")
counted_spaces = space.many().map(len)
newline = string("\n") | string("\r\n") | string("\r")

# scaladoc comment noise
start = (whitespace.optional("") + string("/**") + space.optional("")).map(ignore)
linestart = (non_newline_whitespace.optional("")
             + newline + non_newline_whitespace.optional("")
             + string("*")
             + space.optional("")).map(linestart_clean)
end = (whitespace.optional("") + string("*").at_least(1).concat() + string("/")).map(ignore)

# convert tags
token = any_char.until(whitespace | eof, min=1).map(join_tokens)  # consume non-whitespace characters
name = token.map(parse_name)
param_id = string("@param").map(convert_param)
param = (param_id + whitespace + name).map(finalise_param)
return_id = string("@return").map(convert_returns)

# convert code expressions
code_others = string("`").should_fail(
    "not end of code") >> any_char  # match anything that is not end of code and return unchanged
boolean = string_from('true', 'false').map(capitalise)
string_expr = string("String").result("str")
int_expr = string_from("Int", "Long", "Integer", "Short").result("int")
float_expr = string_from("Float", "Double").result("float")
method_or_variable_name = (string("_").many().concat()
                           + regex(r"[a-z]")
                           + (letter | decimal_digit).many().concat()
                           + string("_").many().concat()
                           ).map(camel_to_snake)
class_name = (string("_").many().concat()
              + regex(r"[A-Z]")
              + (letter | decimal_digit).many().concat()
              + string("_").many().concat()
              )
code_expr = (boolean
             | string_expr
             | int_expr
             | float_expr
             | method_or_variable_name
             | class_name
             | code_others
             ).many().map(join_tokens)
code = string("`") + code_expr + string("`")
code_unparsed = (string("`") + any_char.until(string("`"), consume_other=True).concat()).map(report_unparsed)

# find links (only extracts text for now)
link_string = string("[[") >> any_char.until(string("]]")).concat() << string("]]")

link_value = ((token >> whitespace >> (class_name | method_or_variable_name).map(as_code)) |
              (class_name | method_or_variable_name).map(as_code))


@generate("link")
def link():
    link_str = yield link_string
    return link_value.parse(link_str)


# inline markup
inline = link | code | code_unparsed | non_newline_whitespace | token

# lines
line = inline.until((end | eof | newline)).map(join_tokens)
blank_line = non_newline_whitespace.until(newline).map(join_tokens)


def directive_line(indent, output_indent):
    """Identify an indented line of a block and return it with converted indent
    :param indent: indent in scaladoc block
    :param output_indent: indent in python docstring block
    """
    return linestart + (blank_line | ((space * indent).result(" " * output_indent) + line))


def directive(name, pythonname=None):
    if pythonname is None:
        pythonname = name

    @generate(name)
    def directive_parser():
        result = []
        leading_spaces = yield (start | linestart) >> counted_spaces
        directive_start = yield string("@" + name).result(" " * leading_spaces + f".. {pythonname}::") + line
        result.append(directive_start)  # this picks up any remaining text in the first line
        blanks = yield (linestart + blank_line).many().concat()  # blank lines don't matter in terms of indentation
        result.append(blanks)
        first_indent = yield peek(linestart >> counted_spaces)
        if first_indent > leading_spaces:
            output = yield directive_line(first_indent, leading_spaces + 3).many().concat()
            result.append(output)
        return join_tokens(result)

    return directive_parser


doc_converter = alt(directive("note"), directive("see", "seealso"),
                    # multiline blocks first as they handle new lines specially
                    start, end, linestart,  # strip out scaladoc newline formatting
                    param, return_id,  # handle simple tags
                    inline  # handle inline markup
                    ).many().map(join_tokens)


def convert_docstring(docs):
    if docs:
        cleaned = doc_converter.parse(docs)
        return cleaned.rstrip("\n")
    else:
        return docs
