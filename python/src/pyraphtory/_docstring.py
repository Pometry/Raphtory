from parsy import string, whitespace, any_char, seq, peek, alt, eof, string_from, Parser, regex, letter, decimal_digit, \
    generate, ParseError

code_conversions = {}


def camel_to_snake(name: str):
    from pyraphtory.interop import camel_to_snake as convert
    from pyraphtory._codegen import clean_identifier
    if name[0].islower():
        name = convert(name)
    return clean_identifier(name)


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


def debug(name):
    """useful for printing debug info during parsing"""

    def debug_wrapped(s):
        print(f"{name} parsed {s}")
        return s
    return debug_wrapped


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


# whitespace parsers
non_newline_whitespace = regex(r"[^\S\r\n]+")
non_whitespace_char = regex(r"\S")
space = string(" ")
counted_spaces = space.many().map(len)
newline = (string("\n") | string("\r\n") | string("\r")).result("\n")

# scaladoc line start and end comments
start = (non_newline_whitespace.optional("") + string("/**") + space.optional("")).result("")
line_start = start | (non_newline_whitespace.optional("") + string("*") + space.optional("")).result("")
end = (whitespace.optional("") + string("*").at_least(1).concat() + string("/")).result("")
line_end = newline | end

# blank lines
blank_remaining_line = non_newline_whitespace.until(line_end | eof).concat() + line_end.optional("")
blank_line = line_start.optional("") + blank_remaining_line

# separate by spaces
token = non_whitespace_char.until(whitespace | line_end | eof, min=1).concat()  # consume non-whitespace characters

# token that should be converted to snake_case
name = token.map(parse_name)

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
    try:
        return link_value.parse(link_str)
    except ParseError:
        return link_str


# inline markup
inline = link | code | code_unparsed | non_newline_whitespace | token

# lines
remaining_line = inline.until(line_end | eof).concat() + line_end.optional("")
line = line_start.optional("") + remaining_line

# convert tags
def labeled_tag(tag_name, converted_name=None):
    if converted_name is None:
        converted_name = tag_name
    id = string(f"@{tag_name}").result(f":{converted_name}")
    return (id + non_newline_whitespace + name).map(finalise_param)


def field_list_item(tag_name, converted_name=None):
    id_and_label = labeled_tag(tag_name, converted_name)

    @generate(f"{tag_name}_item")
    def list_item():
        leading_spaces = yield line_start >> counted_spaces
        first_line = yield id_and_label + remaining_line
        body = yield indented_block(leading_spaces)
        return " " * leading_spaces + first_line + body

    return list_item

param = field_list_item("param")
throws = field_list_item("throws", "raises")
tparam = field_list_item("tparam").result("")

return_id = string("@return").result(":returns:")

@generate("return")
def return_():
    leading_spaces = yield line_start >> counted_spaces
    first_line = yield return_id + remaining_line
    body = yield indented_block(leading_spaces)
    return " " * leading_spaces + first_line + body


field_list_item = param | tparam | throws | return_

# rst field lists need blank lines before and after
field_list = blank_line.optional("\n") + field_list_item.at_least(1).concat() + blank_line.optional("\n")


def indented_line(indent, output_indent):
    """Identify an indented line of a block and return it with converted indent

    :param indent: indent in scaladoc block
    :param output_indent: indent in python docstring block
    """
    return blank_line | (line_start + ((space * indent).result(" " * output_indent) + remaining_line))


def indented_block(leading_spaces, output_indent=None):
    """Parse an indented block of text

    :param leading_spaces: outer indent of the block
    :param output_indent: indent of output (defaults to the indent in the original)
    """
    @generate("indented_block")
    def indented_block_parser():
        result = []
        blanks = yield blank_line.many().concat()
        result.append(blanks)
        first_indent = yield peek(line_start >> counted_spaces).optional(0)
        nonlocal output_indent
        if output_indent is None:
            output_indent = first_indent
        if first_indent > leading_spaces:
            output = yield indented_line(first_indent, leading_spaces + output_indent).many().concat()
            result.append(output)
        return join_tokens(result)
    return indented_block_parser


def directive(name, pythonname=None):
    """Parse a directive-style tag

    :param name: name of scala tag is `@name`
    :param pythonname: name in the output (defaults to name)
    """
    if pythonname is None:
        pythonname = name

    @generate(name)
    def directive_parser():
        result = []
        leading_spaces = yield line_start >> counted_spaces
        directive_start = yield string("@" + name).result(" " * leading_spaces + f".. {pythonname}::") + remaining_line
        result.append(directive_start)  # this picks up any remaining text in the first line
        block = yield indented_block(leading_spaces, 3)
        result.append(block)
        return join_tokens(result)

    return directive_parser


doc_converter = alt(tparam, directive("note"), directive("see", "seealso"),
                    field_list,
                    end,
                    line,
                    ).until(eof).map(join_tokens)


def convert_docstring(docs):
    docs = str(docs)
    if docs:
        try:
            cleaned = doc_converter.parse(docs)
            return cleaned.rstrip("\n")
        except ParseError as e:
            print(e)
            return docs
    else:
        return docs
