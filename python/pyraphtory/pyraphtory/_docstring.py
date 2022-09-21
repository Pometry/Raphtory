from parsy import string, whitespace, any_char, seq, peek, alt, eof, string_from, Parser, regex, letter, decimal_digit, generate

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


non_newline_whitespace = regex(r"[^\S\r\n]+")


# scaladoc comment noise
start = (whitespace.optional("") + string("/**") + string(" ").optional("")).map(ignore)
linestart = (non_newline_whitespace.optional("") + string("\n") + whitespace.optional("") + string("*").times(1, 1).concat() + string(" ").optional("")).map(linestart_clean)
end = (whitespace.optional("") + string("*").at_least(1).concat() + string("/")).map(ignore)

# convert tags
token = any_char.until(whitespace | eof, consume_other=False, min=1).map(join_tokens)
name = token.map(parse_name)
param_id = string("@param").map(convert_param)
param = (param_id + whitespace + name).map(finalise_param)
return_id = string("@return").map(convert_returns)

# convert code expressions
code_others = string("`").should_fail("not end of code") >> any_char  # match anything that is not end of code and return unchanged
boolean = string_from('true', 'false').map(capitalise)
string_expr = string("String").result("str")
int_expr = string_from("Int", "Long", "Integer", "Short").result("int")
method_or_variable_name = (string("_").many().concat() + regex(r"[a-z]") + (letter | decimal_digit).many().concat() + string("_").many().concat()).map(camel_to_snake)
class_name = (string("_").many().concat() + regex(r"[A-Z]") + (letter | decimal_digit).many().concat() + string("_").many().concat())
code_expr = (boolean | string_expr | int_expr | method_or_variable_name | class_name | code_others).many().map(join_tokens)
code = string("`") + code_expr + string("`")
code_unparsed = (string("`") + any_char.until(string("`"), consume_other=True).concat()).map(report_unparsed)

# find links (only extracts text for now)
link = string("[[") >>((token >> whitespace >> (class_name | method_or_variable_name)) | (class_name | method_or_variable_name)) << string("]]")


doc_converter = alt(start, end, linestart, whitespace, link,
                    code, code_unparsed,
                    param, return_id,
                    token).many().map(join_tokens)


def convert_docstring(docs):
    if docs:
        cleaned = doc_converter.parse(docs)
        return cleaned.rstrip("\n")
    else:
        return docs
