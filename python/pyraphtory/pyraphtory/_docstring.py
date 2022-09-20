from parsy import string, whitespace, any_char, seq, peek, alt, eof, string_from



def parse_name(name):
    from pyraphtory.interop import camel_to_snake
    from pyraphtory._codegen import clean_identifier
    converted = camel_to_snake(name)
    clean = clean_identifier(converted)
    return clean


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

start = (whitespace.optional("") >> string("/**") >> string(" ").optional("")).map(ignore)
linestart = (string("\n") << whitespace.optional("") << string("*") << string(" ").optional(""))
end = (string("*").many() >> string("/")).map(ignore)
token = any_char.until(whitespace | eof, consume_other=False, min=1).map(join_tokens)
name = token.map(parse_name)
param_id = string("@param").map(convert_param)
param = (param_id + whitespace + name).map(finalise_param)
return_id = string("@return").map(convert_returns)
boolean = string_from('`true`', '`false`').map(capitalise)

doc_converter = alt(start, end, linestart, whitespace,
                    boolean,
                    param, return_id,
                    token).many().map(join_tokens)


def convert_docstring(docs):
    cleaned = doc_converter.parse(docs)
    return cleaned.rstrip("\n")
