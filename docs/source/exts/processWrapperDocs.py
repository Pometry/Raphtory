from sphinx.application import Sphinx
from pyraphtory.interop import ScalaClassProxy, InstanceOnlyMethod, ScalaObjectProxy, WithImplicits, OverloadedMethod
import pyraphtory
from sphinx.util import logging
from sphinx.locale import _, __
from sphinx.ext.autodoc import MethodDocumenter, ClassDocumenter, safe_getattr, ObjectMembers, get_class_members, ModuleDocumenter, AttributeDocumenter
from typing import *

logger = logging.getLogger(__name__)


def setup(app: Sphinx):
    app.add_autodocumenter(InstanceOnlyMethodDocumenter)
    app.add_autodocumenter(MetaclassMethodDocumenter)
    app.add_autodocumenter(ScalaClassProxyDocumenter)
    app.add_autodocumenter(ImplicitMethodDocumenter)
    app.add_autodocumenter(OverloadedMethodDocumenter)
    app.connect('autodoc-process-signature', process_signature)
    app.connect('autodoc-before-process-signature', before_process_signature)


def fix_signature_link(signature: str):
    parts = signature.split(": ")
    parts[1:] = [fix_part(p) for p in parts[1:]]
    return ": ".join(parts)

def fix_part(part: str):
    if part.startswith("~"):
        return part
    if part.startswith("pyraphtory"):
        return "~" + part
    return part


def before_process_signature(app, obj, bound_method):
    if hasattr(obj, "__globals__"):
        obj.__globals__.update(vars(pyraphtory))  # inject definitions for resolving annotations
    print(obj)

def process_signature(app, what, name, obj, options, signature, return_annotation):
    fixed_signature = fix_signature_link(signature) if signature is not None else None
    if fixed_signature != signature:
        print(signature)
    return signature, return_annotation


def unpack_class_method(obj, name=None):
    if hasattr(obj, "__self__"):
        if isinstance(obj.__self__, type):
            if name is None:
                name = obj.__name__
            for c in obj.__self__.mro():
                if name in c.__dict__:
                    obj = c.__dict__[name]
                    break
    return obj

class ImplicitsSignatureMixin:
    # def format_signature(self, **kwargs):
    #     sig = super().format_signature(**kwargs)  # type: ignore
    #     if hasattr(self.object, "__self__"):
    #         cls = self.object.__self__
    #         method_class = cls.__class__.__dict__.get(self.object_name)
    #     else:
    #         method_class = self.object
    #     if isinstance(method_class, InstanceOnlyMethod):
    #         method_class = method_class.__func__
    #     if isinstance(method_class, WithImplicits):
    #         sig = "[*implicits]" + sig
    #     return sig
    pass


class MyMethodDocumenter(MethodDocumenter):
    pass


class ImplicitMethodDocumenter(ImplicitsSignatureMixin, MyMethodDocumenter):
    objtype = "implicitmethod"
    priority = AttributeDocumenter.priority + 1

    @classmethod
    def can_document_member(cls, member: Any, membername: str, isattr: bool, parent: Any
                            ) -> bool:
        value = isinstance(member, WithImplicits)
        return value


class OverloadedMethodDocumenter(ImplicitsSignatureMixin, MyMethodDocumenter):
    objtype = "overloadedmethod"
    priority = AttributeDocumenter.priority + 1

    @classmethod
    def can_document_member(cls, member: Any, membername: str, isattr: bool, parent: Any
                            ) -> bool:
        value = isinstance(member, OverloadedMethod)
        return value


class InstanceOnlyMethodDocumenter(ImplicitsSignatureMixin, MyMethodDocumenter):  # type: ignore
    """
    Specialized Documenter subclass for instance-only.
    """
    objtype = "instancemethod"
    priority = AttributeDocumenter.priority + 1
    @classmethod
    def can_document_member(cls, member: Any, membername: str, isattr: bool, parent: Any
                            ) -> bool:
        actual_method = unpack_class_method(member, membername)
        return isinstance(actual_method, InstanceOnlyMethod)


    def import_object(self, raiseerror: bool = False) -> bool:
        ret = super().import_object(raiseerror)
        if not ret:
            return ret
        # recover from InstanceOnly methods being bound as class methods if the method of the same name exists
        # as a class method
        actual_method = unpack_class_method(self.object, self.object_name)
        if isinstance(actual_method, InstanceOnlyMethod):
            self.object = actual_method.__func__
        return ret


class MetaclassMethodDocumenter(ImplicitsSignatureMixin, MyMethodDocumenter):
    objtype = "metaclassmethod"
    member_order = MethodDocumenter.member_order - 1
    priority = AttributeDocumenter.priority + 1  # make sure these are not parsed as attributes!

    def __init__(self, directive: "DocumenterBridge", name: str, indent: str = '') -> None:
        super().__init__(directive, name, indent)

    @classmethod
    def can_document_member(cls, member: Any, membername: str, isattr: bool, parent: Any
                            ) -> bool:
        value = isinstance(member, ClassMethodWrapper)
        return value

    def import_object(self, raiseerror: bool = False) -> bool:
        ret = super().import_object(raiseerror)
        return ret

    def add_directive_header(self, sig: str) -> None:
        super(MethodDocumenter, self).add_directive_header(sig)
        self.add_line('   :classmethod:', self.get_sourcename())
        self.add_line('   :noindex:', self.get_sourcename())



class ClassMethodWrapper:
    """Wrap classmethod part of masked instance-only method"""
    def __init__(self, method):
        self.method = method


class ScalaClassProxyDocumenter(ClassDocumenter):
    objtype = "proxy"
    directivetype = "class"
    priority = ClassDocumenter.priority + 1

    @classmethod
    def can_document_member(cls, member: Any, membername: str, isattr: bool, parent: Any
                            ) -> bool:
        return isinstance(member, ScalaObjectProxy)

    def get_object_members(self, want_all: bool) -> Tuple[bool, ObjectMembers]:
        flag, members = super().get_object_members(want_all)
        if isinstance(self.object, ScalaObjectProxy):
            class_members = [(name, ClassMethodWrapper(getattr(self.object, name)))
                             for name, cmember in self.object.__class__.__dict__.items()
                             if isinstance(cmember, InstanceOnlyMethod)]
            members = class_members + members
        return flag, members

    def filter_members(self, members: ObjectMembers, want_all: bool
                       ) -> List[Tuple[str, Any, bool]]:
        class_methods = []
        other_members = []
        for name, member in members:
            if isinstance(member, ClassMethodWrapper):
                class_methods.append((name, member, False))
            else:
                other_members.append((name, member))
        other_members = super().filter_members(other_members, want_all)
        return class_methods + other_members

