from sphinx.application import Sphinx
from pyraphtory.interop import ScalaClassProxy, InstanceOnlyMethod, ScalaObjectProxy, WithImplicits, OverloadedMethod
import pyraphtory
from sphinx.util import logging, inspect
from sphinx.ext.autodoc import MethodDocumenter, ClassDocumenter, safe_getattr, ObjectMembers, get_class_members, \
    ModuleDocumenter, AttributeDocumenter, ALL, Documenter
from typing import *

from sphinx.util.docstrings import prepare_docstring

logger = logging.getLogger(__name__)


def setup(app: Sphinx):
    app.add_autodocumenter(InstanceOnlyMethodDocumenter)
    app.add_autodocumenter(MetaclassMethodDocumenter)
    app.add_autodocumenter(ScalaClassProxyDocumenter)
    app.add_autodocumenter(ImplicitMethodDocumenter)
    app.add_autodocumenter(OverloadedMethodDocumenter)
    app.add_autodocumenter(OverloadedClassMethodDocumenter)
    app.add_autodocumenter(OverloadedMethodInstanceDocumenter)
    app.add_autodocumenter(OverloadedClassMethodInstanceDocumenter)
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


def unpack_instance_only_method(obj, name=None):
    obj = unpack_class_method(obj, name)
    if isinstance(obj, InstanceOnlyMethod):
        obj = obj.__func__
    return obj


def unpack_implicits_method(obj):
    if isinstance(obj, WithImplicits):
        obj = obj._method
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


class UnpackImplicitsMixin:
    def import_object(self, raiseerror: bool = False) -> bool:
        ret = super().import_object(raiseerror)
        if not ret:
            return ret
        self.object = unpack_implicits_method(self.object)
        return ret


class UnpackInstanceOnlyMixin:
    def import_object(self, raiseerror: bool = False) -> bool:
        ret = super().import_object(raiseerror)
        if not ret:
            return ret
        self.object = unpack_instance_only_method(self.object, self.object_name)
        return ret


class ClassMethodMixin:
    member_order = MethodDocumenter.member_order - 1

    def add_directive_header(self, sig: str) -> None:
        obj = super()
        super(MethodDocumenter, self).add_directive_header(sig)
        self.add_line('   :classmethod:', self.get_sourcename())


class NoIndexMixin:
    def add_directive_header(self, sig: str) -> None:
        super().add_directive_header(sig)
        self.add_line('   :noindex:', self.get_sourcename())


class ImplicitMethodDocumenter(ImplicitsSignatureMixin, MethodDocumenter):
    objtype = "implicitmethod"
    priority = AttributeDocumenter.priority + 1

    @classmethod
    def can_document_member(cls, member: Any, membername: str, isattr: bool, parent: Any
                            ) -> bool:
        value = isinstance(member, WithImplicits)
        return value


class OverloadedMethodInstanceWrapper:
    def __init__(self, method):
        self.method = method


class OverloadedMethodInstanceDocumenterMixin(NoIndexMixin, UnpackImplicitsMixin):
    def import_object(self, raiseerror: bool = False) -> bool:
        overload = self.objpath[-1]
        self.objpath = self.objpath[:-1]
        ret = super().import_object(raiseerror)
        if not ret:
            return ret
        index = int(overload.rsplit("_", 1)[-1])
        self.object = self.object._methods[index]
        return ret

    def get_doc(self) -> Optional[List[List[str]]]:
        docstring = self.object.__doc__
        if docstring:
            tab_width = self.directive.state.document.settings.tab_width
            return [prepare_docstring(docstring, tab_width)]
        return []


class OverloadedMethodInstanceDocumenter(OverloadedMethodInstanceDocumenterMixin, UnpackInstanceOnlyMixin, MethodDocumenter):
    objtype = "overloadedmethodinstance"
    priority = AttributeDocumenter.priority + 1

    @classmethod
    def can_document_member(cls, member: Any, membername: str, isattr: bool, parent: Any
                            ) -> bool:
        value = False
        if isinstance(member, OverloadedMethodInstanceWrapper):
            value = not inspect.isclassmethod(member.method)
        return value


class OverloadedClassMethodInstanceDocumenter(OverloadedMethodInstanceDocumenterMixin, ClassMethodMixin, MethodDocumenter):
    objtype = "overloadedclassmethodinstance"
    priority = AttributeDocumenter.priority + 1
    @classmethod
    def can_document_member(cls, member: Any, membername: str, isattr: bool, parent: Any
                            ) -> bool:
        value = False
        if isinstance(member, OverloadedMethodInstanceWrapper):
            value = inspect.isclassmethod(member.method)
        return value




class OverloadedMethodDocumenterMixin(ImplicitsSignatureMixin, UnpackImplicitsMixin):
    objtype = "overloadedmethod"
    priority = AttributeDocumenter.priority + 1

    def document_members(self, all_members: bool = False) -> None:
        """Generate reST for member documentation.

        If *all_members* is True, document all members, else those given by
        *self.options.members*.
        """
        want_all = True
        # find out which members are documentable
        members_check_module, members = self.get_object_members(want_all)

        # document non-skipped members
        memberdocumenters: List[Tuple[Documenter, bool]] = []
        for (mname, member, isattr) in self.filter_members(members, want_all):
            classes = [cls for cls in self.documenters.values()
                       if cls.can_document_member(member, mname, isattr, self)]
            if not classes:
                # don't know how to document this member
                continue
            # prefer the documenter with the highest priority
            classes.sort(key=lambda cls: cls.priority)
            # give explicitly separated module name, so that members
            # of inner classes can be documented
            full_mname = self.modname + '::' + '.'.join(self.objpath + [mname])
            documenter = classes[-1](self.directive, full_mname, self.indent)
            memberdocumenters.append((documenter, isattr))

        member_order = self.options.member_order or self.config.autodoc_member_order
        memberdocumenters = self.sort_members(memberdocumenters, member_order)

        for documenter, isattr in memberdocumenters:
            documenter.generate(
                all_members=True, real_modname=self.real_modname,
                check_module=members_check_module and not isattr)

    def get_object_members(self, want_all: bool) -> Tuple[bool, ObjectMembers]:
        return False,  [(f"{self.object_name}_{i}", OverloadedMethodInstanceWrapper(m))
                        for i, m in enumerate(self.object._methods)]

    def filter_members(self, members: ObjectMembers, want_all: bool
                       ) -> List[Tuple[str, Any, bool]]:
        return [(name, member, False) for name, member in members]

    def get_doc(self) -> Optional[List[List[str]]]:
        """Decode and return lines of the docstring(s) for the object.

        When it returns None, autodoc-process-docstring will not be called for this
        object.
        """
        docstring = "Overloaded method with alternatives"
        tab_width = self.directive.state.document.settings.tab_width
        return [prepare_docstring(docstring, tab_width)]


class InstanceOnlyMethodDocumenter(ImplicitsSignatureMixin, UnpackInstanceOnlyMixin, MethodDocumenter):  # type: ignore
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


class OverloadedMethodDocumenter(OverloadedMethodDocumenterMixin, InstanceOnlyMethodDocumenter):
    objtype = "overloadedmethod"
    priority = InstanceOnlyMethodDocumenter.priority + 1

    @classmethod
    def can_document_member(cls, member: Any, membername: str, isattr: bool, parent: Any
                            ) -> bool:
        if isinstance(parent, ModuleDocumenter):
            value = False
        else:
            actual_method = unpack_implicits_method(unpack_instance_only_method(member, membername))
            value = isinstance(actual_method, OverloadedMethod)
        return value


class MetaclassMethodDocumenter(NoIndexMixin, ImplicitsSignatureMixin, ClassMethodMixin, MethodDocumenter):
    objtype = "metaclassmethod"
    priority = AttributeDocumenter.priority + 1  # make sure these are not parsed as attributes!

    def __init__(self, directive: "DocumenterBridge", name: str, indent: str = '') -> None:
        super().__init__(directive, name, indent)

    @classmethod
    def can_document_member(cls, member: Any, membername: str, isattr: bool, parent: Any
                            ) -> bool:
        value = isinstance(member, ClassMethodWrapper)
        return value


class OverloadedClassMethodDocumenter(OverloadedMethodDocumenterMixin, NoIndexMixin, ClassMethodMixin, MethodDocumenter):
    objtype = "overloadedclassmethod"
    priority = MetaclassMethodDocumenter.priority + 1
    @classmethod
    def can_document_member(cls, member: Any, membername: str, isattr: bool, parent: Any
                            ) -> bool:
        value = False
        if isinstance(member, ClassMethodWrapper):
            value = isinstance(member.method, OverloadedMethod)
        return value


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

