from sphinx.application import Sphinx
from pyraphtory.interop import ScalaClassProxy, InstanceOnlyMethod, ScalaObjectProxy
from sphinx.util import logging
from sphinx.locale import _, __
from sphinx.ext.autodoc import MethodDocumenter, ClassDocumenter, safe_getattr, ObjectMembers, get_class_members, ModuleDocumenter
from typing import *

logger = logging.getLogger(__name__)


def setup(app: Sphinx):
    app.add_autodocumenter(InstanceOnlyMethodDocumenter)
    app.add_autodocumenter(ScalaClassProxyDocumenter)
    app.add_autodocumenter(ClassMethodDocumenter)


class InstanceOnlyMethodDocumenter(MethodDocumenter):  # type: ignore
    """
    Specialized Documenter subclass for instance-only.
    """
    priority = MethodDocumenter.priority + 1  # must be more than FunctionDocumenter

    @classmethod
    def can_document_member(cls, member: Any, membername: str, isattr: bool, parent: Any
                            ) -> bool:
        if isinstance(parent, ClassDocumenter):
            if hasattr(member, "__self__"):
                try:
                    actual_method = member.__self__.__class__.__dict__[membername]
                    if isinstance(actual_method, InstanceOnlyMethod):
                        return True
                except Exception as e:
                    pass
        return False

    def import_object(self, raiseerror: bool = False) -> bool:
        ret = super().import_object(raiseerror)
        if not ret:
            return ret

        actual_method = self.object.__self__.__class__.__dict__[self.object_name]
        self.object = actual_method.__func__

        return ret


class ClassMethodWrapper:
    """Wrap classmethod part of masked instance-only method"""
    def __init__(self, method):
        self.method = method


class ClassMethodDocumenter(MethodDocumenter):  # type: ignore
    """
    Specialized Documenter subclass for instance-only.
    """
    priority = MethodDocumenter.priority + 1  # must be more than FunctionDocumenter

    @classmethod
    def can_document_member(cls, member: Any, membername: str, isattr: bool, parent: Any
                            ) -> bool:
        if isinstance(parent, ScalaClassProxyDocumenter):
            return isinstance(member, ClassMethodWrapper)
        return False

    def import_object(self, raiseerror: bool = False) -> bool:
        ret = super().import_object(raiseerror)
        self.object = self.object.method
        return ret



class ScalaClassProxyDocumenter(ClassDocumenter):
    priority = ClassDocumenter.priority + 1
    @classmethod
    def can_document_member(cls, member: Any, membername: str, isattr: bool, parent: Any
                            ) -> bool:
        ret = isinstance(member, ScalaObjectProxy)
        return ret

    def get_object_members(self, want_all: bool) -> Tuple[bool, ObjectMembers]:
        members = get_class_members(self.object, self.objpath, self.get_attr,
                                    self.config.autodoc_inherit_docstrings)
        class_members = [(name, ClassMethodWrapper(cmember.__func__.__get__(self.object.__class__, self.object.__class__.__class__)))
                         for name, cmember in self.object.__class__.__dict__.items()
                         if isinstance(cmember, InstanceOnlyMethod)]

        if not want_all:
            if not self.options.members:
                return False, []  # type: ignore
            # specific members given
            member_list = []
            for name in self.options.members:
                if name in members:
                    member_list.append(members[name])
                else:
                    logger.warning(__('missing attribute %s in object %s') %
                                   (name, self.fullname), type='autodoc')
        elif self.options.inherited_members:
            member_list = list(members.values())
        else:
            member_list = [m for m in members.values() if m.class_ == self.object]
        return False, member_list + class_members
