from dataclasses import dataclass, asdict
from typing import List, Optional


@dataclass
class Type(object):
    name: str

@dataclass
class Property(object):
    key: str


@dataclass
class ImmutableProperty(Property):
    value: str


@dataclass
class Properties(object):
    properties: List[Property]


@dataclass
class VertexAdd(object):
    update_time: int
    index: int
    src_id: int
    properties: Properties
    v_type: Optional[Type]
    # specifying this makes sure it can't be decoded as the wrong type
    _type: str = "com.raphtory.internals.graph.GraphAlteration.VertexAdd"


@dataclass
class EdgeAdd(object):
    update_time: int
    index: int
    src_id: int
    dst_id: int
    properties: Properties
    e_type: Optional[Type]
    # specifying this makes sure it can't be decoded as the wrong type
    _type: str = "com.raphtory.internals.graph.GraphAlteration.EdgeAdd"


class BaseBuilder(object):
    def __init__(self):
        from pemja import findClass
        self.interop = findClass('com.raphtory.internals.management.PythonInterop')
        self.actions = []

    def _set_jvm_builder(self, jvm_builder):
        self._proxy = jvm_builder

    def _index(self):
        return self._proxy.get_current_index()

    def get_actions(self):
        return self.actions

    def reset_actions(self):
        self.actions.clear()

    def parse_tuple(self, line: str):
        pass

    # TODO: revise these methods to call actual JVM methods not return objects
    def add_vertex(self, timestamp: int, src_id: int, props: List[Property], tpe: str, index=None):
        if index is None:
            index = self._index()
        self.actions.append(asdict(VertexAdd(timestamp, index, src_id, Properties(props), Type(tpe))))

    def add_edge(self, timestamp: int, src_id: int, tar_id: int, props: List[Property], tpe: str, index=None):
        if index is None:
            index = self._index()
        self.actions.append(asdict(EdgeAdd(timestamp, index, src_id, tar_id, Properties(props), Type(tpe))))

    def assign_id(self, s: str):
        return self.interop.assignId(s)
