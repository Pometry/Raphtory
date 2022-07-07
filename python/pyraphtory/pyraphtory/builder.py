from typing import List, Optional
from dataclasses import dataclass, asdict, field, InitVar
from abc import ABC


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
    property: List[Property]


@dataclass
class VertexAdd(object):
    update_time: int
    src_id: int
    properties: Properties
    v_type: Optional[Type]


@dataclass
class EdgeAdd(object):
    update_time: int
    src_id: int
    dst_id: int
    properties: Properties
    e_type: Optional[Type]


class BaseBuilder:
    def __init__(self):
        from pemja import findClass
        self.interop = findClass('com.raphtory.internals.management.PythonInterop')
        self.actions = []

    def get_actions(self):
        return self.actions

    def reset_actions(self):
        self.actions.clear()

    def parse_tuple(self, line: str):
        pass

    def add_vertex(self, timestamp: int, src_id: int, props: List[Property], tpe: str):
        self.actions.append(asdict(VertexAdd(timestamp, src_id, Properties(props), Type(tpe))))

    def add_edge(self, timestamp: int, src_id: int, tar_id: int, props: List[Property], tpe: str):
        self.actions.append(asdict(EdgeAdd(timestamp, src_id, tar_id, Properties(props), Type(tpe))))

    def assign_id(self, s: str):
        return self.interop.assignId(s)
