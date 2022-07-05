from typing import List, Optional
from dataclasses import dataclass, asdict, field, InitVar


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


def assign_id(s: str):
    from pemja import findClass

    LHF = findClass('com.raphtory.internals.management.PythonInterop')
    id = LHF.assignId(s)  # .xx3().hashChars(s)
    return id


class BaseBuilder:
    def __init__(self):
        self.actions = []

    def get_actions(self):
        return self.actions

    def parse_tuple(self, line: str):
        src_node, target_node, timestamp, *_ = line.split(",")

        src_id = assign_id(src_node)
        tar_id = assign_id(target_node)

        self.add_vertex(int(timestamp), src_id, [ImmutableProperty("name", src_node)], "Character")
        self.add_vertex(int(timestamp), tar_id, [ImmutableProperty("name", target_node)], "Character")
        self.add_edge(int(timestamp), src_id, tar_id, [], "Character Co-occurence")

    def add_vertex(self, timestamp: int, src_id: int, props: List[Property], tpe: str):
        self.actions.append(asdict(VertexAdd(timestamp, src_id, Properties(props), Type(tpe))))

    def add_edge(self, timestamp: int, src_id: int, tar_id: int, props: List[Property], tpe: str):
        self.actions.append(asdict(EdgeAdd(timestamp, src_id, tar_id, Properties(props), Type(tpe))))


class LotrGraphBuilder(BaseBuilder):
    def __init__(self):
        super(LotrGraphBuilder, self).__init__()
