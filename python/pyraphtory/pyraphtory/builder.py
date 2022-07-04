from typing import List
from dataclasses import dataclass, asdict, field, InitVar


@dataclass
class Type(object):
    name: str


@dataclass
class Property(object):
    key: str


@dataclass
class Vertex(object):
    timestamp: int
    src_id: int
    props: List[Property]
    tpe: str


class BaseBuilder:
    def __init__(self):
        self.actions = []

    def get_actions(self):
        return self.actions

    def parse_tuple(self, line: str):
        pass

    def add_vertex(self, timestamp: int, src_id: int, props: List[Property], tpe: str):
        self.actions.append(Vertex(timestamp, src_id, props, tpe))


class LotrGraphBuilder(BaseBuilder):
    def __init__(self):
        super(LotrGraphBuilder, self).__init__()
