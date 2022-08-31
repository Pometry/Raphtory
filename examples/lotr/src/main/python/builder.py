from pyraphtory.builder import *

class LotrGraphBuilder(BaseBuilder):

    def parse_tuple(self, line: str):
        src_node, target_node, timestamp, *_ = line.split(",")

        src_id = self.assign_id(src_node)
        tar_id = self.assign_id(target_node)

        self.add_vertex(int(timestamp), src_id, Properties(ImmutableProperty("name", src_node)), Type("Character"))
        self.add_vertex(int(timestamp), tar_id, Properties(ImmutableProperty("name", target_node)), Type("Character"))
        self.add_edge(int(timestamp), src_id, tar_id, Type("Character Co-occurence"))