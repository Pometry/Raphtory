from pyraphtory.builder import *

from datetime import datetime
# to datetime object:


class BotsGraphBuilder(BaseBuilder):
    def __init__(self):
        super(BotsGraphBuilder, self).__init__()

    def parse_tuple(self, line: str):
        data = line.split(",")
        userID  = data[0]
        timestamp = datetime.fromisoformat(data[1]).timestamp()
        replyID = data[2]
        sentiment = data[9]

        src_id = self.assign_id(userID)

        print(src_id)

        self.add_vertex(int(timestamp), src_id, [ImmutableProperty("name", userID)], "User")

        if (replyID!= ""):
            tar_id = self.assign_id(replyID)
            self.add_vertex(int(timestamp), tar_id, [ImmutableProperty("name", replyID)], "Character")
            self.add_edge(int(timestamp), src_id, tar_id, [ImmutableProperty("sentiment", sentiment)], "Retweeted")