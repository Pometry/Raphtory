from raphtory import graphql

server = graphql.GraphServer(work_dir="data/apache")
server.run()
