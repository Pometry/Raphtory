import logging
import raphtory

# Set up logging
logging.basicConfig(level=logging.INFO)

logging.info("Generating reddit hyperlink graph")
g = raphtory.graph_loader.reddit_hyperlink_graph_local("soc-redditHyperlinks-title.tsv")


logging.info("Adding props to graph")
g.add_constant_properties({"prop1": "this is a long prop for this graph"})

logging.info("Saving to ./reddit_graph.bincode")
g.save_to_file("reddit_graph.bincode")

logging.info("Complete")