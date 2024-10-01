import sys
from .raphtory import *

sys.modules["raphtory.algorithms"] = algorithms
sys.modules["raphtory.graph_gen"] = graph_gen
sys.modules["raphtory.graph_loader"] = graph_loader
sys.modules["raphtory.vectors"] = vectors
sys.modules["raphtory.graphql"] = graphql
