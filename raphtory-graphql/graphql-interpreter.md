# Zero allocation stack based graphql interpreter

Imagine a graphql request

```
  graph(path: "lotr") {
            window(start: 200, end: 800) {
              node(name: "Frodo") {
                after(time: 500) {
                  history {
                    list {
                      timestamp
                      eventId
                    }
                  }
                  neighbours {
                    list {
                      name
                        before(time: 300) {
                        history {
                          list {
                            timestamp
                            eventId
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
```

Instead of the usual execution in async-graphql where you have to return vectors or structs we can write a push based execution with the help of a stack, the query above would be planned as such
(load_graph, "lotr") the stack would have [Value:Graph(g)] g is of type DynamicGraph
(node, "Frodo") the stack [Value:Graph(g), Value:Node(n)]
(after, 500) the stack [Value:Graph(g), Value:Node(n), Value:Node(n)] raphtory can hide node types as NodeView<DynamicGraph>
(history) would peek at the head of the stack and call history on position 2 and place a history object on the head of the stack [Value:Graph(g), Value:Node(n), Value:Node(n), Value:History(h)]
(list) would peek at the head of the stack and call the iterator for history and push timestamp and eventId onto the stack
since they are leaves when they are popped off the stack timestamp and eventId write themselves onto the output sink in valid GraphQL output
