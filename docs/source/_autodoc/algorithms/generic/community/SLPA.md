`com.raphtory.algorithms.generic.community.SLPA`
(com.raphtory.algorithms.generic.community.SLPA)=
# SLPA

{s}`SLPA(iterNumber: Int = 50, speakerRule: Rule = ChooseRandom(), listenerRule: Rule = MostCommon())`
 : find overlapping communities using synchronous label propagation

This returns the overlapping communities of the graph by a variant of the synchronous label propagation algorithm.
In this algorithm, each node maintains a memory of labels. At each step, a vertex chooses a label from its memory
to send to all its neighbours according to a Speaking Rule (defaults to choosing a label at random from memory).
Then each vertex chooses a label from its received queue of labels to add to memory according to a Listening Rule
(defaults to choosing the most commonly received label). Instead of converging based on some criterium, the
labels a node "remembers" is stored and the algorithm terminates after a set number of iterations. At this point,
Raphtory returns the name of each vertex and its memory of labels (the size of which is the number of iterations).

## Parameters

 {s}`iterNumber: Int = 50`
   : Number of iterations to run (default 50).

 {s}`speakerRule: Rule = ChooseRandom()`
   : rule for choosing label to propagate (default: ChooseRandom())

 {s}`listenerRule: Rule = MostCommon()`
   : rule for choosing label to add to mem (default: MostCommon())

## States

 {s}`memory: Queue[Long]`
   : memory of labels

## Returns

 | vertex name       | label memory             |
 | ----------------- | ------------------------ |
 | {s}`name: String` | {s}`memory: Array[Long]` |

## Rules

{s}`Fifo()`
 : Return labels in first-in-first-out order

{s}`MostCommon()`
 : Return most common label

{s}`ChooseRandom()`
 : Sample label at random from list of labels

 ```{note}
 This implementation is based on the paper SLPA: Uncovering Overlapping Communities in Social Networks via A
 Speaker-listener Interaction Dynamic Process by Jierui Xie, Boleslaw K. Szymanski and Xiaoming Liu (2011)
 ```