`com.raphtory.util.Sampling`
(com.raphtory.util.Sampling)=
# Sampling

Extended sampling methods for {s}`scala.util.Random`.

To make these methods available to instances of {s}`scala.util.Random`, use

```{code-block} scala
import com.raphtory.algorithms.utils.Sampling._
```

## Methods

 {s}`sample(weights: Seq[Double]): Int`
   : Weighted random sampling. Returns integer {s}`i` with probability proportional to {s}`weights(i)`.
     ```{note}
     This implementation uses binary search to sample the index.
     ```