`com.raphtory.core.components.graphbuilder.Properties`

(com.raphtory.core.components.graphbuilder.Properties)=
# Properties

Properties are characteristic attributes like name, etc. assigned to Vertices and Edges by the [Graph Builder](com.raphtory.core.components.graphbuilder.GraphBuilder).

## Members

{s}`Type(name: String)`
  : Vertex/Edge type (this is not a {s}`Property`)

{s}`Property`
 : sealed trait defining different types of properties

   **Attributes**

   {s}`key: String`
     : property name

   {s}`value: Any`
     : property value

{s}`Properties(property: Property*)`
  : Wrapper class for properties

{s}`ImmutableProperty(key: String, value: String)`
  : {s}`Property` with a fixed value (the value should be the same for each update to the entity)

{s}`StringProperty(key: String, value: String)`
  : {s}`Property` with a {s}`String` value

{s}`LongProperty(key: String, value: Long)`
  : {s}`Property` with a {s}`Long` value

{s}`DoubleProperty(key: String, value: Double)`
  : {s}`Property` with a {s}`Double` value

{s}`FloatProperty(key: String, value: Float)`
  : {s}`Property` with a {s}`Float` value

```{seealso}
[](com.raphtory.core.components.graphbuilder.GraphBuilder)
```