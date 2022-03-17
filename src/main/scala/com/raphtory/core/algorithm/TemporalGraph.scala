package com.raphtory.core.algorithm

/**
  * {s}`TemporalGraph`
  *  : Public interface for the analysis API
  *
  * A {s}`TemporalGraph` is a {s}`RaphtoryGraph` with only one view. Because of this, there is a unique underlying
  * timeline with a start time and, optionally, an end time. It offers methods to modify this timeline.
  * There are also methods to create a collection of views over it, transforming this graph into a {s}`RaphtoryGraph`.
  * If any graph operation is invoked from this instance, it is applied only over the elements of the graph within
  * the timeline.
  *
  * ## Methods
  *
  *  {s}`from(startTime: Long): TemporalGraph`
  *    : Set the start of the timeline to {s}`startTime`.
  *
  *      {s}`startTime: Long`
  *      : time interpreted in milliseconds by default
  *
  *  {s}`from(startTime: String): TemporalGraph`
  *    : Set the start of the timeline to {s}`startTime`. The format of the timestamp can be set in the configuration
  *    path: {s}`"raphtory.query.timeFormat"`. By default is {s}`"yyyy-MM-dd HH:mm:ss[.SSS]"`.
  *
  *      {s}`startTime: String`
  *      : timestamp
  *
  *  {s}`until(endTime: Long): TemporalGraph`
  *    : Set the end of the timeline to {s}`endTime`.
  *
  *      {s}`endTime: Long`
  *        : time interpreted in milliseconds by default
  *
  *  {s}`until(endTime: String): TemporalGraph`
  *    : Set the end of the timeline to {s}`endTime`. The format of the timestamp can be set in the configuration
  *    path: {s}`"raphtory.query.timeFormat"`. By default is {s}`"yyyy-MM-dd HH:mm:ss[.SSS]"`.
  *
  *      {s}`endTime: String`
  *        : timestamp
  *
  *  {s}`slice(startTime: Long, endTime: Long): TemporalGraph`
  *    : Set the start and the end of the timeline to {s}`startTime` and {s}`endTime` respectively.
  *    {s}`graph.slice(startTime, endTime)` is equivalent to {s}`graph.from(startTime).until(endTime)`
  *
  *  {s}`slice(startTime: String, endTime: String): TemporalGraph`
  *     : Set the start and the end of the timeline to {s}`startTime` and {s}`endTime` respectively.
  *    {s}`graph.slice(startTime, endTime)` is equivalent to {s}`graph.from(startTime).until(endTime)`.
  *    The format of the timestamps can be set in the configuration
  *    path: {s}`"raphtory.query.timeFormat"`. By default is {s}`"yyyy-MM-dd HH:mm:ss[.SSS]"`.
  *
  *   {s}`raphtorize(increment: Long): RaphtoryGraph`
  *     : Create a collection of incrementally growing views over the graph beginning from the start of the
  *     timeline with the step size set by {s}`increment`.
  *     If the timeline has an end, the last view is created at that point.
  *     If not, it produces an unbounded collection of views.
  *
  *       {s}`increment: Long`
  *         : the step size
  *
  *  {s}`raphtorize(increment: String): RaphtoryGraph`
  *     : Create a collection of incrementally growing views over the graph beginning from the start of the
  *     timeline with the step size set by {s}`increment`.
  *     If the timeline has an end, the last view is created at that point.
  *     If not, it produces an unbounded collection of views.
  *
  *       {s}`increment: String`
  *         : the step size in natural language. E.g. {s}`"2 seconds"`, {s}`"1 months"`, {s}`"1 day and 12 hours"`
  *
  *  {s}`raphtorize(increment: Long, window: Long): RaphtoryGraph`
  *     : Create a collection of equally sized views over the graph beginning from the start of the
  *     timeline with the step size set by {s}`increment`.
  *     If the timeline has an end, the last view is created at that point.
  *     If not, it produces an unbounded collection of views.
  *
  *       {s}`increment: Long`
  *         : the step size
  *
  *       {s}`window: Long`
  *         : the window size
  *
  *  {s}`raphtorize(increment: String, window: String): RaphtoryGraph`
  *     : Create a collection of equally sized views over the graph beginning from the start of the
  *     timeline with the step size set by {s}`increment`.
  *     If the timeline has an end, the last view is created at that point.
  *     If not, it produces an unbounded collection of views.
  *
  *       {s}`increment: String`
  *          : the step size in natural language. E.g. {s}`"2 seconds"`, {s}`"1 months"`, {s}`"1 day and 12 hours"`
  *
  *       {s}`window: String`
  *         : the window size in natural language
  *
  *  {s}`raphtorize(increment: Long, windows: List[Long]): RaphtoryGraph`
  *    : Create a collection of views over the graph beginning from the start of the
  *     timeline with the step size set by {s}`increment` and for every window size set in {s}`windows`.
  *     If the timeline has an end, the last view is created at that point.
  *     If not, it produces an unbounded collection of views.
  *
  *       {s}`increment: Long`
  *         : the step size
  *
  *       {s}`windows: Long`
  *         : the window sizes
  *
  *  {s}`raphtorize(increment: String, windows: List[String]): RaphtoryGraph`
  *     : Create a collection of views over the graph beginning from the start of the
  *     timeline with the step size set by {s}`increment` and for every window size set in {s}`windows`.
  *     If the timeline has an end, the last view is created at that point.
  *     If not, it produces an unbounded collection of views.
  *
  *       {s}`increment: String`
  *          : the step size in natural language. E.g. {s}`"2 seconds"`, {s}`"1 months"`, {s}`"1 day and 12 hours"`
  *
  *       {s}`windows: String`
  *         : the window sizes in natural language
  *
  * ```{seealso}
  * [](com.raphtory.core.algorithm.RaphtoryGraph)
  * ```
  */
trait TemporalGraph extends RaphtoryGraph {
  def from(startTime: Long): TemporalGraph
  def from(startTime: String): TemporalGraph
  def until(endTime: Long): TemporalGraph
  def until(endTime: String): TemporalGraph
  def slice(startTime: Long, endTime: Long): TemporalGraph
  def slice(startTime: String, endTime: String): TemporalGraph
  def raphtorize(increment: Long): RaphtoryGraph
  def raphtorize(increment: String): RaphtoryGraph
  def raphtorize(increment: Long, window: Long): RaphtoryGraph
  def raphtorize(increment: String, window: String): RaphtoryGraph
  def raphtorize(increment: Long, windows: List[Long]): RaphtoryGraph
  def raphtorize(increment: String, windows: List[String]): RaphtoryGraph
}
