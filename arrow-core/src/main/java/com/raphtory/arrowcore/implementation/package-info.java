/**
 * Raphtory Arrow storage layer implementation
 * <p>
 * The Raphtory Arrow storage layer manages the storage and retrieval of Raphtory Vertices and Edges within a set of Arrow files.
 * <p>
 * <b>Files</b><p>
 * The storage is split into 'partitions' - where each partition manages a sub-set of the total set of vertices or edges.
 * <p>
 * Each partition containing vertices will usually consist of 1 million vertices in a single arrow file ({@code vertex-p0.arrow} for vertex partition 0)
 * along with another file for history records ({@code vertexHistory-p0.arrow}) and a further file for each property within the schema ({@code vertexProperty-p0.0.arrow} for property 0).
 * <p>
 * Edge partitions are similar to vertex partitions and generate files of the form {@code edge-p0.arrow, edgeHistory-p0.arrow} and {@code edgeProperty-p0.0.arrow}
 *<p>
 * <b>Main Classes</b><p>
 * {@link com.raphtory.arrowcore.implementation.RaphtoryArrowPartition} - The main entry point into the Raphtory Arrow storage layer.<p>
 * {@link com.raphtory.arrowcore.implementation.VertexPartitionManager} - Manages a set of vertex arrow files.<p>
 * {@link com.raphtory.arrowcore.implementation.EdgePartitionManager} - Manages a set of edge arrow files.<p>
 * {@link com.raphtory.arrowcore.implementation.VertexIterator} - Iterates over vertices.<p>
 * {@link com.raphtory.arrowcore.implementation.EdgeIterator} - Iterates over edges.<p>
 */
package com.raphtory.arrowcore.implementation;