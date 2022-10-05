/* Copyright (C) Pometry Ltd - All Rights Reserved.
 *
 * This file is proprietary and confidential. Unauthorised
 * copying of this file, via any medium is strictly prohibited.
 *
 */

package com.raphtory.arrowcore.implementation;

import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;

import java.io.File;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This class provides a mapping of global to local entity ids.
 *<p>
 * A global-id is generated from the contents of a vertex, eg if it has
 * a unique field of some kind. A hash is formed from the unique field and
 * this is used as the global entity id.
 *<p>
 * When a vertex is added to Arrow, it is placed within a specific row in a
 * specific file - the numbering of both of which are used to generate a local-id.
 *<p>
 * <p>TODO: Compare the usage of RWLocks to just using synchronized...
 *
 * @see GlobalEntityIdStore
 */
public class LocalEntityIdStore {
    private final int _nMaps;
    private final int _mapSize;
    private final Long2LongOpenHashMap[] _map;
    private final ReadWriteLock[] _locks;
    private final int _nMaps_mask;

    /**
     * Creates a new local-entity-id store.
     *
     * @param dir the directory for the store (currently unused)
     * @param nMaps the number of internal maps to use (improves concurrency)
     * @param mapSize the initial size of each map
     */
    public LocalEntityIdStore(String dir, int nMaps, int mapSize) {
        File _dir = new File(dir);
        if (!_dir.exists()) {
            try {
                _dir.mkdirs();
            }
            catch (Exception e) {
                System.err.println("Exception: " + e);
                e.printStackTrace(System.err);
            }
        }


        nMaps = RaphtoryArrowPartition.roundUpToPowerOfTwo(nMaps);
        mapSize = RaphtoryArrowPartition.roundUpToPowerOfTwo(mapSize);

        _nMaps = nMaps;
        _mapSize = mapSize;
        int nMaps_shift = Integer.numberOfTrailingZeros(nMaps);
        int nMaps_divisor = 1 << nMaps_shift;
        _nMaps_mask = nMaps_divisor - 1;
        _map = new Long2LongOpenHashMap[_nMaps];
        _locks = new ReentrantReadWriteLock[_nMaps];

        for (int i=0; i<_nMaps; ++i) {
            _map[i] = new Long2LongOpenHashMap(_mapSize);
            _map[i].defaultReturnValue(-1L);
            _locks[i] = new ReentrantReadWriteLock();
        }
    }


    /**
     * Maps the supplied global-id to it's local-entity-id, if available.
     *
     * @param globalEntityId the global entity id to map
     *
     * @return the previously mapped local-node-id or -1L if not available
     */
    public long getLocalNodeId(long globalEntityId) {
        // Identify which map should contain this item
        int mapId = (int)(globalEntityId & _nMaps_mask);
        Long2LongOpenHashMap map = _map[mapId];

        // Get a read-lock
        _locks[mapId].readLock().lock();

        try {
            long it = map.get(globalEntityId);
            if (it != -1L) {
                return it;
            }
        }
        finally {
            // Release the read-lock
            _locks[mapId].readLock().unlock();
        }

        return -1L;
    }


    public void close() {
        // NOP
    }


    /**
     * Maps a particular global-entity-id to a local-node-id
     *
     * @param globalId the global-id to map
     * @param localId the local-id to be mapped
     *
     * @return the previously mapped value, or -1L if not previously mapped
     */
    public long setMapping(long globalId, long localId) {
        int mapId = (int)(globalId & _nMaps_mask);

        Long2LongOpenHashMap map = _map[mapId];
        _locks[mapId].writeLock().lock();

        try {
            return map.putIfAbsent(globalId, localId);
        }
        finally {
            _locks[mapId].writeLock().unlock();
        }
    }
}