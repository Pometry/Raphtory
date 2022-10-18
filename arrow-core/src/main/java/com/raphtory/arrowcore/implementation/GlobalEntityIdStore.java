/* Copyright (C) Pometry Ltd - All Rights Reserved.
 *
 * This file is proprietary and confidential. Unauthorised
 * copying of this file, via any medium is strictly prohibited.
 *
 */

package com.raphtory.arrowcore.implementation;

/**
 * This class is used to map between an entity's unique name (or other value)
 * to a numeric global-id.
 *<p>
 * Eg. For bitcoin data, the txhash or address_chain field can be used to map
 * between a vertex and it's global unique id.
 *<p>
 * The implementation used here is a hash-function.
 */
public class GlobalEntityIdStore {
    public GlobalEntityIdStore() {
    }


    /**
     * Returns the numeric global-id for this value
     *
     * @param nodeName the unique value to map
     *
     * @return the global-id (result of a hash function)
     */
    public long getGlobalNodeId(StringBuilder nodeName) {
        return net.openhft.hashing.LongHashFunction.xx3().hashChars(nodeName);
    }


    public void close() {
        // NOP
    }
}