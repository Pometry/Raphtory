/* Copyright (C) Pometry Ltd - All Rights Reserved.
 *
 * This file is proprietary and confidential. Unauthorised
 * copying of this file, via any medium is strictly prohibited.
 *
 */

package com.raphtory.arrowcore.util;

/**
 * LRUListItem - a doubly-linked list item within an LRUList.
 * Objects implementing this interface can be placed into an LRUList.
 *
 * @param <T> the type of the implementation
 */
public interface LRUListItem<T> {
    /**
     * Sets the "next" pointer in this node in the list
     *
     * @param it the new target of the next pointer
     */
    public void setNext(T it);


    /**
     * Sets the "previous" pointer in this node in the list
     *
     * @param it the new target of the previous pointer
     */
    public void setPrev(T it);


    /**
     * @return the next pointer in this node
     */
    public T getNext();


    /**
     * @return the previous pointer in this node
     */
    public T getPrev();
}
