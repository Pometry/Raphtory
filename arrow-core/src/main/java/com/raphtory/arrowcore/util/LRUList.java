/* Copyright (C) Pometry Ltd - All Rights Reserved.
 *
 * This file is proprietary and confidential. Unauthorised
 * copying of this file, via any medium is strictly prohibited.
 *
 */

package com.raphtory.arrowcore.util;

/**
 * This class maintains a least-recently-used list of items of type T
 * using a doubly-linked list.
 *
 * Implemented as an intrusive linked list to reduce the number
 * of objects required to maintain very long lists.
 *
 * @param <T> The type of objects held in this list.
 */
public class LRUList<T extends LRUListItem<T>> {
    private T _lruHead = null;
    private T _lruTail = null;
    private volatile int _nItems = 0;

    /**
     * Creates a new empty LRUList.
     */
    public LRUList() {
    }


    /**
     * Invoked when an item in the list has been used in some way.
     * Results in the item being the most recently used item in the list.
     *
     * @param it the item that was recently "touched"
     */
    public void touched(T it) {
        remove(it);
        push(it);
    }


    /**
     * @return Returns whether or not the list is not empty
     */
    public boolean hasItems() {
        return _nItems != 0;
    }


    /**
     * @return the most recently used item
     */
    public T getHead() {
        return _lruHead;
    }


    /**
     * @return the least recently used item
     */
    public T getTail() {
        return _lruTail;
    }


    /**
     * @return the number of items in this list
     */
    public int size() {
        return _nItems;
    }


    /**
     * Pushes it onto the list, it then becomes the most recently used item.
     * @param it
     */
    private void push(T it) {
        ++_nItems;

        it.setNext(_lruHead);
        it.setPrev(null);

        if (_lruHead == null) { // Empty list
            _lruHead = it;
            _lruTail = it;
        } else {
            _lruHead.setPrev(it);
            _lruHead = it;
        }
    }


    /**
     * Removes it from the list.
     * @param it the item to remove
     */
    public void remove(T it) {
        if (_nItems == 0 || it == null) {
            return;
        }

        if (it.getNext() == null && it.getPrev() == null) { // 1 element list - or not in the list?
            if (_lruHead == it || _lruTail == it) {
                _lruHead = null;
                _lruTail = null;
                --_nItems;
            }
            return;
        }

        --_nItems;

        if (_lruHead == it) { // At head
            _lruHead = it.getNext();
            if (_lruHead != null) {
                _lruHead.setPrev(null);
            } else {
                _lruTail = null;
            }

            it.setNext(null);
            it.setPrev(null);
            return;
        }

        if (_lruTail == it) { // At tail
            _lruTail = _lruTail.getPrev();
            _lruTail.setNext(null);
            it.setNext(null);
            it.setPrev(null);
            return;
        }

        T prev = it.getPrev();
        T next = it.getNext();

        prev.setNext(next);
        next.setPrev(prev);
        it.setNext(null);
        it.setPrev(null);
    }


    /**
     * Empties the entire list
     */
    public void clear() {
        _lruHead = null;
        _lruTail = null;
        _nItems = 0;
    }
}