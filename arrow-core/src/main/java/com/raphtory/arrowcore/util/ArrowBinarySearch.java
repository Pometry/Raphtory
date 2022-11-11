package com.raphtory.arrowcore.util;

import org.apache.arrow.algorithm.sort.VectorValueComparator;
import org.apache.arrow.vector.ValueVector;

public class ArrowBinarySearch {
    public static <V extends ValueVector> int binarySearch(V targetVector, VectorValueComparator<V> comparator, V keyVector, int keyIndex) {
        comparator.attachVectors(keyVector, targetVector);
        int low = 0;
        int high = targetVector.getValueCount() - 1;

        while (low <= high) {
            int mid = low + (high - low) / 2;
            int cmp = comparator.compare(keyIndex, mid);
            if (cmp < 0) {          // mid > key
                high = mid - 1;
            }
            else if (cmp > 0) {     // mid < key
                low = mid + 1;
            }
            else {                  // mid == key
                return mid;
            }
        }

        return -(low + 1);  // key not found
    }
}