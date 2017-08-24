package org.apache.ignite.alfapoc.affinity;


/**
 * Hashing algorithm that must return hash for object
 */
public interface HashingAlgorithm {
    /**
     * Return hash for object. must always return same hash for same object
     *
     * @param object Object
     *
     * @return hash
     */
    int hash(Object object);
}
