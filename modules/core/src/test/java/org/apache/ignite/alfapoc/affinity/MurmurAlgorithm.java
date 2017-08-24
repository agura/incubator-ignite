package org.apache.ignite.alfapoc.affinity;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;

import java.io.Serializable;

public class MurmurAlgorithm implements HashingAlgorithm, Serializable {

    @Override
    public int hash(Object object) {
        HashCode hashCode = Hashing.murmur3_128()
                .hashObject(object, (o, primitiveSink) -> {
                    primitiveSink.putInt(o.hashCode());
                });

        return Math.abs(hashCode.asInt());
    }
}
