/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.util;

import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.util.typedef.T6;

public class DU {
    private static final AtomicLong OP_SEQ_NUM = new AtomicLong();

    private static final Object[] NO_ARGS = new Object[] {};

    public static final Queue<T6<Long, Long, Long, String, Object[], StackTraceElement[]>> OPS =
        new ConcurrentLinkedQueue<>();

    public static long op() {
        return doOp(null, NO_ARGS);
    }

    public static long op(Object[] args) {
        return doOp(null, args);
    }

    public static long op(long parent, Object... args) {
        return doOp(parent, args);
    }

    private static long doOp(Long parent, Object[] args) {
        long ts = System.currentTimeMillis();

        long seq = OP_SEQ_NUM.incrementAndGet();

        String threadName = Thread.currentThread().getName();

        StackTraceElement[] stack = new Throwable().getStackTrace();

        OPS.add(new T6<>(ts, seq, parent, threadName, args, stack));

        return seq;
    }

    public static void printOps() {
        System.out.println("!!! DEBUG");

        for (T6<Long, Long, Long, String, Object[], StackTraceElement[]> op : DU.OPS) {
            StackTraceElement[] trace = op.get6();

            Long parent = op.get3();

            System.out.println(op.get1() + "\t" + op.get2() + "\t" + (parent == null ? "--" : parent) + "\t" +
                op.get4()  + "\t" + trace[2] + "\t" + Arrays.asList(op.get5()));


            if (trace.length > 3) {
                for (int i = 3; i < trace.length; i++) {
                    System.out.println("\t\t\t\t\t\t\t\t" + trace[i].getClassName() + "#" + trace[i].getMethodName() +
                        ":" + trace[i].getLineNumber());
                }
            }
        }
    }

    public static void reset() {
        OP_SEQ_NUM.set(0);

        OPS.clear();
    }
}
