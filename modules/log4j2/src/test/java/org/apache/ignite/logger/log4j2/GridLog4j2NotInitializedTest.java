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

package org.apache.ignite.logger.log4j2;

import junit.framework.*;
import org.apache.ignite.*;
import org.apache.ignite.testframework.junits.common.*;

/**
 * Log4j not initialized test.
 */
@GridCommonTest(group = "Logger")
public class GridLog4j2NotInitializedTest extends TestCase {
    /** */
    public void testLogInitialize() {
        IgniteLogger log = new Log4J2Logger().getLogger(GridLog4j2NotInitializedTest.class);

        if (log.isDebugEnabled())
            log.debug("This is 'debug' message.");
        else
            System.out.println("DEBUG level is not enabled.");

        if (log.isInfoEnabled())
            log.info("This is 'info' message.");
        else
            System.out.println("INFO level is not enabled.");

        log.warning("This is 'warning' message.");
        log.error("This is 'error' message.");
    }
}
