/**
 * Copyright (c) 2010-2016 Yahoo! Inc., 2017 YCSB contributors All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb;

import org.apache.htrace.core.Tracer;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Creates a DB layer by dynamically classloading the specified DB class.
 */
public final class BenchRunFactory {
  private BenchRunFactory() {}

  public static BenchRun newRunner(String execClass, Properties properties, final Tracer tracer, final AtomicInteger counter) {
    ClassLoader classLoader = BenchRunFactory.class.getClassLoader();

    BenchRun ret;

    try {
      Class<?> runClass = classLoader.loadClass(execClass);

      ret = (BenchRun) runClass.getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      e.printStackTrace(System.err);
      return null;
    }

    ret.setProperties(properties);

    return new BenchRunWrapper(ret, tracer, counter);
  }
}
