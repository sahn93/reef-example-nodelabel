/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package reef.example.nodelabel;

import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.util.EnvironmentUtils;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Client for REEF node label example on YARN.
 */
public class REEFYarnNodeLabelTest {

  private static final Logger LOG = Logger.getLogger(REEFYarnNodeLabelTest.class.getName());

  /**
   * Number of milliseconds to wait for the job to complete.
   */
  private static final int JOB_TIMEOUT = 10000; // 10 sec.

  /**
   * Empty private constructor to prohibit instantiation
   */
  private REEFYarnNodeLabelTest() {
  }

  /**
   * @return the configuration of the runtime
   */
  private static Configuration getRuntimeConfiguration() {
    final Configuration runtimeConf = YarnClientConfiguration.CONF.build();
    return runtimeConf;
  }

  /**
   * @return the configuration of the REEFYarnNodeLabelTest driver.
   */
  private static Configuration getDriverConfiguration() {
    // TODO: build DriverConfiguration
    final Configuration driverConf = DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(REEFYarnNodeLabelTestDriver.class))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "TEST_REEFYarnNodeLabelTest")
        .set(DriverConfiguration.ON_DRIVER_STARTED, REEFYarnNodeLabelTestDriver.StartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, REEFYarnNodeLabelTestDriver.EvaluatorAllocatedHandler.class)
        .build();
    return driverConf;
  }

  /**
   * Start a REEF job.
   *
   * @param args command line parameters.
   * @throws BindException      configuration error.
   * @throws InjectionException configuration error.
   */
  public static void main(final String[] args) throws BindException, InjectionException {
    final Configuration runtimeConf = getRuntimeConfiguration();
    final Configuration driverConf = getDriverConfiguration();

    final LauncherStatus status = DriverLauncher
        .getLauncher(runtimeConf)
        .run(driverConf, JOB_TIMEOUT);
    LOG.log(Level.INFO, "REEF job completed: {0}", status);
  }
}
