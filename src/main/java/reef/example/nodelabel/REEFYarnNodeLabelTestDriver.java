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

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;
import org.apache.reef.wake.time.event.StopTime;
import org.junit.Assert;

import javax.inject.Inject;
import java.io.IOException;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Driver code for the REEF Yarn node label test Application
 */
@Unit
public class REEFYarnNodeLabelTestDriver {
  private static final Logger LOG = Logger.getLogger(REEFYarnNodeLabelTestDriver.class.getName());

  private final EvaluatorRequestor evaluatorRequestor;

  private final String NODE_LABEL_EXPRESSION = "mylabel";
  private final int LABELED_REQUEST_NUM = 3;
  private final int DEFAULT_REQUEST_NUM = 5;
  private int labeled_container_count;
  private int default_container_count;

  /**
   * Job driver constructor - instantiated via TANG.
   *
   * @param requestor evaluator requestor object used to create new evaluator containers.
   */
  @Inject
  public REEFYarnNodeLabelTestDriver(final EvaluatorRequestor requestor) {
    this.evaluatorRequestor = requestor;
    this.labeled_container_count = 0;
    this.default_container_count = 0;
    LOG.log(Level.FINE, "Instantiated 'REEFYarnNodeLabelTestDriver'");
  }

  /**
   * Handles the StartTime event: Request as single Evaluator.
   */
  public final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {

      for (int i = 1; i <= LABELED_REQUEST_NUM; i++) {
        REEFYarnNodeLabelTestDriver.this.evaluatorRequestor.submit(EvaluatorRequest.newBuilder()
            .setNumber(1)
            .setMemory(64)
            .setNumberOfCores(1)
            .setNodeLabelExpression(REEFYarnNodeLabelTestDriver.this.NODE_LABEL_EXPRESSION)
            .build());
        LOG.log(Level.INFO, "Requested " + i + " evaluators with node label: " +
            REEFYarnNodeLabelTestDriver.this.NODE_LABEL_EXPRESSION);
      }

      for (int i = 1; i <= DEFAULT_REQUEST_NUM; i++) {
        REEFYarnNodeLabelTestDriver.this.evaluatorRequestor.submit(EvaluatorRequest.newBuilder()
            .setNumber(1)
            .setMemory(64)
            .setNumberOfCores(1)
            .build());
        LOG.log(Level.INFO, "Requested " + i + " evaluators without node label");
      }


    }
  }

  /**
   * Handles AllocatedEvaluator: Submit the HelloTask
   */
  public final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      LOG.log(Level.INFO, "Allocated evaluator: {0}", allocatedEvaluator);

      final String host = allocatedEvaluator.getEvaluatorDescriptor().getNodeDescriptor()
          .getInetSocketAddress().getHostString();

      final int port = allocatedEvaluator.getEvaluatorDescriptor().getNodeDescriptor()
          .getInetSocketAddress().getPort();

      final NodeId nodeid = NodeId.newInstance(host, port);
      LOG.log(Level.INFO, "NodeId: " + nodeid);

      YarnClient client = YarnClient.createYarnClient();
      client.init(new YarnConfiguration());
      client.start();

      Set<String> nodelabels = null;

      try {
        nodelabels = client.getNodeToLabels().get(nodeid);
      } catch (YarnException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }

      LOG.log(Level.INFO, "Node labels on this node: " + nodelabels);

      if (nodelabels == null) {
        REEFYarnNodeLabelTestDriver.this.default_container_count++;
      } else if (nodelabels.contains("mylabel")) {
        REEFYarnNodeLabelTestDriver.this.labeled_container_count++;
      }

      LOG.log(Level.INFO, "Submitting Dummy REEF task to AllocatedEvaluator: {0}", allocatedEvaluator);

      // TODO: build TaskConfiguration
      final Configuration taskConfiguration = TaskConfiguration.CONF
          .set(TaskConfiguration.TASK, DummyTask.class)
          .set(TaskConfiguration.IDENTIFIER, "DummyTask")
          .build();;

      allocatedEvaluator.submitTask(taskConfiguration);

      allocatedEvaluator.close();
    }
  }

  public final class StopHandler implements EventHandler<StopTime> {

    @Override
    public void onNext(StopTime stopTime) {
      LOG.log(Level.INFO, "# of total default containers: {0}",
          REEFYarnNodeLabelTestDriver.this.default_container_count);
      Assert.assertEquals(REEFYarnNodeLabelTestDriver.this.default_container_count,
          REEFYarnNodeLabelTestDriver.this.DEFAULT_REQUEST_NUM);

      LOG.log(Level.INFO, "# of total labeled containers: {0}",
          REEFYarnNodeLabelTestDriver.this.labeled_container_count);
      Assert.assertEquals(REEFYarnNodeLabelTestDriver.this.labeled_container_count,
          REEFYarnNodeLabelTestDriver.this.LABELED_REQUEST_NUM);

    }
  }
}
