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

  private final String nodeLabelExpression;

  /**
   * Job driver constructor - instantiated via TANG.
   *
   * @param requestor evaluator requestor object used to create new evaluator containers.
   */
  @Inject
  public REEFYarnNodeLabelTestDriver(final EvaluatorRequestor requestor) {
    this.evaluatorRequestor = requestor;
    this.nodeLabelExpression = "mylabel";
    LOG.log(Level.FINE, "Instantiated 'REEFYarnNodeLabelTestDriver'");
  }

  /**
   * Handles the StartTime event: Request as single Evaluator.
   */
  public final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      final EvaluatorRequest reqToMylabel = EvaluatorRequest.newBuilder()
          .setNumber(1)
          .setMemory(500)
          .setNumberOfCores(1)
          .setNodeLabelExpression(REEFYarnNodeLabelTestDriver.this.nodeLabelExpression)
          .build();
      LOG.log(Level.INFO, "Requested Evaluator with node label: " + REEFYarnNodeLabelTestDriver.this.nodeLabelExpression);
      REEFYarnNodeLabelTestDriver.this.evaluatorRequestor.submit(reqToMylabel);

      final EvaluatorRequest reqToDefault = EvaluatorRequest.newBuilder()
          .setNumber(1)
          .setMemory(500)
          .setNumberOfCores(1)
          .build();
      LOG.log(Level.INFO, "Requested Evaluator without node label");
      REEFYarnNodeLabelTestDriver.this.evaluatorRequestor.submit(reqToDefault);
    }
  }

  /**
   * Handles AllocatedEvaluator: Submit the HelloTask
   */
  public final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      LOG.log(Level.INFO, "Evaluator Allocated: {0}", allocatedEvaluator);

      final String host = allocatedEvaluator.getEvaluatorDescriptor().getNodeDescriptor()
          .getInetSocketAddress().getHostString();
      LOG.log(Level.INFO, "Host: " + host);

      final int port = allocatedEvaluator.getEvaluatorDescriptor().getNodeDescriptor()
          .getInetSocketAddress().getPort();
      LOG.log(Level.INFO, "Port: " + port);

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

      LOG.log(Level.INFO, "Node Labels on this node: " + nodelabels);

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
}
