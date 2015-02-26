/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/** Example scheduler to launch Docker containers. */
public class TestDockerScheduler implements Scheduler {

  /** Docker image name e..g. "fedora/apache". */
  private final String imageName;

  /** Number of instances to run. */
  private final int desiredInstances;

  /** List of pending instances. */
  private final List<String> pendingInstances = new ArrayList<String>();

  /** List of running instances. */
  private final List<String> runningInstances = new ArrayList<String>();

  /** Task ID generator. */
  private final AtomicInteger taskIDGenerator = new AtomicInteger();

  /** Constructor. */
  public TestDockerScheduler(String imageName, int desiredInstances) {
    this.imageName = imageName;
    this.desiredInstances = desiredInstances;
  }

  @Override
  public void registered(SchedulerDriver schedulerDriver, Protos.FrameworkID frameworkID, Protos.MasterInfo masterInfo) {
    System.out.format("registered() master=%s:%s, framework=%s\n", masterInfo.getIp(), masterInfo.getPort(), frameworkID);
  }

  @Override
  public void reregistered(SchedulerDriver schedulerDriver, Protos.MasterInfo masterInfo) {
    System.out.println("reregistered()");
  }

  @Override
  public void resourceOffers(SchedulerDriver schedulerDriver, List<Protos.Offer> offers) {

    System.out.format("resourceOffers() with %d offers\n", offers.size());

    for (Protos.Offer offer : offers) {

      List<Protos.TaskInfo> tasks = new ArrayList<Protos.TaskInfo>();
      if (runningInstances.size() + pendingInstances.size() < desiredInstances) {

        // generate a unique task ID
        Protos.TaskID taskId = Protos.TaskID.newBuilder()
            .setValue(Integer.toString(taskIDGenerator.incrementAndGet())).build();

        System.out.format("Launching task %s\n", taskId.getValue());
        pendingInstances.add(taskId.getValue());

        // docker image info
        Protos.ContainerInfo.DockerInfo.Builder dockerInfoBuilder = Protos.ContainerInfo.DockerInfo.newBuilder();
        dockerInfoBuilder.setImage(imageName);
        dockerInfoBuilder.setNetwork(Protos.ContainerInfo.DockerInfo.Network.BRIDGE);

        // container info
        Protos.ContainerInfo.Builder containerInfoBuilder = Protos.ContainerInfo.newBuilder();
        containerInfoBuilder.setType(Protos.ContainerInfo.Type.DOCKER);
        containerInfoBuilder.setDocker(dockerInfoBuilder.build());

        // create task to run
        Protos.TaskInfo task = Protos.TaskInfo.newBuilder()
            .setName("task " + taskId.getValue())
            .setTaskId(taskId)
            .setSlaveId(offer.getSlaveId())
            .addResources(Protos.Resource.newBuilder()
                .setName("cpus")
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(1)))
            .addResources(Protos.Resource.newBuilder()
                .setName("mem")
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(128)))
            .setContainer(containerInfoBuilder)
            .setCommand(Protos.CommandInfo.newBuilder().setShell(false))
            .build();

        tasks.add(task);
      }
      Protos.Filters filters = Protos.Filters.newBuilder().setRefuseSeconds(1).build();
      schedulerDriver.launchTasks(offer.getId(), tasks, filters);
    }
  }

  @Override
  public void offerRescinded(SchedulerDriver schedulerDriver, Protos.OfferID offerID) {
    System.out.println("offerRescinded()");
  }

  @Override
  public void statusUpdate(SchedulerDriver driver, Protos.TaskStatus taskStatus) {

    final String taskId = taskStatus.getTaskId().getValue();

    System.out.format("statusUpdate() task %s is in state %s\n",
        taskId, taskStatus.getState());

    switch (taskStatus.getState()) {
      case TASK_RUNNING:
        pendingInstances.remove(taskId);
        runningInstances.add(taskId);
        break;
      case TASK_FAILED:
      case TASK_FINISHED:
        pendingInstances.remove(taskId);
        runningInstances.remove(taskId);
        break;
    }

    System.out.format("Number of instances: pending=%d, running=%d\n",
        pendingInstances.size(), runningInstances.size());
  }

  @Override
  public void frameworkMessage(SchedulerDriver schedulerDriver, Protos.ExecutorID executorID, Protos.SlaveID slaveID, byte[] bytes) {
    System.out.println("frameworkMessage()");
  }

  @Override
  public void disconnected(SchedulerDriver schedulerDriver) {
    System.out.println("disconnected()");
  }

  @Override
  public void slaveLost(SchedulerDriver schedulerDriver, Protos.SlaveID slaveID) {
    System.out.println("slaveLost()");
  }

  @Override
  public void executorLost(SchedulerDriver schedulerDriver, Protos.ExecutorID executorID, Protos.SlaveID slaveID, int i) {
    System.out.println("executorLost()");
  }

  @Override
  public void error(SchedulerDriver schedulerDriver, String s) {
    System.out.format("error() %s\n", s);
  }

}
