package scheduler;

import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;

import java.util.ArrayList;
import java.util.List;

public class CountToTenScheduler implements org.apache.mesos.Scheduler {

    private int launchedTasks = 0;
    private final org.apache.mesos.Protos.ExecutorInfo countToTenExecutor;

    public CountToTenScheduler(org.apache.mesos.Protos.ExecutorInfo countToTenExecutor) {
        this.countToTenExecutor = countToTenExecutor;
    }

    @Override
    public void registered(SchedulerDriver schedulerDriver, org.apache.mesos.Protos.FrameworkID frameworkID,
                           org.apache.mesos.Protos.MasterInfo masterInfo) {

    }

    @Override
    public void reregistered(SchedulerDriver schedulerDriver, org.apache.mesos.Protos.MasterInfo masterInfo) {

    }

    private static final int NUMBER_OF_CPUS = 1;
    private static final int AMOUNT_OF_MEMORY_IN_MB = 128;

    @Override
    public void resourceOffers(SchedulerDriver schedulerDriver, List<org.apache.mesos.Protos.Offer> list) {

        for (Protos.Offer offer : list) {
            List<Protos.TaskInfo> tasks = new ArrayList<>();
            Protos.TaskID taskId = Protos.TaskID.newBuilder().setValue(Integer.toString(launchedTasks++)).build();
            System.out.println("Launching CountToTen " + taskId.getValue() + " Count To Ten Java");

            Protos.Resource.Builder cpus = Protos.Resource.newBuilder()
                    .setName("cpus")
                    .setType(Protos.Value.Type.SCALAR)
                    .setScalar(Protos.Value.Scalar.newBuilder().setValue(NUMBER_OF_CPUS));

            Protos.Resource.Builder memory = Protos.Resource.newBuilder()
                    .setName("memory")
                    .setType(Protos.Value.Type.SCALAR)
                    .setScalar(Protos.Value.Scalar.newBuilder().setValue(AMOUNT_OF_MEMORY_IN_MB));

            org.apache.mesos.Protos.TaskInfo printNumbers = org.apache.mesos.Protos.TaskInfo.newBuilder()
                    .setName("printNumbers " + taskId.getValue())
                    .setTaskId(taskId)
                    .setSlaveId(offer.getSlaveId())
                    .addResources(cpus)
                    .addResources(memory)
                    .setExecutor(org.apache.mesos.Protos.ExecutorInfo.newBuilder(countToTenExecutor))
                    .build();

            List<Protos.OfferID> offerIDS = new ArrayList<>();
            offerIDS.add(offer.getId());

            tasks.add(printNumbers);

            schedulerDriver.declineOffer(offer.getId());
            schedulerDriver.launchTasks(offerIDS, tasks);
        }
    }

    @Override
    public void offerRescinded(SchedulerDriver schedulerDriver, org.apache.mesos.Protos.OfferID offerID) {

    }

    @Override
    public void statusUpdate(SchedulerDriver schedulerDriver, org.apache.mesos.Protos.TaskStatus taskStatus) {

    }

    @Override
    public void frameworkMessage(SchedulerDriver schedulerDriver, org.apache.mesos.Protos.ExecutorID executorID,
                                 org.apache.mesos.Protos.SlaveID slaveID, byte[] bytes) {

    }

    @Override
    public void disconnected(SchedulerDriver schedulerDriver) {

    }

    @Override
    public void slaveLost(SchedulerDriver schedulerDriver, org.apache.mesos.Protos.SlaveID slaveID) {

    }

    @Override
    public void executorLost(SchedulerDriver schedulerDriver, org.apache.mesos.Protos.ExecutorID executorID,
                             org.apache.mesos.Protos.SlaveID slaveID, int i) {

    }

    @Override
    public void error(SchedulerDriver schedulerDriver, String s) {

    }
}
