package executor;

import org.apache.mesos.*;
import org.apache.mesos.Executor;

public class CountToTenExecutor implements Executor {

    public static void main(String[] args) {
        MesosExecutorDriver driver = new MesosExecutorDriver(new CountToTenExecutor());
        System.exit(driver.run() == Protos.Status.DRIVER_STOPPED ? 0 : 1);
    }

    @Override
    public void registered(ExecutorDriver executorDriver, Protos.ExecutorInfo executorInfo,
                           Protos.FrameworkInfo frameworkInfo, Protos.SlaveInfo slaveInfo) {

    }

    @Override
    public void reregistered(ExecutorDriver executorDriver, Protos.SlaveInfo slaveInfo) {

    }

    @Override
    public void disconnected(ExecutorDriver executorDriver) {

    }

    @Override
    public void launchTask(ExecutorDriver executorDriver, Protos.TaskInfo taskInfo) {
        Protos.TaskStatus status = Protos.TaskStatus.newBuilder()
                .setTaskId(taskInfo.getTaskId())
                .setState(Protos.TaskState.TASK_RUNNING)
                .build();
        executorDriver.sendStatusUpdate(status);

        String myStatus = "Hello Framework";
        executorDriver.sendFrameworkMessage(myStatus.getBytes());

        System.out.println("Execute Task");

        status = Protos.TaskStatus.newBuilder()
                .setTaskId(taskInfo.getTaskId())
                .setState(Protos.TaskState.TASK_FINISHED)
                .build();
        executorDriver.sendStatusUpdate(status);
    }

    @Override
    public void killTask(ExecutorDriver executorDriver, Protos.TaskID taskID) {

    }

    @Override
    public void frameworkMessage(ExecutorDriver executorDriver, byte[] bytes) {

    }

    @Override
    public void shutdown(ExecutorDriver executorDriver) {

    }

    @Override
    public void error(ExecutorDriver executorDriver, String s) {

    }
}
