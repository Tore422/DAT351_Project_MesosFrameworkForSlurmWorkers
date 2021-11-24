package executor;

import org.apache.mesos.*;

import java.io.IOException;

public class SlurmFrameworkExecutor implements Executor {

    public static void main(String[] args) {
        ExecutorDriver executorDriver = new MesosExecutorDriver(new SlurmFrameworkExecutor());
        System.exit(executorDriver.run() == Protos.Status.DRIVER_STOPPED ? 0 : 1);
    }

    @Override
    public void registered(ExecutorDriver executorDriver, Protos.ExecutorInfo executorInfo,
                           Protos.FrameworkInfo frameworkInfo, Protos.SlaveInfo slaveInfo) {
        System.out.println("Registered executor: " + executorInfo.getExecutorId().getValue()
                + ",\nrunning on Mesos-agent with ID: " + slaveInfo.getId().getValue()
                + ",\nwith framework: " + frameworkInfo.getId().getValue());
    }

    @Override
    public void reregistered(ExecutorDriver executorDriver, Protos.SlaveInfo slaveInfo) {
        System.out.println("Re-registered executor with Mesos-agent: " + slaveInfo.getId().getValue());
    }

    @Override
    public void disconnected(ExecutorDriver executorDriver) {
        System.out.println("Disconnected executor from Mesos-agent");
    }

    @Override
    public void launchTask(ExecutorDriver executorDriver, Protos.TaskInfo taskInfo) {
        System.out.println("SlurmFrameworkExecutor launched task with ID: " + taskInfo.getTaskId().getValue());
        Protos.TaskStatus status = Protos.TaskStatus.newBuilder()
                .setTaskId(taskInfo.getTaskId())
                .setState(Protos.TaskState.TASK_RUNNING)
                .build();
        executorDriver.sendStatusUpdate(status);
        try {
            runProcess(taskInfo.getCommand().getValue());
        } catch (Exception e) {
            System.out.println("Task command failed");
            status = Protos.TaskStatus.newBuilder()
                    .setTaskId(taskInfo.getTaskId())
                    .setState(Protos.TaskState.TASK_FAILED)
                    .build();
            executorDriver.sendStatusUpdate(status);
            Thread.currentThread().interrupt();
        }
        status = Protos.TaskStatus.newBuilder()
                .setTaskId(taskInfo.getTaskId())
                .setState(Protos.TaskState.TASK_FINISHED)
                .build();
        executorDriver.sendStatusUpdate(status);
    }

    private void runProcess(String command) throws IOException, InterruptedException {
        Process process = Runtime.getRuntime().exec(command);
        System.out.println(command + ", stdout: " + process.getInputStream());
        System.out.println(command + ", stderr: " + process.getErrorStream());
        process.waitFor();
        System.out.println(command + ", exitValue() " + process.exitValue());
    }


    @Override
    public void killTask(ExecutorDriver executorDriver, Protos.TaskID taskID) {
        System.out.println("Task with ID: " + taskID.getValue() + ", has been killed.");
    }

    @Override
    public void frameworkMessage(ExecutorDriver executorDriver, byte[] bytes) {
        String message = new String(bytes);
        System.out.println("SlurmFrameworkExecutor received a message: " + message);
    }

    @Override
    public void shutdown(ExecutorDriver executorDriver) {
        System.out.println("SlurmFrameworkExecutor was shutdown.");
    }

    @Override
    public void error(ExecutorDriver executorDriver, String s) {
        System.out.println("SlurmFrameworkExecutor encountered an error: " + s);
    }
}
