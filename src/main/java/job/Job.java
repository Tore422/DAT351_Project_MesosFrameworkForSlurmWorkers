package job;

import org.apache.mesos.Protos;

import java.util.UUID;

public class Job {

    private final String id;
    private double cpus;
    private double memory;
    private String command;
    private int numberOfRetries;
    private JobState state;

    private static final int MAX_NUMBER_OF_RETRIES = 5;

    public Job() {
        this.id = UUID.randomUUID().toString();
        this.numberOfRetries = MAX_NUMBER_OF_RETRIES;
        this.state = JobState.PENDING;
    }

    public Job(double cpus, double memory, String command) {
        this.id = UUID.randomUUID().toString();
        this.cpus = cpus;
        this.memory = memory;
        this.command = command;
        this.numberOfRetries = MAX_NUMBER_OF_RETRIES;
        this.state = JobState.PENDING;
    }

    public Protos.TaskInfo makeJobIntoTask(Protos.SlaveID targetSlaveID) {
        UUID uuid = UUID.randomUUID();
        Protos.TaskID taskID = Protos.TaskID.newBuilder()
                .setValue(uuid.toString())
                .build();
        return Protos.TaskInfo.newBuilder()
                .setName("task " + taskID.getValue())
                .setTaskId(taskID)
                .addResources(Protos.Resource.newBuilder()
                        .setName("cpus")
                        .setType(Protos.Value.Type.SCALAR)
                        .setScalar(Protos.Value.Scalar.newBuilder()
                                .setValue(this.cpus)))
                .addResources(Protos.Resource.newBuilder()
                        .setName("memory")
                        .setType(Protos.Value.Type.SCALAR)
                        .setScalar(Protos.Value.Scalar.newBuilder()
                                .setValue(this.memory)))
                .setSlaveId(targetSlaveID)
                .setCommand(Protos.CommandInfo.newBuilder()
                        .setValue(this.command))
                .build();
    }

    public void launch() {
        state = JobState.STAGING;
    }

    public void started() {
        state = JobState.RUNNING;
    }

    public void succeeded() {
        state = JobState.SUCCESSFUL;
    }

    public void failed() {
        if (numberOfRetries <= 0) {
            state = JobState.FAILED;
        } else {
            numberOfRetries--;
            state = JobState.PENDING;
        }
    }

    public String getId() {
        return id;
    }

    public double getCpus() {
        return cpus;
    }

    public double getMemory() {
        return memory;
    }

    public String getCommand() {
        return command;
    }

    public int getNumberOfRetries() {
        return numberOfRetries;
    }

    public JobState getState() {
        return state;
    }

    @Override
    public String toString() {
        return "Job = {Id: " + this.id
                + " , CPU: " + this.cpus
                + " , Memory: " + this.memory
                + " , Number of retries: " + this.numberOfRetries
                + " , State: " + this.state
                + " , Command: '" + this.command + "' }";
    }
}
