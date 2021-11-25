package scheduler;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import job.Job;
import job.JobState;
import org.apache.mesos.*;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.*;

public class SlurmFrameworkScheduler implements Scheduler {

    private List<Job> jobs;

    public SlurmFrameworkScheduler() {
        Gson gson = new Gson();
        String fileName = "jobs.json";
        try {
            JsonReader reader = new JsonReader(new FileReader(fileName));
            jobs = gson.fromJson(reader, new TypeToken<List<Job>>() {
            }.getType());
        } catch (FileNotFoundException e) {
            System.out.println("Could not find the file: " + fileName);
            jobs = new ArrayList<>();
        }
    }

    @Override
    public void registered(SchedulerDriver schedulerDriver, Protos.FrameworkID frameworkID,
                           Protos.MasterInfo masterInfo) {
        System.out.println("Framework registered with master: " + masterInfo.getId()
                + ", with framework id: " + frameworkID.getValue());
    }

    @Override
    public void reregistered(SchedulerDriver schedulerDriver, Protos.MasterInfo masterInfo) {
        System.out.println("Framework re-registered with master: " + masterInfo.getId());
    }

    @Override
    public void resourceOffers(SchedulerDriver schedulerDriver, List<Protos.Offer> list) {
        checkIfJobsRemain(schedulerDriver);
        Queue<Job> pendingJobs = new LinkedList<>();
        for (Job job : jobs) {
            if (job.getState() == JobState.PENDING) {
                pendingJobs.add(job);
            }
        }
        for (Protos.Offer offer : list) {
            if (pendingJobs.isEmpty()) {
                schedulerDriver.declineOffer(offer.getId());
                break;
            }
            schedulerDriver.launchTasks(Collections.singletonList(offer.getId()),
                    doFirstFit(offer, pendingJobs));
        }
    }

    /**
     * Terminates the framework if no more jobs remain.
     *
     * @param schedulerDriver
     */
    private void checkIfJobsRemain(SchedulerDriver schedulerDriver) {
        boolean finishedAllJobs = true;
        for (Job job : jobs) {
            if (job.getState() == JobState.PENDING
                    || job.getState() == JobState.STAGING
                    || job.getState() == JobState.RUNNING) {
                finishedAllJobs = false;
                break;
            }
        }
        if (finishedAllJobs) {
            schedulerDriver.stop();
        }
    }

    private Collection<Protos.TaskInfo> doFirstFit(Protos.Offer offer, Queue<Job> pendingJobs) {
        List<Protos.TaskInfo> jobsToBeLaunched = new ArrayList<>();
        List<Job> launchedJobs = new ArrayList<>();
        double offerCpus = 0;
        double offerMemory = 0;
        for (Protos.Resource resource : offer.getResourcesList()) {
            if (resource.getName().equals("cpus")) {
                offerCpus += resource.getScalar().getValue();
            } else if (resource.getName().equals("mem")) {
                offerMemory += resource.getScalar().getValue();
            }
        }
        for (Job job : pendingJobs) {
            double jobCpuRequirement = job.getCpus();
            double jobMemoryRequirement = job.getMemory();
            if (jobCpuRequirement <= offerCpus && jobMemoryRequirement <= offerMemory) {
                offerCpus -= jobCpuRequirement;
                offerMemory -= jobMemoryRequirement;
                jobsToBeLaunched.add(job.makeJobIntoTask(offer.getSlaveId()));
                launchedJobs.add(job);
                job.launch();
            }
        }
        for (Job job : launchedJobs) {
            job.started();
        }
        pendingJobs.removeAll(launchedJobs);
        return jobsToBeLaunched;
    }

    @Override
    public void offerRescinded(SchedulerDriver schedulerDriver, Protos.OfferID offerID) {
        System.out.println("Offer with ID: " + offerID.getValue() + " was rescinded.");
    }

    @Override
    public void statusUpdate(SchedulerDriver schedulerDriver, Protos.TaskStatus taskStatus) {
        for (Job job : jobs) {
            if (job.getId().equals(taskStatus.getTaskId().getValue())) {
                Protos.TaskState newTaskState = taskStatus.getState();
                if (newTaskState == Protos.TaskState.TASK_RUNNING) {
                    job.started();
                } else if (newTaskState == Protos.TaskState.TASK_FINISHED) {
                    job.succeeded();
                } else if (newTaskState == Protos.TaskState.TASK_ERROR) {
                    job.failed();
                }
                break;
            }
        }
    }

    @Override
    public void frameworkMessage(SchedulerDriver schedulerDriver, Protos.ExecutorID executorID, Protos.SlaveID slaveID,
                                 byte[] bytes) {
        String message = new String(bytes);
        System.out.println("Received a message from executor: " + executorID.getValue()
                + ",\nrunning on Mesos-agent with ID: " + slaveID.getValue()
                + ",\nmessage: " + message);
    }

    @Override
    public void disconnected(SchedulerDriver schedulerDriver) {
        System.out.println("Framework disconnected from master.");
    }

    @Override
    public void slaveLost(SchedulerDriver schedulerDriver, Protos.SlaveID slaveID) {
        System.out.println("Lost connection to Mesos-agent with ID: " + slaveID.getValue());
    }

    @Override
    public void executorLost(SchedulerDriver schedulerDriver, Protos.ExecutorID executorID, Protos.SlaveID slaveID,
                             int i) {
        System.out.println("Executor: " + executorID.getValue()
                + ",\nrunning on Mesos-agent with ID: " + slaveID.getValue()
                + ", has terminated.");
    }

    @Override
    public void error(SchedulerDriver schedulerDriver, String s) {
        System.out.println("Framework encountered an error: " + s);
    }
}
