package scheduler;

import org.apache.mesos.v1.Protos.FrameworkInfo;
import org.apache.mesos.v1.scheduler.*;

public class MyScheduler implements Scheduler {

    public static void main(String[] args) {
        FrameworkInfo frameworkInfo = FrameworkInfo.newBuilder()
                .setUser("user")
                .setName("Slurm Worker Nodes Job Scheduler Framework")
                .build();

        MyScheduler scheduler = new MyScheduler();
    }

    @Override
    public void connected(Mesos mesos) {

    }

    @Override
    public void disconnected(Mesos mesos) {

    }

    @Override
    public void received(Mesos mesos, Protos.Event event) {

    }
}
