package main;

import org.apache.mesos.*;
import scheduler.SlurmFrameworkScheduler;

public class SlurmFrameworkMain {

    public static void main(String[] args) {
        Protos.FrameworkInfo frameworkInfo = Protos.FrameworkInfo.newBuilder()
                .setName("Slurm Worker Framework")
                .setUser("")
                .build();
        Scheduler slurmFrameworkScheduler = new SlurmFrameworkScheduler();
        String mesosMasterAddress = "zk://" + args[0] + "/mesos";
        SchedulerDriver driver = new MesosSchedulerDriver(slurmFrameworkScheduler, frameworkInfo, mesosMasterAddress);
        int status = driver.run() == Protos.Status.DRIVER_STOPPED ? 0 : 1;
        driver.stop();
        System.exit(status);
    }
}
