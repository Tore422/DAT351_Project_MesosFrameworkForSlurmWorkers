package main;

import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import scheduler.CountToTenScheduler;

public class SlurmFrameworkMain {

    public static void main(String[] args) {

        String path = System.getProperty("user.dir") + "/target/CountToTen.jar";

        Protos.CommandInfo.URI uri = Protos.CommandInfo.URI.newBuilder()
                .setValue(path)
                .setExtract(false)
                .build();

        String countToTenCommand = "java -cp CountToTen.jar executor.CountToTenExecutor";
        Protos.CommandInfo commandInfoCountToTen = Protos.CommandInfo.newBuilder()
                .setValue(countToTenCommand)
                .addUris(uri)
                .build();

        Protos.ExecutorInfo executorSleep = Protos.ExecutorInfo.newBuilder()
                .setExecutorId(Protos.ExecutorID.newBuilder().setValue("CountToTenExecutor"))
                .setCommand(commandInfoCountToTen)
                .setName("Count To Ten (Java)")
                .build();

        final int FAILOVER_TIMEOUT = 120000;
        Protos.FrameworkInfo.Builder frameworkBuilder = Protos.FrameworkInfo.newBuilder()
                .setFailoverTimeout(FAILOVER_TIMEOUT)
                .setUser("")
                .setName("Count To Ten Framework (Java)");

        frameworkBuilder.setPrincipal("test-framework-java");

        MesosSchedulerDriver driver = new MesosSchedulerDriver(new CountToTenScheduler(),
                frameworkBuilder.build(), args[0]);

        int status = driver.run() == Protos.Status.DRIVER_STOPPED ? 0 : 1;
        driver.stop();
        System.exit(status);
    }


}
