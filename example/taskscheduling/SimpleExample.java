package taskscheduling;

import java.util.*;

import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;


public class SimpleExample {

    @SuppressWarnings("unused")

    private static List<Cloudlet> createCloudlets(List<Job> jobs, int brokerId) {
        List<Cloudlet> cloudletList = new ArrayList<>();

        if (jobs == null || jobs.isEmpty()) {
            return cloudletList;
        }

        int id = 0; // Cloudlet ID counter
        for (Job job : jobs) {
            long length = (long) (job.getRuntime() * 1000); // Convert job runtime to CloudSim MI (Million Instructions)
            int pesNumber = 1; // Number of CPU cores required
            long fileSize = 300; // Arbitrary file size
            long outputSize = 300; // Arbitrary output size
            UtilizationModel utilizationModel = new UtilizationModelFull();

            // Create Cloudlet from Job details
            Job cloudlet = new Job(job.getId(), job.getName(), job.getRuntime(), job.getLevel(), id, length, pesNumber, fileSize, outputSize,
                    utilizationModel);
            cloudlet.setUserId(brokerId); // Assign Cloudlet to Broker
            cloudletList.add(cloudlet);
            id++;
        }
        return cloudletList;
    }

    private static List<Vm> createVMs(int brokerId) {
        List<Vm> vmList = new ArrayList<>();
        int[] lengths = {800, 1200, 1600}; // 3 different VM types
        int numReplicas = 4; // 4 replicas of each VM type
        int id = 0; // Unique ID for each VM

        for (int length : lengths) {
            for (int i = 0; i < numReplicas; i++) {
                int ram = 512; // VM memory (MB)
                long bw = 1000; // Bandwidth
                long size = 10000; // VM storage
                int pesNumber = 1; // Number of CPU cores
                String vmm = "Xen";

                Vm vm = new Vm(id, brokerId, length, pesNumber, ram, bw, size, vmm, new CloudletSchedulerTimeShared());
                vmList.add(vm);
                id++;
            }
        }
        return vmList;
    }

    public static void main(String[] args) {
        Log.printLine("Starting Cloudsim Workflow Simulation...");

        try {
            CloudSim.init(1, Calendar.getInstance(), false);
            Datacenter datacenter = createDatacenter("My_Datacenter");


            // Algorithms to test
            MyBroker.SchedulingAlgorithm[] algorithms = {
//                    MyBroker.SchedulingAlgorithm.SJF,
//                    MyBroker.SchedulingAlgorithm.LB,
                    MyBroker.SchedulingAlgorithm.STB
            };

            // Workflows to test
            String[] workflows = {
//                    "resources/CyberShake_500_1.xml",
                    "resources/LIGO_500_1.xml",
//                    "resources/SIPHT_500_1.xml",
//                    "resources/Montage_500_1.xml"
            };

            for (String workflow : workflows) {
                for (MyBroker.SchedulingAlgorithm algorithm : algorithms) {
                    System.out.println("\nRunning workflow: " + workflow + " with algorithm: " + algorithm);
                    long realStartTime = System.nanoTime(); // Get start time

                    // Create Broker
                    MyBroker broker = new MyBroker("Broker_" + algorithm.name(), algorithm);

                    // Create VMs and Submit to Broker
                    List<Vm> vmList = createVMs(broker.getId());
                    System.out.println("Created " + vmList.size() + "VMs.");
                    broker.submitVmList(vmList);

                    // Parse Workflow Jobs
                    List<Job> jobs = WorkflowParser.parseWorkflow(workflow);

                    // Convert Jobs to Cloudlets
                    List<Cloudlet> cloudlets = createCloudlets(jobs, broker.getId());

                    // Extract job dependencies
                    Map<Integer, List<Integer>> jobDependencies = broker.extractJobDependencies(jobs);
                    broker.setJobDependencies(jobDependencies);

                    broker.submitCloudletList(cloudlets);

                    // Step 5: Starts the simulation
                    CloudSim.startSimulation();
                    CloudSim.stopSimulation();

                    // Calculate execution time
                    long realEndTime = System.nanoTime();
                    double executionTime = (realEndTime - realStartTime) / 1e9;

                    System.out.printf("*** Completed workflow: %s with algorithm: %s in %.2f seconds. ***%n", workflow, algorithm, executionTime);
                }
            }

            Log.printLine("All experiments completed!");
        } catch (Exception e) {
            e.printStackTrace();
            Log.printLine("Unwanted errors happen");
        }
    }


    private static Datacenter createDatacenter(String name) {
        List hostList = new ArrayList<>();
        List peList = new ArrayList<>();

        int hostMips = 30000; // Increased total MIPS capacity
        peList.add(new Pe(0, new PeProvisionerSimple(hostMips)));

        int hostId = 0;
        int ram = 20480; // Host RAM (MB)
        long storage = 1000000; // Host storage
        int bw = 100000; // Bandwidth

        hostList.add(new Host(
                hostId,
                new RamProvisionerSimple(ram),
                new BwProvisionerSimple(bw),
                storage,
                peList,
                new VmSchedulerTimeShared(peList)
        ));

        String arch = "x86";
        String os = "Linux";
        String vmm = "Xen";
        double time_zone = 10.0;
        double cost = 3.0;
        double costPerMem = 0.05;
        double costPerStorage = 0.001;
        double costPerBw = 0.0;
        LinkedList storageList = new LinkedList<>();

        DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
                arch, os, vmm, hostList, time_zone, cost, costPerMem,
                costPerStorage, costPerBw);

        Datacenter datacenter = null;
        try {
            datacenter = new Datacenter(name, characteristics, new VmAllocationPolicySimple(hostList), storageList, 0);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return datacenter;
    }

}