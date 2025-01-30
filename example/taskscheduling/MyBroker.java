package taskscheduling;

import java.util.*;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.lists.VmList;


public class MyBroker extends DatacenterBroker {

    private final int datacenterId = 2;
    private static final double SCHEDULE_INTERVAL = 600; // 10 minutes in simulated time
    private boolean hasInitialized = false; // Prevent duplicate VM creation

    private final Set<Integer> completedJobs = new HashSet<>();
    private final Set<Integer> runningVms = new HashSet<>();
    private final SchedulingAlgorithm schedulingAlgorithm;
    private final Map<Integer, Integer> jobDepths = new HashMap<>();
    private Map<Integer, List<Integer>> jobDependencies;

    public enum SchedulingAlgorithm {
        SJF, LB, STB
    }

    public MyBroker(String name, SchedulingAlgorithm algorithm) throws Exception {
        super(name);
        this.schedulingAlgorithm = algorithm;
    }

    public void setJobDependencies(Map<Integer, List<Integer>> dependencies) {
        this.jobDependencies = dependencies;
    }


    @Override
    public void startEntity() {
        if (getVmList().isEmpty()) {
            System.err.println("No VMs available.");
            return;
        }

        if (getCloudletList().isEmpty()) {
            System.err.println("No cloudlets available.");
            return;
        }

        // Submit VMs at the start
        if (!hasInitialized) {
            initializeVms();
            hasInitialized = true;
        }

        // Start the periodic scheduling loop
        schedule(getId(), SCHEDULE_INTERVAL, CloudSimTags.VM_DATACENTER_EVENT);
    }

    private void initializeVms() {
        for (Vm vm : getVmList()) {
            sendNow(datacenterId, CloudSimTags.VM_CREATE_ACK, vm);
        }
    }


    @Override
    public void processEvent(SimEvent ev) {
        switch (ev.getTag()) {
            case CloudSimTags.VM_CREATE_ACK:
                handleVmCreation(ev);
                break;
            case CloudSimTags.CLOUDLET_RETURN:
                handleTaskCompletion(ev);
                break;
            case CloudSimTags.VM_DATACENTER_EVENT:
                executePeriodicScheduling();
                break;
            case CloudSimTags.END_OF_SIMULATION:
                shutdownEntity();
                break;
            default:
                processOtherEvent(ev);
                break;
        }
    }

    private void handleVmCreation(SimEvent ev) {
        int[] data = (int[]) ev.getData();
        int datacenterId = data[0];
        int vmId = data[1];
        int result = data[2];

        if (result == CloudSimTags.TRUE) {
            Vm vm = VmList.getById(getVmList(), vmId);
            if (vm != null) {
                getVmsCreatedList().add(vm); // Add successfully created VM to the tracking list
                System.out.println(CloudSim.clock() + ": " + getName() + " - VM #" + vmId + " successfully created.");
            }
        } else {
            System.out.println(CloudSim.clock() + ": " + getName() + " - VM #" + vmId + " creation failed.");
        }
    }


    private void executePeriodicScheduling() {
        System.out.println("Periodic scheduling triggered at: " + CloudSim.clock());

        List<Cloudlet> readyTasks = filterReadyTasks();
        if (readyTasks.isEmpty()) {
            System.out.println("No tasks are ready for scheduling.");
            return;
        }

        List<Cloudlet> cloudletList = new ArrayList<>();
        for (Object obj : readyTasks) {
            if (obj instanceof Cloudlet) {
                cloudletList.add((Cloudlet) obj);
            }
        }

        // Apply the selected scheduling algorithm
        switch (schedulingAlgorithm) {
            case SJF:
                applySJFScheduling(readyTasks);
                break;
            case LB:
                applyLBScheduling(getCloudletList(), jobDepths, jobDependencies);
                break;
            case STB:
                applySTBScheduling(cloudletList, computeSubsequentTaskCounts(jobDependencies));
                break;
        }

        // Schedule next execution
        schedule(getId(), SCHEDULE_INTERVAL, CloudSimTags.VM_DATACENTER_EVENT);
    }

    private void applySJFScheduling(List<Cloudlet> readyTasks) {

        // Sort ready tasks by execution time
        readyTasks.sort(Comparator.comparingLong(Cloudlet::getCloudletLength));

        // Sort available VMs by processing power
        List<Vm> availableVms = findAvailableVms();
        availableVms.sort(Comparator.comparingDouble(Vm::getMips).reversed());


        if (availableVms.isEmpty()) {
            // all Vms are busy
            System.out.println("No available VMs.");
            return;
        }

        Queue<Vm> vmQueue = new LinkedList<>(availableVms);
        List<Cloudlet> unscheduledCloudlets = new ArrayList<>(readyTasks); // Track unassigned tasks

        // Assign the shortest tasks to the fastest VMs
        for (Cloudlet task : readyTasks) {
            if (vmQueue.isEmpty()) {
                System.out.println("No VM available for task " + task.getCloudletId());
                break;
            }

            Vm vm = vmQueue.poll();
            task.setVmId(vm.getId());
            System.out.println("Assigning task " + task.getCloudletId() + " to VM " + vm.getId());

            sendNow(datacenterId, CloudSimTags.CLOUDLET_SUBMIT, task);
            runningVms.add(vm.getId());
            unscheduledCloudlets.remove(task);
        }

        // Instead of removing all tasks, keep unassigned ones for the next cycle
        getCloudletList().clear();
        getCloudletList().addAll(unscheduledCloudlets);
    }

    private void applyLBScheduling(List<Cloudlet> cloudlets, Map<Integer, Integer> jobDepths, Map<Integer, List<Integer>> jobDependencies) {
        if (cloudlets.isEmpty()) return;

        // Compute job depths only if missing
        if (jobDepths.isEmpty()) {
            jobDepths.putAll(computeJobDepths(getCloudletList(), jobDependencies));
        }

        // Sort tasks by depth (smallest first)
        cloudlets.sort(Comparator.comparingInt(task -> {
            int taskId = task.getCloudletId();
            return jobDepths.getOrDefault(taskId, -1);
        }));

        // Sort available VMs by speed (fastest first)
        List<Vm> availableVms = findAvailableVms();
        availableVms.sort(Comparator.comparingDouble(Vm::getMips).reversed());

        if (availableVms.isEmpty()) {
            System.out.println("No available VMs. Tasks will wait.");
            return;
        }

        Queue<Vm> vmQueue = new LinkedList<>(availableVms);
        List<Cloudlet> unscheduledCloudlets = new ArrayList<>(cloudlets);

        // Assign lowest-depth tasks to fastest VMs
        for (Cloudlet task : cloudlets) {
            if (vmQueue.isEmpty()) {
                System.out.println("No VM available for task " + task.getCloudletId());
                break;
            }

            Vm vm = vmQueue.poll(); // Get the fastest available VM
            task.setVmId(vm.getId());

            int taskId = task.getCloudletId();
            System.out.println("Assigning Task " + taskId +
                    " (Depth: " + jobDepths.getOrDefault(taskId, -1) + ") to VM " + vm.getId());

            sendNow(datacenterId, CloudSimTags.CLOUDLET_SUBMIT, task);
            runningVms.add(vm.getId());
            unscheduledCloudlets.remove(task);
        }

        // Keep unassigned tasks for the next cycle
        getCloudletList().clear();
        getCloudletList().addAll(unscheduledCloudlets);
    }

    private void applySTBScheduling(List<Cloudlet> cloudlets, Map<Integer, Integer> subsequentTaskCounts) {
        if (cloudlets.isEmpty()) return;

        cloudlets.sort(Comparator.comparingInt(task -> {
            if (!(task instanceof Cloudlet)) {
                throw new IllegalStateException("Found an invalid object in cloudlets list: " + task);
            }

            Cloudlet cloudletTask = (Cloudlet) task;  //  Explicitly cast to Cloudlet
            return subsequentTaskCounts.getOrDefault(cloudletTask.getCloudletId(), 0);

        }).reversed());


        // Sort available VMs by speed
        List<Vm> availableVms = findAvailableVms();
        availableVms.sort(Comparator.comparingDouble(Vm::getMips).reversed());

        if (availableVms.isEmpty()) {
            System.out.println("No available VMs. Tasks will wait.");
            return;
        }

        Queue<Vm> vmQueue = new LinkedList<>(availableVms);
        List<Cloudlet> unscheduledCloudlets = new ArrayList<>(cloudlets);

        // Assign highest-priority tasks to fastest VMs
        for (Cloudlet task : cloudlets) {
            if (vmQueue.isEmpty()) {
                System.out.println("No VM available for task " + task.getCloudletId());
                break;
            }

            Vm vm = vmQueue.poll();
            task.setVmId(vm.getId());

            int subsequentCount = subsequentTaskCounts.getOrDefault(task.getCloudletId(), 0);

            System.out.println("Assigning Task " + task.getCloudletId() +
                    " (Subsequent Tasks: " + subsequentCount + ") to VM " + vm.getId());

            sendNow(datacenterId, CloudSimTags.CLOUDLET_SUBMIT, task);
            runningVms.add(vm.getId());
            unscheduledCloudlets.remove(task);
        }

        // Keep unassigned tasks for the next cycle
        getCloudletList().clear();
        getCloudletList().addAll(unscheduledCloudlets);
    }


    private List<Vm> findAvailableVms() {
        List<Vm> availableVms = new ArrayList<>();
        for (Vm vm : getVmsCreatedList()) {
            if (!runningVms.contains(vm.getId())) {
                availableVms.add(vm);
            }
        }
        return availableVms;
    }

    protected void handleTaskCompletion(SimEvent ev) {
        Cloudlet cloudlet = (Cloudlet) ev.getData();
        System.out.println("Task Completed: " + cloudlet.getCloudletId());

        completedJobs.add(cloudlet.getCloudletId());
        runningVms.remove(cloudlet.getVmId());

        if (getCloudletList().isEmpty() && runningVms.isEmpty()) {
            shutdownEntity();
        }
    }

    private List<Cloudlet> filterReadyTasks() {
        List<Cloudlet> readyTasks = new ArrayList<>();
        for (Cloudlet cloudlet : getCloudletList()) {
            Job job = (Job) cloudlet;
            if (areDependenciesCompleted(job)) {
                readyTasks.add(cloudlet);
            }
        }
        return readyTasks;
    }

    private boolean areDependenciesCompleted(Job job) {
        for (int parentId : job.getParentIds()) {
            if (!completedJobs.contains(parentId)) {
                System.out.println("Task " + job.getId() + " is waiting for parent " + parentId);
                return false;
            }
        }
        return true;
    }

    public Map<Integer, Integer> getJobDepths() {
        return jobDepths;
    }

    private Map<Integer, Integer> computeJobDepths(List<Job> jobs, Map<Integer, List<Integer>> jobDependencies) {
        Map<Integer, Integer> jobDepths = new HashMap<>();

        for (Job job : jobs) {
            int level = job.getLevel();
            if (level != -1) {
                jobDepths.put(job.getId(), level);
            } else {
                int computedDepth = computeDepth(job.getId(), jobDependencies, jobDepths);
                jobDepths.put(job.getId(), computedDepth);
            }
        }

        return jobDepths;
    }

    private int computeDepth(int jobId, Map<Integer, List<Integer>> jobDependencies, Map<Integer, Integer> jobDepths) {
        if (jobDepths.containsKey(jobId) && jobDepths.get(jobId) != -1) {
            return jobDepths.get(jobId);
        }

        if (!jobDependencies.containsKey(jobId) || jobDependencies.get(jobId).isEmpty()) {
            jobDepths.put(jobId, 0);
            System.out.println("Task " + jobId + " is a root task. Assigned Depth 0.");
            return 0;
        }

        int maxParentDepth = 0;
        for (int parent : jobDependencies.get(jobId)) {
            if (!jobDepths.containsKey(parent) || jobDepths.get(parent) == -1) {
                jobDepths.put(parent, computeDepth(parent, jobDependencies, jobDepths));
            }
            maxParentDepth = Math.max(maxParentDepth, jobDepths.get(parent));
        }

        int depth = maxParentDepth + 1;
        jobDepths.put(jobId, depth);
        return depth;
    }


    public Map<Integer, List<Integer>> extractJobDependencies(List<Job> jobs) {
        Map<Integer, List<Integer>> dependencies = new HashMap<>();

        for (Job job : jobs) {
            dependencies.put(job.getId(), new ArrayList<>());
        }

        for (Job job : jobs) {
            for (int parentId : job.getParentIds()) {
                dependencies.computeIfAbsent(parentId, k -> new ArrayList<>()).add(job.getId());
            }
        }

        return dependencies;
    }

    private Map<Integer, Integer> computeSubsequentTaskCounts(Map<Integer, List<Integer>> dependencies) {
        Map<Integer, Integer> subsequentTaskCounts = new HashMap<>();

        // Initialize all tasks with 0 subsequent tasks
        for (int jobId : dependencies.keySet()) {
            subsequentTaskCounts.put(jobId, 0);
        }

        // Use a recursive function to compute total descendant tasks
        for (int jobId : dependencies.keySet()) {
            subsequentTaskCounts.put(jobId, countDescendants(jobId, dependencies, new HashSet<>()));
        }

        return subsequentTaskCounts;
    }

    private int countDescendants(int jobId, Map<Integer, List<Integer>> dependencies, Set<Integer> visited) {
        if (!dependencies.containsKey(jobId) || dependencies.get(jobId).isEmpty()) {
            return 0; // No children → No subsequent tasks
        }

        if (visited.contains(jobId)) {
            return 0; // Prevent infinite loops in cyclic graphs
        }

        visited.add(jobId);

        int count = 0;
        for (int childJob : dependencies.get(jobId)) {
            count += 1 + countDescendants(childJob, dependencies, visited);
        }

        return count;
    }

}