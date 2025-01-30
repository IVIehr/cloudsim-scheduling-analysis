package taskscheduling;

import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.Cloudlet;

import java.util.ArrayList;
import java.util.List;

public class Job extends Cloudlet {
    private int id;
    private String name;
    private double runtime;
    private int level;
    private List<Uses> usesList;
    private List<Integer> parentIds;
    private List<Integer> childIds;

    public Job(int id, String name, double runtime, int level,
               int cloudletId, long length, int pesNumber, long fileSize, long outputSize, UtilizationModel utilizationModel) {
        super(cloudletId, length, pesNumber, fileSize, outputSize, utilizationModel, utilizationModel, utilizationModel);
        this.id = id;
        this.name = name;
        this.runtime = runtime;
        this.level = level;
        this.usesList = new ArrayList<>();
        this.parentIds = new ArrayList<>();
        this.childIds = new ArrayList<>();
    }

    public void addUses(Uses uses) {
        this.usesList.add(uses);
    }

    public void addParentId(int parentId) {
        this.parentIds.add(parentId);
    }

    public void addChildId(int childId) {
        this.childIds.add(childId);
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public double getRuntime() {
        return runtime;
    }

    public int getLevel() {
        return level;
    }

    public List<Uses> getUsesList() {
        return usesList;
    }

    public List<Integer> getParentIds() {
        return parentIds;
    }

    public List<Integer> getChildIds() {
        return childIds;
    }
}
