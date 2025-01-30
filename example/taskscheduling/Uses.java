package taskscheduling;

public class Uses {
    private String file;
    private String type; // "input" or "output"

    public Uses(String file, String type) {
        this.file = file;
        this.type = type;
    }

    public String getFile() {
        return file;
    }

    public String getType() {
        return type;
    }
}
