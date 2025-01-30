package taskscheduling;

import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.UtilizationModelFull;
import org.w3c.dom.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

public class WorkflowParser {
    private static String getResourcePath(String resourceFile) {
        try {
            // Get the resource as a URL
            ClassLoader classLoader = WorkflowParser.class.getClassLoader();
            URL resource = classLoader.getResource(resourceFile);

            if (resource == null) {
                System.err.println("Error: Resource file not found: " + resourceFile);
                return null;
            }

            // If it's a normal file in the file system, return the absolute path
            File file = new File(resource.toURI());
            if (file.exists()) {
                return file.getAbsolutePath();
            }

            // If running from a JAR, extract the file to a temp directory
            InputStream inputStream = classLoader.getResourceAsStream(resourceFile);
            if (inputStream != null) {
                Path tempFile = Files.createTempFile("workflow_", ".xml");
                Files.copy(inputStream, tempFile, StandardCopyOption.REPLACE_EXISTING);
                return tempFile.toAbsolutePath().toString();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    public static List<Job> parseWorkflow(String filePath) {
        List<Job> jobList = new ArrayList<>();

        try {
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document document = dBuilder.parse(getResourcePath(filePath));
            document.getDocumentElement().normalize();

            // Parse Jobs
            NodeList jobNodes = document.getElementsByTagName("job");
            for (int i = 0; i < jobNodes.getLength(); i++) {
                Element jobElement = (Element) jobNodes.item(i);
                Job job = parseJob(jobElement);
                jobList.add(job);
            }

            // Parse Parent-Child Relationships
            parseDependencies(document, jobList);

        } catch (Exception e) {
            e.printStackTrace();
        }

        return jobList;
    }

    private static Job parseJob(Element jobElement) {
        String id = jobElement.getAttribute("id");
        String name = jobElement.getAttribute("name");
        double runtime = Double.parseDouble(jobElement.getAttribute("runtime"));
        int level = jobElement.hasAttribute("level") ? Integer.parseInt(jobElement.getAttribute("level")) : -1;

        long length = (long) (runtime * 1000); // Convert job runtime to CloudSim MI (Million Instructions)
        int pesNumber = 1; // Number of CPU cores required
        long fileSize = 300; // Arbitrary file size
        long outputSize = 300; // Arbitrary output size
        UtilizationModel utilizationModel = new UtilizationModelFull();

        Job job = new Job(Integer.parseInt(id.replaceAll("\\D+", "")), name, runtime, level,
                Integer.parseInt(id.replaceAll("\\D+", "")), length, pesNumber, fileSize, outputSize, utilizationModel);

        // Parse Uses (Input/Output Files)
        NodeList usesNodes = jobElement.getElementsByTagName("uses");
        for (int i = 0; i < usesNodes.getLength(); i++) {
            Element usesElement = (Element) usesNodes.item(i);
            String file = usesElement.getAttribute("file");
            String type = usesElement.getAttribute("type"); // "input" or "output"
            job.addUses(new Uses(file, type));
        }

        return job;
    }

    private static void parseDependencies(Document document, List<Job> jobs) {
        NodeList childNodes = document.getElementsByTagName("child");

        for (int i = 0; i < childNodes.getLength(); i++) {
            Element childElement = (Element) childNodes.item(i);
            String childIdRaw = childElement.getAttribute("ref");
            int childId = Integer.parseInt(childIdRaw.replaceAll("\\D+", ""));

            NodeList parentNodes = childElement.getElementsByTagName("parent");
            List<Integer> parents = new ArrayList<>();
            for (int j = 0; j < parentNodes.getLength(); j++) {
                Element parentElement = (Element) parentNodes.item(j);
                String parentIdRaw = parentElement.getAttribute("ref");
                int parentId = Integer.parseInt(parentIdRaw.replaceAll("\\D+", ""));
                parents.add(parentId);

                // Add relationships
                for (Job job : jobs) {
                    if (job.getId() == parentId) {
                        job.addChildId(childId);
                    }
                    if (job.getId() == childId) {
                        job.addParentId(parentId);
                    }
                }
            }
        }
    }
}
