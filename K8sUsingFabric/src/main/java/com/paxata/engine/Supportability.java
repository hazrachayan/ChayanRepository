package com.paxata.engine;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.ContainerResource;
import io.fabric8.kubernetes.client.dsl.LogWatch;
import io.fabric8.kubernetes.client.dsl.PrettyLoggable;

import java.io.File;
import java.io.FileWriter;
import java.util.List;

public class Supportability {
    public static final String MONGO_NAMESPACE = "mongo";
    public static final String MY_NAMESPACE = "chayan-supportability";
    public static final String PAX_POD_NAME = "pax-installation-paxserver";
    public static final String PAX_CONTAINER = "paxserver";
    public static final String MONGO_POD_NAME = "mongo-deployment-";
    public static final String MONGO_CONTAINER = "mongo";
    public static final String PIPELINE_POD_NAME = "pax-installation-pipeline";
    public static final String PIPELINE_MASTER_CONTAINER = "spark-kubernetes-driver";
    public static final String PIPELINE_WORKER_CONTAINER = "executor";
    public static final String PIPELINE_PROXY_POD_NAME = "pax-installation-pipeline-proxy";
    public static final String PIPELINE_PROXY_CONTAINER = "pipeline-proxy";
    public static final int LINES_OF_LOGS = 1000;
    public static final String OUTPUT_DIRECTORY = "./target/output/";

    KubernetesClient client = null;

    Supportability() {
        client = new DefaultKubernetesClient();
    }

    void close(){
        client.close();
    }

    public void namespaceTree(String nameSpace){
        PodList pods = client.pods().inNamespace(nameSpace).list();
        for(Pod pod: pods.getItems())
        {
            System.out.println("Pod: " + pod.getMetadata().getName());
            List<Container> containers = pod.getSpec().getContainers();
            for(int i=0; i<containers.size();i++){
                System.out.println("-> -> Containers: " + containers.get(i).getName());
            }
        }
    }

    public void execute(String nameSpace){
        PodList pods = client.pods().inNamespace(nameSpace).list();
        int workerCounter = 0;
        for(Pod pod: pods.getItems())
        {
            System.out.println("Pod: " + pod.getMetadata().getName());
            if(pod.getMetadata().getName().equalsIgnoreCase(PAX_POD_NAME))
            {
                List<Container> containers = pod.getSpec().getContainers();
                for(int i=0; i<containers.size();i++){
                    if (containers.get(i).getName().equalsIgnoreCase(PAX_CONTAINER)){
                        readLog(nameSpace, pod.getMetadata().getName(), containers.get(i).getName(),
                                LINES_OF_LOGS, new File(OUTPUT_DIRECTORY + File.pathSeparator + "paxserver/logs/frontend.log"));
                        System.out.println("-> -> Containers: " + containers.get(i).getName());

                        downloadFileFromContainer(nameSpace, pod.getMetadata().getName(),
                                containers.get(i).getName(),"/usr/local/paxata/server/config/px.properties",
                                new File(OUTPUT_DIRECTORY + File.pathSeparator + "paxserver/config/px.properties"));

                        downloadFileFromContainer(nameSpace, pod.getMetadata().getName(),
                                containers.get(i).getName(),"/usr/local/paxata/server/config/px-default.properties",
                                new File(OUTPUT_DIRECTORY + File.pathSeparator + "paxserver/config/px-default.properties"));
                    }

                }
            }

            if(pod.getMetadata().getName().contains(MONGO_POD_NAME))
            {
                List<Container> containers = pod.getSpec().getContainers();
                for(int i=0; i<containers.size();i++){
                    if (containers.get(i).getName().equalsIgnoreCase(MONGO_CONTAINER)){
                        readLog(nameSpace, pod.getMetadata().getName(), containers.get(i).getName(),
                                LINES_OF_LOGS, new File(OUTPUT_DIRECTORY + File.pathSeparator + "mongo/logs/mongo.log"));
                        System.out.println("-> -> Containers: " + containers.get(i).getName());
                    }

                }
            }

            if(pod.getMetadata().getName().contains(PIPELINE_POD_NAME))
            {

                List<Container> containers = pod.getSpec().getContainers();
                for(int i=0; i<containers.size();i++){
                    if (containers.get(i).getName().equalsIgnoreCase(PIPELINE_MASTER_CONTAINER)){
                        readLog(nameSpace, pod.getMetadata().getName(), containers.get(i).getName(),
                                LINES_OF_LOGS, new File(OUTPUT_DIRECTORY + File.pathSeparator + "pipeline/logs/master.log"));
                        System.out.println("-> -> Containers: " + containers.get(i).getName());
                    }
                    if (containers.get(i).getName().equalsIgnoreCase(PIPELINE_WORKER_CONTAINER)){
                        readLog(nameSpace, pod.getMetadata().getName(), containers.get(i).getName(),
                                LINES_OF_LOGS, new File(OUTPUT_DIRECTORY
                                        + File.pathSeparator + "pipeline/logs/" + workerCounter + "-worker.log"));
                        System.out.println("-> -> Containers: " + containers.get(i).getName() + "\n" + workerCounter + "-worker.log");
                        workerCounter++;
                    }

                }
            }

            if(pod.getMetadata().getName().contains(PIPELINE_PROXY_POD_NAME))
            {
                int counter = 0;
                List<Container> containers = pod.getSpec().getContainers();
                for(int i=0; i<containers.size();i++){
                    if (containers.get(i).getName().equalsIgnoreCase(PIPELINE_PROXY_CONTAINER)){
                        readLog(nameSpace, pod.getMetadata().getName(), containers.get(i).getName(),
                                LINES_OF_LOGS, new File(OUTPUT_DIRECTORY + File.pathSeparator + "pipeline-proxy/logs/pipeline-proxy.log"));
                        System.out.println("-> -> Containers: " + containers.get(i).getName());
                    }

                }
            }
        }
    }

    public void downloadFileFromContainer(String namespace, String podname, String container, String filePathInContainer, File downloadLocation){
        createFolder(downloadLocation);
        client.pods()
                .inNamespace(namespace)
                .withName(podname)
                .inContainer(container)
                .file(filePathInContainer)
                .copy(downloadLocation.toPath());
    }

    public void readLog(String namespace, String podname, String container, int lineCount, File filePath){//https://www.programcreek.com/java-api-examples/index.php?api=io.fabric8.kubernetes.client.dsl.LogWatch
        try{
            PrettyLoggable<String, LogWatch> tailingLines = client.pods().inNamespace(namespace)
                    .withName(podname)
                    .inContainer(container)
                    .tailingLines(lineCount);
            String log = tailingLines.getLog();
            createFolder(filePath);
            if (!log.isEmpty()) {
                FileWriter fileWriter = new FileWriter(new File(filePath.getAbsolutePath()));
                fileWriter.write(log);
                fileWriter.close();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void createFolder(File filename){
        if(!filename.exists())
        {
            String directory = filename.getParent();
            new File(directory).mkdirs();
        }
    }

    public static void main(String[] args) {
        Supportability supportability = new Supportability();
        supportability.namespaceTree(MONGO_NAMESPACE);
        supportability.namespaceTree(MY_NAMESPACE);
        supportability.execute(MONGO_NAMESPACE);
        supportability.execute(MY_NAMESPACE);
        supportability.close();
    }
}
