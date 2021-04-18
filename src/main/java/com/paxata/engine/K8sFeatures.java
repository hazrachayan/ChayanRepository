package com.paxata.engine;

import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.LogWatch;
import io.fabric8.kubernetes.client.dsl.PrettyLoggable;

import java.io.File;
import java.io.FileWriter;
import java.util.List;

public class K8sFeatures {
    public static final String NAMESPACE = "chayan-supportability";
    public static final String POD_NAME = "pax-installation-paxserver";
    public static final String CONTAINER = "paxserver";

    KubernetesClient client = null;

    K8sFeatures(){
        client = new DefaultKubernetesClient();
    }

    void close(){
        client.close();
    }

    public void getPods(){
        PodList pods = client.pods().inNamespace(NAMESPACE).list();
        for(Pod pod: pods.getItems())
        {
            System.out.println("Pod: " + pod.getMetadata().getName());
        }
    }

    public void getNamespace(){
        NamespaceList namespaces = client.namespaces().list();
        for(Namespace namespace: namespaces.getItems()){
            System.out.println("Namespace: " + namespace.getMetadata().getName());
        }
    }

    public void getServices(){
        ServiceList serviceList = client.services()
                .inNamespace(NAMESPACE).list();
        for(Service service: serviceList.getItems()){
            System.out.println("Services: " + service.getMetadata().getName());
        }
    }

    public void getClusters(){
        List<Container> containers = client.pods().inNamespace(NAMESPACE).withName(POD_NAME).get().getSpec().getContainers();
        for(int i=0; i<containers.size();i++){
            System.out.println("Containers: " + containers.get(i).getName());
        }
    }

    public void readLog(int lineCount, String filePath){//https://www.programcreek.com/java-api-examples/index.php?api=io.fabric8.kubernetes.client.dsl.LogWatch
        try{
            PrettyLoggable<String, LogWatch> tailingLines = client.pods().inNamespace(NAMESPACE)
                    .withName(POD_NAME)
                    .inContainer(CONTAINER)
                    .tailingLines(lineCount);
            String log = tailingLines.getLog();
            if (!log.isEmpty()) {
                FileWriter fileWriter = new FileWriter(new File(filePath));
                fileWriter.write(log);
                fileWriter.close();
            }
            System.out.println("logs written to file");
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void downloadFileFromContainer(String filePathInContainer, File downloadLocation){
            client.pods()
                    .inNamespace(NAMESPACE)
                    .withName(POD_NAME)
                    .inContainer(CONTAINER)
                    .file(filePathInContainer)
                    .copy(downloadLocation.toPath());
        System.out.println("Downloaded successfully!");
    }


    public static void main(String[] args) {
        System.out.println("Start");
        K8sFeatures k8SFeatures = new K8sFeatures();
        k8SFeatures.getNamespace();
        k8SFeatures.getPods();
        k8SFeatures.getServices();
        k8SFeatures.getClusters();
        k8SFeatures.readLog(100000, "/Users/chayan.hazra/Downloads/frontend.log");
        k8SFeatures.downloadFileFromContainer("/usr/local/paxata/server/config/px.properties",
                new File("/Users/chayan.hazra/Downloads/px.properties"));
        k8SFeatures.close();
        System.out.println("end");
    }
}
