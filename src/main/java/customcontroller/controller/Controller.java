package customcontroller.controller;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Controller {
    private final BlockingQueue<Deployment> eventQueue;
    private final KubernetesClient client;
    private final SharedIndexInformer<Deployment> deploymentInformer;
    public static final Logger logger = LoggerFactory.getLogger(Controller.class.getSimpleName());

    public Controller( KubernetesClient client, SharedIndexInformer<Deployment> deploymentInformer) {
        this.eventQueue = new LinkedBlockingQueue<>();
        this.client = client;
        this.deploymentInformer = deploymentInformer;
        initInformerEventHandler();
    }

    private void initInformerEventHandler(){
        deploymentInformer.addEventHandler(new ResourceEventHandler<Deployment>() {
            @Override
            public void onAdd(Deployment deployment) {
                handleAdd(deployment);
            }

            @Override
            public void onUpdate(Deployment oldDeployment, Deployment newDeployment) {
                handleUpdate();
            }

            @Override
            public void onDelete(Deployment deployment, boolean b) {
                handleDelete();
            }
        });
    }

    public void run() {
        logger.info("Custom controller is running...");
        // Start informer to begin watching and processing events
        deploymentInformer.run();
        while (!deploymentInformer.hasSynced()) {
            // Wait till Informer syncs
        }

        // Start a separate thread to handle events asynchronously
        Thread eventHandlerThread = new Thread(() -> {
            while (true) {
                try {
                    Deployment deployment = eventQueue.take();
                    System.out.println(deployment.getSpec().getReplicas());
                    System.out.println(deployment.getMetadata().getName());
                    reconcileDeployment(deployment);

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    logger.info("Error occurred while handling event: {}", e.getMessage());
                }
            }
        });
        eventHandlerThread.setDaemon(true);
        eventHandlerThread.start();

        // Block the main thread to keep the controller running
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            logger.info("Custom Controller interrupted: {}", e.getMessage());
        }
    }

    // Reconciliation logic for a specific deployment
    public void reconcileDeployment(Deployment deployment) {
        String deploymentName = deployment.getMetadata().getName();

        // Get the desired replicas from the deployment
        int desiredReplicas = deployment.getSpec().getReplicas();
        System.out.println("Reconciling deployment: " + deploymentName + ", desired replicas: " + desiredReplicas);

        Map<String, Integer> podCountsPerNode = new HashMap<>();
        // Count the number of pods for this deployment already scheduled on each node
        PodList podList = client.pods().inNamespace(deployment.getMetadata().getNamespace())
                .withLabel("app", deploymentName).list();
        for (Pod pod : podList.getItems()) {
            String nodeName = pod.getSpec().getNodeName();
            podCountsPerNode.put(nodeName, podCountsPerNode.getOrDefault(nodeName, 0) - 1);
        }

        // Iterate over the desired number of replicas and schedule pods accordingly
        int scheduledPods = 0;
        for (int i = 0; i < desiredReplicas; i++) {
            if (scheduledPods >= 3) {
                // Limit of 3 pods per node reached, stop scheduling
                System.out.println("Maximum pod limit reached for deployment: " + deploymentName);
                break;
            }

            // Find the node with the fewest pods scheduled
            String targetNode = null;
            int minPods = Integer.MAX_VALUE;
            for (Map.Entry<String, Integer> entry : podCountsPerNode.entrySet()) {
                if (entry.getValue() < minPods) {
                    minPods = entry.getValue();
                    targetNode = entry.getKey();
                }
            }

            if (targetNode != null) {
                // Schedule the pod on the selected node
                System.out.println("Scheduling pod for deployment: " + deploymentName + ", on node: " + targetNode);

                // Create the pod and apply it to the Kubernetes cluster
                Pod pod = createPod(deploymentName);
                client.pods().inNamespace(deployment.getMetadata().getNamespace()).createOrReplace(pod);

                // Increment the scheduled pod count for the node
                podCountsPerNode.put(targetNode, minPods + 1);
                scheduledPods = minPods+1;
            }
        }
    }

    // Create a sample pod based on the deployment
    private Pod createPod(String deploymentName) {
        Pod pod = new Pod();
        pod.getMetadata().setName("pod-" + deploymentName);
        pod.getSpec().setNodeName("");
        // Set the necessary pod spec based on your deployment requirements
        return pod;
    }

    private void handleAdd(Deployment deployment){
        if ((deployment.getMetadata().getName()).equals("coredns")){
            return;
        }
        eventQueue.add(deployment);
    }

    private void handleUpdate(){

    }

    private void handleDelete(){

    }

}
