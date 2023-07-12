package customcontroller.controller;

import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
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
                handleAddAndUpdate(deployment);
            }

            @Override
            public void onUpdate(Deployment oldDeployment, Deployment newDeployment) {
                // todo
            }

            @Override
            public void onDelete(Deployment deployment, boolean b) {
                // todo
            }
        });
    }

    public void run() throws InterruptedException {
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
                    Thread.sleep(5000);
                    Deployment deployment = eventQueue.take();
                    System.out.println("Deployment: " + deployment.getMetadata().getName());
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

        List<Node> nodeList = client.nodes().list().getItems();
        List<Pod> pods = client.pods().inNamespace(deployment.getMetadata().getNamespace())
                .withLabel("app", deployment.getSpec().getSelector().getMatchLabels().get("app")).list().getItems();

        Map<String, Integer> podCountsPerNode = new HashMap<>();
        for (Node node : nodeList) {
            podCountsPerNode.put(node.getMetadata().getName(), 0);
        }
        for (Pod pod : pods) {
            String nodeName = pod.getSpec().getNodeName();
            podCountsPerNode.put(nodeName, podCountsPerNode.getOrDefault(nodeName, 0) + 1);
        }

        List<Map.Entry<String, Integer>> sortedPodCounts = new ArrayList<>(podCountsPerNode.entrySet());
        sortedPodCounts.sort(Comparator.comparingInt(Map.Entry::getValue));

        podCountsPerNode.forEach((key, value) -> {
            System.out.println(key + " --> " + value);
        });

        //Iterate over the pods and check if a pod needs to be rescheduled
        int ct = 0;
        for (Pod pod : pods) {
            String nodeName = pod.getSpec().getNodeName();
            int podCountOnNode = podCountsPerNode.getOrDefault(nodeName, 0);
            if (podCountOnNode > 3) {
                // Find a target node with fewer pods
                String targetNode = null;
                for (Map.Entry<String, Integer> entry : sortedPodCounts) {
                    if (entry.getValue() < 3) {
                        targetNode = entry.getKey();
                        break;
                    }
                }
                if (targetNode != null && !"minikube".equals(targetNode)) {
                    // Reschedule the pod to the target node
                    System.out.println("yes");
                    pod.getSpec().setNodeName(targetNode);
                    client.pods().inNamespace(deployment.getMetadata().getNamespace()).withName(deployment.getMetadata().getName()).patch(pod);
                    System.out.println(pod.getMetadata().getName() + " rescheduled on  " + targetNode);

                    System.out.println("yes");
                    podCountsPerNode.put(targetNode, podCountsPerNode.getOrDefault(targetNode, 0) + 1);
                    podCountsPerNode.put(nodeName, podCountsPerNode.getOrDefault(nodeName, 0) - 1);
                } else {
                    ct++;
                    podCountsPerNode.put(nodeName, podCountsPerNode.getOrDefault(nodeName, 0) - 1);
                }
            }
        }
        if (ct>0){
            System.out.println("Extra pods present: " + ct);
            System.out.println("Extra nodes required: " + Math.ceil((double) ct / 3));
            int replicas = deployment.getSpec().getReplicas();
            deployment.getSpec().setReplicas(replicas-ct);
            client.apps().deployments().inNamespace(deployment.getMetadata().getNamespace()).withName(deployment.getMetadata().getName()).patch(deployment);
        }
    }

    private void handleAddAndUpdate(Deployment deployment) {
        if ((deployment.getMetadata().getName()).equals("coredns")){
            return;
        }
        eventQueue.add(deployment);
    }
}
