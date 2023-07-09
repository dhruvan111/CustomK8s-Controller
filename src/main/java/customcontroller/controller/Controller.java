package customcontroller.controller;

import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
//                handleAdd(deployment);
            }

            @Override
            public void onUpdate(Deployment oldDeployment, Deployment newDeployment) {
//                handleUpdate();
            }

            @Override
            public void onDelete(Deployment deployment, boolean b) {
//                handleDelete();
            }
        });
    }

}
