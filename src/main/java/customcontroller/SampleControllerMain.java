package customcontroller;
import customcontroller.controller.Controller;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.informers.SharedInformerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SampleControllerMain {
    public static final Logger logger = LoggerFactory.getLogger(SampleControllerMain.class.getSimpleName());
    public static void main(String[] args) {
        try (KubernetesClient client = new KubernetesClientBuilder().build()){

            SharedInformerFactory informerFactory = client.informers();
            SharedIndexInformer<Deployment> deploymentInformer = informerFactory.sharedIndexInformerFor(Deployment.class, 10 * 60 * 1000);

            Controller controller = new Controller(client, deploymentInformer);
            controller.run();
        } catch (Exception e) {
            logger.info("Exception when interacting with Kubernetes: {} ", e.getMessage());
        }
    }
}