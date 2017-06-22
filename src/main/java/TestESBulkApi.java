
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;

/**
 * Created by qmgeng on 16/4/25.
 */
public class TestESBulkApi {
    protected final static Logger LOGGER = LoggerFactory.getLogger(TestESBulkApi.class);

    public static void main(String[] args) throws Exception {
        Settings settings = Settings.builder()
                .put("cluster.name", "es5.datacube")
                .put("client.transport.sniff", true)
                .put("node.name", "sample_client_node")
                .put("transport.ping_schedule", "10s")
                .put("xpack.security.transport.ssl.enabled", false)
                .put("xpack.security.user", "elastic:123456")
                .put("transport.tcp.port", 9300).build();
        TransportClient client = new PreBuiltXPackTransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.130.11.37"), 9300));

        final ClusterHealthResponse res = client.admin().cluster().health(new ClusterHealthRequest()).actionGet();
        System.out.println(res.getClusterName());

    }

}
