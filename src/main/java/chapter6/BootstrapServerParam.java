package chapter6;


import org.apache.kafka.common.Node;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataResponse;

import static org.apache.kafka.common.requests.MetadataResponse.TopicMetadata;
import static org.apache.kafka.common.requests.MetadataResponse.PartitionMetadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by 朱小厮 on 2019-03-02.
 */
public class BootstrapServerParam {
    public static final String topic = "topic-demo";

    public static MetadataResponse getMetadata(int throttleTimeMs) {
        Node node = new Node(0, "localhost", 9093);
        List<Node> brokers = Collections.singletonList(node);
        int controllerId = 0;
        String clusterId = "64PniqfkRHa4ASFUisNXrw";

        List<Node> empty = new ArrayList<>();
        PartitionMetadata pMeta1 = new PartitionMetadata(Errors.NONE, 0, node, brokers, brokers, empty);
        PartitionMetadata pMeta2 = new PartitionMetadata(Errors.NONE, 1, node, brokers, brokers, empty);
        PartitionMetadata pMeta3 = new PartitionMetadata(Errors.NONE, 2, node, brokers, brokers, empty);
        PartitionMetadata pMeta4 = new PartitionMetadata(Errors.NONE, 3, node, brokers, brokers, empty);
        List<PartitionMetadata> pMetaList = new ArrayList<>();
        pMetaList.add(pMeta1);
        pMetaList.add(pMeta2);
        pMetaList.add(pMeta3);
        pMetaList.add(pMeta4);
        TopicMetadata tMeta1 = new TopicMetadata(Errors.NONE, topic, false, pMetaList);

        List<TopicMetadata> tMetaList = new ArrayList<>();
        tMetaList.add(tMeta1);
        return new MetadataResponse(throttleTimeMs, brokers, clusterId, controllerId, tMetaList);

    }
}
