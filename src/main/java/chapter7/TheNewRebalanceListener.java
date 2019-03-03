package chapter7;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Collections;

/**
 * Created by 朱小厮 on 2019-03-02.
 */
public class TheNewRebalanceListener implements ConsumerRebalanceListener {
    Collection<TopicPartition> lastAssignment = Collections.emptyList();

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        for (TopicPartition topicPartition : partitions) {
//            commitOffsets(partition);
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> assignment) {
//        for(TopicPartition partition: difference(lastAssignment, assignment)){
////            cleanupState(partition);
////        }
////        for (TopicPartition partition : difference(assignment, lastAssignment)) {
////            initializeState(partition);
////        }
////        for (TopicPartition partition : assignment) {
////            initializeOffset(partition);
////        }
////        this.lastAssignment = assignment;
    }
}
