package chapter11;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 代码清单11-2
 * Created by 朱小厮 on 2018/8/1.
 */
public class ConsumerInterceptorTTL implements
        ConsumerInterceptor<String, String> {

    @Override
    public ConsumerRecords<String, String> onConsume(
            ConsumerRecords<String, String> records) {
        long now = System.currentTimeMillis();
        Map<TopicPartition, List<ConsumerRecord<String, String>>> newRecords
                = new HashMap<>();
        for (TopicPartition tp : records.partitions()) {
            List<ConsumerRecord<String, String>> tpRecords = records.records(tp);
            List<ConsumerRecord<String, String>> newTpRecords = new ArrayList<>();
            for (ConsumerRecord<String, String> record : tpRecords) {
                Headers headers = record.headers();
                long ttl = -1;
                for (Header header : headers) {//判断headers中是否有key为"ttl"的Header
                    if (header.key().equalsIgnoreCase("ttl")) {
                        ttl = BytesUtils.bytesToLong(header.value());
                    }
                }
                //消息超时判定
                if (ttl > 0 && now - record.timestamp() < ttl * 1000) {
                    newTpRecords.add(record);
                } else {//没有设置ttl,无需超时判定
                    newTpRecords.add(record);
                }
            }
            if (!newTpRecords.isEmpty()) {
                newRecords.put(tp, newTpRecords);
            }
        }
        return new ConsumerRecords<>(newRecords);
    }


    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }
}
