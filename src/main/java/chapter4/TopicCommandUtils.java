package chapter4;

/**
 * 代码清单4-1 & 4-2
 * Created by 朱小厮 on 2018/9/9.
 */
public class TopicCommandUtils {

    public static void main(String[] args) {
//        createTopic();
//        describeTopic();
        listTopic();
    }

    /**
     * 代码清单4-1
     */
    public static void createTopic(){
        String[] options = new String[]{
                "--zookeeper", "localhost:2181/kafka",
                "--create",
                "--replication-factor", "1",
                "--partitions", "1",
                "--topic", "topic-create-api"
        };
        kafka.admin.TopicCommand.main(options);
    }

    /**
     * 代码清单4-2
     */
    public static void describeTopic(){
        String[] options = new String[]{
                "--zookeeper", "localhost:2181/kafka",
                "--describe",
                "--topic", "topic-create"
        };
        kafka.admin.TopicCommand.main(options);
    }

    public static void listTopic(){
        String[] options = new String[]{
                "--zookeeper", "localhost:2181/kafka",
                "--list"
        };
        kafka.admin.TopicCommand.main(options);
    }
}
