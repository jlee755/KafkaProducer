package kafka.producer;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.ExecutionException;
import java.io.IOException;

import java.nio.file.*;
import java.io.File;
import java.util.Random;
import java.util.Arrays;

public class ProducerThread extends Thread {
    private String kafkaBrokers;
    private String threadName;
    private Long delay;
    private String topicName;
    private Integer messageCt;
    private Producer<Long, String> producer;
    private Boolean simulation = false;
    private Random generator = new Random();

    public ProducerThread(String threadName, String kafkaBrokers, String topicName, Integer messageCt, Long delay) {
        super(threadName);
        this.kafkaBrokers = kafkaBrokers;
        this.threadName = threadName;
        this.topicName = topicName;
        this.messageCt = messageCt;
        this.delay = delay;
        producer = ProducerCreator.createProducer(this.kafkaBrokers);
    }

    @Override
    public void run() {
        System.out.println("ProducerThread - START "+Thread.currentThread().getName());
        System.out.println("messageCt: " + messageCt);
        Integer messagesSent = 0;
        try {
            while ((messagesSent < messageCt) || (messageCt == 0)){
                if (simulation) {
                    messagesSent += simulateMessage();
                } else {
                    messagesSent += produceMessage();
                }
                if (delay > 0) {
                    Thread.sleep(delay);
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("ProducerThread - END "+Thread.currentThread().getName());
    }

    private Integer simulateMessage() {
        System.out.println(threadName + ": Fake Message");
        return 1;
    }

    private Integer produceMessage() {
        

        final ProducerRecord<Long, String> record = new ProducerRecord(topicName, generate7400Message(choose7400File()));
        try {
            RecordMetadata metadata = producer.send(record).get();
            System.out.println("Record sent to partition " + metadata.partition() + " with offset " + metadata.offset());
            return 1;
        } catch (ExecutionException e) {
            System.out.println("Error in sending record");
            System.out.println(e);
            return 0;
        } catch (InterruptedException e) {
            System.out.println("Error in sending record");
            System.out.println(e);
            return 0;
        }
    }

    private String generateSimpleJsonMessage() {
        return "{\"C\" : \"c\"}";
    }

    private String generateMediumJsonMessage() {
        return "{" +
                "\"LOREM\" : \"Lorem ipsum dolor sit amet, consectetur adipiscing elit.\"," +
                "\"IPSUM\" : \"Etiam at leo eleifend diam varius malesuada. Donec venenatis lectus arcu, et euismod quam tristique in.\"," +
                " \"DOLOR\" : \"Vestibulum fermentum leo sodales velit aliquet cursus.\"" +
                "}";
    }

    private String generateLargeJsonMessage() {
        StringBuffer sb = new StringBuffer("{");
        sb.append("\t\"LOREM\" : \"Lorem ipsum dolor sit amet, consectetur adipiscing elit.\",");
        for (int i = 0; i<50; i++) {
            sb.append("\t\"IPSUM"+i+"\" : \"Etiam at leo eleifend diam varius malesuada. Donec venenatis lectus arcu, et euismod quam tristique in.\",");
        }
        sb.append("\t\"DOLOR\" : \"Vestibulum fermentum leo sodales velit aliquet cursus.\"");
        sb.append("}");
        return sb.toString();
    }

    private String choose7400File() {
        String pathdir = "./sampledata";
        File folder = new File(pathdir);
        String[] files = folder.list();
        int randomIndex = generator.nextInt(files.length);
        return pathdir + "/" + files[randomIndex];
    }

    private String generate7400Message(String filepath) {
        String outString = "";

        try {
            outString = new String(Files.readAllBytes(Paths.get(filepath)));
        } catch (IOException e) {
            e.printStackTrace();
        }

        return outString;
    }
}
