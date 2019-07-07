package kafka;

import kafka.producer.ProducerThread;
import org.apache.commons.cli.*;

public class App {
	public static String DEFAULT_KAFKA_BROKERS = "localhost:9092";
	public static String DEFAULT_TOPIC_NAME="marklogic";
	public static String DEFAULT_THREAD_COUNT="4";
	public static String DEFAULT_MESSAGE_COUNT="1000";

	public static Long DELAY=0l;

	public static void main(String[] args) {
		Options options = new Options();

		Option kafkaHostOption = new Option("h", "host", true, "Kafka Host & IP list");
		kafkaHostOption.setRequired(false);
		options.addOption(kafkaHostOption);

		Option topicNameOption = new Option("t", "topic", true, "Topic Name");
		topicNameOption.setRequired(false);
		options.addOption(topicNameOption);

		Option threadsOptions = new Option("c", "threads", true, "Thread Count");
		threadsOptions.setRequired(false);
		options.addOption(threadsOptions);

		Option messagesOptions = new Option("m", "messages", true, "Messages Count");
		messagesOptions.setRequired(false);
		options.addOption(messagesOptions);

		CommandLineParser parser = new DefaultParser();
		HelpFormatter formatter = new HelpFormatter();
		CommandLine cmd;

		try {
			cmd = parser.parse(options, args);
		} catch (ParseException e) {
			System.out.println(e.getMessage());
			formatter.printHelp("kafka-producer", options);

			System.exit(1);
			return;
		}
		Integer threadCt = Integer.parseInt(cmd.getOptionValue("threadsOptions", DEFAULT_THREAD_COUNT));
		Integer messageCt = Integer.parseInt(cmd.getOptionValue("messages", DEFAULT_MESSAGE_COUNT));
		String kafkaBrokers = cmd.getOptionValue("host", DEFAULT_KAFKA_BROKERS);
		String topicName = cmd.getOptionValue("topic", DEFAULT_TOPIC_NAME);
		System.out.println("threadCt: " + threadCt);
		System.out.println("MessageCt: " + messageCt);
		System.out.println("host: " + kafkaBrokers);
		System.out.println("topic: " + topicName);

		for (int threadNum = 0 ; threadNum < threadCt; threadNum++) {
			Thread t = new ProducerThread("t" + threadNum, kafkaBrokers, topicName, messageCt, DELAY);
			t.start();
		}
	}
}