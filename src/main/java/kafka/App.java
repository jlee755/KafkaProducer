package kafka;

import kafka.producer.ProducerThread;
import org.apache.commons.cli.*;

public class App {
	public static String DEFAULT_THREAD_COUNT="4";
	public static String TOPIC_NAME="test";
	public static Long DELAY=0l;
	public static String DEFAULT_MESSAGE_COUNT="1000";

	public static void main(String[] args) {
		Options options = new Options();

		Option threads = new Option("t", "threads", true, "Thread Count");
		threads.setRequired(false);
		options.addOption(threads);

		Option messages = new Option("m", "messages", true, "Messages Count");
		threads.setRequired(false);
		options.addOption(messages);

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
		Integer threadCt = Integer.parseInt(cmd.getOptionValue("threads", DEFAULT_THREAD_COUNT));
		Integer messageCt = Integer.parseInt(cmd.getOptionValue("messages", DEFAULT_MESSAGE_COUNT));
		System.out.println("threadCt: " + threadCt);
		System.out.println("MessageCt: " + messageCt);

		for (int threadNum = 0 ; threadNum < threadCt; threadNum++) {
			Thread t = new ProducerThread("t" + threadNum, TOPIC_NAME, messageCt, DELAY);
			t.start();
		}
	}
}