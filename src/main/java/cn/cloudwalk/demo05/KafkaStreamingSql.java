package cn.cloudwalk.demo05;

import java.util.Arrays;
import java.util.List;

public class KafkaStreamingSql {
	public static void main(String[] args) {
		List<Runnable> jobs = Arrays.asList(new RandomNetworkDataProducer(), new NetworkDataProcessor(), new IgniteStreamingSQLQuery());
		jobs.parallelStream().map(Thread::new).forEach(Thread::run);
	}
}