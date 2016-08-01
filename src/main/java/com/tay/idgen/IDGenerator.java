package com.tay.idgen;

import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IDGenerator {
	private static final Logger LOGGER = LoggerFactory.getLogger(IDGenerator.class);
	private final static long twepoch = 1402974492729L;
	
	private final static long workerIdBits = 5L;
	private final static long datacenterIdBits = 5L;
	private final static long maxWorkerId = -1L ^ (-1L << workerIdBits);
	private final static long maxDatacenterId = -1L ^ (-1L << datacenterIdBits);
	private final static long sequenceBits = 12L;
	
	private final static long workerIdShift = sequenceBits;
	private final static long datacenterIdShift = sequenceBits + workerIdBits;
	private final static long timestampLeftShift = sequenceBits + workerIdBits + datacenterIdBits;
	private final static long sequenceMask = -1L ^ (-1L << sequenceBits);
	
	private static long lastTimestamp = -1L;
	
	private static Random random = new Random();
	
	private String workerId;
	private String datacenterId;
	private long sequence = 0L;
	
	public void setWorkerId(String workerId) {
		this.workerId = workerId;
	}

	public void setDatacenterId(String datacenterId) {
		this.datacenterId = datacenterId;
	}		
	
	public IDGenerator() {	
		this(Math.abs(random.nextLong()) % maxWorkerId, Math.abs(random.nextLong()) % maxDatacenterId);
	}
	public IDGenerator(long workerId, long datacenterId) {		
		if (Long.valueOf(workerId) > maxWorkerId || Long.valueOf(workerId) < 0) {
			throw new IllegalArgumentException(String.format("worker Id can't be greater than %d or less than 0", maxWorkerId));
		}
		if (Long.valueOf(datacenterId) > maxDatacenterId || Long.valueOf(datacenterId) < 0) {
			throw new IllegalArgumentException(String.format("datacenter Id can't be greater than %d or less than 0", maxDatacenterId));
		}
		this.workerId = String.valueOf(workerId);
		this.datacenterId = String.valueOf(datacenterId);
		LOGGER.info(String.format("worker starting. timestamp left shift %d, datacenter id bits %d, worker id bits %d, sequence bits %d, workerid %d", timestampLeftShift, datacenterIdBits, workerIdBits,sequenceBits, workerId));
	}

	public synchronized long nextId() {
		long timestamp = timeGen();
		if (timestamp < lastTimestamp) {
			LOGGER.error(String.format("clock is moving backwards.  Rejecting requests until %d.", lastTimestamp));
			throw new RuntimeException(String.format("Clock moved backwards.  Refusing to generate id for %d milliseconds", lastTimestamp - timestamp));
		}
		if (lastTimestamp == timestamp) {
			sequence = (sequence + 1) & sequenceMask;
			if (sequence == 0) {
				timestamp = tilNextMillis(lastTimestamp);
			}
		} else {
			sequence = 0L;
		}
		lastTimestamp = timestamp;
		return ((timestamp - twepoch) << timestampLeftShift) | (Long.valueOf(datacenterId) << datacenterIdShift) | (Long.valueOf(workerId) << workerIdShift) | sequence;
	}
	private long tilNextMillis(long lastTimestamp) {
		long timestamp = timeGen();
		while (timestamp <= lastTimestamp) {
			timestamp = timeGen();
		}
		return timestamp;
	}

	private long timeGen() {
		return System.currentTimeMillis();
	}
	public static void main(String[] args) {
		IDGenerator worker = new IDGenerator(2,0);
		for (int i = 0; i < 20; i++) {
			System.out.println(worker.nextId());
		}
	}
}
