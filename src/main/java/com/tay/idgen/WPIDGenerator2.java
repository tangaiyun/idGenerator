package com.tay.idgen;

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/*
 * @author tay
 * @since 2016-11-20
 */
public class WPIDGenerator2 {
	private static final Logger LOGGER = LoggerFactory.getLogger(WPIDGenerator2.class);
	private static final int ipLastSegmentBits = 8;
	private static final int shardIdBits = 4;
	private static final int tableIdBits = 8;
	private final static int sequenceBits = 12;
	private final static int timestampBits = 40;
	private final static int RADIX = 16;
	// 时间为2010-11-20 10:17:16
	private final static long since = 1290219436000L;
	// 时间为2016-11-20 10:17:16

//	private final static long sinceSecond = 1290219436L;
	
// shard id 4 bit|time since 40 bit |ip 8 bit|| table id 8 bit |sequence 12 bit|
	private final static int tableIdShift = sequenceBits;
	private final static int ipLastSegmentShift = sequenceBits + tableIdBits;
	private final static int timestampLeftShift = sequenceBits + tableIdBits + ipLastSegmentBits;
	private final static int shardIdShift = sequenceBits + tableIdBits + ipLastSegmentBits + timestampBits;
	private final static int sequenceMask = -1 ^ (-1 << sequenceBits);

	private static long lastTimestamp = -1L;
	private static long ipLastSegment = 1L;
	private long sequence = 0L;

	static {
		InetAddress ia = null;
		try {
			ia = InetAddress.getLocalHost();
		} catch (UnknownHostException e) {
			LOGGER.error("id生成器所在服务器主机名无法解析.");
			throw new RuntimeException("id生成器所在服务器主机名无法解析.");
		}
		String localip = ia.getHostAddress();
		String lastSegment = localip.substring(localip.lastIndexOf(".") + 1);
		ipLastSegment = Long.parseLong(lastSegment);
		System.out.println("ip last segment:" + ipLastSegment);
	}
	

	public synchronized String nextStringId(int shardId, TableEnum table) {
		long timestamp = System.currentTimeMillis();
		if (timestamp < lastTimestamp) {
			LOGGER.error(String.format("时钟被向后调整, 拒绝请求直到 %d.", lastTimestamp));
			throw new RuntimeException(String.format("时钟被向后调整. %d 毫秒内拒绝id生成", lastTimestamp - timestamp));
		}
		if (shardId <= 0 || shardId >= (1 << shardIdBits)) {
			LOGGER.error(String.format(" shardId 不应小于等于0和大于等于%d.", 1 << shardIdBits));
			throw new RuntimeException(String.format(" shardId 不应小于0和大于%d.", 1 << shardIdBits));
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
		long timeSpan = timestamp - since;
		BigInteger result = new BigInteger(shardId + "").shiftLeft(shardIdShift);
		result = result.or(new BigInteger(timeSpan + "").shiftLeft(timestampLeftShift));
		result = result.or(new BigInteger(ipLastSegment + "").shiftLeft(ipLastSegmentShift));
		result = result.or(new BigInteger(table.getId() + "").shiftLeft(tableIdShift));
		result = result.or(new BigInteger(sequence + ""));
	
		return result.toString(RADIX).toUpperCase();
	}
	
	
	private long tilNextMillis(long lastTimestamp) {
		long timestamp = System.currentTimeMillis();
		while (timestamp <= lastTimestamp) {
			timestamp = System.currentTimeMillis();
		}
		return timestamp;
	}
	
	public static String getSequence(String id) {
		BigInteger result = new BigInteger(id, RADIX);
		result = result.and(new BigInteger("4095"));
		return result.toString();
	}
	
	public static String getTableId(String id) {
		BigInteger result = new BigInteger(id, RADIX);
		result = result.shiftRight(sequenceBits);
		result = result.and(new BigInteger("255"));
		return result.toString();
	}
	
	public static String getHostIp(String id) {
		BigInteger result = new BigInteger(id, RADIX);
		result = result.shiftRight(sequenceBits + tableIdBits);
		result = result.and(new BigInteger("255"));
		return result.toString();
	}
	
	public static String getTimeSpan(String id) {
		BigInteger result = new BigInteger(id, RADIX);
		result = result.shiftRight(sequenceBits + tableIdBits +ipLastSegmentBits);
		result = result.and(new BigInteger(((1L<<timestampBits)-1)+""));
		return result.toString();
	}

	public static String getShardId(String id) {
		BigInteger result = new BigInteger(id, RADIX);
		result = result.shiftRight(sequenceBits + tableIdBits + ipLastSegmentBits + timestampBits);
		result = result.and(new BigInteger("15"));
		return result.toString();

	}

	public static void main(String[] args) {
		
		System.out.println(System.currentTimeMillis() - since);
		WPIDGenerator2 idg = new WPIDGenerator2();
		
		String strId  = idg.nextStringId(1, TableEnum.ACCOUNT);

		System.out.println("strId: " + strId);
		System.out.println("str tableid :" + getTableId(strId));
		System.out.println("str shardid :" + getShardId(strId));
		System.out.println("str hostIp :" + getHostIp(strId));
		System.out.println("str timeSpan :" + getTimeSpan(strId));
		System.out.println("str sequence :" + getSequence(strId));
		
		long t1 = System.currentTimeMillis();
		for(int i=0;i<1000000;i++) {
			idg.nextStringId(5, TableEnum.USER);
		}
		long t2 = System.currentTimeMillis();
		
		System.out.println("abc: "+ (t2-t1));
//		for(int i=0;i<50;i++) {
//			long nId = idg.nextLongId(5, TableEnum.USER);
//			System.out.println("nId: " + nId);
//			System.out.println("tableid :" + getTableId(nId));
//			System.out.println("shardid :" + getShardId(nId));
//			System.out.println("hostIp :" + getHostIp(nId));
//			System.out.println("timeSpan :" + getTimeSpan(nId));
//			System.out.println("sequence :" + getSequence(nId));
//		}
//		
//		for(int i=0;i<10;i++) {
//			long nId = idg.nextLongId(5, TableEnum.ACCOUNT);
//			System.out.println("nId: " + nId);
//			System.out.println("tableid :" + getTableId(nId));
//			System.out.println("shardid :" + getShardId(nId));
//			System.out.println("hostIp :" + getHostIp(nId));
//			System.out.println("timeSpan :" + getTimeSpan(nId));
//			System.out.println("sequence :" + getSequence(nId));
//		}
	}
}
