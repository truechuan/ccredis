package cc.utils.ccredis;

import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import org.yaml.snakeyaml.Yaml;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

/**
 * redis初始化工厂类
 * 
 * @author chuan
 * @date 2015-11-4
 */

@SuppressWarnings( { "unchecked" })
public class RedisFactory {

	private static Logger logger = Logger.getLogger(RedisFactory.class.getName());

	/**
	 * 配置文件路径
	 */
	private String confPath = "/redis.yaml";

	/**
	 * 默认超时时间
	 */
	private final int timeout = Protocol.DEFAULT_TIMEOUT;

	/**
	 * ip端口正则校验
	 */
	private static final Pattern ipPortPattern = Pattern
			.compile("(2[5][0-5]|2[0-4]\\d|1\\d{2}|\\d{1,2})\\.(25[0-5]|2[0-4]\\d|1\\d{2}|\\d{1,2})\\.(25[0-5]|2[0-4]\\d|1\\d{2}|\\d{1,2})\\.(25[0-5]|2[0-4]\\d|1\\d{2}|\\d{1,2}):\\d{0,5}");

	/**
	 * redis配置信息map
	 */
	private static final Map redisConf = Maps.newConcurrentMap();

	/**
	 * redis实例map 实例一个redis pool
	 */
	private static final Map<String, JedisPool> redisPoools = Maps.newConcurrentMap();

	/**
	 * 默认redis实例名称
	 */
	private static final String DEFAULT_REDIS = "default";

	public RedisFactory() {
		initialize();
	}

	private static final RedisFactory redisFactory = new RedisFactory();

	public static RedisFactory getInstance() {
		return redisFactory;
	}

	/**
	 * redis初始化入口
	 */
	public void initialize() {

		initRedisConfInfo();

		checkRedisConfInfo();

		initRedisPool();

	}

	/**
	 * 获取jedis连接
	 * @param key
	 * @return
	 */
	public Jedis getJedis(String key) {
		try {
			return jedisFromPool(key).getResource();
		} catch (Exception e) {
			getJedisPool(key);
			return jedisFromPool(key).getResource();
		}
	}

	/**
	 * 在连接池断开初始化连接 synchronized 排队加锁，防止并发
	 * 
	 * @param key
	 */
	private synchronized void getJedisPool(String key) {
		try {
			Jedis jedis = jedisFromPool(key).getResource();
			jedis.close();
			return;
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		initRedis(key);
	}

	private JedisPool jedisFromPool(String key) {
		return redisPoools.containsKey(key) ? redisPoools.get(key) : redisPoools.get(DEFAULT_REDIS);
	}

	/**
	 * 初始化redis连接池
	 */
	private void initRedisPool() {
		Set<Entry> entrySet = redisConf.entrySet();
		for (Entry entry : entrySet) {
			initRedis((String) entry.getKey());
		}
	}

	/**
	 * @param entry
	 */
	private void initRedis(String key) {
		Map value = (Map) redisConf.get(key);
		try {
			URI server = new URI("redis://"+value.get("server"));
			Map poolInfo = (Map) value.get("pool");
			JedisPool pool = new JedisPool(getPoolConfigure(poolInfo),server,timeout);
			redisPoools.put(key, pool);
			//判断jedis pool 是的初始化完成
			Jedis jedis = pool.getResource();
			jedis.close();
			logger.info("init redis pool success:" + key);
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 
	 * @param poolInfo
	 * @return
	 */
	private JedisPoolConfig getPoolConfigure(Map poolInfo) {
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxTotal((Integer) poolInfo.get("maxTotal"));
		config.setMaxIdle((Integer) poolInfo.get("maxIdle"));
		config.setMaxWaitMillis(Long.parseLong(String.valueOf(poolInfo.get("maxWait"))));
		return config;
	}

	/**
	 * 校验配置信息
	 */
	private void checkRedisConfInfo() {
		Preconditions.checkNotNull(redisConf);
		Preconditions.checkArgument(!redisConf.isEmpty());
		Preconditions.checkNotNull(redisConf.get(DEFAULT_REDIS));
		Set<Entry> entrySet = redisConf.entrySet();
		for (Entry entry : entrySet) {
			Map value = (Map) entry.getValue();
			String server = (String) value.get("server");
			Preconditions.checkArgument(!Strings.isNullOrEmpty(server));
			if (!ipPortPattern.matcher(server).matches()) {
				throw new RuntimeException("redis servers ip configure error: " + server + " is error.");
			}
		}

	}

	/**
	 * 初始化配置信息
	 */
	private void initRedisConfInfo() {
		try {
			Reader reader = new InputStreamReader(RedisFactory.class.getResourceAsStream(confPath), "UTF-8");
			Map load = new Yaml().loadAs(reader, HashMap.class);
			redisConf.clear();
			redisConf.putAll(load);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException("Redis configure info error:" + e.getMessage());
		}

	}
}
