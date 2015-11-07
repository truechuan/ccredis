package cc.utils.ccredis;

import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;


import org.junit.Assert;
import org.junit.Test;
import org.junit.runners.JUnit4;
import org.yaml.snakeyaml.Yaml;

import redis.clients.jedis.Jedis;

import com.google.common.base.Preconditions;

/**
 * @author chuan
 * @date 2015-11-5
 */
public class TestRedis {
	
	//@Test
	public void tets1(){
		Map map = new HashMap();
		System.out.println(Preconditions.checkElementIndex(map.size(), 0));
		
	}
	
	//@Test
	@SuppressWarnings("unchecked")
	public void readYaml() throws UnsupportedEncodingException{
		String confPath = "/redis.yaml";
		Reader reader = new InputStreamReader(TestRedis.class.getResourceAsStream(confPath),"UTF-8");
		Map load = new Yaml().loadAs(reader,HashMap.class);
		System.out.println(load.toString());
	}
	
	//@Test
	public void getRedis(){
		Jedis jedis = RedisFactory.getInstance().getJedis("cc");
		System.out.println(jedis);
	}
	
	//@Test
	public void threadTest(){
		for (int i = 0; i < 100; i++) {
			new Thread(new Runnable() {
				
				@Override
				public void run() {
					String randStr = "test"+new Random().nextInt(1000);
					String randStr1 = "test"+new Random().nextInt(1000);
					Jedis jedis = RedisFactory.getInstance().getJedis("cc");
					jedis.set(randStr, randStr1);
					Assert.assertEquals(randStr1, jedis.get(randStr));
					jedis.del(randStr);
					Assert.assertNull(jedis.get(randStr));
					jedis.close();
				}
			}).start();
		}
	}

}
