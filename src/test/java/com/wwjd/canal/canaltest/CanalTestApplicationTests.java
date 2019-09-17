package com.wwjd.canal.canaltest;

import com.wwjd.canal.canaltest.kafka.KafkaSender;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class CanalTestApplicationTests {
	@Autowired
	private KafkaSender sender;

	@Test
	public void contextLoads() {

		for (int i = 0; i < Integer.MAX_VALUE; i++) {
			System.out.println("send message = " + i);
			sender.send(i + "");
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
	}
	}

}
