package com.people.merge;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/*
 * 队列更改为nginx
 * add by zling
 * 2019/09/02
 * */
public class CreateCrawlNewArticleFiles {
	private static final Logger LOG = LoggerFactory
			.getLogger(CreateCrawlNewArticleFiles.class);

	public static void main(String[] args) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd-HH");
		// 采集目录机器目录
		String path = "/data/hadoop/";
		//nigex队列url
		final String url = "AMPQ_CRAWL_ARTICLE_URL";
		// 队列名
		final String queue = "AMPQ_CRAWL_ARTICLE_QUEUE_NAME";
		// exchange名称
		final String exchange = "ARTICLE_CRAWL_EXCHANGE";
		final String routkey = "ARTICLE_CRAWL_KEY";
		// mq一次读取50条
		final int limit = 50;
		final Gson gson = new Gson();
		try {
			ThreadPoolExecutor executor = new ThreadPoolExecutor(30, 30, 5000L,
					TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(1000));



			executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());

			while (true) {
				try {
					String date = format.format(new Date(System
							.currentTimeMillis()));
					// 保存数据目录
					final String filename = path + date + ".txt";
					final List<String> messages = new ArrayList();
					executor.execute(new Runnable() {
						@Override
						public void run() {
							// 读取mq数据
							final List<String> list = MQUtils.readMQ(url,
									exchange, queue, routkey, limit);
							if (list.size() > 0) {
								for (final String article : list) {
									String articleStr = (String) article;
									if (articleStr != null) {
										// 转换数据结构Article
										NewsBean acle = gson.fromJson(
												articleStr, NewsBean.class);
										messages.add(acle.toString());
									}

								}
								if (messages != null) {
									try {
										fileWriter(filename, messages);
									} catch (IOException e) {
										LOG.error("filewriter-error:", e);
									}
								}
							}
						}
					});
					// 5milliseconds
					Thread.sleep(5);
				} catch (Exception e) {
					LOG.error("error:", e);

				}
			}

		} catch (Exception e) {
			LOG.error("error:", e);
		}
	}

	public static void fileWriter(String fileName, List<String> clist) throws IOException {
        //创建一个FileWriter对象
        FileWriter fw = new FileWriter(fileName,true);
        //遍历clist集合写入到fileName中
        for (String str: clist){
        	if(str!=null&&!str.trim().equals("")){
        		 fw.write(str);
                 fw.write("\n");
        	}
        }
        //刷新缓冲区
        fw.flush();
        //关闭文件流对象
        fw.close();
        fw = null;
	}

}
