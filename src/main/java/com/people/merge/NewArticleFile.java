package com.people.merge;

import com.google.gson.Gson;

import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class NewArticleFile {

	public static void main(String[] args){
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd-HH");
		String path = "ARTICLE_PATH";
		String url = "AMPQ_ARTICLE_URL";
		String queue = "AMPQ_ARTICLE_QUEUE_NAME";
		String exchange = "ARTICLE_EXCHANGE";
		String routkey = "ARTICLE_KEY";
		
		int limit = 300;
		Gson gson = new Gson();
		while(true){
			try {
				String date = format.format(new Date(System.currentTimeMillis()));
				String filename = date+".txt";
				filename =path+ filename;
				List<String> list = MQUtils.readMQ(url,
						exchange, queue, routkey, limit);
				List<String> messages = new ArrayList();
				
				for(int i=0;i<list.size();i++){
					if(list.get(i)!=null){
						
						String w = (String) list.get(i);
						if(w!=null){
							//NewsBean a = gson.fromJson(w, NewsBean.class);
							messages.add(w);
						}
						
						
					}
				}
				if(messages!=null){
					fileWriter(filename, messages);
				}
				Thread.sleep(4);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
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
