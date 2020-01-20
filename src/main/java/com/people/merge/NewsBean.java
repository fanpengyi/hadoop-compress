package com.people.merge;


import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

/**
 * 新闻信息 ，文章实时、回溯转化后写入下游的 bean
 * 包含了采集预处理、舆情预处理所有属性值
 *
 *
 *
 * */
public class NewsBean implements java.io.Serializable{

	private static final long serialVersionUID = 4237858262691222450L;
	
	private long id;
	
	private String title;		//标题
	

	private String content;		//正文

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}
}
