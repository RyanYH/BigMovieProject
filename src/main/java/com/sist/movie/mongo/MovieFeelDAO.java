package com.sist.movie.mongo;

import java.util.*;

import com.mongodb.*;

public class MovieFeelDAO {
	private MongoClient mc;
	private DB db;
	private DBCollection dbc;
	public MovieFeelDAO()
	{
		try{
			mc = new MongoClient("localhost");
			db = mc.getDB("mydb");
			dbc = db.getCollection("movie");
		}catch(Exception ex)
		{
			System.out.println(ex.getMessage());
		}
	}
	public void movieFeelSave(FeelVO vo)
	{
		try{
			BasicDBObject obj = 
					new BasicDBObject();
			obj.put("title", vo.getTitle());
			obj.put("feel", vo.getFeel());
			obj.put("count", vo.getCount());
			dbc.insert(obj);
		
		}catch(Exception ex){
			System.out.println();
		}
	}
	public List<String> movieRecommend(String feel)
	{
		List<String> list = new ArrayList<String>();
		try{
			DBCursor cursor = dbc.find();
			while(cursor.hasNext())
			{
				BasicDBObject obj = (BasicDBObject)cursor.next();
				String strFeel = obj.getString("feel");
				String strTitle = obj.getString("title");
				if(strFeel.equals(feel))
				{
					list.add(strTitle);
				}
			}
			cursor.close();
		}catch(Exception ex)
		{
			System.out.println(ex.getMessage());
		}
		return list;
	}
	
}

