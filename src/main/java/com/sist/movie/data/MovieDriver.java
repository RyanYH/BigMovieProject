package com.sist.movie.data;

import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.sist.movie.dao.MovieDataManager;
import com.sist.movie.mongo.FeelVO;
import com.sist.movie.mongo.MovieFeelDAO;

public class MovieDriver {
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		File dir = new File("./output");
		if(dir.exists())
		{
			File[] files = dir.listFiles();
			for(File f:files)
			{
				f.delete();
			}
			dir.delete();
		}
		Configuration conf = new Configuration();
		Job job = new Job(conf, "MovieCount");
		job.setJarByClass(MovieDriver.class);
		job.setMapperClass(MovieMapper.class);
		job.setReducerClass(MovieReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(
				"/home/sist/bigdataStudy/.metadata/.plugins/org.eclipse.wst.server.core/tmp0/wtpwebapps/BigMovieProject/desc.txt"));
		FileOutputFormat.setOutputPath(job, new Path("./output"));
		// Run
		job.waitForCompletion(true);
		MovieDataManager m = new MovieDataManager();
		FeelVO vo = m.rGraph("수상한 그녀");
		MovieFeelDAO dao = new MovieFeelDAO();
		dao.movieFeelSave(vo);
	}
}