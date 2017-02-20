package com.github.geolookup;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.commons.io.FileUtils;

import com.github.geolookup.Coordinate;

public class Search_MR {

	final static String indexFile="files/index.txt#index.txt";
	final static String inputPath="/input";
	final static String outputPath="/output";
	
static class GeoSearchMapper
  extends Mapper<LongWritable, Text, Text, Text>  {
	ArrayList<Coordinate> idx = new ArrayList<Coordinate>();
    private void readIndex(Path path) throws IOException{
    	String file=path.toString();
    	BufferedReader br=new BufferedReader(new InputStreamReader(new FileInputStream(file)));
    	while(true)
    		{
    			try{
    				String line=br.readLine();
    				idx.add(new Coordinate(Double.parseDouble(line.split(";")[0]),
    	            		Double.parseDouble(line.split(";")[1]) ) );
    			}
    			catch (Exception e)
    			{
    				break;
    			}
    		}
    	br.close();
    }
                               
    public void setup(Context context) throws IOException{
    	readIndex(DistributedCache.getLocalCacheFiles(context.getConfiguration())[0]);
    }
	 
	 public void map(LongWritable key, Text value,  Context context )
	     throws IOException,  InterruptedException  {
	   
		 double latLimit=Double.parseDouble(context.getConfiguration().get("latLimit"));
		 double lonLimit=Double.parseDouble(context.getConfiguration().get("lonLimit"));
		 double maxDistMiles=Double.parseDouble(context.getConfiguration().get("maxDistMiles"));
		 
		 double searchLat=Double.parseDouble(value.toString().split(";")[0].trim());
		 double searchLon=Double.parseDouble(value.toString().split(";")[1].trim());
		 
		 // implementation of binary search
		 // Step 1 - Get the limits in which the lat can fall
		 int low=0;
		 int hi=idx.size()-1;
		 int mid=(hi+low)/2;
		 while (low<=hi)
		 {
			 if( searchLat-latLimit > idx.get(mid).getLat() )
				 low=mid+1;
			 else if ( searchLat+latLimit < idx.get(mid).getLat() )
				 hi=mid-1;
			 else if(Math.abs(idx.get(mid).getLat()-searchLat) <latLimit) // When mid is almost correct
			 {
				 if(searchLat-latLimit >= idx.get((low+mid)/2).getLat() )
					 low=(low+mid)/2;
				 else break;
				 if(searchLat+latLimit <= idx.get((mid+hi)/2).getLat() )
					 hi=(mid+hi)/2;
				 else break;
			 }
			 else break;
			 mid=(hi+low)/2;
		 }
		 // currently hi = the max index after which the lat will exceed the lat limit
		 // and low = the min index beyond which the lad will exceed the lat limit.
		 
		 // Step 2 - Filter long values and find min distance to map searched location
		 // Binary Search would not work on this.
		 double minDist=Double.MAX_VALUE;
		 double dist=0.0;
		 int indexMatched=-1;
		 for(int i=low;i<=hi;i++)
		 {
			 // 2 cases - if search long is near international date line, take abs value.
			 // then filter and narrow down search based on longitude.
			 // else ignore
			 if(	(searchLon >=180-lonLimit || searchLon >=-180+lonLimit)	|| 
			        (idx.get(i).getLon()<=searchLon+lonLimit && idx.get(i).getLon()>=searchLon-lonLimit)
			        )
			 {
				 // Find the lat long with min spherical distance.
				 dist=geoDist(searchLat, searchLon, idx.get(i).getLat(), idx.get(i).getLon());
				 if( dist < minDist && dist < maxDistMiles)
				 {
					 minDist=dist;
					 indexMatched=i;
				 }
			 }
		 }
		 
		 if(indexMatched!=-1) {
			 context.write(value, new Text(idx.get(indexMatched).getLat()+";"+idx.get(indexMatched).getLon()));
		 }
	 }
	 // Haversine Geo Distance formula
	 public double geoDist(double inputlat1, double inputlong1, double inputlat2, double inputlong2) 
	 {
		 double long2 = inputlong2 * Math.PI/180;
		 double lat2 = inputlat2 * Math.PI/180;
		 double long1 = inputlong1 * Math.PI/180;
		 double lat1 = inputlat1 * Math.PI/180;
		 
		 double  dlon =  long2 - long1;
		 double  dlat = lat2 - lat1;
		 double  a = Math.pow(Math.sin(dlat/2), 2)+ Math.cos(lat1) * Math.cos(lat2) * Math.pow(Math.sin(dlon/2),2);
		 double  c = 2* Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
		 double result = 3961*c;
		 //for kilometers, use 6373 instead
		 return result;
	 }
}

	public static void main(String[] args) throws Exception {

		FileUtils.deleteDirectory(new File(outputPath));
		
		Configuration conf = new Configuration();
		DistributedCache.addCacheFile(new URI(indexFile), conf);
		conf.set("latLimit","0.5");
		conf.set("lonLimit","3.9");
		conf.set("maxDistMiles","30");
		
		Job job=new Job(conf);
		job.setJarByClass(Search_MR.class); 
		
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		 
		job.setMapperClass(GeoSearchMapper.class);
		job.setNumReduceTasks(0);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1); 
	}
}
