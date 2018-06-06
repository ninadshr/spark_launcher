package com.ns.spark.launcher;

import java.io.IOException;

import org.apache.spark.launcher.SparkLauncher;

public class JavaLauncher {

	public static void main(String[] args) throws InterruptedException {
		
		System.out.println("I'm here");
		Process spark;
		try {
			spark = new SparkLauncher()
				    .setSparkHome("/opt/cloudera/parcels/SPARK2")
				    .setAppResource("SparkLauncherJava-0.0.1-SNAPSHOT-jar-with-dependencies.jar")
				    .setMainClass("com.ns.spark.launcher.SparkWordCount")
				    .setMaster("yarn")
				    .launch();
			spark.waitFor();
			System.out.println(spark.getErrorStream());
			System.out.println();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
			  
	}
}
