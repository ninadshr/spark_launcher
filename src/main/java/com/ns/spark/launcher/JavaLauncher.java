package com.ns.spark.launcher;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
/*
 * This is a sample launcher code to trigger remote Spark applications in cases of WebUI or
 * remote calls
 */
public class JavaLauncher {

	public static void main(String[] args) throws InterruptedException {
		
		System.out.println("Launcher called! Attempting to launch Spark application.");
		SparkAppHandle spark;
		try {
			spark = new SparkLauncher()
			        //Spark2 home directory.
					.setSparkHome("/opt/cloudera/parcels/SPARK2-2.3.0.cloudera2-1.cdh5.13.3.p0.316101/lib/spark2")
			        //Absolute path to application directory
					.setAppResource("/root/SparkLauncherJava/target/SparkLauncherJava-0.0.1-SNAPSHOT-jar-with-dependencies.jar")
			        //Spark application driver in above provided jar
					.setMainClass("com.ns.spark.launcher.SparkWordCount")
			        .setMaster("yarn")
			        .setDeployMode("cluster")
			        .startApplication();
			   final CountDownLatch countDownLatch = new CountDownLatch(1);
			   spark.addListener(new SparkAppHandle.Listener() {
			       //CountDownLatch counter to determine application progress
				   @Override
			       public void stateChanged(SparkAppHandle handle) {
			           if (handle.getState().isFinal()) {
			               countDownLatch.countDown();
			           }
			       }
			       //Can be overridden to publish appropriate progress details
				   @Override
			       public void infoChanged(SparkAppHandle handle) {
			       }
			   });
			   countDownLatch.await();
			   System.out.println(spark.getAppId() + " ended in state " + spark.getState());
		
		} catch (IOException e) {
			//Replace with more meaningful error handling
			e.printStackTrace();
		} 
			  
	}
}
