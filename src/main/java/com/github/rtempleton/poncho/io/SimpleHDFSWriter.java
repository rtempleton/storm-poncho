package com.github.rtempleton.poncho.io;

import java.util.Properties;

import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;

import com.github.rtempleton.poncho.StormUtils;

public class SimpleHDFSWriter extends HdfsBolt{
	
	private static final String HDFS_FILESYS = "HDFS.fs.defaultFS";
	private static final String HDFS_OUTPUT_PATH = "HDFS.output.path";
	private static final long serialVersionUID = 1L;

	/**
	 * Creates a HdfsBolt using the following default properties<br>
	 * CountSyncPolicy = 500<br>
	 * DelimitedRecordFormat with quoted ("\"") field delimiters<br>
	 * FileRotationPolicy of 64MB<br>
	 * <p>
	 * <b>REQUIRED PROPERTIES</b> that must be defined within the accompanying Properties argument
	 * <br><i>HDFS.fs.defaultFS</i> - the HDFS file system URL - "hdfs://sandbox.hortonworks.com:8020"
	 * <br><i>HDFS.output.path</i> - path to the HDFS directory where the bolt will write it's output.
	 * <p>
	 * If your topology contains more than one implementation of this bolt, it's properties can be uniquely defined by preprending the above property name with the writerName value provided in this constructor.<br>
	 * Exmaple: If you have two implementations of this bolt, one that writes "good" records and another that writes "bad" records
	 * you would construct this SimpleHDFSWriter with the writerName of "good" and identify it's unique properties as "goodHDFS.fs.defaultFS=..." and "goodHDFS.output.path=..."
	 * 
	 * @param props - the Properties
	 * @param writerName - The optional name of this SimpleHDFSWriter.
 	 */
	public SimpleHDFSWriter(Properties props, String writerName) {
		
		//In the event there is more than one SimpleHDFSWriter, prepend the writerName value to the required property name to find it's distinct value.  
		String outputDir = (writerName==null || writerName.isEmpty()) ? StormUtils.getRequiredProperty(props, HDFS_OUTPUT_PATH) : StormUtils.getRequiredProperty(props, writerName + HDFS_OUTPUT_PATH);
		FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath(outputDir);
		String fsUrl = (writerName==null || writerName.isEmpty()) ? StormUtils.getRequiredProperty(props, HDFS_FILESYS) : StormUtils.getRequiredProperty(props, writerName + HDFS_FILESYS);
		
		//Synchronize data buffer with the filesystem every 5000 tuples
		SyncPolicy syncPolicy = new CountSyncPolicy(500);
		
		RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter("");
		
		// Rotate data files when they reach 64 MB
		FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(64f, FileSizeRotationPolicy.Units.MB);

		this.withFsUrl(fsUrl)
		        .withFileNameFormat(fileNameFormat)
		        .withRotationPolicy(rotationPolicy)
		        .withRecordFormat(format)
		        .withSyncPolicy(syncPolicy);
		
	}

}
