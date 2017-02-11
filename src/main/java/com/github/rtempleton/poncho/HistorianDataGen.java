package com.github.rtempleton.poncho;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;

import org.joda.time.DateTime;

public class HistorianDataGen {

	public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
		// TODO Auto-generated method stub
		new HistorianDataGen(); 
	}
	
	public HistorianDataGen() throws FileNotFoundException, UnsupportedEncodingException{
		DateTime ts = DateTime.now();
		final int min = 50;
		final int max = 150;
		PrintWriter writer = new PrintWriter("/Users/rtempleton/tmp/data.csv", "UTF-8");
		
		
		for (int i = 0;i<1000000;i++){
			ts=ts.minusMinutes(1);
			String tss = ts.toString("yyyy-MM-dd HH:mm:ss");
			double d = (Math.random()*(max-min))+min;
			writer.println(String.format("1,3,%1s,%2$.2f", tss, d));
		}
		
		writer.close();
	}

}
