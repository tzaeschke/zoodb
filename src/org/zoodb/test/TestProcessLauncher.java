package org.zoodb.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class TestProcessLauncher {

	public static Process launchProcess(String optionsAsString, Class<?> mainClass, 
			String[] arguments) {
		try {
			ProcessBuilder processBuilder = createProcess(optionsAsString, mainClass, arguments);
			processBuilder.redirectErrorStream(true);
			Process prc = processBuilder.start();
			
			
			InputStream is = prc.getInputStream();
		    InputStreamReader isr = new InputStreamReader(is);
		    BufferedReader br = new BufferedReader(isr);
		    String line;
		    while ((line = br.readLine()) != null) {
		      System.out.println(line);
		    }
//		    System.out.println("Program terminated!");
			
			return prc;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}


	private static ProcessBuilder createProcess(String optionsAsString, Class<?> cls, 
			String[] arguments) {
		String jvm = 
			System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
		String cp = System.getProperty("java.class.path");

		String[] options = optionsAsString.split(" ");
		List < String > command = new ArrayList <String>();
		command.add(jvm);
		command.addAll(Arrays.asList(options));
		command.add(cls.getName());
		command.addAll(Arrays.asList(arguments));

		ProcessBuilder processBuilder = new ProcessBuilder(command);
		Map< String, String > environment = processBuilder.environment();
		environment.put("CLASSPATH", cp);
		return processBuilder;
	}
}