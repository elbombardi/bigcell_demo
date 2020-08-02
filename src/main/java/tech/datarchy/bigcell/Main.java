package tech.datarchy.bigcell;

import tech.datarchy.bigcell.demo1.SparkDemo1;

public class Main {
	
	public static void main(String[] args) throws InterruptedException {
		SparkDemo1 demo = new SparkDemo1(); 
		demo.init(); 
		demo.repl();
	}
	

}
