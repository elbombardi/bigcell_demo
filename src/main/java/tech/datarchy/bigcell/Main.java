package tech.datarchy.bigcell;

import tech.datarchy.bigcell.demo1.Demo1;

public class Main {
	
	public static void main(String[] args) throws InterruptedException {
		Demo1 demo = new Demo1(); 
		demo.init(); 
		demo.repl();
	}
	

}
