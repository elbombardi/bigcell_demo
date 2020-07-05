package tech.datarchy.bigcell.demo1;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

public class Demo1 {
	final static private Logger LOG = Logger.getLogger(Demo1.class); 
	
	private BigCellCore core; 
	
	private LineReader lineReader; 
	
	public void init() {
		LOG.debug("BigCell Demo 1 : Initialisation");
		
		core = new BigCellSparkCore("BigCell_Demo1_" + System.currentTimeMillis());
		
		Terminal terminal = null;
		try {
			terminal = TerminalBuilder.terminal();
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		} 

		lineReader = LineReaderBuilder.builder()
                .terminal(terminal)
                .build();		
		
		LOG.info(
				"\n  ____  _        _____     _ _  \n" +
				" |  _ \\(_)      / ____|   | | | \n" +
				" | |_) |_  __ _| |     ___| | | \n" +
				" |  _ <| |/ _` | |    / _ \\ | | \n" +
				" | |_) | | (_| | |___|  __/ | | \n" +
				" |____/|_|\\__, |\\_____\\___|_|_| \n" +
				"           __/ |                \n" +
				"          |___/                 Demo 1 \n");
		
		LOG.debug("BigCell Demo 1 : Attach Shutdown Hook"); 
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				shutdown(); 
			}
		});
	}
	
	public void repl() {
		while(true) {
			String line = lineReader.readLine("bigcell> ");
			try {
				BigCellCommandLineInterpreter.interpret(line, core);	
			} catch (Exception e) {
				e.printStackTrace(); 
			}
		}
	}
	
	private void shutdown() {
		LOG.debug("BigCell Demo 1 : Finalisation ... ");
		if (core != null) {
			core.close();
		}
		LOG.debug("BigCell Demo 1 : Bye!");
	}
}
