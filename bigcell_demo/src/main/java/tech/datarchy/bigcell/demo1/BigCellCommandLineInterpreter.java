package tech.datarchy.bigcell.demo1;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.Validate;

/**
 * 
 * @author wissem
 * 
 *         <p>load csv <file>									done
 *         <p>load parquet <file> 								done
 *         <p>load json <file> 									done
 *         <p>load spreedsheet <name> 							
 *         <p>add row <row_num> <row content in json> 			
 *         <p>add column <name> <formula> 						
 *         <p>update row <row_num> <row content in json> 		
 *         <p>remove row <row_num> 								done
 *         <p>remove column <name> 								done	
 *         <p>hide column <name> 								done
 *         <p>apply sort <expression> 							
 *         <p>apply filter <expression> 						
 *         <p>cancel sort
 *         <p>cancel filter 
 *         <p>print [<start_row> <end_row> <columns>]
 *         <p>count												done
 *         <p>undo 												done
 *         <p>redo 												done
 *         <p>save 
 *         <p>save_as <filename> 
 *         <p>close												done
 */
class BigCellCommandLineInterpreter {
	
	public static void interpret(String command, BigCellCore context) {
		if (command == null || command.isEmpty()) {
			return;
		}
		
		List<String> parsedCommand = null;
		try {
			parsedCommand = CommandLineParser.parse(command);
		} catch (ParseException e) {
			System.out.println("Error : " + e.getMessage() + " => " + e.getErrorOffset()); 
		}
		
		if (parsedCommand == null || parsedCommand.isEmpty()) {
			return; 
		}
		
		String cmd = parsedCommand.get(0); 
		List<String> subcmd = parsedCommand.subList(1, parsedCommand.size());
		
		switch (cmd) {
		case "load": {
			new Load(subcmd).interpret(context); 
			break;
		}
		case "remove": {
			new Remove(subcmd).interpret(context);
			break;
		}
		case "add": {
			new Add(subcmd).interpret(context);
			break;
		}
		case "update": {
			new Update(subcmd).interpret(context);
			break;
		}
		case "hide": {
			new Hide(subcmd).interpret(context);
			break;
		}
		case "apply": {
			new Apply(subcmd).interpret(context);
			break;
		}
		case "cancel": {
			new Cancel(subcmd).interpret(context);
			break;
		}
		case "print": {
			new Print(subcmd).interpret(context);
			break;
		}
		case "count": {
			new Count().interpret(context); 
			break;
		}
		case "undo": {
			new Undo().interpret(context);
			break;
		}
		case "redo": {
			new Redo().interpret(context);
			break;
		}
		case "save": {
			new Save().interpret(context);
			break;
		}
		case "save_as": {
			new SaveAs(subcmd).interpret(context);
			break;
		}
		case "close": {
			new Close().interpret(context);
			break;
		}
		case "clear" : {
			System.out.print("\\u001b[2J");
			break;
		}
		default : {
			System.out.println("Unkown command : '" + cmd + "'"); 
			System.out.println("Usage : [load|remove|add|update|hide|apply|cancel|print|undo|redo|save|save_as|close]"); 
		}
		}
	}

	static interface BigCellCommand {
		void interpret(BigCellCore context);
	}

	static class CommandLineParser {
		
		enum STATES {
			OUTSIDE_TOKEN, 
			INSIDE_TOKEN, 
			INSIDE_STRING
		}
		
		static final List<Character> ESCAPE = Arrays.asList(' ', '\t', '\n', '\r'); 
		
		public static List<String> parse(String command) throws ParseException {
			Validate.notBlank(command); 
			
			List<String> res = new ArrayList<String>();
			String acc = ""; 
			STATES state = STATES.OUTSIDE_TOKEN;
			for (char c : command.toCharArray()) {
				switch (state) {
				case OUTSIDE_TOKEN : {
					if (ESCAPE.contains(c)) {
						continue; 
					}
					
					if (c == '"') {
						state = STATES.INSIDE_STRING;
						if (!acc.isEmpty()) {
							res.add(acc);
							acc = "";
						}
						continue;
					}
					
					state = STATES.INSIDE_TOKEN;
					if (!acc.isEmpty()) {
						res.add(acc);
						acc = "";
					}
					acc += c;
					break; 
				}
				case INSIDE_STRING : {
					if (c == '"') {
						state = STATES.OUTSIDE_TOKEN;
						if (!acc.isEmpty()) {
							res.add(acc);
							acc = "";
						}
						continue;
					}
					
					acc += c; 
					break; 
				}
				
				case INSIDE_TOKEN : {
					if (ESCAPE.contains(c)) {
						state = STATES.OUTSIDE_TOKEN;
						if (!acc.isEmpty()) {
							res.add(acc);
							acc = "";
						}
						continue; 
					}
					
					if (c == '"') {
						state = STATES.INSIDE_STRING;
						if (!acc.isEmpty()) {
							res.add(acc);
							acc = "";
						}
						continue;
					}
					
					acc += c; 
					break; 
				}
				}
			}
			
			if (state == STATES.INSIDE_TOKEN) {
				res.add(acc);
				acc = "";
				state = STATES.OUTSIDE_TOKEN;
			}

			if (state != STATES.OUTSIDE_TOKEN) {
				if (state == STATES.INSIDE_STRING) {
					throw new ParseException("Closing brackets are missing", command.indexOf("\"")); 
				}
				throw new ParseException("Syntax error", 0); 
			}
			
			return res; 
		}
	}

	static abstract class AbstractBigCellCommand implements BigCellCommand {
		protected List<String> command; 
		protected String usage; 
		
		protected List<String> subcmd() {
			if (command == null || command.isEmpty()) {
				return null; 
			}
			return command.subList(1, command.size());
		}
		
		protected void printUsage() {
			throw new IllegalArgumentException("Syntax Error \n Usage : " + usage); 
		}
		
		protected void printUsage(String message) {
			throw new IllegalArgumentException("Syntax Error : " + message + "\n Usage: " + usage); 
		}
	}
	
	static class Load extends AbstractBigCellCommand {
		
		public Load(List<String> command) {
			this.command = command; 
			this.usage = "load [csv|parquet|json|spreadsheet]"; 
			
			if (command == null || command.isEmpty() ) {
				this.printUsage(); 
			}
		}
		
		@Override
		public void interpret(BigCellCore context) {
			switch (command.get(0)) {
			case "csv": {
				new LoadCsv(subcmd()).interpret(context); 
				break;
			}
			case "parquet": {
				new LoadParquet(subcmd()).interpret(context);
				break;
			}
			case "json": {
				new LoadJson(subcmd()).interpret(context);
				break;
			}
			case "spreadsheet": {
				new LoadSpreadsheet(subcmd()).interpret(context);
				break;
			}
			default : {
				this.printUsage(); 
			}
			}
		}
		
		// load csv <file>
		static class LoadCsv extends AbstractBigCellCommand {
			public LoadCsv(List<String> command) {
				this.command = command; 
				this.usage = "load csv <file>"; 
				if (command == null || command.isEmpty() ) {
					this.printUsage(); 
				}
			}
			@Override
			public void interpret(BigCellCore context) {
				context.loadCsv(command.get(0)); 
				context.preview(); 
			}
		}
		
		// load parquet <file>
		static class LoadParquet extends AbstractBigCellCommand {
			public LoadParquet(List<String> command) {
				this.command = command; 
				this.usage = "load parquet <file>"; 
				if (command == null || command.isEmpty() ) {
					this.printUsage(); 
				}
			}
			@Override
			public void interpret(BigCellCore context) {
				context.loadParquet(command.get(0)); 
				context.preview(); 
			}
		}
		
		// load Json <file>
		static class LoadJson extends AbstractBigCellCommand {
			public LoadJson(List<String> command) {
				this.command = command; 
				this.usage = "load json <file>"; 
				if (command == null || command.isEmpty() ) {
					this.printUsage(); 
				}
			}
			@Override
			public void interpret(BigCellCore context) {
				context.loadJson(command.get(0)); 
				context.preview(); 
			}
		}
		
		// load spreadsheet <file>
		static class LoadSpreadsheet extends AbstractBigCellCommand {
			public LoadSpreadsheet(List<String> command) {
				this.command = command; 
				this.usage = "load spreadsheet <file>"; 
				if (command == null || command.isEmpty() ) {
					this.printUsage(); 
				}
			}
			@Override
			public void interpret(BigCellCore context) {
				context.loadJson(command.get(0)); 
				context.preview(); 
			}
		}
		
	}
	
	// remove [row|column]
	static class Remove extends AbstractBigCellCommand {

		public Remove(List<String> command) {
			this.command = command;
			this.usage = "remove [row|column]"; 
			
			if (command == null || command.isEmpty() ) {
				this.printUsage(); 
			}
		}

		@Override
		public void interpret(BigCellCore context) {
			switch (command.get(0)) {
			case "row": {
				new RemoveRow(subcmd()).interpret(context); 
				break;
			}
			case "column": {
				new RemoveColumn(subcmd()).interpret(context);
				break;
			}
			default : {
				this.printUsage(); 
			}
			}
		}
		
		//remove row <row_number>
		static class RemoveRow extends AbstractBigCellCommand {
			private Long rowNumber;
			public RemoveRow(List<String> command) {
				this.command = command; 
				this.usage = "remove row <row_number>"; 
				if (command == null || command.isEmpty()) {
					printUsage();
				}
				try {
					rowNumber = Long.parseLong(command.get(0));	
				} catch (NumberFormatException e) {
					printUsage(); 
					
				}
				  
			}
			@Override
			public void interpret(BigCellCore context) {
				context.removeRow(rowNumber);
				context.preview();
			}
		}
		
		//remove column <column_name>
		static class RemoveColumn extends AbstractBigCellCommand {
			private String columnName; 
			public RemoveColumn(List<String> command) {
				this.command = command; 
				this.usage = "remove column <column_name>"; 
				if (this.command == null || this.command.isEmpty()) {
					printUsage();
				}
				this.columnName = this.command.get(0); 
			}
			@Override
			public void interpret(BigCellCore context) {
				context.removeColumn(columnName);
				context.preview();
			}
		}
		
	}
	
	// add [row|column]
	static class Add extends AbstractBigCellCommand {

		public Add(List<String> command) {
			this.command = command;
		}

		@Override
		public void interpret(BigCellCore context) {
		}
	}
	
	// update row 
	static class Update extends AbstractBigCellCommand {

		public Update(List<String> command) {
			this.command = command;
		}

		@Override
		public void interpret(BigCellCore context) {
		}
	}

	// hide column
	static class Hide extends AbstractBigCellCommand {
		
		public Hide(List<String> command) {
			this.command = command;
			this.usage = "hide column";
			if (command == null || command.isEmpty()) {
				printUsage(); 
			}
		}

		@Override
		public void interpret(BigCellCore context) {
			new HideColumn(subcmd()).interpret(context); 
		}
		
		static class HideColumn extends AbstractBigCellCommand {
			private String columnName; 
			public HideColumn(List<String> command) {
				this.command = command;
				this.usage = "hide column <column_name>"; 
				if (command == null || command.isEmpty()) {
					printUsage(); 
				}
				columnName = command.get(0); 
			}
			@Override
			public void interpret(BigCellCore context) {
				context.hideColumn(columnName);
				context.preview();
			}
		}
	}
	
	// apply [sort|filter]
	static class Apply extends AbstractBigCellCommand {

		public Apply(List<String> command) {
			this.command = command;
		}

		@Override
		public void interpret(BigCellCore context) {
		}
	}
	
	// print [<start_row> <end_row> <columns>]
	static class Print extends AbstractBigCellCommand {

		public Print(List<String> command) {
			this.command = command;
		}

		@Override
		public void interpret(BigCellCore context) {
			context.preview();
		}
	}

	// count
	static class Count extends AbstractBigCellCommand {
		@Override
		public void interpret(BigCellCore context) {
			System.out.println("Number of lines : " + context.count()); 
		}
	}

		
	// cancel [filter|sort]
	static class Cancel extends AbstractBigCellCommand {

		public Cancel(List<String> command) {
			this.command = command;
		}

		@Override
		public void interpret(BigCellCore context) {
		}
	}
	
	// undo
	static class Undo extends AbstractBigCellCommand {
		@Override
		public void interpret(BigCellCore context) {
			context.undo(); 
			context.preview(); 
		}
	}
	
	// redo
	static class Redo extends AbstractBigCellCommand {
		@Override
		public void interpret(BigCellCore context) {
			context.redo(); 
			context.preview(); 
		}
	}
	
	// save
	static class Save extends AbstractBigCellCommand {
		@Override
		public void interpret(BigCellCore context) {
			context.save();
		}
	}
	
	// save_as
	static class SaveAs extends AbstractBigCellCommand {

		public SaveAs(List<String> command) {
			this.command = command;
		}

		@Override
		public void interpret(BigCellCore context) {
		}
	}
	
	// close
	static class Close extends AbstractBigCellCommand {
		@Override
		public void interpret(BigCellCore context) {
			System.exit(0);
		}
	}
	
}
