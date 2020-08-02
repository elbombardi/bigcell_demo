package tech.datarchy.bigcell.demo1;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import com.fasterxml.jackson.module.scala.util.Strings;

import tech.datarchy.bigcell.demo1.BigCellCore.BigCellSpreadsheet.BigCellColumn;

/**
 * 
 * @author wissem
 */
//   		 <p> load csv <file> 									done
//           <p> load parquet <file> 								done
//           <p> load json <file> 									done
//           <p> load spreedsheet <name>
//           <p> add row <row_num> <row content in json>
//           <p> add column <name> <formula> 						done			
//           <p> update row <row_num> <row content in json>		
//           <p> remove row <row_num> 								done
//           <p> remove column <name> 								done
//           <p> hide column <name> 								done
//           <p> apply sort <expression>
//           <p> apply filter <expression>
//           <p> cancel sort
//           <p> cancel filter
//           <p> print [<start_row> <end_row> <columns>]
//           <p> count 												done
//           <p> undo 												done
//           <p> redo 												done
//           <p> save
//           <p> save_as <filename>
//           <p> close 												done
//   		 <p> clear
class BigCellCommandLineInterpreter {
	
	private static Root rootCommand = new Root(); 
	
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
		
		rootCommand.interpret(parsedCommand, context);
		
	}
	
	static interface BigCellCommand {
		void interpret(List<String> command, BigCellCore context);
	}

	static abstract class AbstractBigCellCommand implements BigCellCommand {
		protected Map<String, AbstractBigCellCommand> children; 
		protected String usage; 
		
		@Override
		public void interpret(List<String> command, BigCellCore context) {
			if (children == null) {
				return; 
			}
			if (command == null || command.isEmpty()) {
				this.printUsage();
				return;
			}
			BigCellCommand cmd = children.get(command.get(0)); 
			if (cmd == null) {
				this.printUsage();
				return;
			}
			
			cmd.interpret(subcmd(command), context);
		}
		
		protected static List<String> subcmd(List<String> command) {
			if (command == null || command.isEmpty()) {
				return null;
			}
			return command.subList(1, command.size());
		}
		
		protected String getUsage() {
			StringBuilder sb = new StringBuilder(); 
			if (children != null && !children.isEmpty()) {
				for (AbstractBigCellCommand child : children.values()) {
					sb.append(child.getUsage());
				}
			} else {
				sb.append(usage).append("\n");
			}
			return sb.toString(); 
		}
		
		protected void printUsage() {
			throw new IllegalArgumentException(getUsage()); 
		}

	}
	
	static class Root extends AbstractBigCellCommand {
		
		@Override
		public void interpret(List<String> command, BigCellCore context) {
			new Clear().interpret(command, context);
			super.interpret(command, context);
		}
		
		public Root() {
			children = new LinkedHashMap<>(); 
			children.put("load", new Load());
			children.put("remove", new Remove());
			children.put("add", new Add());
			children.put("update", new Update());
			children.put("hide", new Hide());
			children.put("apply", new Apply());
			children.put("cancel", new Cancel());
			children.put("print", new Print());
			children.put("count", new Count());
			children.put("undo", new Undo());
			children.put("redo", new Redo());
			children.put("save", new Save());
			children.put("save_as", new SaveAs());
			children.put("close", new Close());
			children.put("clear", new Clear());
		}

	}
	
	static class Clear extends AbstractBigCellCommand {
		public Clear() {
			this.usage =  "clear"; 
		}
		
		@Override
		public void interpret(List<String> command, BigCellCore context) {
			for (int clear = 0; clear < 1000; clear++) {
				System.out.println("\b");
			}
		}
	}
	
	static class Load extends AbstractBigCellCommand {

		public Load() {
			children = new LinkedHashMap<>(); 
			children.put("csv", new LoadCsv());
			children.put("parquet", new LoadParquet());
			children.put("json", new LoadJson());
			children.put("spreadsheet", new LoadSpreadsheet());
		}

		// load csv <file>
		static class LoadCsv extends AbstractBigCellCommand {
			public LoadCsv() {
				this.usage = "load csv <file>";
			}

			@Override
			public void interpret(List<String> command, BigCellCore context) {
				if (command == null || command.isEmpty()) {
					this.printUsage();
				}
				context.loadCsv(command.get(0));
				context.preview();
			}
		}

		// load parquet <file>
		static class LoadParquet extends AbstractBigCellCommand {
			public LoadParquet() {
				this.usage = "load parquet <file>";
			}

			@Override
			public void interpret(List<String> command, BigCellCore context) {
				if (command == null || command.isEmpty()) {
					this.printUsage();
				}
				context.loadParquet(command.get(0));
				context.preview();
			}
		}

		// load Json <file>
		static class LoadJson extends AbstractBigCellCommand {
			public LoadJson() {
				this.usage = "load json <file>";

			}

			@Override
			public void interpret(List<String> command, BigCellCore context) {
				if (command == null || command.isEmpty()) {
					this.printUsage();
				}
				context.loadJson(command.get(0));
				context.preview();
			}
		}

		// load spreadsheet <file>
		static class LoadSpreadsheet extends AbstractBigCellCommand {
			public LoadSpreadsheet() {
				this.usage = "load spreadsheet <file>";
			}

			@Override
			public void interpret(List<String> command, BigCellCore context) {
				if (command == null || command.isEmpty()) {
					this.printUsage();
				}				
				context.loadJson(command.get(0));
				context.preview();
			}
		}

	}

	// remove [row|column]
	static class Remove extends AbstractBigCellCommand {

		public Remove() {
			children = new LinkedHashMap<>(); 
			children.put("row", new RemoveRow());
			children.put("column", new RemoveColumn());
		}

		// remove row <row_number>
		static class RemoveRow extends AbstractBigCellCommand {
			public RemoveRow() {
				this.usage = "remove row <row_number>";
			}

			@Override
			public void interpret(List<String> command, BigCellCore context) {
				if (command == null || command.isEmpty()) {
					printUsage();
				}
				Long rowNumber = null;
				try {
					rowNumber = Long.parseLong(command.get(0));
				} catch (NumberFormatException e) {
					printUsage();
				}
				context.removeRow(rowNumber);
				context.preview();
			}
		}

		// remove column <column_name>
		static class RemoveColumn extends AbstractBigCellCommand {
			public RemoveColumn() {
				this.usage = "remove column <column_name>";
			}

			@Override
			public void interpret(List<String> command, BigCellCore context) {
				if (command == null || command.isEmpty()) {
					printUsage();
				}
				String columnName = command.get(0);
				context.removeColumn(columnName);
				context.preview();
			}
		}

	}

	// add [row|column]
	static class Add extends AbstractBigCellCommand {

		public Add() {
			children = new LinkedHashMap<>(); 
			children.put("row", new AddRow());
			children.put("column", new AddColumn());
		}

		// add row
		static class AddRow extends AbstractBigCellCommand {
			public AddRow() {
				this.usage = "add row <row num> <row content as json>";
			}

			@Override
			public void interpret(List<String> command, BigCellCore context) {
			}
		}

		// add column <column_name> <formula>
		static class AddColumn extends AbstractBigCellCommand {
			public AddColumn() {
				this.usage = "add column <column name> <formula>";
			}

			@Override
			public void interpret(List<String> command, BigCellCore context) {
				if (command == null || command.size() < 2) {
					printUsage();
				}
				String columnName = command.get(0);
				String formula = command.get(1);
				BigCellColumn col = new BigCellColumn(columnName);
				col.setFormula(formula);
				col.setComputed(true);
				context.addColumn(col);
				context.preview();
			}
		}
	}

	// update row
	static class Update extends AbstractBigCellCommand {
		public Update() {
			children = new LinkedHashMap<>(); 
			children.put("row", new UpdateRow());
		}
		
		static class UpdateRow extends AbstractBigCellCommand {
			public UpdateRow() {
				this.usage = "update row <row num> <json>";
			}
			
			@Override
			public void interpret(List<String> command, BigCellCore context) {
				//TODO
			}
		}
		
	}

	// hide column
	static class Hide extends AbstractBigCellCommand {

		public Hide() {
			children = new LinkedHashMap<>(); 
			children.put("column", new HideColumn());
		}

		static class HideColumn extends AbstractBigCellCommand {
			public HideColumn() {
				this.usage = "hide column <column_name>";
			}

			@Override
			public void interpret(List<String> command, BigCellCore context) {
				if (command == null || command.isEmpty()) {
					printUsage();
				}
				String columnName = command.get(0);
				context.hideColumn(columnName);
				context.preview();
			}
		}
	}

	// apply [sort|filter]
	static class Apply extends AbstractBigCellCommand {
		public Apply() {
			children = new LinkedHashMap<>(); 
			children.put("sort", new ApplySort());
			children.put("filter", new ApplyFilter());
		}
		
		static class ApplySort extends AbstractBigCellCommand {
			public ApplySort() {
				this.usage = "apply sort (<column_name>:[desc|asc])+";
			}

			@Override
			public void interpret(List<String> command, BigCellCore context) {
				
			}
		}
		
		static class ApplyFilter extends AbstractBigCellCommand {
			public ApplyFilter() {
				this.usage = "apply filter <filter expression>";
			}

			@Override
			public void interpret(List<String> command, BigCellCore context) {
				
			}
		}
	}

	// print [<start_row> <end_row> <columns>]
	static class Print extends AbstractBigCellCommand {
		public Print() {
			this.usage = "print [<start_row> <end_row> <columns>]";
		}
		
		@Override
		public void interpret(List<String> command, BigCellCore context) {
			context.preview();
		}
	}

	// count
	static class Count extends AbstractBigCellCommand {
		public Count() {
			this.usage = "count";
		}
		
		@Override
		public void interpret(List<String> command, BigCellCore context) {
			System.out.println(String.format("Number of lines : %,d ", context.count()));
		}
	}

	// cancel [filter|sort]
	static class Cancel extends AbstractBigCellCommand {
		public Cancel() {
			children = new LinkedHashMap<>(); 
			children.put("sort", new CanelSort());
			children.put("filter", new CancelFilter());
		}
		
		static class CanelSort extends AbstractBigCellCommand {
			public CanelSort() {
				this.usage = "cancel sort";
			}

			@Override
			public void interpret(List<String> command, BigCellCore context) {
				
			}
		}
		
		static class CancelFilter extends AbstractBigCellCommand {
			public CancelFilter() {
				this.usage = "cancel filter";
			}

			@Override
			public void interpret(List<String> command, BigCellCore context) {
				
			}
		}
	}

	// undo
	static class Undo extends AbstractBigCellCommand {
		public Undo() {
			this.usage = "undo";
		}
		
		@Override
		public void interpret(List<String> command, BigCellCore context) {
			context.undo();
			context.preview();
		}
	}

	// redo
	static class Redo extends AbstractBigCellCommand {
		public Redo() {
			this.usage = "redo";
		}
		
		@Override
		public void interpret(List<String> command, BigCellCore context) {
			context.redo();
			context.preview();
		}
	}

	// save
	static class Save extends AbstractBigCellCommand {
		public Save() {
			this.usage = "save";
		}
		
		@Override
		public void interpret(List<String> command, BigCellCore context) {
			context.save();
		}
	}

	// save_as
	static class SaveAs extends AbstractBigCellCommand {
		public SaveAs() {
			this.usage = "save_as <filename>";
		}
		
		@Override
		public void interpret(List<String> command, BigCellCore context) {
		}
	}

	// close
	static class Close extends AbstractBigCellCommand {
		public Close() {
			this.usage = "close";
		}
		
		@Override
		public void interpret(List<String> command, BigCellCore context) {
			System.exit(0);
		}
	}
	
	static class CommandLineParser {

		enum STATES {
			OUTSIDE_TOKEN, INSIDE_TOKEN, INSIDE_STRING
		}

		static final List<Character> ESCAPE = Arrays.asList(' ', '\t', '\n', '\r');

		public static List<String> parse(String command) throws ParseException {
			Validate.notBlank(command);

			List<String> res = new ArrayList<String>();
			String acc = "";
			STATES state = STATES.OUTSIDE_TOKEN;
			for (char c : command.toCharArray()) {
				switch (state) {
				case OUTSIDE_TOKEN: {
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
				case INSIDE_STRING: {
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

				case INSIDE_TOKEN: {
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

}
