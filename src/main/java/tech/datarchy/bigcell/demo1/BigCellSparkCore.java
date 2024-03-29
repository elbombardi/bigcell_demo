package tech.datarchy.bigcell.demo1;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.monotonically_increasing_id;
import static org.apache.spark.sql.functions.row_number;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Stack;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import tech.datarchy.bigcell.demo1.BigCellCore.BigCellSpreadsheet.BigCellColumn;
import tech.datarchy.bigcell.demo1.BigCellCore.BigCellSpreadsheet.BigCellFilter;
import tech.datarchy.bigcell.demo1.BigCellCore.BigCellSpreadsheet.BigCellRow;
import tech.datarchy.bigcell.demo1.BigCellCore.BigCellSpreadsheet.BigCellSort;
import tech.datarchy.bigcell.demo1.BigCellCore.BigCellSpreadsheet.BigCellSpreadSheetMeta;
import tech.datarchy.bigcell.demo1.BigCellCore.BigCellSpreadsheet.BigCellWindow;

/**
 * 
 * @author wissem
 *
 */
public class BigCellSparkCore implements BigCellCore<Dataset> {
	
	private final String ROW_NUM_COLUMN 		= "bigcell_row_num";  
	private final String ORDER_COLUMN 			= "bigcell_order";
	private final String NATURAL_ORDER_COLUMN 	= "bigcell_natural_order";
	private final String VISIBLE_ROW_COLUMN 	= "bigcell_row_visible";
			
	private SparkSession spark; 
	
	private Stack<BigCellSpreadsheet<Dataset>> mainStack; 
	
	private Stack<BigCellSpreadsheet<Dataset>> redoStack;
	
	
	public BigCellSparkCore(String appName) {

		spark = SparkSession
				.builder()
				.appName(appName)
//				.master("local[*]")
				.master("yarn")
				//.config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000")
				//.config("spark.hadoop.yarn.resoursemanager.address", "localhost:8030")
				.getOrCreate(); 
		
		mainStack = new Stack<BigCellSpreadsheet<Dataset>>();
		redoStack = new Stack<BigCellSpreadsheet<Dataset>>();
	}
	
	@Override
	public void loadCsv(String filename) {
		mainStack.removeAllElements(); 
		
		Dataset data = spark.read().option("header", "true").csv(filename); 
		BigCellSpreadsheet<Dataset> spreadsheet = initSpreadsheet(data);
		push(spreadsheet); 
		
	}

	@Override
	public void loadParquet(String filename) {
		mainStack.removeAllElements(); 
		
		Dataset<Row> data = spark.read().parquet(filename); 
		BigCellSpreadsheet<Dataset> spreadsheet = initSpreadsheet(data);
		
		push(spreadsheet); 
	}
	
	@Override
	public void loadJson(String filename) {
		mainStack.removeAllElements(); 
		
		Dataset<Row> data = spark.read().json(filename); 
		BigCellSpreadsheet<Dataset> spreadsheet = initSpreadsheet(data);
		
		push(spreadsheet); 
	}
	
	@Override
	public void loadSpreadsheet(String filename) {
		// TODO Auto-generated method stub
	}

	@Override
	public void addRow(BigCellRow row) {
		checkOpenSpreadsheet(); 
	}

	@Override
	public void addColumn(BigCellColumn column) {
		checkOpenSpreadsheet(); 
		
		Dataset data = mainStack.peek().data().withColumn(column.getName(), expr(column.getFormula()));
		
		BigCellSpreadSheetMeta meta = mainStack.peek().meta().clone(); 
		meta.getColumns().add(column); 
		
		push(new BigCellSpreadsheet<Dataset>(data, meta)); 
	}

	@Override
	public void updateRow(BigCellRow row) {
		checkOpenSpreadsheet(); 
	}

	@Override
	public void removeRow(Long rowNumber) {
		checkOpenSpreadsheet(); 
		
		WindowSpec window = Window.orderBy(ORDER_COLUMN).partitionBy(lit(1)); 
		BigCellSpreadsheet<Dataset> spreadsheet = mainStack.peek();
		
//		Dataset data = spreadsheet.data()
//					.filter(expr(ROW_NUM_COLUMN + " != " + rowNumber))
//					.withColumn(ROW_NUM_COLUMN, row_number().over(window)); 

		Dataset data = spreadsheet.data();
		push(new BigCellSpreadsheet<Dataset>(data, spreadsheet.meta().clone()));
	}

	@Override
	public void removeColumn(String columnName) {
		checkOpenSpreadsheet(); 
		
		BigCellSpreadsheet<Dataset> last = peek(); 
		
		BigCellSpreadSheetMeta meta = last.meta().clone(); 
		BigCellColumn column = meta.getColumn(columnName); 
		if (column == null) {
			throw new IllegalArgumentException("There exists no column with this name"); 
		}
		meta.getColumns().remove(column); 
	
		Dataset data = last.data().drop(columnName); 

		push(new BigCellSpreadsheet<Dataset>(data, meta));
	}

	@Override
	public void hideColumn(String columnName) {
		checkOpenSpreadsheet(); 
		
		BigCellSpreadsheet<Dataset> last = peek(); 
		BigCellSpreadSheetMeta meta = last.meta().clone(); 
		BigCellColumn col = meta.getColumn(columnName); 
		if (col == null) {
			throw new IllegalArgumentException("There exists no column with this name"); 
		}
		col.setHidden(true);
		push(new BigCellSpreadsheet<Dataset>(last.data(), meta)); 
	}

	@Override
	public void applySort(BigCellSort sort) {
		checkOpenSpreadsheet(); 
	}

	@Override
	public void applyFilter(BigCellFilter filter) {
		checkOpenSpreadsheet(); 
		BigCellSpreadsheet<Dataset> spreadsheet = peek(); 
		BigCellSpreadSheetMeta meta = spreadsheet.meta().clone(); 
		meta.applyFilter(filter);
		push(new BigCellSpreadsheet<Dataset>(spreadsheet.data(), meta));
	}

	@Override
	public void cancelSort() {
		checkOpenSpreadsheet(); 
	}

	@Override
	public void cancelFilter() {
		checkOpenSpreadsheet(); 
		BigCellSpreadsheet<Dataset> spreadsheet = peek(); 
		BigCellSpreadSheetMeta meta = spreadsheet.meta().clone(); 
		meta.cancelFilter();
		push(new BigCellSpreadsheet<Dataset>(spreadsheet.data(), meta));
	}

	@Override
	public void read(BigCellWindow window) {
		// TODO Auto-generated method stub
	}

	@Override
	public Long count() {
		checkOpenSpreadsheet();
		BigCellSpreadsheet<Dataset> last = peek(); 
		Dataset data = last.data(); 
		if (last.meta().isFiltered()) {
			data = data.filter(last.meta().getFilter().getExpression()); 
			System.out.println("Filtered with : '" + last.meta().getFilter().getExpression() + "'");
		}
		return data.count();
	}
	
	@Override
	public void undo() {
		if (mainStack.size() <= 1) {
			return; 
		}
		mainStack.peek().data().unpersist();
		redoStack.push(mainStack.pop());
		mainStack.peek().data().persist();
	}

	@Override
	public void redo() {
		if (redoStack.isEmpty()) {
			return; 
		}
		mainStack.peek().data().unpersist();
		mainStack.push(redoStack.pop());
		mainStack.peek().data().persist();
	}
	
	@Override
	public void save() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void saveAs() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void preview() {
		checkOpenSpreadsheet();
		BigCellSpreadsheet<Dataset> last = peek(); 
		Dataset dataset = last.data().select(getColumnsList(last.meta()));
		if (last.meta().isFiltered()) {
			dataset = dataset.filter(last.meta().getFilter().getExpression()); 
			System.out.println("Filtered with : '" + last.meta().getFilter().getExpression() + "'");
		}
		dataset.show();
	}
	
	@Override
	public void close() {
		if (spark != null) {
			spark.close(); 
		}
	}
	
	private BigCellSpreadsheet<Dataset> initSpreadsheet(Dataset data) {
		BigCellSpreadSheetMeta meta = new BigCellSpreadSheetMeta(); 
		meta.setColumns(new ArrayList<>());
		
		Arrays.asList(data.columns()).forEach( col -> {
			BigCellColumn column = new BigCellColumn(col);
			meta.getColumns().add(column); 
		});
		
//		WindowSpec window = Window.orderBy(ORDER_COLUMN).partitionBy(lit(1)); 
//
//		data = data.withColumn(ORDER_COLUMN, monotonically_increasing_id())
//				         .withColumn(NATURAL_ORDER_COLUMN, col(ORDER_COLUMN))
//				         .withColumn(ROW_NUM_COLUMN, row_number().over(window));

		return new BigCellSpreadsheet<Dataset>(data, meta);
		
	}
	
	private void checkOpenSpreadsheet() {
		if (mainStack.isEmpty()) {
			throw new IllegalStateException("There is no open spreadsheet"); 
		}
	}
	
	private Column[] getColumnsList(BigCellSpreadSheetMeta meta) {
		List<Column> columns = new ArrayList<>(); 
//		columns.add(col(ROW_NUM_COLUMN)); 
		meta.getColumns().forEach( c -> {
			if (!c.isHidden()) {
				columns.add(col(c.getName())); 	
			}
		});
		return columns.toArray(new Column[0]); 
	}
	
	private void push(BigCellSpreadsheet<Dataset> spreadsheet) {
		if (!mainStack.isEmpty()) {
			mainStack.peek().data().unpersist(); 
		}
		spreadsheet.data().persist();
		mainStack.push(spreadsheet); 
		redoStack.removeAllElements();
	}
	
	private BigCellSpreadsheet<Dataset> peek() {
		if (mainStack.isEmpty()) {
			return null; 
		}
		return mainStack.peek(); 
	}
	
}
