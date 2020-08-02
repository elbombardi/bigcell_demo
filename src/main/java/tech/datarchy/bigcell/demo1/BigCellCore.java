package tech.datarchy.bigcell.demo1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import tech.datarchy.bigcell.demo1.BigCellCore.BigCellSpreadsheet.BigCellColumn;
import tech.datarchy.bigcell.demo1.BigCellCore.BigCellSpreadsheet.BigCellFilter;
import tech.datarchy.bigcell.demo1.BigCellCore.BigCellSpreadsheet.BigCellRow;
import tech.datarchy.bigcell.demo1.BigCellCore.BigCellSpreadsheet.BigCellSort;
import tech.datarchy.bigcell.demo1.BigCellCore.BigCellSpreadsheet.BigCellWindow;

interface BigCellCore<T> {
	
	void loadCsv(String filename); 
	void loadParquet(String filename);
	void loadJson(String filename); 
	void loadSpreadsheet(String filename); 
	void addRow(BigCellRow row); 
	void addColumn(BigCellColumn column); 
	void updateRow(BigCellRow row); 
	void removeRow(Long rowNumber);
	void removeColumn(String columnName);
	void hideColumn(String columnName); 
	void applySort(BigCellSort sort);
	void applyFilter(BigCellFilter filter); 
	void cancelSort(); 
	void cancelFilter(); 
	void read(BigCellWindow window);
	Long count();
	void undo(); 
	void redo(); 
	void preview(); 
	void save(); 
	void saveAs();
	void close(); 
	 
	static class BigCellSpreadsheet<T> {
		
		private T data;
		private BigCellSpreadSheetMeta meta;

		public BigCellSpreadsheet(T data, BigCellSpreadSheetMeta meta) {
			this.data = data; 
			this.meta = meta; 
		}
		
		public BigCellSpreadSheetMeta meta() {
			return meta;
		}

		public T data() {
			return data;
		} 
		
		static class BigCellSpreadSheetMeta {
			
			private String path;
			private boolean filtered = false;
			private boolean sorted = false;
			private BigCellFilter filter; 
			private List<BigCellColumn> columns;
			
			public BigCellSpreadSheetMeta clone() {
				BigCellSpreadSheetMeta clone = new BigCellSpreadSheetMeta(); 
				clone.setFiltered(filtered);
				if (filter != null) {
					clone.setFilter(filter.clone());
				}
				clone.setSorted(sorted);
				clone.setPath(path);
				clone.setColumns(new ArrayList<>());
				for (BigCellColumn col : columns) {
					clone.getColumns().add(col.clone()); 
				}
				return clone; 
			}
			
			public boolean isFiltered() {
				return filtered;
			}

			public void setFiltered(boolean filtered) {
				this.filtered = filtered;
			}

			public BigCellFilter getFilter() {
				return filter;
			}
			
			public void setFilter(BigCellFilter filter) {
				this.filter = filter;
			}
			
			public void applyFilter(BigCellFilter filter) {
				this.filter = filter; 
				this.filtered = filter != null;
			}
			
			public void cancelFilter() {
				this.filtered = false; 
				this.filter = null;
			}
			
			public List<BigCellColumn> getColumns() {
				return columns;
			}
			
			public BigCellColumn getColumn(String columnName) {
				if (columns == null) {
					return null;
				}
				for (BigCellColumn col : columns) {
					if (col.getName().equals(columnName)) {
						return col; 
					}
				}
				return null; 
			}
			
			public void setColumns(List<BigCellColumn> columns) {
				this.columns = columns;
			}

			public boolean isSorted() {
				return sorted;
			}

			public void setSorted(boolean sorted) {
				this.sorted = sorted;
			}
			
			public String getPath() {
				return path;
			}
			
			public void setPath(String path) {
				this.path = path;
			}
		}
		
		static class BigCellColumn {

			private String name; 
			private boolean computed = false;
			private boolean hidden = false; 
			private String formula; 

			public BigCellColumn(String name) {
				this.name = name; 
			}
			
			public BigCellColumn clone() {
				BigCellColumn clone = new BigCellColumn(name); 
				clone.setComputed(computed);
				clone.setHidden(hidden); 
				clone.setFormula(formula);
				return clone; 
			}
			
			public String getName() {
				return name;
			}
			
			public void setComputed(boolean computed) {
				this.computed = computed;
			}
			
			public boolean isComputed() {
				return computed;
			}
			
			public void setHidden(boolean hidden) {
				this.hidden = hidden;
			}
			
			public boolean isHidden() {
				return hidden;
			}
			
			public void setFormula(String formula) {
				this.formula = formula;
			}
			
			public String getFormula() {
				return formula;
			}
		}
		
		static class BigCellRow {

			private Long number; 
			private Map<String, Object> values;
			
			public BigCellRow(Long number, Map<String, Object> values) {
				this.number = number; 
				this.values = values; 
			}
			
			public Long getNumber() {
				return number;
			} 
			
			public Map<String, Object> getValues() {
				return values; 
			}

		}
		
		static class BigCellSort extends HashMap<String, BigCellSortDirection> {}

		static enum BigCellSortDirection {
			ASC, 
			DESC;
		}
		
		static class BigCellWindow {
			Long rowLowerBound;
			Long rowUpperBound; 
			List<String> columns; 
			
			public BigCellWindow(Long rowLowerBound, Long rowUpperBound, List<String> columns) {
				this.rowLowerBound = rowLowerBound;
				this.rowUpperBound = rowUpperBound; 
				this.columns = columns;
			}

			public Long getRowLowerBound() {
				return rowLowerBound;
			}

			public List<String> getColumns() {
				return columns;
			}

			public Long getRowUpperBound() {
				return rowUpperBound;
			}
			
		}
		
		static class BigCellFilter {
			private String expression; 
			
			public BigCellFilter clone() {
				BigCellFilter clone = new BigCellFilter(this.expression);
				return clone;
			}
			
			public BigCellFilter(String expression) {
				this.expression = expression; 
			}
			
			public String getExpression() {
				return expression;
			}
		}
	}

}
