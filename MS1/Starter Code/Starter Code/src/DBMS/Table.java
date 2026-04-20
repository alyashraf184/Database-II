package DBMS;

import java.io.Serializable;
import java.util.ArrayList;

public class Table implements Serializable {

    private static final long serialVersionUID = 1L;

    private String tableName;
    private String[] columnNames;
    private int pagesCount;
    private int recordsCount;
    private ArrayList<String> trace;

    public Table(String tableName, String[] columnNames) {
        this.tableName = tableName;
        this.columnNames = columnNames;
        this.pagesCount = 0;
        this.recordsCount = 0;
        this.trace = new ArrayList<String>();
    }

    public String getTableName() {
        return tableName;
    }

    public String[] getColumnNames() {
        return columnNames;
    }

    public int getPagesCount() {
        return pagesCount;
    }

    public void setPagesCount(int pagesCount) {
        this.pagesCount = pagesCount;
    }

    public int getRecordsCount() {
        return recordsCount;
    }

    public void setRecordsCount(int recordsCount) {
        this.recordsCount = recordsCount;
    }

    public ArrayList<String> getTraceList() {
        return trace;
    }

    public void addTrace(String  LTrace) {
        trace.add( LTrace);
    }

    public String getFullTrace() {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < trace.size(); i++) {
            sb.append(trace.get(i));
            sb.append("\n");
        }

        sb.append("Pages Count: ").append(pagesCount)
          .append(", Records Count: ").append(recordsCount);

        return sb.toString();
    }

    public String getLastTrace() {
        if (trace == null || trace.size() == 0) {
            return "";
        }
        return trace.get(trace.size() - 1);
    }
}