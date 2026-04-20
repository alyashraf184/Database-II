package DBMS;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.junit.Test;

public class DBApp {

    public static int dataPageSize = 2;

    public static void createTable(String tableName, String[] columnsNames) {
        
        Table DBTable = new Table(tableName, columnsNames);

        String LTrace = "Table name:" + tableName + ", columnsNames:" + Arrays.toString(columnsNames);

        DBTable.addTrace( LTrace);

        FileManager.storeTable(tableName, DBTable);
    }

    public static void insert(String tableName, String[] record) {

        long start = System.currentTimeMillis();

        Table DBTable = FileManager.loadTable(tableName);
        
        if (DBTable == null) {

            return;

        }

        int targetPageNumber;

        if (DBTable.getPagesCount() == 0) {

            Page p = new Page();
            p.addRecord(record);
            FileManager.storeTablePage(tableName, 0, p);
            DBTable.setPagesCount(1);
            DBTable.setRecordsCount(1);
            targetPageNumber = 0;

        } 

        else

             {

            int lastPageNumber = DBTable.getPagesCount() - 1;
            Page lastPage = FileManager.loadTablePage(tableName, lastPageNumber);

            if (lastPage == null) {

                lastPage = new Page();

            }

            if (!lastPage.isFull(dataPageSize)) {

                lastPage.addRecord(record);
                FileManager.storeTablePage(tableName, lastPageNumber, lastPage);

                DBTable.setRecordsCount(DBTable.getRecordsCount() + 1);
                targetPageNumber = lastPageNumber;

            } 

            else 

                {
                Page newPage = new Page();
                newPage.addRecord(record);

                int newPageNumber = DBTable.getPagesCount();
                FileManager.storeTablePage(tableName, newPageNumber, newPage);

                DBTable.setPagesCount(DBTable.getPagesCount() + 1);
                DBTable.setRecordsCount(DBTable.getRecordsCount() + 1);
                targetPageNumber = newPageNumber;

            }
        }

        long end = System.currentTimeMillis();
        long executionTime = end - start;

        String  LTrace = "Inserted:" + Arrays.toString(record) + ", at page number:" + targetPageNumber + ", execution time (mil):" + executionTime;

        DBTable.addTrace( LTrace);
        FileManager.storeTable(tableName, DBTable);

    }

    public static ArrayList<String[]> select(String tableName) {

        long start = System.currentTimeMillis();

        ArrayList<String[]> result = new ArrayList<String[]>();

        Table DBTable = FileManager.loadTable(tableName);
        if (DBTable == null) {
            return result;

        }

        for (int i = 0; i < DBTable.getPagesCount(); i++) {
            
            Page p = FileManager.loadTablePage(tableName, i);
            if (p == null) {
                continue;

            }

            ArrayList<String[]> rows = p.getRows();

            for (int j = 0; j < rows.size(); j++) {

                result.add(rows.get(j));

            }
        }

        long end = System.currentTimeMillis();
        long executionTime = end - start;

        String  LTrace = "Select all pages:" + DBTable.getPagesCount() + ", records:" + result.size() + ", execution time (mil):" + executionTime;

        DBTable.addTrace( LTrace);
        FileManager.storeTable(tableName, DBTable);

        return result;
    }

    public static ArrayList<String[]> select(String tableName, int pageNumber, int recordNumber) {
        long start = System.currentTimeMillis();

        ArrayList<String[]> result = new ArrayList<String[]>();

        Table DBTable = FileManager.loadTable(tableName);
        if (DBTable == null) {
            return result;
            
        }

        Page p = FileManager.loadTablePage(tableName, pageNumber);
        if (p != null) {
            String[] record = p.getRecord(recordNumber);
            if (record != null) {
                result.add(record);
            }
        }

        long end = System.currentTimeMillis();
        long executionTime = end - start;

        String  LTrace = "Select pointer page:" + pageNumber
                + ", record:" + recordNumber
                + ", total output count:" + result.size()
                + ", execution time (mil):" + executionTime;

        DBTable.addTrace( LTrace);
        FileManager.storeTable(tableName, DBTable);

        return result;
    }

    public static ArrayList<String[]> select(String tableName, String[] columnNames, String[] values) {
        long start = System.currentTimeMillis();

        ArrayList<String[]> result = new ArrayList<String[]>();

        Table DBTable = FileManager.loadTable(tableName);
        if (DBTable == null) {
            return result;
        }

        ArrayList<String> recordsPerPageTrace = new ArrayList<String>();

        for (int pageIndex = 0; pageIndex < DBTable.getPagesCount(); pageIndex++) {
            Page p = FileManager.loadTablePage(tableName, pageIndex);
            if (p == null) {
                continue;
            }

            int matchesInThisPage = 0;

            for (int rowIndex = 0; rowIndex < p.size(); rowIndex++) {
                String[] row = p.getRecord(rowIndex);
                if (row == null) {
                    continue;
                }

                boolean matches = true;

                for (int c = 0; c < columnNames.length; c++) {
                    int columnIndex = getColumnIndex(DBTable.getColumnNames(), columnNames[c]);

                    if (columnIndex == -1) {
                        matches = false;
                        break;
                    }

                    if (!row[columnIndex].equals(values[c])) {
                        matches = false;
                        break;
                    }
                }

                if (matches) {
                    result.add(row);
                    matchesInThisPage++;
                }
            }

            if (matchesInThisPage > 0) {
                recordsPerPageTrace.add("[" + pageIndex + ", " + matchesInThisPage + "]");
            }
        }

        long end = System.currentTimeMillis();
        long executionTime = end - start;

        StringBuilder recordsPerPageString = new StringBuilder();
        recordsPerPageString.append("[");
        for (int i = 0; i < recordsPerPageTrace.size(); i++) {
            recordsPerPageString.append(recordsPerPageTrace.get(i));
            if (i < recordsPerPageTrace.size() - 1) {
                recordsPerPageString.append(", ");
            }
        }
        recordsPerPageString.append("]");

        String  LTrace = "Select condition:" + Arrays.toString(columnNames) + "->" + Arrays.toString(values) + ", Records per page:" + recordsPerPageString.toString() + ", records:" + result.size() + ", execution time (mil):" + executionTime;

        DBTable.addTrace( LTrace);
        FileManager.storeTable(tableName, DBTable);

        return result;
    }

    public static String getFullTrace(String tableName) {
        Table DBTable = FileManager.loadTable(tableName);
        if (DBTable == null) {
            return "";
        }
        return DBTable.getFullTrace();
    }

    public static String getLastTrace(String tableName) {
        Table DBTable = FileManager.loadTable(tableName);
        if (DBTable == null) {
            return "";
        }
        return DBTable.getLastTrace();
    }

    private static int getColumnIndex(String[] columns, String target) {
        for (int i = 0; i < columns.length; i++) {
            if (columns[i].equals(target)) {
                return i;
            }
        }
        return -1;
    }

    public static void main(String[] args) throws IOException {
        FileManager.reset();

        String[] cols = {"id", "name", "major", "semester", "gpa"};
        createTable("student", cols);

        String[] r1 = {"1", "stud1", "CS", "5", "0.9"};
        String[] r2 = {"2", "stud2", "BI", "7", "1.2"};
        String[] r3 = {"3", "stud3", "CS", "2", "2.4"};
        String[] r4 = {"4", "stud4", "DMET", "9", "1.2"};
        String[] r5 = {"5", "stud5", "BI", "4", "3.5"};

        insert("student", r1);
        insert("student", r2);
        insert("student", r3);
        insert("student", r4);
        insert("student", r5);

        System.out.println("Output of selecting the whole table content:");
        ArrayList<String[]> result1 = select("student");
        printResult(result1);

        System.out.println("----------------------------------------");

        System.out.println("Output of selecting the output by position:");
        ArrayList<String[]> result2 = select("student", 1, 1);
        printResult(result2);

        System.out.println("----------------------------------------");

        System.out.println("Output of selecting the output by column condition:");
        ArrayList<String[]> result3 = select("student", new String[]{"gpa"}, new String[]{"1.2"});
        printResult(result3);

        System.out.println("----------------------------------------");

        System.out.println("Full Trace of the table:");
        System.out.println(getFullTrace("student"));

        System.out.println("----------------------------------------");

        System.out.println("Last Trace of the table:");
        System.out.println(getLastTrace("student"));

        System.out.println("----------------------------------------");

        System.out.println("The trace of the Tables Folder:");
        System.out.println(FileManager.trace());
    }

    private static void printResult(ArrayList<String[]> result) {
        for (int i = 0; i < result.size(); i++) {
            String[] row = result.get(i);
            for (int j = 0; j < row.length; j++) {
                System.out.print(row[j] + " ");
            }
            System.out.println();
        }
    }
}