package DBMS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class DBApp {

    // Change this only if your starter code already has a different fixed value.
    // The PDF says dataPageSize is set once at the start of the class. :contentReference[oaicite:1]{index=1}
    public static int dataPageSize = 2;

    public static void createTable(String tableName, String[] columnsNames) {
        Table t = new Table(tableName, columnsNames);

        String traceLine = "Table created name:" + tableName + ", columnsNames:" + Arrays.toString(columnsNames);
        t.addTrace(traceLine);

        FileManager.storeTable(tableName, t);
    }

    public static void insert(String tableName, String[] record) {
        long start = System.currentTimeMillis();

        Table t = FileManager.loadTable(tableName);
        if (t == null) {
            return;
        }

        int targetPageNumber;

        if (t.getPagesCount() == 0) {
            Page p = new Page();
            p.addRecord(record);

            FileManager.storeTablePage(tableName, 0, p);

            t.setPagesCount(1);
            t.setRecordsCount(1);
            targetPageNumber = 0;
        } else {
            int lastPageNumber = t.getPagesCount() - 1;
            Page lastPage = FileManager.loadTablePage(tableName, lastPageNumber);

            if (lastPage == null) {
                lastPage = new Page();
            }

            if (!lastPage.isFull(dataPageSize)) {
                lastPage.addRecord(record);
                FileManager.storeTablePage(tableName, lastPageNumber, lastPage);

                t.setRecordsCount(t.getRecordsCount() + 1);
                targetPageNumber = lastPageNumber;
            } else {
                Page newPage = new Page();
                newPage.addRecord(record);

                int newPageNumber = t.getPagesCount();
                FileManager.storeTablePage(tableName, newPageNumber, newPage);

                t.setPagesCount(t.getPagesCount() + 1);
                t.setRecordsCount(t.getRecordsCount() + 1);
                targetPageNumber = newPageNumber;
            }
        }

        long end = System.currentTimeMillis();
        long executionTime = end - start;

        String traceLine = "Inserted:" + Arrays.toString(record)
                + ", at page number:" + targetPageNumber
                + ", execution time (mil):" + executionTime;

        t.addTrace(traceLine);
        FileManager.storeTable(tableName, t);
    }

    public static ArrayList<String[]> select(String tableName) {
        long start = System.currentTimeMillis();

        ArrayList<String[]> result = new ArrayList<String[]>();

        Table t = FileManager.loadTable(tableName);
        if (t == null) {
            return result;
        }

        for (int i = 0; i < t.getPagesCount(); i++) {
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

        String traceLine = "Select all pages:" + t.getPagesCount()
                + ", records:" + result.size()
                + ", execution time (mil):" + executionTime;

        t.addTrace(traceLine);
        FileManager.storeTable(tableName, t);

        return result;
    }

    public static ArrayList<String[]> select(String tableName, int pageNumber, int recordNumber) {
        long start = System.currentTimeMillis();

        ArrayList<String[]> result = new ArrayList<String[]>();

        Table t = FileManager.loadTable(tableName);
        if (t == null) {
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

        String traceLine = "Select pointer page:" + pageNumber
                + ", record:" + recordNumber
                + ", total output count:" + result.size()
                + ", execution time (mil):" + executionTime;

        t.addTrace(traceLine);
        FileManager.storeTable(tableName, t);

        return result;
    }

    public static ArrayList<String[]> select(String tableName, String[] columnNames, String[] values) {
        long start = System.currentTimeMillis();

        ArrayList<String[]> result = new ArrayList<String[]>();

        Table t = FileManager.loadTable(tableName);
        if (t == null) {
            return result;
        }

        ArrayList<String> recordsPerPageTrace = new ArrayList<String>();

        for (int pageIndex = 0; pageIndex < t.getPagesCount(); pageIndex++) {
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
                    int columnIndex = getColumnIndex(t.getColumnNames(), columnNames[c]);

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

        String traceLine = "Select condition:" + Arrays.toString(columnNames)
                + "->" + Arrays.toString(values)
                + ", Records per page:" + recordsPerPageString.toString()
                + ", records:" + result.size()
                + ", execution time (mil):" + executionTime;

        t.addTrace(traceLine);
        FileManager.storeTable(tableName, t);

        return result;
    }

    public static String getFullTrace(String tableName) {
        Table t = FileManager.loadTable(tableName);
        if (t == null) {
            return "";
        }
        return t.getFullTrace();
    }

    public static String getLastTrace(String tableName) {
        Table t = FileManager.loadTable(tableName);
        if (t == null) {
            return "";
        }
        return t.getLastTrace();
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