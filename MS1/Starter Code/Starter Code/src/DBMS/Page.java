package DBMS;

import java.io.Serializable;
import java.util.ArrayList;

public class Page implements Serializable {

    private static final long serialVersionUID = 1L;

    private ArrayList<String[]> rows;

    public Page() {
        rows = new ArrayList<String[]>();
    }

    public void addRecord(String[] record) {
        rows.add(record);
    }

    public String[] getRecord(int index) {
        if (index < 0 || index >= rows.size()) {
            return null;
        }
        return rows.get(index);
    }

    public ArrayList<String[]> getRows() {
        return rows;
    }

    public int size() {
        return rows.size();
    }

    public boolean isFull(int pageSize) {
        return rows.size() >= pageSize;
    }
}