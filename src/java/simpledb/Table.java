package simpledb;

/**
 * Created by Eason on 1/14/16.
 */

/**
 * A class that contains info of a Table
 *
 */

public class Table {

    private DbFile dbFile;
    private String name;
    private String pkeyField;
    private int tableId;

    public Table(DbFile file, String name, String pkeyField) {
        this.dbFile = file;
        this.name = name;
        this.pkeyField = pkeyField;
        this.tableId = file.getId();
    }

    public DbFile getDbFile() {
        return dbFile;
    }

    public String getName() {
        return name;
    }

    public String getPkeyField() {
        return pkeyField;
    }

    public int getTableId() {
        return tableId;
    }
}
