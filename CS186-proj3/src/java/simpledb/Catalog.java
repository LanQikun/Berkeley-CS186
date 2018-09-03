package simpledb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas. For now, this is a stub catalog that must be populated
 * with tables by a user program before it can be used -- eventually, this
 * should be converted to a catalog that reads a catalog table from disk.
 */

public class Catalog {

    private HashMap<Integer, DbFile> idToFile;
    private HashMap<Integer, String> idToPkey;
    private HashMap<Integer, String> idToName;
    private HashMap<String, Integer> nameToId;

    /**
     * Constructor. Creates a new, empty catalog.
     */
    public Catalog() {
        idToFile = new HashMap<>();
        idToPkey = new HashMap<>();
        idToName = new HashMap<>();
        nameToId = new HashMap<>();
    }

    /**
     * Add a new table to the catalog. This table's contents are stored in the
     * specified DbFile.
     *
     * @param file
     *            the contents of the table to add; file.getId() is the
     *            identfier of this file/tupledesc param for the calls
     *            getTupleDesc and getFile
     * @param name
     *            the name of the table -- may be an empty string. May not be
     *            null. If a name conflict exists, use the last table to be
     *            added as the table for a given name. todo
     * @param pkeyField
     *            the name of the primary key field
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        if (name == null || pkeyField == null) {
            throw new IllegalArgumentException();
        }
        int tableid = file.getId();
        idToFile.put(tableid, file);
        idToName.put(tableid, name);
        idToPkey.put(tableid, pkeyField);
        nameToId.put(name, tableid);
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog. This table has tuples formatted using the
     * specified TupleDesc and its contents are stored in the specified DbFile.
     *
     * @param file
     *            the contents of the table to add; file.getId() is the
     *            identfier of this file/tupledesc param for the calls
     *            getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     *
     * @throws NoSuchElementException
     *             if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        if (name == null || !nameToId.containsKey(name)) {
            throw new NoSuchElementException();
        }
        return nameToId.get(name);
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     *
     * @param tableid
     *            The id of the table, as specified by the DbFile.getId()
     *            function passed to addTable
     * @throws NoSuchElementException
     *             if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        if (!idToFile.containsKey(tableid)) {
            throw new NoSuchElementException();
        }
        return idToFile.get(tableid).getTupleDesc();
    }

    /**
     * Returns the DbFile that can be used to read the contents of the specified
     * table.
     *
     * @param tableid
     *            The id of the table, as specified by the DbFile.getId()
     *            function passed to addTable
     */
    public DbFile getDbFile(int tableid) throws NoSuchElementException {
        // some code goes here
        if (!idToFile.containsKey(tableid)) {
            throw new NoSuchElementException();
        }
        return idToFile.get(tableid);
    }

    public String getPrimaryKey(int tableid) {
        if (!idToPkey.containsKey(tableid)) {
            throw new NoSuchElementException();
        }
        return idToPkey.get(tableid);
    }

    public Iterator<Integer> tableIdIterator() {
        return idToName.keySet().iterator();
    }

    public String getTableName(int id) {
        if (!idToName.containsKey(id)) {
            throw new NoSuchElementException();
        }
        return idToName.get(id);
    }


    /**
     * Delete all tables from the catalog
     */
    public void clear() {
        idToName.clear();
        idToPkey.clear();
        idToFile.clear();
        nameToId.clear();
    }

    /**
     * Reads the schema from a file and creates the appropriate tables in the
     * database.
     * 
     * 这里原本默认给的是绝对路径，我添加了对相对路径的处理
     * 默认了dat文件和catalog在同一目录下
     * 
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder = new File(catalogFile).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)));

            while ((line = br.readLine()) != null) {
                // assume line is of the format name (field type, field type,
                // ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                // System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")"))
                        .trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<String>();
                ArrayList<Type> types = new ArrayList<Type>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().toLowerCase().equals("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().toLowerCase().equals("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                //默认了dat文件和catalog在同一目录下
                String tableName = (baseFolder == null ? "" : baseFolder + File.separator)
                        + name + ".dat";
                HeapFile tabHf = new HeapFile(new File(tableName), t);
                addTable(tabHf, name, primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}
