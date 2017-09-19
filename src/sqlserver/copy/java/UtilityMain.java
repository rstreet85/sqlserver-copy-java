/*
 * 
 */
package sqlserver.copy.java;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

/**
 * This program reads the data from SQL Server table, prints to CSV, then inserts CSV into new SQL Server table.
 *
 * @author Robert Streetman
 */
public class UtilityMain {
    private static String urlInput;
    private static String urlOutput;
    private static String tblNameInput;
    private static String tblNameOutput;
    private static String tblSchemaInput;
    private static String tblSchemaOutput;
    private static File tableFile;
    
    /**
     * @param args url_input, input_tbl_schema, input_tbl_name, url_output, output_tbl_schema, output_tbl_name
     */
    public static void main(String[] args) {
        //These parameters were provided in original service
        urlInput = args[0];
        tblSchemaInput = args[1];
        tblNameInput = args[2];
        urlOutput = args[3];
        tblSchemaOutput = args[4];
        tblNameOutput = args[5];
        
        readTable();
        writeTable();
    }
    
    private static void readTable() {
        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            
            try (Connection conn = DriverManager.getConnection(urlInput)) {
                ArrayList<String> columnNames = new ArrayList();
                ArrayList<String> columnTypes = new ArrayList();
                ArrayList<String> columnNullable = new ArrayList();
                //These attributes will be used for detailed info
                ArrayList<String> columnCharLength = new ArrayList();
                ArrayList<String> columnNumLength = new ArrayList();
                ArrayList<String> columnNumRadix = new ArrayList();
                ArrayList<String> columnDateLength = new ArrayList();
                ArrayList<String> columnBinLength = new ArrayList();
                ArrayList<String> columnConstraints = new ArrayList();

                //Grab info about table from INFORMATION_SCHEMA
                PreparedStatement pStmnt = conn.prepareStatement(
                    "SELECT col.COLUMN_NAME, col.DATA_TYPE, col.CHARACTER_MAXIMUM_LENGTH, "
                    + "col.NUMERIC_PRECISION, col.NUMERIC_PRECISION_RADIX, col.DATETIME_PRECISION, "
                    + "col.CHARACTER_OCTET_LENGTH, col.IS_NULLABLE, usage.CONSTRAINT_NAME FROM "
                    + "INFORMATION_SCHEMA.COLUMNS col LEFT JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE "
                    + "usage ON usage.TABLE_NAME=col.TABLE_NAME AND usage.COLUMN_NAME=col.COLUMN_NAME WHERE "
                    + "col.TABLE_NAME=? AND col.TABLE_SCHEMA=?;");
                pStmnt.setString(1, tblNameInput);
                pStmnt.setString(2, tblSchemaInput);

                //Read column data from INFORMATION_SCHEMA
                try (ResultSet results = pStmnt.executeQuery()) {
                    while (results.next()) {
                        String name = results.getString("COLUMN_NAME");
                        String type = results.getString("DATA_TYPE");
                        String charLength = results.getString("CHARACTER_MAXIMUM_LENGTH");
                        String numLength = results.getString("NUMERIC_PRECISION");
                        String numRadix = results.getString("NUMERIC_PRECISION_RADIX");
                        String dateLength = results.getString("DATETIME_PRECISION");
                        String binLength = results.getString("CHARACTER_OCTET_LENGTH");
                        String nullable = results.getString("IS_NULLABLE");
                        String constraint = results.getString("CONSTRAINT_NAME");

                        constraint = (constraint == null) ? "" : constraint ;
                        nullable = (nullable.equals("YES")) ? "NULL" : "NOT NULL";

                        //Set data type parameters
                        if (type.equals("numeric") || type.equals("decimal")) {
                            type += "(" + numLength + "," + numRadix + ")";
                        } else if (type.equals("varchar") || type.equals("nvarchar")
                                || type.equals("nchar") || type.equals("char")) {
                            type += "(" + charLength + ")";
                        } else if (type.equals("datetime") || type.equals("datetime2")) {
                            type += "(" + dateLength + ")";
                        }

                        //Mark primary keys
                        if (constraint.contains("PK")) {
                            nullable += " PRIMARY KEY";
                        }

                        columnNames.add(name);
                        columnTypes.add(type);
                        columnNullable.add(nullable);
                        columnCharLength.add(charLength);
                        columnNumLength.add(numLength);
                        columnNumRadix.add(numRadix);
                        columnDateLength.add(dateLength);
                        columnBinLength.add(binLength);
                        columnConstraints.add(constraint);
                    }
                }

                //Create CSV filewriter
                tableFile = new File(tblNameInput + ".csv");
                CSVFormat csvFormat = CSVFormat.DEFAULT.withFirstRecordAsHeader();
                CSVPrinter csvWriter = new CSVPrinter(
                        new BufferedWriter( new FileWriter(tableFile)), csvFormat);
                
                //Build query to download all data from table
                StringBuilder qryTable = new StringBuilder();
                qryTable.append("SELECT ");
                int numColumns = columnNames.size();

                //Create select statement, write csv header (column names) to string builder
                csvWriter.printRecord(columnNames);
                csvWriter.printRecord(columnTypes);
                csvWriter.printRecord(columnNullable);
                
                for (int i = 0; i < numColumns; i++) {
                    String name = columnNames.get(i);
                    String type = columnTypes.get(i);

                    if (i == 0) {
                        qryTable.append(name);
                    } else {
                        qryTable.append(", ");
                        qryTable.append(name);
                    }

                    if (type.equals("geometry")) {
                        qryTable.append(".STAsText() AS geom");
                    }                
                }

                qryTable.append(" FROM ?.?;");
                pStmnt = conn.prepareStatement(qryTable.toString());
                pStmnt.setString(1, tblSchemaInput);
                pStmnt.setString(2, tblNameInput);

                //Read table rows, print to CSV
                try (ResultSet results = pStmnt.executeQuery()) {
                    csvWriter.printRecords(results);
                    csvWriter.flush();
                    csvWriter.close();
                }            
            } catch (IOException ex) {
                System.out.format("IOException thrown: %s", ex.getMessage());
            } catch (SQLException ex){
                System.out.format("SQLException thrown: %s", ex.getMessage());
            }
        } catch(ClassNotFoundException ex) {
            System.out.format("ClassNotFoundException thrown: %s", ex.getMessage());
        }
    }
    
    private static void writeTable() {
        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            
            try (Connection conn = DriverManager.getConnection(urlOutput);
                    Statement stmt = conn.createStatement()) {
                ArrayList<String> columnNames = new ArrayList();
                ArrayList<String> columnTypes = new ArrayList();
                ArrayList<String> columnConstraints = new ArrayList();
                
                //Build string to delete old temp table, if exists
                PreparedStatement pStmnt = conn.prepareStatement("IF EXISTS (SELECT * FROM sysobjects WHERE id=object_id(N?) AND OBJECTPROPERTY(id, N'IsUserTable')=1) DROP TABLE ?;");
                pStmnt.setString(1, tblNameOutput);
                pStmnt.setString(2, tblNameOutput);

                //Build CREATE statement for table
                StringBuilder qryCreate = new StringBuilder("CREATE TABLE ");
                qryCreate.append(tblNameOutput);
                qryCreate.append(" (");

                //Read headers from file. First line may be blank
                BufferedReader br = new BufferedReader(new FileReader(tableFile));
                String line = br.readLine();

                if (line == null || line.equals("\\s") || line.length() == 0) {
                    line = br.readLine();
                }

                br.close();
                String[] colNames = line.split(",");
                int numColumns = colNames.length;
                System.out.println("Number of columns: " + numColumns);

                for (int i = 0; i < numColumns; i++) {
                    columnNames.add(colNames[i]);
                }

                //Create CSV file reader
                CSVFormat csvForm = CSVFormat.DEFAULT.withHeader(colNames).withDelimiter(',');
                CSVParser parser = new CSVParser(new FileReader(tableFile), csvForm);
                List records = parser.getRecords();
                int numRecords = records.size();

                //Read data types
                CSVRecord tempRecord = (CSVRecord) records.get(1);

                for (String column : columnNames) {
                    columnTypes.add(tempRecord.get(column));
                }

                //Read column constraints
                tempRecord = (CSVRecord) records.get(2);

                for (String column : columnNames) {
                    columnConstraints.add(tempRecord.get(column));
                }

                //Build CREATE statement with headers, types & constraints
                for (int i = 0; i < numColumns; i++) {
                    //qryCreate.append(columnNames.get(i));
                    //qryCreate.append(" ");
                    //qryCreate.append(columnTypes.get(i));
                    //qryCreate.append(" ");
                    //qryCreate.append(columnConstraints.get(i));
                    qryCreate.append("? ? ?");
                    
                    if (i < numColumns - 1) {
                        qryCreate.append(", ");
                    }
                }

                qryCreate.append(");");
                pStmnt = conn.prepareStatement(qryCreate.toString());
                
                //Set parameters in threes
                for (int i = 1; i < numColumns + 1; i++) {
                    pStmnt.setString(i*3 - 2, columnNames.get(i));
                    pStmnt.setString(i*3 - 1, columnTypes.get(i));
                    pStmnt.setString(i*3, columnConstraints.get(i));
                }
                
                pStmnt.executeUpdate();

                //Read records (skip headers) from file, insert into new table
                //TODO: Switch to preparedstatement
                StringBuilder insertQry;
                for (int i = 3; i < numRecords; i++) {
                    tempRecord = (CSVRecord) records.get(i);
                    insertQry = new StringBuilder();
                    insertQry.append("INSERT INTO ");
                    insertQry.append(tblNameOutput);
                    insertQry.append(" (");

                    for (int j = 0; j < numColumns; j++) {
                        insertQry.append(columnNames.get(j));

                        if (j < numColumns - 1) {
                            insertQry.append(", ");
                        }
                    }

                    insertQry.append(") VALUES (");

                    for (int j = 0; j < numColumns; j++) {
                        String entry = tempRecord.get(columnNames.get(j));
                        String type = columnTypes.get(j);

                        if (entry == null || entry.length() == 0 || entry.equals("")) {
                            if (type.equals("text") || type.contains("char") || type.contains("varchar")
                                || type.contains("nchar") || type.contains("nvarchar")) {
                                insertQry.append("''");
                            } else {
                                insertQry.append("NULL");
                            }
                        } else if (type.equals("text") || type.contains("char") || type.contains("varchar")
                                || type.contains("nchar") || type.contains("nvarchar")) {
                            insertQry.append("'");
                            insertQry.append(entry.replace("\'", "\'\'"));
                            insertQry.append("'");
                        } else {
                            insertQry.append(entry);
                        }

                        if (j < numColumns - 1) {
                            insertQry.append(", ");
                        }
                    }

                    insertQry.append(");");
                    System.out.println(insertQry.toString());
                    stmt.execute(insertQry.toString());
                }

                //TODO: Update data to new table BEFORE deleting old table to avoid unneccessary data loss
                /*
                stmt.execute(DBQueries.SVAP02Query02(tblNameOld));
                stmt.execute(DBQueries.SVAP02Query03(tblName, tblNameOld));
                */
            } catch (SQLException | IOException ex) {
                
            }
        } catch(ClassNotFoundException ex) {
            
        }
    }
    
}