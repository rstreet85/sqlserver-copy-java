/*
 * 
 */
package sqlserver.copy.java;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

/**
 * This program reads the data from SQL Server table, prints to CSV, then inserts CSV into new SQL Server table.
 *
 * @author Robert Streetman
 */
public class UtilityMain {
    private static String url;
    private static String tblName;
    private static String tblSchema;
    private static String fileName;
    
    /**
     * @param args url, tbl_schema, tbl_name, csv_file_name
     */
    public static void main(String[] args) {
        //These parameters were provided in original service
        url = args[0];
        tblSchema = args[1];
        tblName = args[2];
        fileName = args[3];
        
        readTable();
        //ToDo
    }
    
    private static void readTable() {
        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            
            try (Connection conn = DriverManager.getConnection(url)) {
                File tableFile;
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
                pStmnt.setString(1, tblName);
                pStmnt.setString(2, tblSchema);

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
                tableFile = new File(fileName);
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
                pStmnt.setString(1, tblSchema);
                pStmnt.setString(2, tblName);

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
    
}