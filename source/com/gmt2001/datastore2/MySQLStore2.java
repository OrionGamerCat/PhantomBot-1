/*
 * Copyright (C) 2017 phantombot.tv
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.gmt2001.datastore2;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Represents and Manages the MySQL-specific implementation of a DataStore2
 * <br><br>
 * To instantiate an instance of MySQLStore2, see {@link com.gmt2001.datastore2.DataStore2#init(String, String[]) DataStore2.init(String, String[])}
 *
 * @author gmt2001
 */
public class MySQLStore2 extends DataStore2 {

    /**
     * The JDBC connection URL
     */
    private final String jdbcUrl;
    /**
     * The MySQL accounts username
     */
    private final String username;
    /**
     * The MySQL accounts password
     */
    private final String pass;
    /**
     * The MySQL schema to select
     */
    private final String schema;

    /**
     * Stub constructor which should never be called
     */
    private MySQLStore2() {
        super();
        jdbcUrl = "";
        username = "";
        pass = "";
        schema = "";
    }

    /**
     * Instantiates a MySQLStore2 with the specified JDBC connection parameters. Internal only. To create a MySQLStore2,
     * see {@link com.gmt2001.datastore2.DataStore2#init(String, String[]) DataStore2.init(String, String[])}
     *
     * @param connectionParameters a string array containing, in order: connection URL, username, password, (optional) schema
     * @throws ClassNotFoundException if the underlying JDBC class used could not be found
     * @throws IllegalArgumentException if a parameter in connectionParameters is missing or blank
     */
    protected MySQLStore2(String[] connectionParameters) throws ClassNotFoundException, IllegalArgumentException {
        super();

        Class.forName("com.mysql.jdbc.Driver");

        if (connectionParameters.length < 3) {
            throw new IllegalArgumentException("Connection parameter missing");
        }

        for (int i = 0; i < connectionParameters.length; i++) {
            String param = connectionParameters[i];
            if (i < 3 && param.isEmpty()) {
                throw new IllegalArgumentException("Invalid connection parameter");
            }
        }

        if (!connectionParameters[0].startsWith("jdbc:mysql://")) {
            jdbcUrl = "jdbc:mysql://" + connectionParameters[0];
        } else {
            jdbcUrl = connectionParameters[0];
        }
        
        username = connectionParameters[1];
        pass = connectionParameters[2];

        if (connectionParameters.length >= 4) {
            schema = connectionParameters[3];
        } else {
            schema = "";
        }
    }

    @Override
    public void connect() {
        if (connected()) {
            return;
        }

        try {
            connection = DriverManager.getConnection(jdbcUrl, username, pass);

            if (!schema.isEmpty()) {
                connection.setCatalog(schema);
                connection.setSchema(schema);
            }
            
            connection.setAutoCommit(true);
        } catch (SQLException ex) {
            com.gmt2001.Console.err.println("Failed to connect to MySQL: " + ex.getMessage());
            com.gmt2001.Console.err.logStackTrace(ex);
        }
    }

    @Override
    protected Statement processTableDefinition(TableBuilder.TableDefinition td) throws SQLException, IllegalArgumentException {
        if (td.fields.isEmpty()) {
            throw new IllegalArgumentException("No fields in table definition");
        }

        Statement s = this.createStatement();

        String sql = "CREATE TABLE IF NOT EXISTS " + td.tname + " (";

        boolean first = true;
        for (TableBuilder.FieldDefinition fd : td.fields) {
            if (!first) {
                sql += ", ";
            }

            first = false;

            sql += fd.fname + " " + fd.dataType.toString();

            if (fd.length >= 0) {
                sql += "(" + fd.length;

                if (fd.scale >= 0) {
                    sql += ", " + fd.scale;
                }

                sql += ")";
            }

            if (fd.unsigned) {
                sql += " UNSIGNED";
            }

            if (fd.notNull) {
                sql += " NOT NULL";
            }

            if (fd.autoIncrement) {
                sql += " AUTO_INCREMENT PRIMARY KEY";
            }

            if (fd.defaultValue != null) {
                sql += " DEFAULT '" + fd.defaultValue.replaceAll("'", "\\'") + "'";
            }
        }

        for (TableBuilder.IndexDefinition id : td.indexes) {
            sql += ", ";

            if (id.indexType == TableBuilder.IndexDefinition.IndexType.PRIMARY) {
                sql += "PRIMARY KEY (";
            } else {
                sql += id.indexType.toString()+ " " + id.iname + " (";
            }

            first = true;
            for (String c : id.columns) {
                if (!first) {
                    sql += ", ";
                }

                first = false;

                sql += c;
            }

            sql += ")";
        }

        for (TableBuilder.ForeignKeyDefinition fkd : td.foreignkeys) {
            sql += ", FOREIGN KEY (";

            first = true;
            for (String c : fkd.localColumns) {
                if (!first) {
                    sql += ", ";
                }

                first = false;

                sql += c;
            }

            sql += ") REFERENCES phantombot2_" + fkd.parentTable + " (";

            first = true;
            for (String c : fkd.parentColumns) {
                if (!first) {
                    sql += ", ";
                }

                first = false;

                sql += c;
            }

            sql += ") ON UPDATE " + fkd.onUpdate.toString() + " ON DELETE " + fkd.onDelete.toString();
        }

        sql += ") ENGINE=";

        if (td.temporary) {
            sql += "MEMORY;";
        } else {
            sql += "InnoDB;";
        }

        com.gmt2001.Console.debug.println("MySQLStore2.TableBuilder: " + sql);
        s.addBatch(sql);

        return s;
    }
}
