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

import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Represents and manages a JDBC-compatible, SQL-based, data storage method
 *
 * @author gmt2001
 */
public abstract class DataStore2 {

    /**
     * Stores the currently instantiated DataStore2 object
     */
    private static DataStore2 instance = null;

    /**
     * The current connection object in use by the DataStore2 instance
     */
    protected Connection connection;

    /**
     * Returns the currently instantiated DataStore2 object, or null if not initialized
     *
     * @return an object of type DataStore2, or null
     */
    public static DataStore2 instance() {
        return instance;
    }

    /**
     * Initializes a new instance of DataStore2
     *
     * @param dataStoreType the string name of a DataStore2 subclass to initialize. Can be a full java class path if located in another package
     * @param connectionParameters a string array containing the required parameters for the selected subclass, such as: connection URL, username,
     * password, or schema
     * @return true if the instance was created. false if an exception occurred when invoking the constructor (see error logs or console for stack trace)
     * @throws IllegalStateException if init was called after a DataStore2 has already been initialized
     * @throws ClassNotFoundException if the class specified in the dataStoreType parameter could not be found by the class loader, or the underlying
     * JDBC class used could not be found
     */
    public static boolean init(String dataStoreType, String[] connectionParameters) throws IllegalStateException, ClassNotFoundException {
        if (instance != null) {
            throw new IllegalStateException("DataStore2 already initialized");
        }

        String prefix;

        if (!dataStoreType.contains(".")) {
            prefix = "com.gmt2001.datastore2.";
        } else {
            prefix = dataStoreType.substring(0, dataStoreType.lastIndexOf(".") + 1);
            dataStoreType = dataStoreType.substring(dataStoreType.lastIndexOf(".") + 1);
        }

        Class<?> t;

        if (!prefix.equals("com.gmt2001.datastore2.")) {
            try {
                URLClassLoader child = new URLClassLoader(new URL[] { new URL("file://./datastores/" + dataStoreType + ".jar") },
                        DataStore2.class.getClassLoader());
                t = Class.forName(prefix + dataStoreType, true, child);
            } catch (MalformedURLException | ClassNotFoundException ex) {
                com.gmt2001.Console.debug.logStackTrace(ex);
                t = Class.forName(prefix + dataStoreType);
            }
        } else {
            t = Class.forName(prefix + dataStoreType);
        }

        try {
            instance = (DataStore2)t.getDeclaredConstructor(String[].class).newInstance((Object) connectionParameters);
            return true;
        } catch (NoSuchMethodException | SecurityException | InstantiationException| IllegalAccessException | IllegalArgumentException
                | InvocationTargetException ex) {
            com.gmt2001.Console.err.printStackTrace(ex);
        }

        instance = null;

        return false;
    }

    /**
     * Ensures the default uncaught exception handler is set
     */
    protected DataStore2(){
        Thread.setDefaultUncaughtExceptionHandler(com.gmt2001.UncaughtExceptionHandler.instance());
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();

        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    /**
     * Initiates a connection to the database, does nothing if already connected
     */
    public abstract void connect();

    /**
     * Determines if the DataStore2 object has a valid connection to the DBMS
     *
     * @return true if connected, false if a Connection object has not been created, or if not properly connected to the database
     */
    public boolean connected() {
        boolean connected = false;

        try {
            if (connection != null && connection.isValid(5)) {
                connected = true;
            }
        } catch (SQLException ex) {
            com.gmt2001.Console.err.logStackTrace(ex);
        }

        return connected;
    }

    /**
     * Creates a TableBuilder object for converting and executing a Create Table statement on the database
     *
     * @return a new TableBuilder object
     */
    public TableBuilder createTableBuilder() {
        return new TableBuilder();
    }

    /**
     * Creates a Transaction object for sending parameterized SQL statements to the database as part of an SQL Transaction
     * <br><br>
     * WARNING: Transaction's should ONLY be used for CREATE, ALTER, DROP, RENAME, TRUNCATE, INSERT, UPDATE, or DELETE statements where no returned
     * result is expected
     *
     * @return a new Transaction object
     */
    public Transaction createTransaction() {
        return new Transaction(connection);
    }

    /**
     * Creates a PreparedStatement object for sending parameterized SQL statements to the database
     * <br><br>
     * A SQL statement with or without IN parameters can be pre-compiled and stored in a PreparedStatement object. This object can then be used to
     * efficiently execute this statement multiple times.
     *
     * @param sql an SQL statement that may contain one or more '?' IN parameter placeholders
     * @return a new default PreparedStatement object containing the pre-compiled SQL statement
     * @throws SQLException if a database access error occurs or this method is called on a closed connection
     */
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return connection.prepareStatement(sql);
    }

    /**
     * Creates a Statement object for sending SQL statements to the database. SQL statements without parameters are normally executed using Statement
     * objects. If the same SQL statement is executed many times, it may be more efficient to use a PreparedStatement object.
     *
     * @return a new default Statement object
     * @throws SQLException if a database access error occurs or this method is called on a closed connection
     */
    public Statement createStatement() throws SQLException {
        return connection.createStatement();
    }

    /**
     * Processes a TableDefinition and returns a Statement with the Create Table query that is valid for the selected database
     *
     * @param td the TableDefinition object to process
     * @return a valid Statement to run the Create Table query for the current database
     * @throws SQLException if a database access error occurs or this method is called on a closed connection
     * @throws IllegalArgumentException if there are no fields defined for the table
     */
    protected abstract Statement processTableDefinition(TableBuilder.TableDefinition td) throws SQLException, IllegalArgumentException;
}
