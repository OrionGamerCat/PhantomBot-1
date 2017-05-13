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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents an SQL transaction, which can be prepared in advance and over time
 *
 * @author gmt2001
 */
public class Transaction {

    /**
     * A list of PreparedStatement objects to execute
     */
    private final List<Statement> statements = new ArrayList<>();
    /**
     * The database Connection object
     */
    private final Connection connection;
    /**
     * Mutex
     */
    private final Object o = new Object();

    /**
     * Stub constructor which should never be called
     */
    private Transaction() {
        Thread.setDefaultUncaughtExceptionHandler(com.gmt2001.UncaughtExceptionHandler.instance());
        connection = null;
    }

    /**
     * Instantiates a new SQL transaction
     *
     * @param connection the JDBC Connection object to execute on
     */
    protected Transaction(Connection connection) {
        Thread.setDefaultUncaughtExceptionHandler(com.gmt2001.UncaughtExceptionHandler.instance());
        this.connection = connection;
    }

    /**
     * Creates a PreparedStatement object for sending parameterized SQL statements to the database and adds it to the statement queue
     * <br><br>
     * NOTE: Do NOT call any {@link java.sql.PreparedStatement#execute() execute} methods on a PreparedStatement returned by this class, use
     * {@link com.gmt2001.datastore2.Transaction#execute() Transaction.execute()} instead
     * @param sql an SQL statement that may contain one or more '?' IN parameter placeholders
     * @return a new default PreparedStatement object containing the pre-compiled SQL statement
     * @throws SQLException if a database access error occurs or this method is called on a closed connection
     */
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        synchronized (o) {
            PreparedStatement statement = connection.prepareStatement(sql);

            statements.add(statement);

            return statement;
        }
    }

    /**
     * Creates a Statement object for sending SQL statements to the database and adds it to the statement queue
     * <br><br>
     * NOTE: Do NOT call any {@link java.sql.Statement#execute() execute} methods on a Statement returned by this class, add the SQL query using
     * {@link java.sql.Statement#addBatch(String) addBatch(String)}, and then use
     * {@link com.gmt2001.datastore2.Transaction#execute() Transaction.execute()} to execute it instead
     *
     * @return a new default Statement object
     * @throws SQLException if a database access error occurs or this method is called on a closed connection
     */
    public Statement createStatement() throws SQLException {
        synchronized (o) {
            Statement statement = connection.createStatement();

            statements.add(statement);

            return statement;
        }
    }

    /**
     * Executes all queued PreparedStatements in batch mode as a transaction, then closes and clears the statement queue
     *
     * @throws SQLException if a database access error occurs, the connection or a statement is closed, a statement fails execution, or a result set
     * is returned
     */
    public void execute() throws SQLException {
        synchronized (o) {
            try {
                connection.setAutoCommit(false);

                for (Statement statement : statements) {
                    int[] results = statement.executeBatch();

                    if (results.length == 0 && statement instanceof PreparedStatement) {
                        ((PreparedStatement)statement).execute();
                    }
                }

                connection.commit();
                connection.setAutoCommit(true);
            } catch (SQLException ex) {
                connection.setAutoCommit(true);
                throw ex;
            }
        }

        clear();
    }

    /**
     * Closes all PreparedStatements and clears the statement queue
     *
     * @throws SQLException if a database access error occurs
     */
    public void clear() throws SQLException {
        synchronized (o) {
            for (Statement statement : statements) {
                statement.close();
            }

            statements.clear();
        }
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();

        clear();
    }
}
