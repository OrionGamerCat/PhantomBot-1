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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.sqlite.SQLiteConfig;

/**
 * Represents and Manages the SQLite3-specific implementation of a DataStore2
 * <br><br>
 * To instantiate an instance of SqliteStore2, see {@link com.gmt2001.datastore2.DataStore2#init(String, String[]) DataStore2.init(String, String[])}
 *
 * @author gmt2001
 */
public class SqliteStore2 extends DataStore2 {

    /**
     * The JDBC connection URL
     */
    private final String jdbcUrl;
    /**
     * The size of the SQLite page cache
     */
    private final int cache_size;
    /**
     * Whether SQLite will use the slower, but safer, synchronous mode to write data to the disk
     */
    private final boolean safe_write;
    /**
     * Whether SQLite will use the rollback journal during transactions
     */
    private final boolean use_journal;

    /**
     * Stub constructor which should never be called
     */
    private SqliteStore2() {
        super();
        jdbcUrl = "";
        cache_size = 0;
        safe_write = false;
        use_journal = false;
    }

    /**
     * Instantiates a SqliteStore2 with the specified JDBC connection parameters. Internal only. To create a SqliteStore2, see
     * {@link com.gmt2001.datastore2.DataStore2#init(String, String[]) DataStore2.init(String, String[])}
     *
     * @param connectionParameters a string array containing a filename to open for SQLite3 configuration, or a blank string array to use the default
     * file name (sqlite3config.txt)
     * <br><br>
     * If the file does not exist or is missing parameters, the defaults will be used
     * @throws ClassNotFoundException if the underlying JDBC class used could not be found
     */
    protected SqliteStore2(String[] connectionParameters) throws ClassNotFoundException {
        super();

        Class.forName("org.sqlite.JDBC");

        String tjdbcUrl = "jdbc:sqlite:phantombot.db";
        int tcache_size = -50000;
        boolean tsafe_write = false;
        boolean tuse_journal = true;
        String fname = "sqlite3config.txt";

        if (connectionParameters.length > 0 && !connectionParameters[0].isEmpty()
                && Files.exists(Paths.get(connectionParameters[0]).toAbsolutePath())) {
            fname = connectionParameters[0];
        }

        if (Files.exists(Paths.get(fname).toAbsolutePath())) {
            try {
                List<String> lines = FileUtils.readLines(Paths.get(fname).toAbsolutePath().toFile());

                for (String line : lines) {
                    if (line.startsWith("dbname=") && line.length() > 8) {
                        tjdbcUrl = line.substring(7);
                    } else if (line.startsWith("cachesize=") && line.length() > 11) {
                        int oldtcache_size = tcache_size;
                        try {
                            tcache_size = Integer.parseInt(line.substring(10));
                        } catch (NumberFormatException ex) {
                            tcache_size = oldtcache_size;
                        }
                    } else if (line.startsWith("safewrite=") && line.length() > 11) {
                        tsafe_write = line.substring(10).equalsIgnoreCase("true") || line.substring(10).equalsIgnoreCase("1");
                    } else if (line.startsWith("journal=") && line.length() > 9) {
                        tsafe_write = line.substring(8).equalsIgnoreCase("true") || line.substring(8).equalsIgnoreCase("1");
                    }
                }
            } catch (IOException ex) {
                com.gmt2001.Console.err.logStackTrace(ex);
            }
        }

        if (!tjdbcUrl.startsWith("jdbc:sqlite:")) {
            tjdbcUrl = "jdbc:sqlite:" + tjdbcUrl;
        }

        jdbcUrl = tjdbcUrl;
        cache_size = tcache_size;
        safe_write = tsafe_write;
        use_journal = tuse_journal;
    }

    @Override
    public void connect() {
        if (connected()) {
            return;
        }

        try {
            SQLiteConfig config = new SQLiteConfig();
            config.setCacheSize(cache_size);
            config.setDateClass("INTEGER");
            config.setJournalMode(use_journal ? SQLiteConfig.JournalMode.TRUNCATE : SQLiteConfig.JournalMode.OFF);
            config.setSynchronous(safe_write ? SQLiteConfig.SynchronousMode.FULL : SQLiteConfig.SynchronousMode.NORMAL);
            config.setTempStore(SQLiteConfig.TempStore.MEMORY);
            config.setTransactionMode(SQLiteConfig.TransactionMode.IMMEDIATE);
            connection = DriverManager.getConnection(jdbcUrl, config.toProperties());
            connection.setAutoCommit(true);
        } catch (SQLException ex) {
            com.gmt2001.Console.err.println("Failed to connect to SQLite3: " + ex.getMessage());
            com.gmt2001.Console.err.logStackTrace(ex);
        }
    }

    @Override
    protected Statement processTableDefinition(TableBuilder.TableDefinition td) throws SQLException, IllegalArgumentException {
        if (td.fields.isEmpty()) {
            throw new IllegalArgumentException("No fields in table definition");
        }

        Statement s = this.createStatement();

        String sql = "CREATE";

        if (td.temporary) {
            sql += " TEMPORARY";
        }

        sql += "TABLE IF NOT EXISTS " + td.tname + " (";

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

            if (fd.notNull) {
                sql += " NOT NULL";
            }

            if (fd.autoIncrement) {
                sql += " PRIMARY KEY AUTOINCREMENT";
            }

            if (fd.defaultValue != null) {
                sql += " DEFAULT '" + fd.defaultValue.replaceAll("'", "\\'") + "'";
            }
        }

        for (TableBuilder.IndexDefinition id : td.indexes) {
            sql += ", ";

            switch (id.indexType) {
                case PRIMARY:
                    sql += "PRIMARY KEY (";
                    break;
                case INDEX:
                case FULLTEXT:
                    continue;
                default:
                    sql += id.indexType.toString() + " (";
                    break;
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

        sql += ");";

        com.gmt2001.Console.debug.println("SqliteStore2.TableBuilder: " + sql);
        s.addBatch(sql);

        for (TableBuilder.IndexDefinition id : td.indexes) {
            if (id.indexType != TableBuilder.IndexDefinition.IndexType.INDEX) {
                continue;
            }

            sql = "CREATE INDEX IF NOT EXISTS " + td.tname + "_INDEX_" + id.iname + " ON " + td.tname + " (";

            first = true;
            for (String c : id.columns) {
                if (!first) {
                    sql += ", ";
                }

                first = false;

                sql += c;
            }

            sql += ")";

            com.gmt2001.Console.debug.println("SqliteStore2.TableBuilder: " + sql);
            s.addBatch(sql);
        }

        return s;
    }
}
