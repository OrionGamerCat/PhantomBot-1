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

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Prepares, converts, and executes a Create Table statement for the current database
 *
 * @author gmt2001
 */
public class TableBuilder {

    /**
     * The current table definition
     */
    private TableDefinition td;

    /**
     * Instantiates a new SQL table builder
     */
    protected TableBuilder() {
    }

    /**
     * Creates a new TableDefinition
     *
     * @param tname the name of the new table
     * @return a new TableDefinition object
     * @throws IllegalArgumentException if the table name is blank
     */
    public TableDefinition createTable(String tname) throws IllegalArgumentException {
        clear();

        td = new TableDefinition(tname);

        return td;
    }

    /**
     * Prepares and executes the Create Table query, then clears the definition
     *
     * @throws SQLException if a database access error occurs, the connection or a statement is closed, a statement fails execution, or a result set
     * is returned
     * @throws IllegalArgumentException if there are no fields defined for the table
     */
    public void execute() throws SQLException, IllegalArgumentException {
        try (Statement s = DataStore2.instance().processTableDefinition(td)) {
            s.executeBatch();
        }

        clear();
    }

    /**
     * Clears all table definition data
     */
    public void clear() {
        td = null;
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();

        clear();
    }

    /**
     * Contains table definition data
     */
    public static class TableDefinition {

        /**
         * The name of the new table, including any prefix
         */
        protected final String tname;
        /**
         * Whether the table will be a temporary (memory) table
         */
        protected boolean temporary = false;
        /**
         * Contains the fields for the table
         */
        protected final List<FieldDefinition> fields = new ArrayList<>();
        /**
         * Contains the indexes for the table
         */
        protected final List<IndexDefinition> indexes = new ArrayList<>();
        /**
         * Contains the foreign keys for the table
         */
        protected final List<ForeignKeyDefinition> foreignkeys = new ArrayList<>();

        /**
         * Stub constructor which should never be called
         */
        private TableDefinition() {
            tname = "";
        }

        /**
         * Creates a new TableDefinition
         *
         * @param tname the name of the new table
         * @throws IllegalArgumentException if the table name is blank
         */
        protected TableDefinition(String tname) throws IllegalArgumentException {
            if (tname.isEmpty()) {
                throw new IllegalArgumentException("Invalid table name");
            }

            this.tname = tname;
        }

        /**
         * Sets the tables parameters
         *
         * @param temporary whether the table will be a temporary (memory) table
         */
        public void setParams(boolean temporary) {
            this.temporary = temporary;
        }

        /**
         * Creates a new FieldDefinition and adds it to the table
         *
         * @param fname the name of the new field
         * @return a new FieldDefinition object
         * @throws IllegalArgumentException if the field name is blank
         */
        public FieldDefinition addField(String fname) throws IllegalArgumentException {
            FieldDefinition fd = new FieldDefinition(fname);

            fields.add(fd);

            return fd;
        }

        /**
         * Creates a new IndexDefinition and adds it to the table
         *
         * @param iname the name of the new index
         * @return a new IndexDefinition object
         * @throws IllegalArgumentException if the field name is blank
         */
        public IndexDefinition addIndex(String iname) throws IllegalArgumentException {
            IndexDefinition id = new IndexDefinition(iname);

            indexes.add(id);

            return id;
        }

        /**
         * Creates a new ForeignKeyDefinition and adds it to the table
         *
         * @return a new ForeignKeyDefinition object
         */
        public ForeignKeyDefinition addForeignKey() {
            ForeignKeyDefinition fkd = new ForeignKeyDefinition();

            foreignkeys.add(fkd);

            return fkd;
        }
    }

    /**
     * Contains a definition for a field
     */
    public static class FieldDefinition {

        /**
         * Represents the potential data types for the field
         */
        public static enum DataType {

            /**
             * Stores a 1 byte integer
             */
            TINYINT,
            /**
             * Stores a 2 byte integer
             */
            SMALLINT,
            /**
             * Stores a 3 byte integer
             */
            MEDIUMINT,
            /**
             * Stores a 4 byte integer
             */
            INTEGER,
            /**
             * Stores a 8 byte integer
             */
            BIGINT,
            /**
             * Stores a double precision floating point number
             */
            DOUBLE,
            /**
             * Stores a single precision floating point number
             */
            FLOAT,
            /**
             * Stores an exact value decimal number
             */
            DECIMAL,
            /**
             * Stores a date and time
             */
            DATETIME,
            /**
             * Stores a variable number of characters. Requires a length parameter
             */
            VARCHAR,
            /**
             * Stores a large amount of text
             */
            TEXT
        }
        /**
         * The name of the new field
         */
        protected final String fname;
        /**
         * The data type of the field
         */
        protected DataType dataType = DataType.INTEGER;
        /**
         * The length or precision parameter, for data types that support it
         * <br><br>
         * If the data type supports skipping this parameter, use -1 to skip it
         */
        protected int length = -1;
        /**
         * The scale parameter, for data types that support it
         * <br><br>
         * If the data type supports skipping this parameter, use -1 to skip it
         */
        protected int scale = -1;
        /**
         * Whether the data type is unsigned, for data types that support it
         */
        protected boolean unsigned = true;
        /**
         * Whether the field is not allowed to be null
         */
        protected boolean notNull = true;
        /**
         * Whether the field should be an auto increment field. If true, field is assumed to also be PRIMARY KEY
         */
        protected boolean autoIncrement = false;
        /**
         * The default value, as a string. Null to skip
         */
        protected String defaultValue = null;

        /**
         * Stub constructor which should never be called
         */
        private FieldDefinition() {
            fname = "";
        }

        /**
         * Creates a new FieldDefinition
         *
         * @param fname the name of the new field
         * @throws IllegalArgumentException if the field name is blank
         */
        protected FieldDefinition(String fname) throws IllegalArgumentException {
            if (fname.isEmpty()) {
                throw new IllegalArgumentException("Invalid field name");
            }

            this.fname = fname;
        }

        /**
         * Sets the fields parameters
         *
         * @param dataType the data type of the field
         * @param length the length or precision parameter, for data types that support it
         * <br><br>
         * If the data type supports skipping this parameter, use -1 to skip it
         * @param scale the scale parameter, for data types that support it
         * <br><br>
         * If the data type supports skipping this parameter, use -1 to skip it
         * @param unsigned whether the data type is unsigned, for data types that support it
         * @param notNull whether the field is not allowed to be null
         * @param autoIncrement whether the field should be an auto increment field
         * @param defaultValue the default value, as a string. Null to skip
         */
        public void setParams(DataType dataType, int length, int scale, boolean unsigned, boolean notNull, boolean autoIncrement,
                String defaultValue) {
            this.dataType = dataType;
            this.length = length;
            this.scale = scale;
            this.unsigned = unsigned;
            this.notNull = notNull;
            this.autoIncrement = autoIncrement;
            this.defaultValue = defaultValue;
        }
    }

    /**
     * Contains a definition for an index
     */
    public static class IndexDefinition {

        /**
         * Represents the potential index types
         */
        public static enum IndexType {

            /**
             * A primary key, a type of unique index used as a row identifier
             */
            PRIMARY,
            /**
             * A regular index
             */
            INDEX,
            /**
             * An index where all rows must have a unique value for the referenced columns
             */
            UNIQUE,
            /**
             * An index that allows fully searching large amounts of text data
             */
            FULLTEXT
        }
        /**
         * The name of the new index
         */
        protected final String iname;
        /**
         * The type of index
         */
        protected IndexType indexType = IndexType.INDEX;
        /**
         * The column(s) referenced by the index
         */
        protected final List<String> columns = new ArrayList<>();

        /**
         * Stub constructor which should never be called
         */
        private IndexDefinition() {
            iname = "";
        }

        /**
         * Creates a new IndexDefinition
         *
         * @param iname the name of the new index
         * @throws IllegalArgumentException if the index name is blank
         */
        protected IndexDefinition(String iname) throws IllegalArgumentException {
            if (iname.isEmpty()) {
                throw new IllegalArgumentException("Invalid index name");
            }

            this.iname = iname;
        }

        /**
         * Sets the indexes parameters
         *
         * @param indexType the type of index
         * @param columns a list of columns (and optional length attributes) referenced by the index
         */
        public void setParams(IndexType indexType, String[] columns) {
            this.indexType = indexType;
            this.columns.addAll(Arrays.asList(columns));
        }
    }

    /**
     * Contains a definition for a foreign key
     */
    public static class ForeignKeyDefinition {

        /**
         * Potential on update and on delete actions
         */
        public static enum Action {

            /**
             * Blocks the action on the parent table until the referring child rows are updated/deleted
             */
            RESTRICT,
            /**
             * Copies the changes/deletion on the parent table to the referring child rows
             */
            CASCADE,
            /**
             * Sets the referring child rows to null
             */
            SETNULL,
            /**
             * Same as RESTRICT, but defers action, allowing referring child rows to be updated/deleted in the same transaction
             * <br><br>
             * NOTE: In some databases, such as MySQL, this is instead exactly the same as RESTRICT
             */
            NOACTION;

            @Override
            public String toString() {
                switch (this) {
                    case RESTRICT:
                        return "RESTRICT";
                    case CASCADE:
                        return "CASCADE";
                    case SETNULL:
                        return "SET NULL";
                    case NOACTION:
                        return "NO ACTION";
                    default:
                        throw new IllegalArgumentException();
                }
            }
        }
        /**
         * The local column(s) referenced by the foreign key
         */
        protected final List<String> localColumns = new ArrayList<>();
        /**
         * The referenced parent table
         */
        protected String parentTable;
        /**
         * The parent column(s) referenced by the foreign key
         */
        protected final List<String> parentColumns = new ArrayList<>();
        /**
         * The on update action
         */
        protected Action onUpdate = Action.RESTRICT;
        /**
         * The on delete action
         */
        protected Action onDelete = Action.RESTRICT;

        /**
         * Creates a new ForeignKeyDefinition
         */
        protected ForeignKeyDefinition() {
        }

        /**
         * Sets the foreign keys parameters
         *
         * @param localColumns the local column(s) referenced by the foreign key
         * @param parentTable the referenced parent table
         * @param parentColumns the parent column(s) referenced by the foreign key
         * @param onUpdate the on update action
         * @param onDelete the on delete action
         */
        public void setParams(String[] localColumns, String parentTable, String[] parentColumns, Action onUpdate, Action onDelete) {
            this.localColumns.addAll(Arrays.asList(localColumns));
            this.parentTable = parentTable;
            this.parentColumns.addAll(Arrays.asList(parentColumns));
            this.onUpdate = onUpdate;
            this.onDelete = onDelete;
        }
    }
}
