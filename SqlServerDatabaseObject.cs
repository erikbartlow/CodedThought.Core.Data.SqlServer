using CodedThought.Core.Data.Interfaces;
using CodedThought.Core.Exceptions;

using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging.Configuration;
using Microsoft.Identity.Client;

using Org.BouncyCastle.Security.Certificates;

using System.Data;
using System.Data.Common;
using System.Text;

namespace CodedThought.Core.Data.SqlServer {

    /// <summary>SqlServerDatabaseObject provides all SQLServer specific functionality needed by DBStore and its family of classes..</summary>
    public class SqlServerDatabaseObject : DatabaseObject, IDatabaseObject, IDbSchema {
        #region Declarations

        private SqlConnection _connection;

        #endregion

        #region Constructor

        public SqlServerDatabaseObject() : base() => _connection = new();

        #endregion Constructor

        #region Transaction and Connection Methods

        /// <summary>Commits updates and inserts. This is only for Oracle database operations.</summary>
        public override void Commit() => CommitTransaction();

        /// <summary>Opens an SqlServer Connection</summary>
        /// <returns></returns>
        protected override IDbConnection OpenConnection() {
            try {
                if (String.IsNullOrEmpty(_connection.ConnectionString))
                    _connection = new(ConnectionString);
                if (_connection.State != ConnectionState.Open)
                    _connection.Open();
                return _connection;
            }
            catch (SqlException ex) {
                throw new CodedThoughtApplicationException("Could not open Connection.  Check connection string" + "/r/n" + ex.Message + "/r/n" + ex.StackTrace, ex);
            }
        }
        /// <summary>
        /// Tests the connection to the database.
        /// </summary>
        /// <returns></returns>
        public override bool TestConnection() {
            try {
                OpenConnection();
                return Connection.State == ConnectionState.Open;
            }
            catch (CodedThoughtException) {
                throw;
            }
        }
        #endregion Transaction and Connection Methods

        #region Other Override Methods
        /// <summary>
        /// Returns the active connection. If the stack has a connection then it is returned.
        /// connection is created.
        /// </summary>
        public override IDbConnection Connection => _connection == null ? (SqlConnection) base.Connection : (SqlConnection) _connection;


        /// <summary>
        /// Creates a Sql Data Adapter object with the passed Command object.
        /// </summary>
        /// <param name="cmd"></param>
        /// <returns></returns>
        protected override IDataAdapter CreateDataAdapter(IDbCommand cmd) => new SqlDataAdapter(cmd as SqlCommand);

        /// <summary>Convert any data type to Char</summary>
        /// <param name="columnName"></param>
        /// <returns></returns>
        public override string ConvertToChar(string columnName) => "CONVERT(varchar, " + columnName + ")";
        /// <summary>Creates the parameter collection.</summary>
        /// <returns></returns>
        public override ParameterCollection CreateParameterCollection() => new(this);

        public override IDataParameter CreateApiParameter(string paraemterName, string parameterValue) => throw new NotImplementedException();

        #region Parameters

        /// <summary>Returns the param connector for SQLServer, @</summary>
        /// <returns></returns>
        public override string ParameterConnector => "@";

        /// <summary>Gets the wild card character.</summary>
        /// <value>The wild card character.</value>
        public override string WildCardCharacter => "%";

        /// <summary>Gets the column delimiter character.</summary>
        public override string ColumnDelimiter => throw new NotImplementedException();

        /// <summary>Creates the SQL server param.</summary>
        /// <param name="srcTableColumnName">Name of the SRC table column.</param>
        /// <param name="paramType">         Type of the param.</param>
        /// <returns></returns>
        private SqlParameter CreatDbServerParam(string srcTableColumnName, SqlDbType paramType) {
            SqlParameter param = new(ToSafeParamName(srcTableColumnName), paramType) {
                SourceColumn = srcTableColumnName
            };
            return param;
        }

        /// <summary>Creates the SQL server param.</summary>
        /// <param name="srcTableColumnName">Name of the SRC table column.</param>
        /// <param name="paramType">         Type of the param.</param>
        /// <param name="size">              The size.</param>
        /// <returns></returns>
        private SqlParameter CreatDbServerParam(string srcTableColumnName, SqlDbType paramType, int size) {
            SqlParameter param = new(ToSafeParamName(srcTableColumnName), paramType, size) {
                SourceColumn = srcTableColumnName
            };
            return param;
        }

        /// <summary>Creates the XML parameter.</summary>
        /// <param name="srcTaleColumnName">Name of the SRC tale column.</param>
        /// <param name="parameterValue">   The parameter value.</param>
        /// <returns></returns>
        public override IDataParameter CreateXMLParameter(string srcTaleColumnName, string parameterValue) {
            IDataParameter returnValue;

            returnValue = CreatDbServerParam(srcTaleColumnName, SqlDbType.Xml);
            returnValue.Value = parameterValue != string.Empty ? parameterValue : DBNull.Value;
            return returnValue;
        }

        /// <summary>Creates a boolean parameter.</summary>
        /// <param name="srcTaleColumnName">Name of the SRC tale column.</param>
        /// <param name="parameterValue">   The parameter value.</param>
        /// <returns></returns>
        public override IDataParameter CreateBooleanParameter(string srcTableColumnName, bool parameterValue) {
            IDataParameter returnValue;

            returnValue = CreatDbServerParam(srcTableColumnName, SqlDbType.Bit);
            returnValue.Value = parameterValue;
            return returnValue;
        }

        /// <summary>Creates parameters for the supported database.</summary>
        /// <param name="obj">  The Business Entity from which to extract the data</param>
        /// <param name="col">  The column for which the data must be extracted from the business entity</param>
        /// <param name="store">The store that handles the IO</param>
        /// <returns></returns>
        public override IDataParameter CreateParameter(object obj, TableColumn col, IDBStore store) {
            object extractedData = store.Extract(obj, col.Name);

            bool isNull;
            int sqlDataType;
            try {
                switch (col.Type) {
                    case DbTypeSupported.dbNVarChar:
                        isNull = (col.IsNullableType && extractedData == null) || extractedData == null || (string) extractedData == "";
                        sqlDataType = (int) SqlDbType.NVarChar;
                        break;

                    case DbTypeSupported.dbVarChar:
                        isNull = (col.IsNullableType && extractedData == null) || extractedData == null || (string) extractedData == "";
                        sqlDataType = (int) SqlDbType.VarChar;
                        break;

                    case DbTypeSupported.dbInt64:
                        isNull = (col.IsNullableType && extractedData == null) || (Int64) extractedData == Int64.MinValue;
                        sqlDataType = (int) SqlDbType.BigInt;
                        break;

                    case DbTypeSupported.dbInt32:
                        isNull = (col.IsNullableType && extractedData == null) || (Int32) extractedData == int.MinValue;
                        sqlDataType = (int) SqlDbType.Int;
                        break;

                    case DbTypeSupported.dbInt16:
                        isNull = (col.IsNullableType && extractedData == null) || (Int16) extractedData == Int16.MinValue;
                        sqlDataType = (int) SqlDbType.SmallInt;
                        break;

                    case DbTypeSupported.dbDouble:
                        isNull = (col.IsNullableType && extractedData == null) || (double) extractedData == double.MinValue;
                        sqlDataType = (int) SqlDbType.Float;
                        break;

                    case DbTypeSupported.dbDateTime:
                        isNull = (col.IsNullableType && extractedData == null) || (DateTime) extractedData == DateTime.MinValue;
                        sqlDataType = (int) SqlDbType.DateTime;
                        break;

                    case DbTypeSupported.dbChar:
                        isNull = (col.IsNullableType && extractedData == null) || extractedData == null || Convert.ToString(extractedData) == "";
                        sqlDataType = (int) SqlDbType.Char;
                        break;

                    case DbTypeSupported.dbBlob:    // Text, not Image
                        isNull = (col.IsNullableType && extractedData == null) || extractedData == null;
                        sqlDataType = (int) SqlDbType.Binary;
                        break;

                    case DbTypeSupported.dbBit:
                        isNull = (col.IsNullableType && extractedData == null) || extractedData == null;
                        sqlDataType = (int) SqlDbType.Bit;
                        break;

                    case DbTypeSupported.dbDecimal:
                        isNull = (col.IsNullableType && extractedData == null) || (decimal) extractedData == decimal.MinValue;
                        sqlDataType = (int) SqlDbType.Decimal;
                        break;

                    case DbTypeSupported.dbImage:
                    case DbTypeSupported.dbVarBinary:
                        isNull = extractedData == null;
                        sqlDataType = (int) SqlDbType.VarBinary;
                        break;

                    case DbTypeSupported.dbGUID:
                        isNull = (col.IsNullableType && extractedData == null) || (Guid) extractedData == Guid.Empty;
                        sqlDataType = (int) SqlDbType.UniqueIdentifier;
                        if (col.IsPrimary && isNull)
                            extractedData = Guid.NewGuid().ToString();
                        break;

                    default:
                        throw new CodedThoughtApplicationException($"Data type, {col.Type}, not supported.");
                }
            }
            catch (Exception ex) {
                throw new CodedThoughtApplicationException("Error creating Parameter", ex);
            }

            SqlParameter parameter = CreatDbServerParam(col.Name, (SqlDbType) sqlDataType);

            parameter.Value = isNull ? DBNull.Value : extractedData;

            return parameter;
        }

        /// <summary>Create an empty parameter for SQLServer</summary>
        /// <returns></returns>
        public override IDataParameter CreateEmptyParameter() {
            IDataParameter returnValue = new SqlParameter();
            return returnValue;
        }

        /// <summary>Creates the output parameter.</summary>
        /// <param name="parameterName">Name of the parameter.</param>
        /// <param name="returnType">   Type of the return.</param>
        /// <returns></returns>
        /// <exception cref="CodedThoughtApplicationException">
        /// Data type not supported. DataTypes currently supported are: DbTypeSupported.dbString, DbTypeSupported.dbInt32, DbTypeSupported.dbDouble, DbTypeSupported.dbDateTime, DbTypeSupported.dbChar
        /// </exception>
        public override IDataParameter CreateOutputParameter(string parameterName, DbTypeSupported returnType) {
            SqlDbType sqlType;
            switch (returnType) {
                case DbTypeSupported.dbNVarChar:
                    sqlType = SqlDbType.NVarChar;
                    break;

                case DbTypeSupported.dbVarChar:
                    sqlType = SqlDbType.VarChar;
                    break;

                case DbTypeSupported.dbInt64:
                    sqlType = SqlDbType.BigInt;
                    break;

                case DbTypeSupported.dbInt32:
                    sqlType = SqlDbType.Int;
                    break;

                case DbTypeSupported.dbInt16:
                    sqlType = SqlDbType.SmallInt;
                    break;

                case DbTypeSupported.dbDouble:
                    sqlType = SqlDbType.Float;
                    break;

                case DbTypeSupported.dbDateTime:
                    sqlType = SqlDbType.DateTime;
                    break;

                case DbTypeSupported.dbChar:
                    sqlType = SqlDbType.Char;
                    break;

                case DbTypeSupported.dbBlob:    // Text, not Image
                    sqlType = SqlDbType.Binary;
                    break;

                case DbTypeSupported.dbBit:
                    sqlType = SqlDbType.Bit;
                    break;

                case DbTypeSupported.dbDecimal:
                    sqlType = SqlDbType.Decimal;
                    break;

                case DbTypeSupported.dbImage:
                    sqlType = SqlDbType.Image;
                    break;

                case DbTypeSupported.dbGUID:
                    sqlType = SqlDbType.UniqueIdentifier;
                    break;

                default:
                    throw new CodedThoughtApplicationException("Data type not supported.  DataTypes currently supported are: DbTypeSupported.dbString, DbTypeSupported.dbInt32, DbTypeSupported.dbDouble, DbTypeSupported.dbDateTime, DbTypeSupported.dbChar");
            }

            SqlParameter returnParam = CreatDbServerParam(parameterName, sqlType);
            returnParam.Direction = ParameterDirection.Output;
            return returnParam;
        }

        /// <summary>Creates and returns a return parameter for the supported database.</summary>
        /// <param name="parameterName"></param>
        /// <param name="returnType">   </param>
        /// <returns></returns>
        /// <exception cref="CodedThoughtApplicationException">
        /// Data type not supported. DataTypes currently supported are: DbTypeSupported.dbString, DbTypeSupported.dbInt32, DbTypeSupported.dbDouble, DbTypeSupported.dbDateTime, DbTypeSupported.dbChar
        /// </exception>
        public override IDataParameter CreateReturnParameter(string parameterName, DbTypeSupported returnType) {
            SqlDbType sqlType;
            switch (returnType) {
                case DbTypeSupported.dbNVarChar:
                    sqlType = SqlDbType.NVarChar;
                    break;

                case DbTypeSupported.dbVarChar:
                    sqlType = SqlDbType.VarChar;
                    break;

                case DbTypeSupported.dbInt64:
                    sqlType = SqlDbType.BigInt;
                    break;

                case DbTypeSupported.dbInt32:
                    sqlType = SqlDbType.Int;
                    break;

                case DbTypeSupported.dbInt16:
                    sqlType = SqlDbType.SmallInt;
                    break;

                case DbTypeSupported.dbDouble:
                    sqlType = SqlDbType.Float;
                    break;

                case DbTypeSupported.dbDateTime:
                    sqlType = SqlDbType.DateTime;
                    break;

                case DbTypeSupported.dbChar:
                    sqlType = SqlDbType.Char;
                    break;

                case DbTypeSupported.dbBlob:    // Text, not Image
                    sqlType = SqlDbType.Binary;
                    break;

                case DbTypeSupported.dbBit:
                    sqlType = SqlDbType.Bit;
                    break;

                case DbTypeSupported.dbDecimal:
                    sqlType = SqlDbType.Decimal;
                    break;

                case DbTypeSupported.dbImage:
                    sqlType = SqlDbType.Image;
                    break;

                case DbTypeSupported.dbGUID:
                    sqlType = SqlDbType.UniqueIdentifier;
                    break;

                default:
                    throw new CodedThoughtApplicationException("Data type not supported.  DataTypes currently supported are: DbTypeSupported.dbString, DbTypeSupported.dbInt32, DbTypeSupported.dbDouble, DbTypeSupported.dbDateTime, DbTypeSupported.dbChar");
            }

            IDataParameter returnParam = CreatDbServerParam(parameterName, sqlType);
            returnParam.Direction = ParameterDirection.ReturnValue;
            return returnParam;
        }

        /// <summary>Creates and returns a string parameter for the supported database.</summary>
        /// <param name="srcTableColumnName"></param>
        /// <param name="parameterValue">    </param>
        /// <returns></returns>
        public override IDataParameter CreateStringParameter(string srcTableColumnName, string parameterValue) {
            IDataParameter returnValue = CreatDbServerParam(srcTableColumnName, SqlDbType.NVarChar);
            returnValue.Value = parameterValue != string.Empty ? parameterValue : DBNull.Value;

            return returnValue;
        }

        /// <summary>Creates a Int32 parameter for the supported database</summary>
        /// <param name="srcTableColumnName"></param>
        /// <param name="parameterValue">    </param>
        /// <returns></returns>
        public override IDataParameter CreateInt32Parameter(string srcTableColumnName, int parameterValue) {
            IDataParameter returnValue = CreatDbServerParam(srcTableColumnName, SqlDbType.Int);
            returnValue.Value = parameterValue != int.MinValue ? parameterValue : DBNull.Value;

            return returnValue;
        }

        /// <summary>Creates a Double parameter based on supported database</summary>
        /// <param name="srcTableColumnName"></param>
        /// <param name="parameterValue">    </param>
        /// <returns></returns>
        public override IDataParameter CreateDoubleParameter(string srcTableColumnName, double parameterValue) {
            IDataParameter returnValue = CreatDbServerParam(srcTableColumnName, SqlDbType.Float);
            returnValue.Value = parameterValue != double.MinValue ? parameterValue : DBNull.Value;

            return returnValue;
        }

        /// <summary>Create a data time parameter based on supported database.</summary>
        /// <param name="srcTableColumnName"></param>
        /// <param name="parameterValue">    </param>
        /// <returns></returns>
        public override IDataParameter CreateDateTimeParameter(string srcTableColumnName, DateTime parameterValue) {
            IDataParameter returnValue = CreatDbServerParam(srcTableColumnName, SqlDbType.DateTime);
            returnValue.Value = parameterValue != DateTime.MinValue ? parameterValue : DBNull.Value;

            return returnValue;
        }

        /// <summary>Creates a Char parameter based on supported database.</summary>
        /// <param name="srcTableColumnName"></param>
        /// <param name="parameterValue">    </param>
        /// <param name="size">              </param>
        /// <returns></returns>
        public override IDataParameter CreateCharParameter(string srcTableColumnName, string parameterValue, int size) {
            IDataParameter returnValue = CreatDbServerParam(srcTableColumnName, SqlDbType.VarChar);
            returnValue.Value = parameterValue != string.Empty ? parameterValue : DBNull.Value;

            return returnValue;
        }

        /// <summary>Creates a Blob parameter based on supported database.</summary>
        /// <param name="srcTableColumnName"></param>
        /// <param name="parameterValue">    </param>
        /// <param name="size">              </param>
        /// <returns></returns>
        public IDataParameter CreateBlobParameter(string srcTableColumnName, byte[] parameterValue, int size) {
            IDataParameter returnValue = CreatDbServerParam(srcTableColumnName, SqlDbType.Text, size);
            returnValue.Value = parameterValue;

            return returnValue;
        }

        /// <summary>Creates the GUID parameter.</summary>
        /// <param name="srcTableColumnName">Name of the SRC table column.</param>
        /// <param name="parameterValue">    The parameter value.</param>
        /// <returns></returns>
        public override IDataParameter CreateGuidParameter(string srcTableColumnName, Guid parameterValue) {
            IDataParameter returnValue = CreatDbServerParam(srcTableColumnName, SqlDbType.UniqueIdentifier);
            returnValue.Value = parameterValue;

            return returnValue;
        }

        public override IDataParameter CreateBetweenParameter(string srcTableColumnName, BetweenParameter betweenParam) => throw new NotImplementedException();

        #endregion Parameters

        #region Add method

        /// <summary>Adds data to the database</summary>
        /// <param name="tableName"></param>
        /// <param name="obj">      </param>
        /// <param name="columns">  </param>
        /// <param name="store">    </param>
        /// <returns></returns>

        public override void Add(string tableName, object obj, List<TableColumn> columns, IDBStore store) => Add(tableName, GetSchemaName(), obj, columns, store);
        /// <summary>Adds data to the database</summary>
        /// <param name="tableName"></param>
        /// <param name="obj">      </param>
        /// <param name="columns">  </param>
        /// <param name="store">    </param>
        /// <returns></returns>

        public override void Add(string tableName, string schemaName, object obj, List<TableColumn> columns, IDBStore store) {
            try {
                ParameterCollection parameters = [];
                StringBuilder sbColumns = new();
                StringBuilder sbValues = new();
                TableColumn? keyColumn = null;

                for (int i = 0; i < columns.Count; i++) {
                    TableColumn col = columns[i];
                    if (col.IsPrimary)
                        keyColumn = col;

                    // If a column is updateable and a primary key column then it is not identity or autogenerated by the INSERT.
                    if (!col.IsIdentity) {
                        //we do not insert columns such as identity columns
                        IDataParameter parameter = CreateParameter(obj, col, store);
                        sbColumns.Append(__comma).Append(col.Name);
                        sbValues.Append(__comma).Append(ParameterConnector).Append(parameter.ParameterName);
                        parameters.Add(parameter);
                    }
                }

                StringBuilder sql = new($"INSERT INTO {GetTableName(schemaName,tableName)} (");
                sql.Append(sbColumns.Remove(0, 2));
                sql.Append(") VALUES (");
                sql.Append(sbValues.Remove(0, 2));
                sql.Append(") ");

                // ================================================================ print sql to output window to debugging purpose
#if DEBUG
                DebugParameters(sql, tableName, parameters);
#endif
                // ================================================================
                BeginTransaction();
                if (store.HasKeyColumn(obj)) {
                    if (keyColumn.IsIdentity) {
                        //Check if we have an identity Column
                        sql.Append("SELECT SCOPE_IDENTITY() ");
                        // ExecuteScalar will execute both the INSERT statement and the SELECT statement.
                        int retval = Convert.ToInt32(ExecuteScalar(sql.ToString(), CommandType.Text, parameters));
                        store.SetPrimaryKey(obj, retval);
                    } else {
                        ExecuteNonQuery(sql.ToString(), CommandType.Text, parameters);
                    }
                } else {
                    ExecuteNonQuery(sql.ToString(), CommandType.Text, parameters);
                }

                // this is the way to get the CONTEXT_INFO of a SQL connection session string contextInfo = System.Convert.ToString( ExecuteScalar( "SELECT dbo.AUDIT_LOG_GET_USER_NAME() ",
                // System.Data.CommandType.Text, null ) );
            }
            catch (CodedThoughtApplicationException irEx) {
                RollbackTransaction();
                // this is not a good method to catch DUPLICATE
                if (irEx.Message.Contains("duplicate key", StringComparison.CurrentCulture)) {
                    throw new FolderException(irEx.Message, (Exception) irEx);
                } else {
                    throw new CodedThoughtApplicationException((string) ("Failed to add record to: " + tableName + "<BR>" + irEx.Message + "<BR>" + irEx.Source), (Exception) irEx);
                }
            }
            catch (Exception ex) {
                RollbackTransaction();
                throw new CodedThoughtApplicationException("Failed to add record to: " + tableName + "<BR>" + ex.Message + "<BR>" + ex.Source, ex);
            }
            finally {
                CommitTransaction();
            }
        }

        #endregion Add method

        #region GetValue Methods

        /// <summary>
        /// Gets a data reader based on table name, columns names etc.
        /// </summary>
        /// <returns><see cref="IDataReader"/></returns>
        /// <remarks>The schema name is no longer utilized.  Please pass the entire table name.</remarks>
        public override IDataReader Get(string tableName, string schemaName, List<string> selectColumns, ParameterCollection parameters, List<string> orderByColumns) {
            IDataReader reader = null;
            try {
                StringBuilder sql = new("SELECT ");
                sql.Append(GenerateColumnList(selectColumns));
                sql.Append($" FROM {GetTableName(schemaName, tableName)}");
                sql.Append(" WITH (READPAST)");
                if (parameters != null && parameters.Count > 0) {
                    sql.Append(" WHERE " + GenerateWhereClauseFromParams(parameters));
                }

                sql.Append(GenerateOrderByClause(orderByColumns));
                reader = ExecuteReader(sql.ToString(), parameters);
            }
            catch (Exception ex) {
                throw new CodedThoughtApplicationException("Failed to add retrieve data from: " + tableName, ex);
            }
            finally {
                CommitTransaction();
            }

            return reader;
        }

        /// <summary>
        /// Get a BLOB from a TEXT or IMAGE column.
        /// In order to get BLOB, a IDataReader's CommandBehavior must be set to SequentialAccess.
        /// That also means to Get columns in sequence is extremely important.
        /// Otherwise the GetBlobValue method won't return correct data.
        /// [EXAMPLE]
        /// DataReaderBehavior = CommandBehavior.SequentialAccess;
        ///	using(IDataReader reader = ExecuteReader("select BigName, ID, BigBlob from BigTable", CommandType.Text))
        ///	{
        ///		while (reader.Read())
        ///		{
        ///			string bigName = reader.GetString(0);
        ///			int id = GetInt32Value( reader, 1);
        ///			byte[] bigText = GetBlobValue( reader, 2 );
        ///		}
        ///	}
        /// </summary>
        /// <param name="reader"></param>
        ///<param name="columnName"></param>
        /// <returns></returns>
        protected override byte[] GetBlobValue(IDataReader reader, string columnName) {
            int position = reader.GetOrdinal(columnName);

            // The DataReader's CommandBehavior must be CommandBehavior.SequentialAccess.
            if (DataReaderBehavior != CommandBehavior.SequentialAccess) {
                throw new CodedThoughtApplicationException("Please set the DataReaderBehavior to SequentialAccess to call this method.");
            }
            SqlDataReader sqlReader = (SqlDataReader) reader;
            int bufferSize = 100;                   // Size of the BLOB buffer.
            byte[] outBuff = new byte[bufferSize];  // a buffer for every read in "bufferSize" bytes
            long totalBytes;                        // The total chars returned from GetBytes.
            long retval;                            // The bytes returned from GetBytes.
            long startIndex = 0;                    // The starting position in the BLOB output.
            byte[] outBytes = null;                 // The BLOB byte[] buffer holder.

            // Read the total bytes into outbyte[] and retain the number of chars returned.
            totalBytes = sqlReader.GetBytes(position, startIndex, outBytes, 0, bufferSize);
            outBytes = new byte[totalBytes];

            // initial reading from the BLOB column
            retval = sqlReader.GetBytes(position, startIndex, outBytes, 0, bufferSize);

            // Continue reading and writing while there are bytes beyond the size of the buffer.
            while (retval == bufferSize) {
                // Reposition the start index to the end of the last buffer and fill the buffer.
                startIndex += bufferSize;
                retval = sqlReader.GetBytes(position, startIndex, outBytes, Convert.ToInt32(startIndex), bufferSize);
            }

            return outBytes;
        }

        /// <summary>
        /// Gets a string from a BLOB, Text (SQLServer) or CLOB (Oracle),. developers should use
        /// this method only if they know for certain that the data stored in the field is a string.
        /// </summary>
        /// <param name="reader"></param>
        ///<param name="columnName"></param>
        /// <returns></returns>
        public override string GetStringFromBlob(IDataReader reader, string columnName) {
            _ = reader.GetOrdinal(columnName);

            string returnValue = Encoding.ASCII.GetString(GetBlobValue(reader, columnName));
            return returnValue;
        }

        #endregion GetValue Methods

        #region Database Specific

        public override string ConnectionName => base.ConnectionName;

        public override DBSupported SupportedDatabase => DBSupported.SqlServer;

        public override string GetTableName(string? defaultSchema, string tableName) {
            if (String.IsNullOrEmpty(defaultSchema)) {
                defaultSchema = GetSchemaName();
            }
            return !string.IsNullOrEmpty(defaultSchema) ? $"[{defaultSchema}].[{tableName}]" : $"[{tableName}";

        }
        public override string GetSchemaName() => !String.IsNullOrEmpty(DefaultSchemaName) ? DefaultSchemaName : String.Empty;

        /// <summary>
        /// Gets the current session default schema name.
        /// </summary>
        /// <returns></returns>
        public override String GetDefaultSessionSchemaNameQuery() {
            try {
                return "SELECT SCHEMA_NAME()";
            }
            catch (Exception) {

                throw;
            }
        }

        #region Schema Definition Queries

        /// <summary>
        /// Gets the query used to list all tables in the database.
        /// </summary>
        /// <returns></returns>
        public override string GetTableListQuery() => "SELECT TABLE_NAME, TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES ORDER BY TABLE_NAME";
        /// <summary>
        /// Gets the query used to list all views in the database.
        /// </summary>
        /// <returns></returns>
        public override string GetViewListQuery() => "SELECT TABLE_NAME, TABLE_SCHEMA FROM INFORMATION_SCHEMA.VIEWS ORDER BY TABLE_NAME";

        /// <summary>Gets the table definition.</summary>
        /// <param name="tableName">Name of the table.</param>
        /// <returns></returns>
        public override String GetTableDefinitionQuery(string tableName) {
            try {
                StringBuilder sql = new();
                List<TableColumn> tableDefinition = new();
                // Remove any brackets since the definitiion query doesn't support that.
                string tName = tableName.Replace("[", "").Replace("]", "");
                string schemaName = DefaultSchemaName.Replace("[", "").Replace("]", "");
                if (tName.Split(".".ToCharArray()).Length > 1) {
                    // The schema name appears to have been passed along with the table name. So parse them out and use them instead of the default values.
                    string[] tableNameData = tName.Split(".".ToCharArray());
                    schemaName = tableNameData[0].Replace("[", "").Replace("]", "");
                    tName = tableNameData[1].Replace("[", "").Replace("]", "");
                }
                sql.Append("SELECT C.COLUMN_NAME, C.DATA_TYPE, ");
                sql.Append("CASE WHEN C.IS_NULLABLE = 'NO' THEN 0 ELSE 1 END as IS_NULLABLE, ");
                sql.Append("CASE WHEN C.CHARACTER_MAXIMUM_LENGTH IS NULL THEN 0 ELSE C.CHARACTER_MAXIMUM_LENGTH END AS CHARACTER_MAXIMUM_LENGTH, ");
                sql.Append("C.ORDINAL_POSITION - 1 as ORDINAL_POSITION, COLUMNPROPERTY(OBJECT_ID(c.TABLE_SCHEMA + '.' + c.TABLE_NAME), c.COLUMN_NAME, 'IsIdentity') as IS_IDENTITY, ");
                sql.Append("CASE WHEN C2.CONSTRAINT_TYPE = 'PRIMARY KEY' THEN 1 ELSE 0 END AS IS_PRIMARY_KEY ");
                sql.Append("FROM INFORMATION_SCHEMA.COLUMNS C ");
                sql.Append("LEFT OUTER JOIN ( ");
                sql.Append("SELECT CON.*, T.CONSTRAINT_TYPE FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS T ");
                sql.Append("INNER JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE CON ON CON.CONSTRAINT_NAME = T.CONSTRAINT_NAME) ");
                sql.Append("C2 ON C2.COLUMN_NAME = C.COLUMN_NAME AND C2.TABLE_NAME = C.TABLE_NAME ");
                sql.AppendFormat("WHERE C.TABLE_NAME = '{0}' AND C.TABLE_SCHEMA = '{1}' ORDER BY C.ORDINAL_POSITION", tName, schemaName);

                return sql.ToString();
            }
            catch { throw; }
        }
        /// <summary>
        /// Gets the query necessary to get a view's high level schema.  This does not include the columns.
        /// </summary>
        /// <param name="viewName"></param>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        public override string GetViewDefinitionQuery(string viewName) {
            try {
                return GetTableDefinitionQuery(viewName);
            }
            catch (Exception) {

                throw;
            }
        }

        #endregion Schema Definition Queries

        #region Schema Methods

        /// <summary>
        /// Gets an enumerable list of <see cref="TableColumn"/> objects for the passed table.
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public override List<TableColumn> GetTableDefinition(string tableName) {
            try {
                List<TableColumn> tableDefinitions = [];

                DataTable dtColumns = ExecuteDataTable(GetTableDefinitionQuery(tableName));
                foreach (DataRow row in dtColumns.Rows) {
                    TableColumn column = new("", DbTypeSupported.dbVarChar, 0, true) {
                        Name = row["COLUMN_NAME"].ToString(),
                        IsNullable = Convert.ToBoolean(row["IS_NULLABLE"]),
                        SystemType = ToSystemType(row["DATA_TYPE"].ToString()),
                        Type = ToDbSupportedType(row["DATA_TYPE"].ToString()),
                        DbType = ToDbType(ToSystemType(row["DATA_TYPE"].ToString())),
                        MaxLength = Convert.ToInt32(row["CHARACTER_MAXIMUM_LENGTH"]),
                        IsIdentity = Convert.ToBoolean(row["IS_IDENTITY"]),
                        OrdinalPosition = Convert.ToInt32(row["ORDINAL_POSITION"])
                    };

                    tableDefinitions.Add(column);
                }
                return tableDefinitions;
            }
            catch { throw; }
        }
        /// <summary>
        /// Gets an enumerable list of <see cref="TableColumn"/> objects for the passed view.
        /// </summary>
        /// <param name="viewName"></param>
        /// <returns></returns>
        public override List<TableColumn> GetViewDefinition(string viewName) {
            try {
                return GetTableDefinition(viewName);
            }
            catch (Exception) {
                throw;
            }
        }

        #endregion Schema Methods

        /// <summary>Gets SQL syntax of Year</summary>
        /// <param name="dateString"></param>
        /// <returns></returns>
        public override string GetYearSQLSyntax(string dateString) => "YEAR(" + dateString + ")";

        /// <summary>Gets database function name</summary>
        /// <param name="functionName"></param>
        /// <returns></returns>
        public override string GetFunctionName(FunctionName functionName) {
            string retStr = string.Empty;
            switch (functionName) {
                case FunctionName.SUBSTRING:
                    retStr = "SUBSTRING";
                    break;

                case FunctionName.ISNULL:
                    retStr = "ISNULL";
                    break;

                case FunctionName.CURRENTDATE:
                    retStr = "GETDATE()";
                    break;

                case FunctionName.CONCATENATE:
                    retStr = "+";
                    break;
            }
            return retStr;
        }

        /// <summary>Gets Date string format.</summary>
        /// <param name="columnName">Name of the column.</param>
        /// <param name="dateFormat">The date format.</param>
        /// <returns></returns>
        public override string GetDateToStringForColumn(string columnName, DateFormat dateFormat) {
            StringBuilder sb = new();
            switch (dateFormat) {
                case DateFormat.MMDDYYYY:
                    sb.Append(" CONVERT(VARCHAR, ").Append(columnName).Append(", 101) ");
                    break;

                case DateFormat.MMDDYYYY_Hyphen:
                    sb.Append(" CONVERT(VARCHAR, ").Append(columnName).Append(", 110) ");
                    break;

                case DateFormat.MonDDYYYY:
                    sb.Append(" CONVERT(VARCHAR, ").Append(columnName).Append(", 107) ");
                    break;

                default:
                    sb.Append(columnName);
                    break;
            }
            return sb.ToString();
        }

        /// <summary>Gets the date to string for value.</summary>
        /// <param name="value">     The value.</param>
        /// <param name="dateFormat">The date format.</param>
        /// <returns></returns>
        public override string GetDateToStringForValue(string value, DateFormat dateFormat) {
            StringBuilder sb = new();
            switch (dateFormat) {
                case DateFormat.MMDDYYYY:
                    sb.Append(" CONVERT(VARCHAR, \"").Append(value).Append("\", 101) ");
                    break;

                case DateFormat.MMDDYYYY_Hyphen:
                    sb.Append(" CONVERT(VARCHAR, \"").Append(value).Append("\", 110) ");
                    break;

                case DateFormat.MonDDYYYY:
                    sb.Append(" CONVERT(VARCHAR, \"").Append(value).Append("\", 107) ");
                    break;

                default:
                    sb.Append("\"" + value + "\"");
                    break;
            }
            return sb.ToString();
        }

        /// <summary>Get CASE (SQL Server) or DECODE (Oracle) SQL syntax.</summary>
        /// <param name="columnName"></param>
        /// <param name="equalValue"></param>
        /// <param name="trueValue"> </param>
        /// <param name="falseValue"></param>
        /// <param name="alias">     </param>
        /// <returns></returns>
        public override string GetCaseDecode(string columnName, string equalValue, string trueValue, string falseValue, string alias) {
            StringBuilder sb = new();

            sb.Append(" (CASE ").Append(columnName);
            sb.Append(" WHEN ").Append(equalValue);
            sb.Append(" THEN ").Append(trueValue).Append(" ELSE ").Append(falseValue).Append(" END) ");
            sb.Append(alias).Append(" ");

            return sb.ToString();
        }

        /// <summary>Get an IsNull (SQLServer) or NVL (Oracle)</summary>
        /// <param name="validateColumnName"></param>
        /// <param name="optionColumnName">  </param>
        /// <returns></returns>
        public override string GetIfNullFunction(string validateColumnName, string optionColumnName) => " IsNULL(" + validateColumnName + ", " + optionColumnName + ") ";

        /// <summary>Get a function name for NULL validation</summary>
        /// <returns></returns>
        public override string GetIfNullFunction() => "IsNULL";

        /// <summary>Get a function name that return current date</summary>
        /// <returns></returns>
        public override string GetCurrentDateFunction() => "GETDATE()";

        /// <summary>Get a database specific date only SQL syntax.</summary>
        /// <param name="dateColumn"></param>
        /// <returns></returns>
        public override string GetDateOnlySqlSyntax(string dateColumn) => "CONVERT(VARCHAR, " + dateColumn + ", 107)";

        /// <summary>Get a database specific syntax that converts string to date. Oracle does not convert date string to date implicitly like SQL Server does when there is a date comparison.</summary>
        /// <param name="dateString"></param>
        /// <returns></returns>
        public override string GetStringToDateSqlSyntax(string dateString) => __singleQuote + dateString + __singleQuote + " ";

        /// <summary>Get a database specific syntax that converts string to date. Oracle does not convert date string to date implicitly like SQL Server does when there is a date comparison.</summary>
        /// <param name="dateSQL"></param>
        /// <returns></returns>
        public override string GetStringToDateSqlSyntax(DateTime dateSQL) => __singleQuote + dateSQL.ToString("G", System.Globalization.DateTimeFormatInfo.InvariantInfo) + __singleQuote + " ";

        /// <summary>Gets date part(Day, month or year) of date</summary>
        /// <param name="datestring"></param>
        /// <param name="dateFormat"></param>
        /// <param name="datePart">  </param>
        /// <returns></returns>
        public override string GetDatePart(string datestring, DateFormat dateFormat, DatePart datePart) {
            string datePartstring = string.Empty;
            switch (datePart) {
                case DatePart.DAY:
                    datePartstring = "day";
                    break;

                case DatePart.MONTH:
                    datePartstring = "month";
                    break;

                case DatePart.YEAR:
                    datePartstring = "year";
                    break;
            }
            string result = "DATEPART( " + datePartstring + ", '" + datestring + "')";
            return result;
        }

        /// <summary>Convert a datestring to datetime when used for between.... and</summary>
        /// <param name="datestring">string</param>
        /// <param name="dateFormat">DateFormat</param>
        /// <returns></returns>
        public override string ToDate(string datestring, DateFormat dateFormat) => __singleQuote + datestring + __singleQuote;

        /// <summary>Converts a database type name to a system type.</summary>
        /// <param name="dbTypeName">Name of the db type.</param>
        /// <returns>System.Type</returns>
        public override Type ToSystemType(string dbTypeName) => dbTypeName.ToLower() switch {
            "bigint" => typeof(System.Int64),
            "varbinary" or "binary" or "timestamp" => typeof(System.Byte[]),
            "bit" => typeof(System.Boolean),
            "char" or "nchar" or "ntext" or "nvarchar" or "text" or "varchar" => typeof(System.String),
            "date" or "datetime" or "datetime2" => typeof(DateTime),
            "numeric" => typeof(System.Decimal),
            "decimal" or "smallmoney" or "money" => typeof(System.Decimal),
            "float" => typeof(System.Double),
            "int" => typeof(System.Int32),
            "smallint" => typeof(System.Int16),
            "variant" => typeof(System.Object),
            "time" => typeof(TimeSpan),
            "tinyint" => typeof(System.Byte),
            "uniqueidentifier" => typeof(Guid),
            _ => typeof(System.String),
        };
        /// <summary>
        /// Converts a database type name to a <see cref="DbTypeSupported"/> type.
        /// </summary>
        /// <param name="dbTypeName"></param>
        /// <returns></returns>
        public override DbTypeSupported ToDbSupportedType(string dbTypeName) => dbTypeName.ToLower() switch {
            "bigint" => DbTypeSupported.dbInt64,
            "varbinary" or "binary" => DbTypeSupported.dbVarBinary,
            "bit" => DbTypeSupported.dbBit,
            "char" or "nchar" => DbTypeSupported.dbChar,
            "ntext" or "nvarchar" => DbTypeSupported.dbNVarChar,
            "text" or "varchar" => DbTypeSupported.dbVarChar,
            "date" or "datetime" => DbTypeSupported.dbDateTime,
            "datetime2" => DbTypeSupported.dbDateTime2,
            "numeric" => DbTypeSupported.dbNumeric,
            "decimal" or "smallmoney" or "money" => DbTypeSupported.dbDecimal,
            "float" => DbTypeSupported.dbDouble,
            "int" => DbTypeSupported.dbInt32,
            "smallint" => DbTypeSupported.dbInt16,
            "variant" => DbTypeSupported.dbSqlVariant,
            "time" or "timestamp" => DbTypeSupported.dbTime,
            "tinyint" => DbTypeSupported.dbTinyInt,
            "uniqueidentifier" => DbTypeSupported.dbGUID,
            _ => DbTypeSupported.dbVarChar,
        };

        /// <summary>
        /// Gets an enumerable list of <see cref="TableSchema"/> objects unless tableName is passed to filter it.
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public override List<TableSchema> GetTableDefinitions() {
            try {
                List<TableSchema> tableDefinitions = [];
                DataTable dtTables = ExecuteDataTable(GetTableListQuery());
                foreach (DataRow row in dtTables.Rows) {
                    TableSchema tableSchema = new() {
                        Name = row["TABLE_NAME"].ToString(),
                        Owner = row["TABLE_SCHEMA"].ToString(),
                        Columns = []
                    };
                    DataTable dtColumns = ExecuteDataTable(GetTableDefinitionQuery(tableSchema.Name));
                    foreach (DataRow col in dtColumns.Rows) {
                        TableColumn column = new("", DbTypeSupported.dbVarChar, 0, true) {
                            Name = col["COLUMN_NAME"].ToString(),
                            IsNullable = Convert.ToBoolean(col["IS_NULLABLE"]),
                            SystemType = ToSystemType(col["DATA_TYPE"].ToString()),
                            Type = ToDbSupportedType(col["DATA_TYPE"].ToString()),
                            DbType = ToDbType(ToDbSupportedType(col["DATA_TYPE"].ToString())),
                            MaxLength = Convert.ToInt32(col["CHARACTER_MAXIMUM_LENGTH"]),
                            IsIdentity = Convert.ToBoolean(col["IS_IDENTITY"]),
                            IsPrimary = Convert.ToBoolean(col["IS_PRIMARY_KEY"]),
                            OrdinalPosition = Convert.ToInt32(col["ORDINAL_POSITION"])
                        };
                        column.DbType = ToDbType(column.SystemType);
                        // Add this column to the list.
                        tableSchema.Columns.Add(column);
                    }
                    tableDefinitions.Add(tableSchema);
                }

                return tableDefinitions;
            }
            catch { throw; }
        }
        /// <summary>
        /// Gets an enumerable list of <see cref="ViewSchema"/> objects unless viewName is passed to filter it.
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public override List<ViewSchema> GetViewDefinitions() {
            try {

                List<ViewSchema> viewDefinitions = [];
                DataTable dtTables = ExecuteDataTable(GetViewListQuery());
                foreach (DataRow row in dtTables.Rows) {
                    ViewSchema viewSchema = new() {
                        Name = row["TABLE_NAME"].ToString(),
                        Owner = row["TABLE_SCHEMA"].ToString(),
                        Columns = []
                    };
                    DataTable dtColumns = ExecuteDataTable(GetViewDefinitionQuery(viewSchema.Name));
                    foreach (DataRow col in dtColumns.Rows) {
                        TableColumn column = new("", DbTypeSupported.dbVarChar, 0, true) {
                            Name = col["COLUMN_NAME"].ToString(),
                            IsNullable = Convert.ToBoolean(col["IS_NULLABLE"]),
                            SystemType = ToSystemType(col["DATA_TYPE"].ToString()),
                            Type = ToDbSupportedType(col["DATA_TYPE"].ToString()),
                            DbType = ToDbType(ToDbSupportedType(col["DATA_TYPE"].ToString())),
                            MaxLength = Convert.ToInt32(col["CHARACTER_MAXIMUM_LENGTH"]),
                            IsIdentity = Convert.ToBoolean(col["IS_IDENTITY"]),
                            IsPrimary = Convert.ToBoolean(col["IS_PRIMARY_KEY"]),
                            OrdinalPosition = Convert.ToInt32(col["ORDINAL_POSITION"])
                        };
                        // Add this column to the list.
                        viewSchema.Columns.Add(column);
                    }
                    viewDefinitions.Add(viewSchema);
                }

                return viewDefinitions;
            }
            catch (Exception) {

                throw;
            }
        }

        /// <summary>
        /// Tests the connection to the database.
        /// </summary>
        /// <returns></returns>
        protected override async Task<IDbConnection> OpenConnectionAsync() {
            try {
                _connection = new SqlConnection(ConnectionString);
                await _connection.OpenAsync();
                return _connection;
            }
            catch (SqlException ex) {
                throw new ApplicationException("Could not open Connection.  Check connection string" + "/r/n" + ex.Message + "/r/n" + ex.StackTrace, ex);
            }
        }
        /// <summary>
        /// Test the connection using an asyncronous process.
        /// </summary>
        /// <returns></returns>
        public override async Task<bool> TestConnectionAsync() {
            try {
                await OpenConnectionAsync();
                return Connection.State == ConnectionState.Open;
            }
            catch { throw; }
        }

        #endregion Database Specific


        #endregion Other Override Methods
    }
}