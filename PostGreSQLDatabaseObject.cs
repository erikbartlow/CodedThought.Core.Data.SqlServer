using CodedThought.Core.Data.Interfaces;
using CodedThought.Core.Exceptions;

using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Logging.Configuration;

using Org.BouncyCastle.Asn1.X509.Qualified;

using System.Data;
using System.Data.Common;
using System.Text;
using NpgsqlTypes;

namespace CodedThought.Core.Data.PostgreSQL
{

    /// <summary>SqlServerDatabaseObject provides all SQLServer specific functionality needed by DBStore and its family of classes..</summary>
    public class PostGreSQLDatabaseObject : DatabaseObject, IDatabaseObject, IDbSchema
    {
        #region Declarations

        private NpgsqlConnection _connection;

        #endregion

        #region Constructor

        public PostGreSQLDatabaseObject() : base()
        {
            _connection = new();
        }

        #endregion Constructor

        #region Transaction and Connection Methods

        /// <summary>Commits updates and inserts. This is only for Oracle database operations.</summary>
        public override void Commit() => CommitTransaction();

        public override IDbConnection Connection => _connection;

        /// <summary>Opens an SqlServer Connection</summary>
        /// <returns></returns>
        protected override IDbConnection OpenConnection()
        {
            try
            {
                _connection = new(ConnectionString);
                _connection.Open();
                return _connection;
            }
            catch (SqlException ex)
            {
                throw new Exceptions.CodedThoughtApplicationException("Could not open Connection.  Check connection string" + "/r/n" + ex.Message + "/r/n" + ex.StackTrace, ex);
            }
        }

        #endregion Transaction and Connection Methods

        #region Other Override Methods

        /// <summary>
        /// Tests the connection to the database.
        /// </summary>
        /// <returns></returns>
        public override bool TestConnection()
        {
            try
            {
                OpenConnection();
                return _connection.State == ConnectionState.Open;
            }
            catch
            {
                throw;
            }
        }
        /// <summary>
        /// Creates a Sql Data Adapter object with the passed Command object.
        /// </summary>
        /// <param name="cmd"></param>
        /// <returns></returns>
        protected override IDataAdapter CreateDataAdapter(IDbCommand cmd) => new Npgsql.NpgsqlDataAdapter(cmd as NpgsqlCommand);

        /// <summary>Convert any data type to Char</summary>
        /// <param name="columnName"></param>
        /// <returns></returns>
        public override string ConvertToChar(string columnName) => "TO_CHAR(varchar, " + columnName + ")";
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
        private NpgsqlParameter CreatDbServerParam(string srcTableColumnName, NpgsqlDbType paramType)
        {
            NpgsqlParameter param = new(ToSafeParamName(srcTableColumnName), paramType);
            param.SourceColumn = srcTableColumnName;
            return param;
        }

        /// <summary>Creates the SQL server param.</summary>
        /// <param name="srcTableColumnName">Name of the SRC table column.</param>
        /// <param name="paramType">         Type of the param.</param>
        /// <param name="size">              The size.</param>
        /// <returns></returns>
        private NpgsqlParameter CreatDbServerParam(string srcTableColumnName, NpgsqlDbType paramType, int size)
        {
            NpgsqlParameter param = new(ToSafeParamName(srcTableColumnName), paramType, size);
            param.SourceColumn = srcTableColumnName;
            return param;
        }

        /// <summary>Creates the XML parameter.</summary>
        /// <param name="srcTaleColumnName">Name of the SRC tale column.</param>
        /// <param name="parameterValue">   The parameter value.</param>
        /// <returns></returns>
        public override IDataParameter CreateXMLParameter(string srcTaleColumnName, string parameterValue)
        {
            IDataParameter returnValue = null;

            returnValue = CreatDbServerParam(srcTaleColumnName, NpgsqlDbType.Xml);
            returnValue.Value = parameterValue != string.Empty ? parameterValue : DBNull.Value;
            return returnValue;
        }

        /// <summary>Creates a boolean parameter.</summary>
        /// <param name="srcTaleColumnName">Name of the SRC tale column.</param>
        /// <param name="parameterValue">   The parameter value.</param>
        /// <returns></returns>
        public override IDataParameter CreateBooleanParameter(string srcTableColumnName, bool parameterValue)
        {
            IDataParameter returnValue = null;

            returnValue = CreatDbServerParam(srcTableColumnName, NpgsqlDbType.Bit);
            returnValue.Value = parameterValue;
            return returnValue;
        }

        /// <summary>Creates parameters for the supported database.</summary>
        /// <param name="obj">  The Business Entity from which to extract the data</param>
        /// <param name="col">  The column for which the data must be extracted from the business entity</param>
        /// <param name="store">The store that handles the IO</param>
        /// <returns></returns>
        public override IDataParameter CreateParameter(object obj, TableColumn col, IDBStore store)
        {
            Boolean isNull = false;
            int sqlDataType = 0;

            object extractedData = store.Extract(obj, col.Name);
            try
            {
                switch (col.Type)
                {
                    case DbTypeSupported.dbNVarChar:
                        isNull = (extractedData == null || (string) extractedData == "");
                        sqlDataType = (int) NpgsqlDbType.Varchar;
                        break;

                    case DbTypeSupported.dbVarChar:
                        isNull = (extractedData == null || (string) extractedData == "");
                        sqlDataType = (int) NpgsqlDbType.Varchar;
                        break;

                    case DbTypeSupported.dbInt64:
                        isNull = ((Int64) extractedData == Int64.MinValue);
                        sqlDataType = (int) NpgsqlDbType.Bigint;
                        break;

                    case DbTypeSupported.dbInt32:
                        isNull = ((Int32) extractedData == int.MinValue);
                        sqlDataType = (int) NpgsqlDbType.Integer;
                        break;

                    case DbTypeSupported.dbInt16:
                        isNull = ((Int16) extractedData == Int16.MinValue);
                        sqlDataType = (int) NpgsqlDbType.Smallint;
                        break;

                    case DbTypeSupported.dbDouble:
                        isNull = ((double) extractedData == double.MinValue);
                        sqlDataType = (int) NpgsqlDbType.Double;
                        break;

                    case DbTypeSupported.dbDateTime:
                        isNull = ((DateTime) extractedData == DateTime.MinValue);
                        sqlDataType = (int) NpgsqlDbType.Date;
                        break;

                    case DbTypeSupported.dbChar:
                        isNull = (extractedData == null || Convert.ToString(extractedData) == "");
                        sqlDataType = (int) NpgsqlDbType.Char;
                        break;
                    case DbTypeSupported.dbImage:
                    case DbTypeSupported.dbVarBinary:
                    case DbTypeSupported.dbBlob:    // Text, not Image
                        isNull = (extractedData == null);
                        sqlDataType = (int) NpgsqlDbType.Bytea;
                        break;

                    case DbTypeSupported.dbBit:
                        isNull = (extractedData == null);
                        sqlDataType = (int) NpgsqlDbType.Bit;
                        break;

                    case DbTypeSupported.dbDecimal:
                        isNull = ((decimal) extractedData == decimal.MinValue);
                        sqlDataType = (int) NpgsqlDbType.Money;
                        break;

                    case DbTypeSupported.dbGUID:
                        isNull = ((Guid) extractedData == Guid.Empty);
                        sqlDataType = (int) NpgsqlDbType.Uuid;
                        break;

                    default:
                        throw new Exceptions.CodedThoughtApplicationException("Data type not supported.  DataTypes currently supported are: DbTypeSupported.dbString, DbTypeSupported.dbInt32, DbTypeSupported.dbDouble, DbTypeSupported.dbDateTime, DbTypeSupported.dbChar");
                }
            }
            catch (Exception ex)
            {
                throw new Exceptions.CodedThoughtApplicationException("Error creating Parameter", ex);
            }

            NpgsqlParameter parameter = CreatDbServerParam(col.Name, (NpgsqlDbType) sqlDataType);

            parameter.Value = isNull ? DBNull.Value : extractedData;

            return parameter;
        }

        /// <summary>Create an empty parameter for SQLServer</summary>
        /// <returns></returns>
        public override IDataParameter CreateEmptyParameter()
        {
            IDataParameter returnValue = null;

            returnValue = new NpgsqlParameter();

            return returnValue;
        }

        /// <summary>Creates the output parameter.</summary>
        /// <param name="parameterName">Name of the parameter.</param>
        /// <param name="returnType">   Type of the return.</param>
        /// <returns></returns>
        /// <exception cref="Exceptions.CodedThoughtApplicationException">
        /// Data type not supported. DataTypes currently supported are: DbTypeSupported.dbString, DbTypeSupported.dbInt32, DbTypeSupported.dbDouble, DbTypeSupported.dbDateTime, DbTypeSupported.dbChar
        /// </exception>
        public override IDataParameter CreateOutputParameter(string parameterName, DbTypeSupported returnType)
        {
            IDataParameter returnParam = null;
            NpgsqlDbType sqlType;
            switch (returnType)
            {
                case DbTypeSupported.dbNVarChar:
                    sqlType = NpgsqlDbType.Varchar;
                    break;

                case DbTypeSupported.dbVarChar:
                    sqlType = NpgsqlDbType.Varchar;
                    break;

                case DbTypeSupported.dbInt64:
                    sqlType = NpgsqlDbType.Bigint;
                    break;

                case DbTypeSupported.dbInt32:
                    sqlType = NpgsqlDbType.Integer;
                    break;

                case DbTypeSupported.dbInt16:
                    sqlType = NpgsqlDbType.Smallint;
                    break;

                case DbTypeSupported.dbDouble:
                    sqlType = NpgsqlDbType.Double;
                    break;

                case DbTypeSupported.dbDateTime:
                    sqlType = NpgsqlDbType.Date;
                    break;

                case DbTypeSupported.dbChar:
                    sqlType = NpgsqlDbType.Char;
                    break;

                case DbTypeSupported.dbImage:
                case DbTypeSupported.dbBlob:    // Text, not Image
                    sqlType = NpgsqlDbType.Bytea;
                    break;

                case DbTypeSupported.dbBit:
                    sqlType = NpgsqlDbType.Bit;
                    break;

                case DbTypeSupported.dbDecimal:
                    sqlType = NpgsqlDbType.Money;
                    break;

                case DbTypeSupported.dbGUID:
                    sqlType = NpgsqlDbType.Uuid;
                    break;

                default:
                    throw new Exceptions.CodedThoughtApplicationException("Data type not supported.  DataTypes currently supported are: DbTypeSupported.dbString, DbTypeSupported.dbInt32, DbTypeSupported.dbDouble, DbTypeSupported.dbDateTime, DbTypeSupported.dbChar");
            }

            returnParam = CreatDbServerParam(parameterName, sqlType);
            returnParam.Direction = ParameterDirection.Output;
            return returnParam;
        }

        /// <summary>Creates and returns a return parameter for the supported database.</summary>
        /// <param name="parameterName"></param>
        /// <param name="returnType">   </param>
        /// <returns></returns>
        /// <exception cref="Exceptions.CodedThoughtApplicationException">
        /// Data type not supported. DataTypes currently supported are: DbTypeSupported.dbString, DbTypeSupported.dbInt32, DbTypeSupported.dbDouble, DbTypeSupported.dbDateTime, DbTypeSupported.dbChar
        /// </exception>
        public override IDataParameter CreateReturnParameter(string parameterName, DbTypeSupported returnType)
        {
            IDataParameter returnParam = null;
            NpgsqlDbType sqlType;
            switch (returnType)
            {
                case DbTypeSupported.dbNVarChar:
                    sqlType = NpgsqlDbType.Varchar;
                    break;

                case DbTypeSupported.dbVarChar:
                    sqlType = NpgsqlDbType.Varchar;
                    break;

                case DbTypeSupported.dbInt64:
                    sqlType = NpgsqlDbType.Bigint;
                    break;

                case DbTypeSupported.dbInt32:
                    sqlType = NpgsqlDbType.Integer;
                    break;

                case DbTypeSupported.dbInt16:
                    sqlType = NpgsqlDbType.Smallint;
                    break;

                case DbTypeSupported.dbDecimal:
                case DbTypeSupported.dbDouble:
                    sqlType = NpgsqlDbType.Money;
                    break;

                case DbTypeSupported.dbDateTime:
                    sqlType = NpgsqlDbType.Date;
                    break;

                case DbTypeSupported.dbChar:
                    sqlType = NpgsqlDbType.Char;
                    break;

                case DbTypeSupported.dbImage:
                case DbTypeSupported.dbBlob:    // Text, not Image
                    sqlType = NpgsqlDbType.Bytea;
                    break;

                case DbTypeSupported.dbBit:
                    sqlType = NpgsqlDbType.Bit;
                    break;

                case DbTypeSupported.dbGUID:
                    sqlType = NpgsqlDbType.Uuid;
                    break;

                default:
                    throw new Exceptions.CodedThoughtApplicationException("Data type not supported.  DataTypes currently supported are: DbTypeSupported.dbString, DbTypeSupported.dbInt32, DbTypeSupported.dbDouble, DbTypeSupported.dbDateTime, DbTypeSupported.dbChar");
            }

            returnParam = CreatDbServerParam(parameterName, sqlType);
            returnParam.Direction = ParameterDirection.ReturnValue;
            return returnParam;
        }

        /// <summary>Creates and returns a string parameter for the supported database.</summary>
        /// <param name="srcTableColumnName"></param>
        /// <param name="parameterValue">    </param>
        /// <returns></returns>
        public override IDataParameter CreateStringParameter(string srcTableColumnName, string parameterValue)
        {
            IDataParameter returnValue = null;

            returnValue = CreatDbServerParam(srcTableColumnName, NpgsqlDbType.Varchar);
            returnValue.Value = parameterValue != string.Empty ? parameterValue : DBNull.Value;

            return returnValue;
        }

        /// <summary>Creates a Int32 parameter for the supported database</summary>
        /// <param name="srcTableColumnName"></param>
        /// <param name="parameterValue">    </param>
        /// <returns></returns>
        public override IDataParameter CreateInt32Parameter(string srcTableColumnName, int parameterValue)
        {
            IDataParameter returnValue = null;

            returnValue = CreatDbServerParam(srcTableColumnName, NpgsqlDbType.Integer);
            returnValue.Value = parameterValue != int.MinValue ? parameterValue : DBNull.Value;

            return returnValue;
        }

        /// <summary>Creates a Double parameter based on supported database</summary>
        /// <param name="srcTableColumnName"></param>
        /// <param name="parameterValue">    </param>
        /// <returns></returns>
        public override IDataParameter CreateDoubleParameter(string srcTableColumnName, double parameterValue)
        {
            IDataParameter returnValue = null;

            returnValue = CreatDbServerParam(srcTableColumnName, NpgsqlDbType.Money);
            returnValue.Value = parameterValue != double.MinValue ? parameterValue : DBNull.Value;

            return returnValue;
        }

        /// <summary>Create a data time parameter based on supported database.</summary>
        /// <param name="srcTableColumnName"></param>
        /// <param name="parameterValue">    </param>
        /// <returns></returns>
        public override IDataParameter CreateDateTimeParameter(string srcTableColumnName, DateTime parameterValue)
        {
            IDataParameter returnValue = null;

            returnValue = CreatDbServerParam(srcTableColumnName, NpgsqlDbType.Date);
            returnValue.Value = parameterValue != DateTime.MinValue ? parameterValue : DBNull.Value;

            return returnValue;
        }

        /// <summary>Creates a Char parameter based on supported database.</summary>
        /// <param name="srcTableColumnName"></param>
        /// <param name="parameterValue">    </param>
        /// <param name="size">              </param>
        /// <returns></returns>
        public override IDataParameter CreateCharParameter(string srcTableColumnName, string parameterValue, int size)
        {
            IDataParameter returnValue = null;

            returnValue = CreatDbServerParam(srcTableColumnName, NpgsqlDbType.Varchar);
            returnValue.Value = parameterValue != string.Empty ? parameterValue : DBNull.Value;

            return returnValue;
        }

        /// <summary>Creates a Blob parameter based on supported database.</summary>
        /// <param name="srcTableColumnName"></param>
        /// <param name="parameterValue">    </param>
        /// <param name="size">              </param>
        /// <returns></returns>
        public IDataParameter CreateBlobParameter(string srcTableColumnName, byte[] parameterValue, int size)
        {
            IDataParameter returnValue = null;

            returnValue = CreatDbServerParam(srcTableColumnName, NpgsqlDbType.Bytea, size);
            returnValue.Value = parameterValue;

            return returnValue;
        }

        /// <summary>Creates the GUID parameter.</summary>
        /// <param name="srcTableColumnName">Name of the SRC table column.</param>
        /// <param name="parameterValue">    The parameter value.</param>
        /// <returns></returns>
        public override IDataParameter CreateGuidParameter(string srcTableColumnName, Guid parameterValue)
        {
            IDataParameter returnValue = null;

            returnValue = CreatDbServerParam(srcTableColumnName, NpgsqlDbType.Uuid);
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

        public override void Add(string tableName, object obj, List<TableColumn> columns, IDBStore store)
        {
            try
            {
                ParameterCollection parameters = new();
                StringBuilder sbColumns = new();
                StringBuilder sbValues = new();

                for (int i = 0; i < columns.Count; i++)
                {
                    TableColumn col = columns[i];

                    if (col.IsInsertable)
                    {
                        //we do not insert columns such as identity columns
                        IDataParameter parameter = CreateParameter(obj, col, store);
                        sbColumns.Append(__comma).Append(col.Name);
                        sbValues.Append(__comma).Append(ParameterConnector).Append(parameter.ParameterName);
                        parameters.Add(parameter);
                    }
                }

                StringBuilder sql = new("INSERT INTO " + tableName + " (");
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
                if (store.HasKeyColumn(obj))
                {
                    //Check if we have an identity Column
                    sql.Append($" RETURNING {columns.Where(c => c.IsIdentity).FirstOrDefault().Name}");
                    // ExecuteScalar will execute both the INSERT statement and the SELECT statement.
                    int retval = Convert.ToInt32(ExecuteScalar(sql.ToString(), CommandType.Text, parameters));
                    store.SetPrimaryKey(obj, retval);
                }
                else
                {
                    ExecuteNonQuery(sql.ToString(), CommandType.Text, parameters);
                }

                // this is the way to get the CONTEXT_INFO of a SQL connection session string contextInfo = System.Convert.ToString( ExecuteScalar( "SELECT dbo.AUDIT_LOG_GET_USER_NAME() ",
                // System.Data.CommandType.Text, null ) );
            }
            catch (Exceptions.CodedThoughtApplicationException irEx)
            {
                RollbackTransaction();
                // this is not a good method to catch DUPLICATE
                if (irEx.Message.IndexOf("duplicate key") >= 0)
                {
                    throw new FolderException(irEx.Message, (Exception) irEx);
                }
                else
                {
                    throw new Exceptions.CodedThoughtApplicationException((string) ("Failed to add record to: " + tableName + "<BR>" + irEx.Message + "<BR>" + irEx.Source), (Exception) irEx);
                }
            }
            catch (Exception ex)
            {
                RollbackTransaction();
                throw new Exceptions.CodedThoughtApplicationException("Failed to add record to: " + tableName + "<BR>" + ex.Message + "<BR>" + ex.Source, ex);
            }
            finally
            {
                CommitTransaction();
            }
        }

        #endregion Add method

        #region GetValue Methods

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
        protected override byte[] GetBlobValue(IDataReader reader, string columnName)
        {
            int position = reader.GetOrdinal(columnName);

            // The DataReader's CommandBehavior must be CommandBehavior.SequentialAccess.
            if (DataReaderBehavior != CommandBehavior.SequentialAccess)
            {
                throw new Exceptions.CodedThoughtApplicationException("Please set the DataReaderBehavior to SequentialAccess to call this method.");
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
            while (retval == bufferSize)
            {
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
        public override string GetStringFromBlob(IDataReader reader, string columnName)
        {
            int position = reader.GetOrdinal(columnName);
            string returnValue = string.Empty;

            returnValue = Encoding.ASCII.GetString(GetBlobValue(reader, columnName));

            return returnValue;
        }

        #endregion GetValue Methods

        #region Database Specific

        public override string ConnectionName => base.ConnectionName;

        public override DBSupported SupportedDatabase => DBSupported.PostgreSQL;

        public override string GetTableName(string defaultSchema, string tableName)
        {
            string? schemaName = defaultSchema;
            if (String.IsNullOrEmpty(schemaName))
            {
                if (String.IsNullOrEmpty(DefaultSchemaName))
                {
                    schemaName = "public";
                }
            }
            return $"{schemaName}.{tableName}";

        }
        public override string GetSchemaName() => DefaultSchemaName == string.Empty ? "public" : $"{DefaultSchemaName}";


        #region Schema Definition Queries

        /// <summary>
        /// Gets the query used to list all tables in the database.
        /// </summary>
        /// <returns></returns>
        public override string GetTableListQuery() => $"SELECT table_name, table_schema FROM information_schema.tables WHERE table_schema='{GetSchemaName()}' AND table_type='BASE TABLE'";
        /// <summary>
        /// Gets the query used to list all views in the database.
        /// </summary>
        /// <returns></returns>
        public override string GetViewListQuery() => $"SELECT table_name, table_schema, view_definition FROM information_schema.views WHERE table_schema='{GetSchemaName()}'";

        /// <summary>
        /// Gets the table's column definition query.
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns><see cref="System.String"/></returns>
        public override String GetTableDefinitionQuery(string tableName)
        {
            try
            {
                StringBuilder sql = new();
                List<TableColumn> tableDefinition = new();
                // Remove any brackets since the definitiion query doesn't support that.
                string tName = tableName.Replace("[", "").Replace("]", "");
                string schemaName = DefaultSchemaName.Replace("[", "").Replace("]", "");
                if (tName.Split(".".ToCharArray()).Length > 1)
                {
                    // The schema name appears to have been passed along with the table name. So parse them out and use them instead of the default values.
                    string[] tableNameData = tName.Split(".".ToCharArray());
                    schemaName = tableNameData[0].Replace("[", "").Replace("]", "");
                    tName = tableNameData[1].Replace("[", "").Replace("]", "");
                }
                sql.Append("SELECT C.COLUMN_NAME, C.DATA_TYPE, ");
                sql.Append("CASE WHEN C.IS_NULLABLE = 'NO' THEN 0 ELSE 1 END as IS_NULLABLE, ");
                sql.Append("CASE WHEN C.CHARACTER_MAXIMUM_LENGTH IS NULL THEN 0 ELSE C.CHARACTER_MAXIMUM_LENGTH END AS CHARACTER_MAXIMUM_LENGTH, ");
                sql.Append("C.ORDINAL_POSITION - 1 as ORDINAL_POSITION, ");
                sql.Append("CASE WHEN C.IS_IDENTITY = 'NO' THEN 0 ELSE 1 END as IS_IDENTITY ");
                sql.Append("FROM INFORMATION_SCHEMA.COLUMNS C ");
                sql.AppendFormat("WHERE C.TABLE_NAME = '{0}' AND C.TABLE_SCHEMA = '{1}' ORDER BY C.ORDINAL_POSITION", tName, schemaName);

                return sql.ToString();
            }
            catch (Exception)
            {
                throw;
            }
        }
        /// <summary>
        /// Gets the query necessary to get a view's high level schema.  This does not include the columns.
        /// </summary>
        /// <param name="viewName"></param>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        public override string GetViewDefinitionQuery(string viewName)
        {
            try
            {
                return GetTableDefinitionQuery(viewName);
            }
            catch (Exception)
            {

                throw;
            }
        }

        /// <summary>
        /// Gets the current session default schema name.
        /// </summary>
        /// <returns></returns>
        public override String GetDefaultSessionSchemaNameQuery()
        {
            try
            {
                return "SELECT CURRENT_SCHEMA()";
            }
            catch (Exception)
            {

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
        public override List<TableColumn> GetTableDefinition(string tableName)
        {
            try
            {
                List<TableColumn> tableDefinitions = [];

                DataTable dtColumns = ExecuteDataTable(GetTableDefinitionQuery(tableName));
                foreach (DataRow row in dtColumns.Rows)
                {
                    TableColumn column = new("", DbTypeSupported.dbVarChar, 0, true)
                    {
                        Name = row["COLUMN_NAME"].ToString(),
                        IsNullable = Convert.ToBoolean(row["IS_NULLABLE"]),
                        SystemType = ToSystemType(row["DATA_TYPE"].ToString()),
                        Type = ToDbSupportedType(row["DATA_TYPE"].ToString()),
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
        public override List<TableColumn> GetViewDefinition(string viewName)
        {
            try
            {
                return GetTableDefinition(viewName);
            }
            catch (Exception)
            {
                throw;
            }
        }
        /// <summary>
        /// Gets an enumerable list of <see cref="TableSchema"/> objects unless tableName is passed to filter it.
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public override List<TableSchema> GetTableDefinitions()
        {
            try
            {
                List<TableSchema> tableDefinitions = [];
                DataTable dtTables = ExecuteDataTable(GetTableListQuery());
                foreach (DataRow row in dtTables.Rows)
                {
                    TableSchema tableSchema = new()
                    {
                        Name = row["TABLE_NAME"].ToString(),
                        Owner = row["TABLE_SCHEMA"].ToString(),
                        Columns = []
                    };
                    DataTable dtColumns = ExecuteDataTable(GetTableDefinitionQuery(tableSchema.Name));
                    foreach (DataRow col in dtColumns.Rows)
                    {
                        TableColumn column = new("", DbTypeSupported.dbVarChar, 0, true)
                        {
                            Name = col["COLUMN_NAME"].ToString(),
                            IsNullable = Convert.ToBoolean(col["IS_NULLABLE"]),
                            SystemType = ToSystemType(col["DATA_TYPE"].ToString()),
                            Type = ToDbSupportedType(col["DATA_TYPE"].ToString()),
                            MaxLength = Convert.ToInt32(col["CHARACTER_MAXIMUM_LENGTH"]),
                            IsIdentity = Convert.ToBoolean(col["IS_IDENTITY"]),
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
        public override List<ViewSchema> GetViewDefinitions()
        {
            try
            {

                List<ViewSchema> viewDefinitions = [];
                DataTable dtTables = ExecuteDataTable(GetViewListQuery());
                foreach (DataRow row in dtTables.Rows)
                {
                    ViewSchema viewSchema = new()
                    {
                        Name = row["TABLE_NAME"].ToString(),
                        Owner = row["TABLE_SCHEMA"].ToString(),
                        Columns = []
                    };
                    DataTable dtColumns = ExecuteDataTable(GetViewDefinitionQuery(viewSchema.Name));
                    foreach (DataRow col in dtColumns.Rows)
                    {
                        TableColumn column = new("", DbTypeSupported.dbVarChar, 0, true)
                        {
                            Name = col["COLUMN_NAME"].ToString(),
                            IsNullable = Convert.ToBoolean(col["IS_NULLABLE"]),
                            SystemType = ToSystemType(col["DATA_TYPE"].ToString()),
                            MaxLength = Convert.ToInt32(col["CHARACTER_MAXIMUM_LENGTH"]),
                            IsIdentity = Convert.ToBoolean(col["IS_IDENTITY"]),
                            OrdinalPosition = Convert.ToInt32(col["ORDINAL_POSITION"])
                        };
                        column.DbType = ToDbType(column.SystemType);

                        // Add this column to the list.
                        viewSchema.Columns.Add(column);
                    }
                    viewDefinitions.Add(viewSchema);
                }

                return viewDefinitions;
            }
            catch (Exception)
            {

                throw;
            }
        }

        #endregion Schema Methods

        /// <summary>Gets SQL syntax of Year</summary>
        /// <param name="dateString"></param>
        /// <returns></returns>
        public override string GetYearSQLSyntax(string dateString) => $"EXTRACT(YEAR FROM TIMESTAMP, '{dateString}')";

        /// <summary>Gets database function name</summary>
        /// <param name="functionName"></param>
        /// <returns></returns>
        public override string GetFunctionName(FunctionName functionName)
        {
            string retStr = string.Empty;
            switch (functionName)
            {
                case FunctionName.SUBSTRING:
                    retStr = "SUBSTRING";
                    break;

                case FunctionName.ISNULL:
                    retStr = "ISNULL";
                    break;

                case FunctionName.CURRENTDATE:
                    retStr = "CURRENT_DATE";
                    break;

                case FunctionName.CONCATENATE:
                    retStr = "CONCAT";
                    break;
            }
            return retStr;
        }

        /// <summary>Gets Date string format.</summary>
        /// <param name="columnName">Name of the column.</param>
        /// <param name="dateFormat">The date format.</param>
        /// <returns></returns>
        public override string GetDateToStringForColumn(string columnName, DateFormat dateFormat)
        {
            StringBuilder sb = new();
            switch (dateFormat)
            {
                case DateFormat.MMDDYYYY:
                    sb.Append($" TO_CHAR({columnName}, 'MM/DD/YYYY')");
                    break;

                case DateFormat.MMDDYYYY_Hyphen:
                    sb.Append($" TO_CHAR({columnName}, 'MM-DD-YYYY')");
                    break;

                case DateFormat.MonDDYYYY:
                    sb.Append($" TO_CHAR({columnName}, 'mon DD, YYYY')");
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
        public override string GetDateToStringForValue(string value, DateFormat dateFormat)
        {
            StringBuilder sb = new();
            switch (dateFormat)
            {
                case DateFormat.MMDDYYYY:
                    sb.Append($" TO_CHAR('{value}', 'MM/DD/YYYY')");
                    break;

                case DateFormat.MMDDYYYY_Hyphen:
                    sb.Append($" TO_CHAR('{value}', 'MM-DD-YYYY')");
                    break;

                case DateFormat.MonDDYYYY:
                    sb.Append($" TO_CHAR('{value}', 'mon DD, YYYY')");
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
        public override string GetCaseDecode(string columnName, string equalValue, string trueValue, string falseValue, string alias)
        {
            StringBuilder sb = new();

            sb.Append(" (CASE ").Append(columnName);
            sb.Append(" WHEN ").Append(equalValue);
            sb.Append(" THEN ").Append(trueValue).Append(" ELSE ").Append(falseValue).Append(" END CASE) ");
            sb.Append(alias).Append(" ");

            return sb.ToString();
        }

        /// <summary>Get an IsNull function</summary>
        /// <param name="validateColumnName"></param>
        /// <param name="optionColumnName">  </param>
        /// <returns></returns>
        public override string GetIfNullFunction(string validateColumnName, string optionColumnName) => " ISNULL(" + validateColumnName + ", " + optionColumnName + ") ";

        /// <summary>Get a function name for NULL validation</summary>
        /// <returns></returns>
        public override string GetIfNullFunction() => "ISNULL";

        /// <summary>Get a function name that return current date</summary>
        /// <returns></returns>
        public override string GetCurrentDateFunction() => "CURRENT_DATE";

        /// <summary>Get a database specific date only SQL syntax.</summary>
        /// <param name="dateColumn"></param>
        /// <returns></returns>
        public override string GetDateOnlySqlSyntax(string dateColumn) => $"TO_CHAR('{dateColumn}', 'mon DD, YYYY')";

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
        public override string GetDatePart(string datestring, DateFormat dateFormat, DatePart datePart)
        {
            string datePartstring = string.Empty;
            switch (datePart)
            {
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
            string result = "date_part( " + datePartstring + ", '" + datestring + "')";
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
        public override System.Type ToSystemType(string dbTypeName) => dbTypeName.ToLower() switch
        {
            "varbinary" or "binary" or "timestamp" => typeof(System.Byte[]),
            "boolean" or "bool" => typeof(System.Boolean),
            "character" or "char" or "varying" or "varchar" => typeof(System.String),
            "date" or "time" or "timestamp" or "timestamptz" => typeof(System.DateTime),
            "decimal" or "smallmoney" or "money" => typeof(System.Decimal),
            "float" => typeof(System.Double),
            "bigint" => typeof(System.Int64),
            "int" => typeof(System.Int32),
            "smallint" or "tinyint" => typeof(System.Int16),
            "UUID" => typeof(System.Guid),
            _ => typeof(System.String)
        };

        public override DbTypeSupported ToDbSupportedType(string dbTypeName) => dbTypeName.ToLower() switch
        {
            "varbinary" or "binary" or "timestamp" => DbTypeSupported.dbVarBinary,
            "boolean" or "bool" => DbTypeSupported.dbBit,
            "character" or "char" or "varying" or "varchar" => DbTypeSupported.dbVarChar,
            "date" or "time" or "timestamp" or "timestamptz" => DbTypeSupported.dbDateTime,
            "decimal" or "smallmoney" or "money" => DbTypeSupported.dbDouble,
            "bigint" => DbTypeSupported.dbInt64,
            "int" => DbTypeSupported.dbInt32,
            "smallint" or "tinyint" => DbTypeSupported.dbInt16,
            "UUID" => DbTypeSupported.dbGUID,
            _ => DbTypeSupported.dbVarChar
        };

        #endregion Database Specific


        #endregion Other Override Methods
    }
}