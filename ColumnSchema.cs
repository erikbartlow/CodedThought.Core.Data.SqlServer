using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using CodedThought.Core.Data.Interfaces;

namespace CodedThought.Core.Data.SqlServer
{
    [DataTable("COLUMNS", "", "INFORMATION_SCHEMA")]
    public class ColumnSchema : IColumnSchema
    {
        [DataColumn("COLUMN_NAME", System.Data.DbType.String)]
        public string ColumnOwner { get; set; }
        [DataColumn("ORDINAL_POSITION", System.Data.DbType.Int32)]
        public string ColumnNumber { get; set; }
        [DataColumn("COLUMN_NAME", System.Data.DbType.String)]
        public string ColumnName { get; set; }
        [DataColumn("DATA_TYPE", System.Data.DbType.String)]
        public string ColumnTypeName { get; set; }
        public DbTypeSupported ColumnType { get; }
        [DataColumn("IS_NULLABLE", System.Data.DbType.String)]
        public bool IsNullable { get; set; }
        [DataColumn("IS_IDENTITY", System.Data.DbType.String)]
        public bool IsIdentity { get; set; }
        [DataColumn("CHARACTER_MAXIMUM_LENGTH", System.Data.DbType.String)]
        public int MaxLength {  get; set; }
        [DataColumn("ORDINAL_POSITION", System.Data.DbType.String)]
        public int OrdinalPosition {  get; set; }
      
    }
}
