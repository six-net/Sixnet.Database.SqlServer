using System.Collections.Generic;
using System.Data.SqlClient;
using Sixnet.Development.Data.Database;

namespace Sixnet.Database.SqlServer
{
    /// <summary>
    /// SqlServer bulk insert options
    /// </summary>
    public class SqlServerBulkInsertOptions : ISixnetBulkInsertionOptions
    {
        /// <summary>
        //  Returns a collection of System.Data.SqlClient.SqlBulkCopyColumnMapping items.
        //  Column mappings define the relationships between columns in the data source and
        //  columns in the destination.
        /// </summary>
        public List<SqlBulkCopyColumnMapping> ColumnMappings { get; set; }

        /// <summary>
        /// Number of seconds for the operation to complete before it times out.
        /// The default is 30 seconds. A value of 0 indicates no limit,the bulk copy will wait indefinitely.
        /// </summary>
        public int BulkCopyTimeout { get; set; } = 30;

        /// <summary>
        /// Number of rows in each batch. At the end of each batch, the rows in the batch are sent to the server.
        /// By default, SqlBulkCopy will process the operation in a single batch
        /// </summary>
        public int BatchSize { get; set; }

        /// <summary>
        /// Indicates whether use transaction
        /// Default is false
        /// </summary>
        public bool UseTransaction { get; set; }
    }
}
