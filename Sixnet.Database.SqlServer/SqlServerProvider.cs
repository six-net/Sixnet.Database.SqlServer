using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using Sixnet.Development.Data.Command;
using Sixnet.Development.Data.Dapper;
using Sixnet.Development.Data.Database;
using Sixnet.Exceptions;

namespace Sixnet.Database.SqlServer
{
    /// <summary>
    /// Imeplements database provider for sqlserver
    /// </summary>
    public class SqlServerProvider : BaseDatabaseProvider
    {
        #region Constructor

        public SqlServerProvider()
        {
            queryDatabaseTablesScript = "SELECT [NAME] AS [TableName] FROM SYSOBJECTS WHERE XTYPE='U' AND CATEGORY=0";
        }

        #endregion

        #region Connection

        /// <summary>
        /// Get database connection
        /// </summary>
        /// <param name="server">Database server</param>
        /// <returns></returns>
        public override IDbConnection GetDbConnection(DatabaseServer server)
        {
            return SqlServerManager.GetConnection(server);
        }

        #endregion

        #region Data command resolver

        /// <summary>
        /// Get data command resolver
        /// </summary>
        /// <returns></returns>
        protected override IDataCommandResolver GetDataCommandResolver()
        {
            return SqlServerManager.GetCommandResolver();
        }

        #endregion

        #region Parameter

        /// <summary>
        /// Convert data command parametes
        /// </summary>
        /// <param name="parameters">Data command parameters</param>
        /// <returns></returns>
        protected override DynamicParameters ConvertDataCommandParameters(DataCommandParameters parameters)
        {
            return parameters?.ConvertToDynamicParameters(SqlServerManager.CurrentDatabaseServerType);
        }

        #endregion

        #region Bulk

        /// <summary>
        /// Bulk insert datas
        /// </summary>
        /// <param name="command">Database bulk insert command</param>
        public override async Task BulkInsertAsync(DatabaseBulkInsertCommand command)
        {
            SixnetException.ThrowIf(command?.DataTable == null, "Not set datatable");
            var bulkInsertOptions = command.BulkInsertionOptions;
            var dbConnection = command.Connection.DbConnection as SqlConnection;
            using (var sqlServerBulkCopy = new SqlBulkCopy(dbConnection, SqlBulkCopyOptions.Default, command.Connection.Transaction.DbTransaction as SqlTransaction))
            {
                if (bulkInsertOptions is SqlServerBulkInsertOptions sqlServerBulkInsertOptions)
                {
                    if (!sqlServerBulkInsertOptions.ColumnMappings.IsNullOrEmpty())
                    {
                        sqlServerBulkInsertOptions.ColumnMappings.ForEach(c =>
                        {
                            sqlServerBulkCopy.ColumnMappings.Add(c);
                        });
                    }
                    if (sqlServerBulkInsertOptions.BulkCopyTimeout > 0)
                    {
                        sqlServerBulkCopy.BulkCopyTimeout = sqlServerBulkInsertOptions.BulkCopyTimeout;
                    }
                    if (sqlServerBulkInsertOptions.BatchSize > 0)
                    {
                        sqlServerBulkCopy.BatchSize = sqlServerBulkInsertOptions.BatchSize;
                    }
                }
                if (sqlServerBulkCopy.ColumnMappings.Count < 1)
                {
                    BuildColumnMapping(sqlServerBulkCopy, command.DataTable);
                }
                sqlServerBulkCopy.DestinationTableName = command.DataTable.TableName;
                await sqlServerBulkCopy.WriteToServerAsync(command.DataTable).ConfigureAwait(false);
                sqlServerBulkCopy.Close();
            }
        }

        /// <summary>
        /// Bulk insert datas
        /// </summary>
        /// <param name="command">Database bulk insert command</param>
        public override void BulkInsert(DatabaseBulkInsertCommand command)
        {
            SixnetException.ThrowIf(command?.DataTable == null, "Not set datatable");
            var bulkInsertOptions = command.BulkInsertionOptions;
            var dbConnection = command.Connection.DbConnection as SqlConnection;
            using (var sqlServerBulkCopy = new SqlBulkCopy(dbConnection, SqlBulkCopyOptions.Default, command.Connection.Transaction.DbTransaction as SqlTransaction))
            {
                if (bulkInsertOptions is SqlServerBulkInsertOptions sqlServerBulkInsertOptions)
                {
                    if (!sqlServerBulkInsertOptions.ColumnMappings.IsNullOrEmpty())
                    {
                        sqlServerBulkInsertOptions.ColumnMappings.ForEach(c =>
                        {
                            sqlServerBulkCopy.ColumnMappings.Add(c);
                        });
                    }
                    if (sqlServerBulkInsertOptions.BulkCopyTimeout > 0)
                    {
                        sqlServerBulkCopy.BulkCopyTimeout = sqlServerBulkInsertOptions.BulkCopyTimeout;
                    }
                    if (sqlServerBulkInsertOptions.BatchSize > 0)
                    {
                        sqlServerBulkCopy.BatchSize = sqlServerBulkInsertOptions.BatchSize;
                    }
                }
                if (sqlServerBulkCopy.ColumnMappings.Count < 1)
                {
                    BuildColumnMapping(sqlServerBulkCopy, command.DataTable);
                }
                sqlServerBulkCopy.DestinationTableName = command.DataTable.TableName;
                sqlServerBulkCopy.WriteToServer(command.DataTable);
                sqlServerBulkCopy.Close();
            }
        }

        /// <summary>
        /// Build column mapping
        /// </summary>
        /// <param name="sqlBulkCopy"></param>
        /// <param name="dataTable"></param>
        static void BuildColumnMapping(SqlBulkCopy sqlBulkCopy, DataTable dataTable)
        {
            foreach (DataColumn column in dataTable.Columns)
            {
                sqlBulkCopy.ColumnMappings.Add(new SqlBulkCopyColumnMapping()
                {
                    SourceColumn = column.ColumnName,
                    DestinationColumn = column.ColumnName
                });
            }
        }

        #endregion
    }
}
