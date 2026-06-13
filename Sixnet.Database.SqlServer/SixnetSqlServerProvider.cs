using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Sixnet.Development.Data.Command;
using Sixnet.Development.Data.Dapper;
using Sixnet.Development.Data.Database;
using Sixnet.Exceptions;

namespace Sixnet.Database.SqlServer
{
    /// <summary>
    /// Imeplements database provider for sqlserver
    /// </summary>
    public class SixnetSqlServerProvider : SixnetBaseDatabaseProvider
    {
        #region Constructor

        public SixnetSqlServerProvider()
        {
            queryDatabasesScript = "SELECT [DBID] AS [ID], [NAME] FROM [SYSDATABASES] WITH (NOLOCK) ORDER BY DBID;";
            queryTablesScript = "SELECT T.[OBJECT_ID] AS [ID], T.[NAME], T.[SCHEMA_ID] AS [SCHEMAID], S.[NAME] AS [SCHEMANAME] FROM SYS.TABLES T WITH (NOLOCK) INNER JOIN SYS.SCHEMAS S  WITH (NOLOCK) ON T.[SCHEMA_ID] = S.[SCHEMA_ID] ORDER BY S.[NAME], T.[NAME];";
            queryViewsScript = "SELECT T.[OBJECT_ID] AS [ID], T.[NAME], T.[SCHEMA_ID] AS [SCHEMAID], S.[NAME] AS [SCHEMANAME] FROM SYS.VIEWS T WITH (NOLOCK) INNER JOIN SYS.SCHEMAS S WITH (NOLOCK) ON T.[SCHEMA_ID] = S.[SCHEMA_ID] ORDER BY S.[NAME], T.[NAME];";
            queryStoredProcedureScript = "SELECT T.[OBJECT_ID] AS [ID], T.[NAME], T.[SCHEMA_ID] AS [SCHEMAID], S.[NAME] AS [SCHEMANAME] FROM SYS.PROCEDURES T WITH (NOLOCK) INNER JOIN SYS.SCHEMAS S WITH (NOLOCK) ON T.[SCHEMA_ID] = S.[SCHEMA_ID] ORDER BY S.[NAME], T.[NAME];";
            queryColumnScript = "SELECT [C].[column_id] AS [ID],[C].[name] AS [Name],[T].[name] AS [DataType],[C].[max_length] AS [Length],[C].[is_nullable] AS [AllowNull],[C].[is_identity] AS [Increment],ISNULL([I].[is_primary_key],0) AS [IsPrimaryKey],[EP].[value] AS [Description] FROM [sys].[columns] AS [C] WITH (NOLOCK) INNER JOIN [sys].[types] AS [T] WITH (NOLOCK) ON [C].[user_type_id]=[T].[user_type_id] LEFT JOIN [sys].[extended_properties] AS [EP] WITH (NOLOCK) ON [EP].[major_id]=[C].[object_id] AND [EP].[minor_id]=[C].[column_id] AND [EP].[name]='MS_Description' LEFT JOIN (SELECT [IC].[object_id],[IC].[column_id],1 AS [is_primary_key] FROM [sys].[index_columns] AS [IC] WITH (NOLOCK) JOIN [sys].[indexes] AS [I] WITH (NOLOCK) ON [IC].[object_id]=[I].[object_id] AND [IC].[index_id]=[I].[index_id] WHERE [I].[is_primary_key]=1) AS [I] ON [C].[object_id]=[I].[object_id] AND [C].[column_id]=[I].[column_id] WHERE [C].[object_id]=OBJECT_ID('{0}') ORDER BY [C].[column_id];";
        }

        #endregion

        #region Connection

        /// <summary>
        /// Get database connection
        /// </summary>
        /// <param name="server">Database server</param>
        /// <returns></returns>
        public override IDbConnection GetDbConnection(SixnetDatabaseServer server)
        {
            return SixnetSqlServerManager.GetConnection(server);
        }

        /// <summary>
        /// Get db connection meta
        /// </summary>
        /// <param name="connection"></param>
        /// <returns></returns>
        public override SixnetDatabaseConnectionMeta GetDbConnectionMeta(IDbConnection connection)
        {
            var sqlBuilder = new SqlConnectionStringBuilder(connection.ConnectionString);
            return new SixnetDatabaseConnectionMeta()
            {
                UserName = sqlBuilder.UserID,
                Password = sqlBuilder.Password,
                DataSource = sqlBuilder.DataSource,
                DatabaseName = sqlBuilder.InitialCatalog,
            };
        }

        #endregion

        #region Data command resolver

        /// <summary>
        /// Get data command resolver
        /// </summary>
        /// <returns></returns>
        protected override ISixnetDataCommandResolver GetDataCommandResolver()
        {
            return SixnetSqlServerManager.GetCommandResolver();
        }

        #endregion

        #region Parameter

        /// <summary>
        /// Convert data command parametes
        /// </summary>
        /// <param name="parameters">Data command parameters</param>
        /// <returns></returns>
        protected override DynamicParameters ConvertDataCommandParameters(SixnetDataCommandParameters parameters)
        {
            return parameters?.ConvertToDynamicParameters(SixnetSqlServerManager.GetCommandResolver().DatabaseType);
        }

        #endregion

        #region Bulk

        /// <summary>
        /// Bulk insert datas
        /// </summary>
        /// <param name="command">Database bulk insert command</param>
        public override async Task BulkInsertAsync(SixnetBulkInsertDatabaseCommand command)
        {
            SixnetException.ThrowIf(command?.DataTable == null, "Not set datatable");
            var bulkInsertOptions = command.BulkInsertionOptions;
            var sqlServerBulkInsertOptions = bulkInsertOptions as SixnetSqlServerBulkInsertOptions;
            var dbConnection = command.Connection.DbConnection as SqlConnection;
            try
            {
                using (var sqlServerBulkCopy = new SqlBulkCopy(dbConnection, sqlServerBulkInsertOptions?.BulkCopyOptions
                    ?? SqlBulkCopyOptions.Default, command.Connection.Transaction.DbTransaction as SqlTransaction))
                {
                    if (sqlServerBulkInsertOptions != null)
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
            catch (Exception ex)
            {
                throw GetSqlException(ex);
            }
        }

        /// <summary>
        /// Bulk insert datas
        /// </summary>
        /// <param name="command">Database bulk insert command</param>
        public override void BulkInsert(SixnetBulkInsertDatabaseCommand command)
        {
            SixnetException.ThrowIf(command?.DataTable == null, "Not set datatable");
            var bulkInsertOptions = command.BulkInsertionOptions;
            var sqlServerBulkInsertOptions = bulkInsertOptions as SixnetSqlServerBulkInsertOptions;
            var dbConnection = command.Connection.DbConnection as SqlConnection;
            try
            {
                using (var sqlServerBulkCopy = new SqlBulkCopy(dbConnection, sqlServerBulkInsertOptions?.BulkCopyOptions
                    ?? SqlBulkCopyOptions.Default, command.Connection.Transaction.DbTransaction as SqlTransaction))
                {
                    if (sqlServerBulkInsertOptions != null)
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
            catch (Exception ex)
            {
                throw GetSqlException(ex);
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

        #region Get exception

        protected override Exception GetSqlException(Exception ex)
        {
            if (ex is SqlException sqlException)
            {
                switch (sqlException.Number)
                {
                    case 2627: // Unique constraint
                    case 2601: // Unique index
                        return new SixnetSqlAlreadExistsException(sqlException.Message, sqlException);
                }
            }
            return ex;
        }

        #endregion
    }
}
