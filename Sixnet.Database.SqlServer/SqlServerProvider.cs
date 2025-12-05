using System;
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
            queryDatabasesScript = "SELECT [DBID] AS [ID], [NAME] FROM [SYSDATABASES] ORDER BY DBID;";
            queryTablesScript = "SELECT T.[OBJECT_ID] AS [ID], T.[NAME], T.[SCHEMA_ID] AS [SCHEMAID], S.[NAME] AS [SCHEMANAME] FROM SYS.TABLES T INNER JOIN SYS.SCHEMAS S ON T.[SCHEMA_ID] = S.[SCHEMA_ID] ORDER BY S.[NAME], T.[NAME];";
            queryViewsScript = "SELECT T.[OBJECT_ID] AS [ID], T.[NAME], T.[SCHEMA_ID] AS [SCHEMAID], S.[NAME] AS [SCHEMANAME] FROM SYS.VIEWS T INNER JOIN SYS.SCHEMAS S ON T.[SCHEMA_ID] = S.[SCHEMA_ID] ORDER BY S.[NAME], T.[NAME];";
            queryStoredProcedureScript = "SELECT T.[OBJECT_ID] AS [ID], T.[NAME], T.[SCHEMA_ID] AS [SCHEMAID], S.[NAME] AS [SCHEMANAME] FROM SYS.PROCEDURES T INNER JOIN SYS.SCHEMAS S ON T.[SCHEMA_ID] = S.[SCHEMA_ID] ORDER BY S.[NAME], T.[NAME];";
            queryColumnScript = "SELECT [C].[column_id] AS [ID],[C].[name] AS [Name],[T].[name] AS [DataType],[C].[max_length] AS [Length],[C].[is_nullable] AS [AllowNull],[C].[is_identity] AS [Increment],ISNULL([I].[is_primary_key],0) AS [IsPrimaryKey],[EP].[value] AS [Description] FROM [sys].[columns] AS [C] INNER JOIN [sys].[types] AS [T] ON [C].[user_type_id]=[T].[user_type_id] LEFT JOIN [sys].[extended_properties] AS [EP] ON [EP].[major_id]=[C].[object_id] AND [EP].[minor_id]=[C].[column_id] AND [EP].[name]='MS_Description' LEFT JOIN (SELECT [IC].[object_id],[IC].[column_id],1 AS [is_primary_key] FROM [sys].[index_columns] AS [IC] JOIN [sys].[indexes] AS [I] ON [IC].[object_id]=[I].[object_id] AND [IC].[index_id]=[I].[index_id] WHERE [I].[is_primary_key]=1) AS [I] ON [C].[object_id]=[I].[object_id] AND [C].[column_id]=[I].[column_id] WHERE [C].[object_id]=OBJECT_ID('{0}') ORDER BY [C].[column_id];";
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
        protected override ISixnetDataCommandResolver GetDataCommandResolver()
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
        public override async Task BulkInsertAsync(BulkInsertDatabaseCommand command)
        {
            SixnetException.ThrowIf(command?.DataTable == null, "Not set datatable");
            var bulkInsertOptions = command.BulkInsertionOptions;
            var sqlServerBulkInsertOptions = bulkInsertOptions as SqlServerBulkInsertOptions;
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
        public override void BulkInsert(BulkInsertDatabaseCommand command)
        {
            SixnetException.ThrowIf(command?.DataTable == null, "Not set datatable");
            var bulkInsertOptions = command.BulkInsertionOptions;
            var sqlServerBulkInsertOptions = bulkInsertOptions as SqlServerBulkInsertOptions;
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
