using System;
using System.Data;
using System.Data.SqlClient;
using Sixnet.Development.Data;
using Sixnet.Development.Data.Command;
using Sixnet.Development.Data.Database;
using Sixnet.Logging;

namespace Sixnet.Database.SqlServer
{
    /// <summary>
    /// Defines sqlserver manager
    /// </summary>
    internal static class SqlServerManager
    {
        #region Fields

        /// <summary>
        /// Gets current database server type
        /// </summary>
        internal const DatabaseServerType CurrentDatabaseServerType = DatabaseServerType.SQLServer;

        /// <summary>
        /// Key word prefix
        /// </summary>
        internal const string KeywordPrefix = "[";

        /// <summary>
        /// Key word suffix
        /// </summary>
        internal const string KeywordSuffix = "]";

        /// <summary>
        /// Default data command resolver
        /// </summary>
        static readonly SqlServerDataCommandResolver DefaultDataCommandResolver = new SqlServerDataCommandResolver();

        #endregion

        #region Get database connection

        /// <summary>
        /// Get sqlserver database connection
        /// </summary>
        /// <param name="server">Database server</param>
        /// <returns>Return database connection</returns>
        internal static IDbConnection GetConnection(SixnetDatabaseServer server)
        {
            return SixnetDataManager.GetDatabaseConnection(server) ?? new SqlConnection(server.ConnectionString);
        }

        #endregion

        #region Wrap keyword

        /// <summary>
        /// Wrap keyword by the KeywordPrefix and the KeywordSuffix
        /// </summary>
        /// <param name="originalValue">Original value</param>
        /// <returns></returns>
        internal static string WrapKeyword(string originalValue)
        {
            return $"{KeywordPrefix}{originalValue}{KeywordSuffix}";
        }

        #endregion

        #region Command resolver

        /// <summary>
        /// Get command resolver
        /// </summary>
        /// <returns>Return a command resolver</returns>
        internal static SqlServerDataCommandResolver GetCommandResolver()
        {
            return DefaultDataCommandResolver;
        }

        #endregion
    }
}
