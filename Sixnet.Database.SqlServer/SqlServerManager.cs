using System.Data;
using System.Data.SqlClient;

using Sixnet.Development.Data;
using Sixnet.Development.Data.Database;

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
        internal const DatabaseType CurrentDatabaseServerType = DatabaseType.SQLServer;

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
        internal static IDbConnection GetConnection(DatabaseServer server)
        {
            return SixnetDataManager.GetDatabaseConnection(server) ?? new SqlConnection(SixnetDataManager.ResolveConnectionString(server));
        }

        #endregion

        #region Format keyword

        internal static string FormatKeyword(string originalValue, DatabaseObjectNameType nameType)
        {
            return SixnetDataManager.FormatDatabaseWordAndName(CurrentDatabaseServerType, originalValue);
        }

        #endregion

        #region Wrap keyword

        /// <summary>
        /// Wrap keyword by the KeywordPrefix and the KeywordSuffix
        /// </summary>
        /// <param name="originalValue">Original value</param>
        /// <returns></returns>
        internal static string WrapKeyword(string originalValue, DatabaseObjectNameType nameType)
        {
            return nameType == DatabaseObjectNameType.ColumnName ? $"{KeywordPrefix}{originalValue}{KeywordSuffix}" : originalValue;
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
