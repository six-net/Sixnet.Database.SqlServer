using System.Data;
using Microsoft.Data.SqlClient;
using Sixnet.Development.Data;
using Sixnet.Development.Data.Database;

namespace Sixnet.Database.SqlServer
{
    /// <summary>
    /// Defines sqlserver manager
    /// </summary>
    internal static class SixnetSqlServerManager
    {
        #region Fields

        /// <summary>
        /// Default data command resolver
        /// </summary>
        static readonly SixnetSqlServerDataCommandResolver DefaultDataCommandResolver = new();

        #endregion

        #region Get database connection

        /// <summary>
        /// Get sqlserver database connection
        /// </summary>
        /// <param name="server">Database server</param>
        /// <returns>Return database connection</returns>
        internal static IDbConnection GetConnection(SixnetDatabaseServer server)
        {
            return SixnetDataManager.GetDatabaseConnection(server) ?? new SqlConnection(SixnetDataManager.ResolveConnectionString(server));
        }

        #endregion

        #region Command resolver

        /// <summary>
        /// Get command resolver
        /// </summary>
        /// <returns>Return a command resolver</returns>
        internal static SixnetSqlServerDataCommandResolver GetCommandResolver()
        {
            return DefaultDataCommandResolver;
        }

        #endregion
    }
}
