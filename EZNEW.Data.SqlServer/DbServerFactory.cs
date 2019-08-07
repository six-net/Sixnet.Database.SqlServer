using EZNEW.Data.Config;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;

namespace EZNEW.Data.SqlServer
{
    /// <summary>
    /// Db Server Factory
    /// </summary>
    internal static class DbServerFactory
    {
        #region get db connection

        /// <summary>
        /// get sql server database connection
        /// </summary>
        /// <param name="server">database server</param>
        /// <returns>db connection</returns>
        public static IDbConnection GetConnection(ServerInfo server)
        {
            IDbConnection conn = DataManager.GetDBConnection?.Invoke(server) ?? new SqlConnection(server.ConnectionString);
            return conn;
        }

        #endregion
    }
}
