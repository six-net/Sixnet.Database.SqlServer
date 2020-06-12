using System;
using System.Data;
using System.Data.SqlClient;
using EZNEW.Data.CriteriaConverter;
using EZNEW.Develop.CQuery.CriteriaConverter;
using EZNEW.Develop.CQuery.Translator;
using EZNEW.Fault;
using EZNEW.Logging;
using EZNEW.Serialize;

namespace EZNEW.Data.SqlServer
{
    /// <summary>
    /// Database server factory
    /// </summary>
    internal static class SqlServerFactory
    {
        /// <summary>
        /// Enable trace log
        /// </summary>
        static readonly bool EnableTraceLog = false;

        static readonly string TraceLogSplit = $"{new string('=', 10)} Database Command Translation Result {new string('=', 10)}";

        static SqlServerFactory()
        {
            EnableTraceLog = TraceLogSwitchManager.ShouldTraceFramework();
        }

        #region Get database connection

        /// <summary>
        /// Get sqlserver database connection
        /// </summary>
        /// <param name="server">Database server</param>
        /// <returns>Return database connection</returns>
        internal static IDbConnection GetConnection(DatabaseServer server)
        {
            IDbConnection conn = DataManager.GetDatabaseConnection(server) ?? new SqlConnection(server.ConnectionString);
            return conn;
        }

        #endregion

        #region Get query translator

        /// <summary>
        /// Get query translator
        /// </summary>
        /// <param name="server">Database server</param>
        /// <returns>Return query translator</returns>
        internal static IQueryTranslator GetQueryTranslator(DatabaseServer server)
        {
            var translator = DataManager.GetQueryTranslator(server.ServerType);
            if (translator == null)
            {
                translator = new SqlServerQueryTranslator();
            }
            return translator;
        }

        #endregion

        #region Criteria converter

        /// <summary>
        /// Parse criteria converter
        /// </summary>
        /// <param name="converter">Criteria converter</param>
        /// <param name="objectName">Object name</param>
        /// <param name="fieldName">Field name</param>
        /// <returns>Reeturn format value</returns>
        internal static string ParseCriteriaConverter(ICriteriaConverter converter, string objectName, string fieldName)
        {
            var criteriaConverterParse = DataManager.GetCriteriaConverterParser(converter?.Name) ?? Parse;
            return criteriaConverterParse(new CriteriaConverterParseOption()
            {
                CriteriaConverter = converter,
                ServerType = DatabaseServerType.SQLServer,
                ObjectName = objectName,
                FieldName = fieldName
            });
        }

        /// <summary>
        /// Parse
        /// </summary>
        /// <param name="converterParseOption">Converter parse option</param>
        /// <returns></returns>
        static string Parse(CriteriaConverterParseOption converterParseOption)
        {
            if (string.IsNullOrWhiteSpace(converterParseOption?.CriteriaConverter?.Name))
            {
                throw new EZNEWException("Criteria convert config name is null or empty");
            }
            string format = null;
            switch (converterParseOption.CriteriaConverter.Name)
            {
                case CriteriaConverterNames.StringLength:
                    format = $"LEN({converterParseOption.ObjectName}.[{converterParseOption.FieldName}])";
                    break;
            }
            if (string.IsNullOrWhiteSpace(format))
            {
                throw new EZNEWException($"Cann't resolve criteria convert:{converterParseOption.CriteriaConverter.Name} for SQL Server");
            }
            return format;
        }

        #endregion

        #region Command translation result log

        /// <summary>
        /// Log execute command
        /// </summary>
        /// <param name="executeCommand">Execte command</param>
        internal static void LogExecuteCommand(DatabaseExecuteCommand executeCommand)
        {
            if (EnableTraceLog)
            {
                LogScriptCore(executeCommand.CommandText, JsonSerializeHelper.ObjectToJson(executeCommand.Parameters));
            }
        }

        /// <summary>
        /// Log script
        /// </summary>
        /// <param name="script">Script</param>
        /// <param name="parameters">Parameters</param>
        internal static void LogScript(string script, object parameters)
        {
            if (EnableTraceLog)
            {
                LogScriptCore(script, JsonSerializeHelper.ObjectToJson(parameters));
            }
        }

        /// <summary>
        /// Log script
        /// </summary>
        /// <param name="script">Script</param>
        /// <param name="parameters">Parameters</param>
        static void LogScriptCore(string script, string parameters)
        {
            LogManager.LogInformation<SqlServerEngine>(TraceLogSplit +
            $"{Environment.NewLine}{Environment.NewLine}{script}" +
            $"{Environment.NewLine}{Environment.NewLine}{parameters}" +
            $"{Environment.NewLine}{Environment.NewLine}");
        }

        #endregion
    }
}
