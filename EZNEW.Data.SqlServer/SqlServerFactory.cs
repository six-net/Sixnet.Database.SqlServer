using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using EZNEW.Dapper;
using EZNEW.Data.CriteriaConverter;
using EZNEW.Develop.Command;
using EZNEW.Develop.Command.Modify;
using EZNEW.Develop.CQuery;
using EZNEW.Develop.CQuery.CriteriaConverter;
using EZNEW.Develop.CQuery.Translator;
using EZNEW.Develop.DataAccess;
using EZNEW.Develop.Entity;
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
        /// Field format key
        /// </summary>
        internal static readonly string FieldFormatKey = ((int)DatabaseServerType.SQLServer).ToString();

        /// <summary>
        /// Parameter prefix
        /// </summary>
        internal const string ParameterPrefix = "@";

        /// <summary>
        /// Calculate operators
        /// </summary>
        internal static readonly Dictionary<CalculateOperator, string> CalculateOperators = new Dictionary<CalculateOperator, string>(4)
        {
            [CalculateOperator.Add] = "+",
            [CalculateOperator.Subtract] = "-",
            [CalculateOperator.Multiply] = "*",
            [CalculateOperator.Divide] = "/",
        };

        /// <summary>
        /// Aggregate functions
        /// </summary>
        internal static readonly Dictionary<OperateType, string> AggregateFunctions = new Dictionary<OperateType, string>(5)
        {
            [OperateType.Max] = "MAX",
            [OperateType.Min] = "MIN",
            [OperateType.Sum] = "SUM",
            [OperateType.Avg] = "AVG",
            [OperateType.Count] = "COUNT",
        };

        /// <summary>
        /// Enable trace log
        /// </summary>
        static readonly bool EnableTraceLog = false;

        /// <summary>
        /// Trace log split
        /// </summary>
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
            return DataManager.GetDatabaseConnection(server) ?? new SqlConnection(server.ConnectionString);
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
            return DataManager.GetQueryTranslator(server.ServerType) ?? new SqlServerQueryTranslator();
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

        #region Get command type

        /// <summary>
        /// Get command type
        /// </summary>
        /// <param name="command">Command</param>
        /// <returns>Return command type</returns>
        public static CommandType GetCommandType(RdbCommand command)
        {
            return command.CommandType == CommandTextType.Procedure ? CommandType.StoredProcedure : CommandType.Text;
        }

        #endregion

        #region Get calculate sign

        /// <summary>
        /// Get calculate sign
        /// </summary>
        /// <param name="calculate">Calculate operator</param>
        /// <returns>Return calculate char</returns>
        public static string GetCalculateChar(CalculateOperator calculate)
        {
            CalculateOperators.TryGetValue(calculate, out var opearterChar);
            return opearterChar;
        }

        #endregion

        #region Get aggregate function name

        /// <summary>
        /// Get aggregate function name
        /// </summary>
        /// <param name="funcType">Function type</param>
        /// <returns>Return aggregate function name</returns>
        public static string GetAggregateFunctionName(OperateType funcType)
        {
            AggregateFunctions.TryGetValue(funcType, out var funcName);
            return funcName;
        }

        #endregion

        #region Aggregate operate must need field

        /// <summary>
        /// Aggregate operate must need field
        /// </summary>
        /// <param name="operateType">Operate type</param>
        /// <returns></returns>
        public static bool AggregateOperateMustNeedField(OperateType operateType)
        {
            return operateType != OperateType.Count;
        }

        #endregion

        #region Format insert fields

        /// <summary>
        /// Format insert fields
        /// </summary>
        /// <param name="fields">Fields</param>
        /// <param name="parameters">Parameters</param>
        /// <returns>first:fields,second:parameter fields,third:parameters</returns>
        public static Tuple<List<string>, List<string>, CommandParameters> FormatInsertFields(int fieldCount, IEnumerable<EntityField> fields, object parameters, int parameterSequence)
        {
            if (fields.IsNullOrEmpty())
            {
                return null;
            }
            List<string> formatFields = new List<string>(fieldCount);
            List<string> parameterFields = new List<string>(fieldCount);
            CommandParameters cmdParameters = ParseParameters(parameters);
            foreach (var field in fields)
            {
                //fields
                formatFields.Add($"[{field.FieldName}]");

                //parameter name
                parameterSequence++;
                string parameterName = field.PropertyName + parameterSequence;
                parameterFields.Add($"{ParameterPrefix}{parameterName}");

                //parameter value
                cmdParameters?.Rename(field.PropertyName, parameterName);
            }
            return new Tuple<List<string>, List<string>, CommandParameters>(formatFields, parameterFields, cmdParameters);
        }

        #endregion

        #region Format fields

        /// <summary>
        /// Format fields
        /// </summary>
        /// <param name="databasePetName">Database object name</param>
        /// <param name="query">Query object</param>
        /// <param name="entityType">Entity type</param>
        /// <param name="forceMustFields">Whether return must query fields</param>
        /// <returns></returns>
        public static IEnumerable<string> FormatQueryFields(string databasePetName, IQuery query, Type entityType, bool forceMustFields, bool convertField)
        {
            if (query == null || entityType == null)
            {
                return Array.Empty<string>();
            }
            var queryFields = GetQueryFields(query, entityType, forceMustFields);
            return queryFields?.Select(field => FormatField(databasePetName, field, convertField)) ?? Array.Empty<string>();
        }

        /// <summary>
        /// Format query fields
        /// </summary>
        /// <param name="databasePetName">Database name</param>
        /// <param name="fields">Fields</param>
        /// <param name="convertField">Whether convert field</param>
        /// <returns></returns>
        public static IEnumerable<string> FormatQueryFields(string databasePetName, IEnumerable<EntityField> fields, bool convertField)
        {
            return fields?.Select(field => FormatField(databasePetName, field, convertField)) ?? Array.Empty<string>();
        }

        #endregion

        #region Format field

        /// <summary>
        /// Format field
        /// </summary>
        /// <param name="databaseObjectName">Database object name</param>
        /// <param name="field">Field</param>
        /// <returns>Return field format value</returns>
        public static string FormatField(string databaseObjectName, EntityField field, bool convertField)
        {
            if (field == null)
            {
                return string.Empty;
            }
            string formatValue = $"{databaseObjectName}.[{field.FieldName}]";
            if (!string.IsNullOrWhiteSpace(field.QueryFormat))
            {
                formatValue = string.Format(field.QueryFormat + " AS [{1}]", formatValue, field.PropertyName);
            }
            else if (field.FieldName != field.PropertyName && convertField)
            {
                formatValue = $"{formatValue} AS [{field.PropertyName}]";
            }
            return formatValue;
        }

        #endregion

        #region Get fields

        /// <summary>
        /// Get query fields
        /// </summary>
        /// <param name="query">Query</param>
        /// <param name="entityType">Entity type</param>
        /// <param name="forceMustFields">Whether return must query fields</param>
        /// <returns></returns>
        public static IEnumerable<EntityField> GetQueryFields(IQuery query, Type entityType, bool forceMustFields)
        {
            return DataManager.GetQueryFields(DatabaseServerType.SQLServer, entityType, query, forceMustFields);
        }

        /// <summary>
        /// Get fields
        /// </summary>
        /// <param name="entityType">Entity type</param>
        /// <param name="propertyNames">Property names</param>
        /// <returns>Return fields</returns>
        public static IEnumerable<EntityField> GetFields(Type entityType, IEnumerable<string> propertyNames)
        {
            return DataManager.GetFields(DatabaseServerType.SQLServer, entityType, propertyNames);
        }

        #endregion

        #region Get default field

        /// <summary>
        /// Get default field
        /// </summary>
        /// <param name="entityType">Entity type</param>
        /// <returns>Return default field name</returns>
        public static string GetDefaultFieldName(Type entityType)
        {
            if (entityType == null)
            {
                return string.Empty;
            }
            return DataManager.GetDefaultField(DatabaseServerType.SQLServer, entityType)?.FieldName ?? string.Empty;
        }

        #endregion

        #region Format parameter name

        /// <summary>
        /// Format parameter name
        /// </summary>
        /// <param name="parameterName">Parameter name</param>
        /// <param name="parameterSequence">Parameter sequence</param>
        /// <returns>Return parameter name</returns>
        public static string FormatParameterName(string parameterName, int parameterSequence)
        {
            return parameterName + parameterSequence;
        }

        #endregion

        #region Parse parameter

        /// <summary>
        /// Parse parameter
        /// </summary>
        /// <param name="originalParameters">Original parameter</param>
        /// <returns>Return command parameters</returns>
        public static CommandParameters ParseParameters(object originalParameters)
        {
            if (originalParameters == null)
            {
                return null;
            }
            if (originalParameters is CommandParameters commandParameters)
            {
                return commandParameters;
            }
            commandParameters = new CommandParameters();
            if (originalParameters is IEnumerable<KeyValuePair<string, string>> stringParametersDict)
            {
                commandParameters.Add(stringParametersDict);
            }
            else if (originalParameters is IEnumerable<KeyValuePair<string, dynamic>> dynamicParametersDict)
            {
                commandParameters.Add(dynamicParametersDict);
            }
            else if (originalParameters is IEnumerable<KeyValuePair<string, object>> objectParametersDict)
            {
                commandParameters.Add(objectParametersDict);
            }
            else if (originalParameters is IEnumerable<KeyValuePair<string, IModifyValue>> modifyParametersDict)
            {
                commandParameters.Add(modifyParametersDict);
            }
            else
            {
                objectParametersDict = originalParameters.ObjectToDcitionary();
                commandParameters.Add(objectParametersDict);
            }
            return commandParameters;
        }

        #endregion

        #region Convert cmd parameters

        /// <summary>
        /// Convert cmd parameters
        /// </summary>
        /// <param name="commandParameters">Command parameters</param>
        /// <returns>Return dynamic parameters</returns>
        public static DynamicParameters ConvertCmdParameters(CommandParameters commandParameters)
        {
            if (commandParameters?.Parameters.IsNullOrEmpty() ?? true)
            {
                return null;
            }
            DynamicParameters dynamicParameters = new DynamicParameters();
            foreach (var item in commandParameters.Parameters)
            {
                var parameter = item.Value;
                dynamicParameters.Add(parameter.Name, parameter.Value
                                    , parameter.DbType, parameter.ParameterDirection
                                    , parameter.Size, parameter.Precision
                                    , parameter.Scale);
            }
            return dynamicParameters;
        }

        #endregion

        #region Get transaction isolation level

        /// <summary>
        /// Get transaction isolation level
        /// </summary>
        /// <param name="dataIsolationLevel">Data isolation level</param>
        /// <returns>Return isolation level</returns>
        public static IsolationLevel? GetTransactionIsolationLevel(DataIsolationLevel? dataIsolationLevel)
        {
            if (!dataIsolationLevel.HasValue)
            {
                dataIsolationLevel = DataManager.GetDataIsolationLevel(DatabaseServerType.SQLServer);
            }
            return DataManager.GetSystemIsolationLevel(dataIsolationLevel);
        }

        #endregion

        #region Get query transaction

        /// <summary>
        /// Get query transaction
        /// </summary>
        /// <param name="connection">Connection</param>
        /// <param name="query">Query</param>
        /// <returns>Return database transaction</returns>
        public static IDbTransaction GetQueryTransaction(IDbConnection connection, IQuery query)
        {
            DataIsolationLevel? dataIsolationLevel = query?.IsolationLevel;
            if (!dataIsolationLevel.HasValue)
            {
                dataIsolationLevel = DataManager.GetDataIsolationLevel(DatabaseServerType.SQLServer);
            }
            var systemIsolationLevel = GetTransactionIsolationLevel(dataIsolationLevel);
            if (systemIsolationLevel.HasValue)
            {
                if (connection.State != ConnectionState.Open)
                {
                    connection.Open();
                }
                return connection.BeginTransaction(systemIsolationLevel.Value);
            }
            return null;
        }

        #endregion

        #region Get execute transaction

        /// <summary>
        /// Get execute transaction
        /// </summary>
        /// <param name="connection">Connection</param>
        /// <param name="executeOption">Execute option</param>
        /// <returns>Return database transaction</returns>
        public static IDbTransaction GetExecuteTransaction(IDbConnection connection, CommandExecuteOption executeOption)
        {
            DataIsolationLevel? dataIsolationLevel = executeOption?.IsolationLevel;
            if (!dataIsolationLevel.HasValue)
            {
                dataIsolationLevel = DataManager.GetDataIsolationLevel(DatabaseServerType.SQLServer);
            }
            var systemIsolationLevel = DataManager.GetSystemIsolationLevel(dataIsolationLevel);
            if (connection.State != ConnectionState.Open)
            {
                connection.Open();
            }
            return systemIsolationLevel.HasValue ? connection.BeginTransaction(systemIsolationLevel.Value) : connection.BeginTransaction();
        }

        #endregion
    }
}
