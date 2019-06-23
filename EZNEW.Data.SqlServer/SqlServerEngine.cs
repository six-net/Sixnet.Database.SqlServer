using Dapper;
using EZNEW.Data.Config;
using EZNEW.Develop.Entity;
using EZNEW.Develop.CQuery;
using EZNEW.Develop.CQuery.Translator;
using EZNEW.Develop.Command;
using EZNEW.Framework.Extension;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using EZNEW.Develop.Command.Modify;
using EZNEW.Framework.Fault;

namespace EZNEW.Data.SqlServer
{
    /// <summary>
    /// imeplements db engine for sqlserver
    /// </summary>
    public class SqlServerEngine : IDbEngine
    {
        #region execute

        /// <summary>
        /// execute command
        /// </summary>
        /// <typeparam name="T">data type</typeparam>
        /// <param name="server">server</param>
        /// <param name="cmds">command</param>
        /// <returns>data numbers</returns>
        public int Execute(ServerInfo server, params ICommand[] cmds)
        {
            return ExecuteAsync(server, cmds).Result;
        }

        /// <summary>
        /// execute command
        /// </summary>
        /// <typeparam name="T">data type</typeparam>
        /// <param name="server">server</param>
        /// <param name="cmds">command</param>
        /// <returns>data numbers</returns>
        public async Task<int> ExecuteAsync(ServerInfo server, params ICommand[] cmds)
        {
            #region group execute commands

            IQueryTranslator translator = QueryTranslator.GetTranslator(server);
            List<DbExecuteCommand> executeCommands = new List<DbExecuteCommand>();
            StringBuilder commandTextBuilder = new StringBuilder();
            CmdParameters parameters = null;
            int statementsCount = 0;
            bool forceReturnValue = false;
            foreach (var cmd in cmds)
            {
                DbExecuteCommand executeCommand = GetExecuteDbCommand(translator, cmd as RdbCommand);
                if (executeCommand == null)
                {
                    continue;
                }
                if (executeCommand.PerformAlone)
                {
                    if (statementsCount > 0)
                    {
                        executeCommands.Add(new DbExecuteCommand()
                        {
                            CommandText = commandTextBuilder.ToString(),
                            CommandType = CommandType.Text,
                            ForceReturnValue = true,
                            Parameters = parameters
                        });
                        statementsCount = 0;
                        translator.ParameterSequence = 0;
                        commandTextBuilder.Clear();
                        parameters = null;
                    }
                    executeCommands.Add(executeCommand);
                    continue;
                }
                commandTextBuilder.AppendLine(executeCommand.CommandText);
                parameters = parameters == null ? executeCommand.Parameters : parameters.Union(executeCommand.Parameters);
                forceReturnValue |= executeCommand.ForceReturnValue;
                statementsCount++;
                if (translator.ParameterSequence >= 2000 || statementsCount >= 1000)
                {
                    executeCommands.Add(new DbExecuteCommand()
                    {
                        CommandText = commandTextBuilder.ToString(),
                        CommandType = CommandType.Text,
                        ForceReturnValue = true,
                        Parameters = parameters
                    });
                    statementsCount = 0;
                    translator.ParameterSequence = 0;
                    commandTextBuilder.Clear();
                    parameters = null;
                }
            }
            if (statementsCount > 0)
            {
                executeCommands.Add(new DbExecuteCommand()
                {
                    CommandText = commandTextBuilder.ToString(),
                    CommandType = CommandType.Text,
                    ForceReturnValue = true,
                    Parameters = parameters
                });
            }

            #endregion

            return await ExecuteCommandAsync(server, executeCommands, cmds.Length > 1).ConfigureAwait(false);
        }

        /// <summary>
        /// execute commands
        /// </summary>
        /// <param name="server">db server</param>
        /// <param name="executeCommands">execute commands</param>
        /// <param name="useTransaction">use transaction</param>
        /// <returns></returns>
        async Task<int> ExecuteCommandAsync(ServerInfo server, IEnumerable<DbExecuteCommand> executeCommands, bool useTransaction)
        {
            int resultValue = 0;
            bool success = true;
            using (var conn = DbServerFactory.GetConnection(server))
            {
                conn.Open();
                IDbTransaction transaction = null;
                if (useTransaction)
                {
                    transaction = conn.BeginTransaction();
                }
                try
                {
                    foreach (var cmd in executeCommands)
                    {
                        var executeResultValue = await conn.ExecuteAsync(cmd.CommandText, ConvertCmdParameters(cmd.Parameters), transaction, commandType: cmd.CommandType).ConfigureAwait(false);
                        success = success && (cmd.ForceReturnValue ? executeResultValue > 0 : true);
                        resultValue += executeResultValue;
                        if (useTransaction && !success)
                        {
                            break;
                        }
                    }
                    if (!useTransaction)
                    {
                        return resultValue;
                    }
                    if (success)
                    {
                        transaction.Commit();
                    }
                    else
                    {
                        resultValue = 0;
                        transaction.Rollback();
                    }
                    return resultValue;
                }
                catch (Exception ex)
                {
                    resultValue = 0;
                    transaction?.Rollback();
                    throw ex;
                }
            }
        }

        /// <summary>
        /// get execute db command
        /// </summary>
        /// <param name="cmd">command</param>
        /// <returns></returns>
        DbExecuteCommand GetExecuteDbCommand(IQueryTranslator queryTranslator, RdbCommand cmd)
        {
            DbExecuteCommand executeCommand = null;
            switch (cmd.Operate)
            {
                case OperateType.Insert:
                    executeCommand = GetInsertExecuteDbCommand(queryTranslator, cmd);
                    break;
                case OperateType.Update:
                    executeCommand = GetUpdateExecuteDbCommand(queryTranslator, cmd);
                    break;
                case OperateType.Delete:
                    executeCommand = GetDeleteExecuteDbCommand(queryTranslator, cmd);
                    break;
            }
            return executeCommand;
        }

        /// <summary>
        /// get insert execute DbCommand
        /// </summary>
        /// <param name="translator">translator</param>
        /// <param name="cmd">cmd</param>
        /// <returns></returns>
        DbExecuteCommand GetInsertExecuteDbCommand(IQueryTranslator translator, RdbCommand cmd)
        {
            string cmdText = string.Empty;
            CmdParameters parameters = null;
            if (cmd.ExecuteMode == CommandExecuteMode.CommandText)
            {
                cmdText = cmd.CommandText;
                parameters = ParseParameters(cmd.Parameters);
            }
            else
            {
                string objectName = DataManager.GetEntityObjectName(ServerType.SQLServer, cmd.EntityType, cmd.ObjectName);
                var fields = DataManager.GetEditFields(ServerType.SQLServer, cmd.EntityType);
                var insertFormatResult = FormatInsertFields(fields, cmd.Parameters, translator.ParameterSequence);
                if (insertFormatResult == null)
                {
                    return null;
                }
                cmdText = string.Format("INSERT INTO [{0}] ({1}) VALUES ({2});"
                                        , objectName
                                        , string.Join(",", insertFormatResult.Item1)
                                        , string.Join(",", insertFormatResult.Item2));
                parameters = insertFormatResult.Item3;
                translator.ParameterSequence += fields.Count;
            }
            CommandType commandType = GetCommandType(cmd);
            return new DbExecuteCommand()
            {
                CommandText = cmdText,
                CommandType = commandType,
                ForceReturnValue = cmd.MustReturnValueOnSuccess,
                Parameters = parameters
            };
        }

        /// <summary>
        /// get update execute command
        /// </summary>
        /// <param name="translator">translator</param>
        /// <param name="cmd">cmd</param>
        /// <returns></returns>
        DbExecuteCommand GetUpdateExecuteDbCommand(IQueryTranslator translator, RdbCommand cmd)
        {
            #region query translate

            var tranResult = translator.Translate(cmd.Query);
            string conditionString = string.Empty;
            if (!tranResult.ConditionString.IsNullOrEmpty())
            {
                conditionString += "WHERE " + tranResult.ConditionString;
            }
            string preScript = tranResult.PreScript;
            string joinScript = tranResult.AllowJoin ? tranResult.JoinScript : string.Empty;

            #endregion

            string cmdText = string.Empty;
            CmdParameters parameters = ParseParameters(cmd.Parameters);
            if (cmd.ExecuteMode == CommandExecuteMode.CommandText)
            {
                cmdText = cmd.CommandText;
            }
            else
            {
                parameters = parameters ?? new CmdParameters();
                string objectName = DataManager.GetEntityObjectName(ServerType.SQLServer, cmd.EntityType, cmd.ObjectName);
                var fields = GetFields(cmd.EntityType, cmd.Fields);
                int parameterSequence = translator.ParameterSequence;
                List<string> updateSetArray = new List<string>();
                foreach (var field in fields)
                {
                    var parameterValue = parameters.GetParameterValue(field.PropertyName);
                    var parameterName = field.PropertyName;
                    string newValueExpression = string.Empty;
                    if (parameterValue != null)
                    {
                        parameterSequence++;
                        parameterName = FormatParameterName(parameterName, parameterSequence);
                        parameters.Rename(field.PropertyName, parameterName);
                        if (parameterValue is IModifyValue)
                        {
                            var modifyValue = parameterValue as IModifyValue;
                            parameters.ModifyValue(parameterName, modifyValue.Value);
                            if (parameterValue is CalculateModifyValue)
                            {
                                var calculateModifyValue = parameterValue as CalculateModifyValue;
                                string calChar = GetCalculateChar(calculateModifyValue.Operator);
                                newValueExpression = string.Format("{0}.[{1}]{2}@{3}"
                                                                    , translator.ObjectPetName
                                                                    , field.FieldName
                                                                    , calChar
                                                                    , parameterName);
                            }
                        }
                    }
                    if (string.IsNullOrWhiteSpace(newValueExpression))
                    {
                        newValueExpression = "@" + parameterName;
                    }
                    updateSetArray.Add(string.Format("{0}.[{1}]={2}"
                                                        , translator.ObjectPetName
                                                        , field.FieldName
                                                        , newValueExpression));
                }
                cmdText = string.Format("{4}UPDATE {0} SET {1} FROM [{2}] AS {0}{5} {3};"
                    , translator.ObjectPetName
                    , string.Join(",", updateSetArray.ToArray())
                    , objectName
                    , conditionString
                    , preScript
                    , joinScript);
                translator.ParameterSequence = parameterSequence;
            }
            //combine parameters
            if (tranResult.Parameters != null)
            {
                var queryParameters = ParseParameters(tranResult.Parameters);
                if (parameters != null)
                {
                    parameters.Union(queryParameters);
                }
                else
                {
                    parameters = queryParameters;
                }
            }
            CommandType commandType = GetCommandType(cmd);
            return new DbExecuteCommand()
            {
                CommandText = cmdText,
                CommandType = commandType,
                ForceReturnValue = cmd.MustReturnValueOnSuccess,
                Parameters = parameters,
                HasPreScript = !string.IsNullOrWhiteSpace(preScript)
            };
        }

        /// <summary>
        /// get delete execute command
        /// </summary>
        /// <param name="translator">translator</param>
        /// <param name="cmd">cmd</param>
        /// <returns></returns>
        DbExecuteCommand GetDeleteExecuteDbCommand(IQueryTranslator translator, RdbCommand cmd)
        {
            #region query translate

            var tranResult = translator.Translate(cmd.Query);
            string conditionString = string.Empty;
            if (!tranResult.ConditionString.IsNullOrEmpty())
            {
                conditionString += "WHERE " + tranResult.ConditionString;
            }
            string preScript = tranResult.PreScript;
            string joinScript = tranResult.AllowJoin ? tranResult.JoinScript : string.Empty;

            #endregion

            string cmdText = string.Empty;
            CmdParameters parameters = ParseParameters(cmd.Parameters);
            if (cmd.ExecuteMode == CommandExecuteMode.CommandText)
            {
                cmdText = cmd.CommandText;
            }
            else
            {
                string objectName = DataManager.GetEntityObjectName(ServerType.SQLServer, cmd.EntityType, cmd.ObjectName);
                cmdText = string.Format("{3}DELETE {0} FROM [{1}] AS {0}{4} {2};"
                    , translator.ObjectPetName
                    , objectName
                    , conditionString
                    , preScript
                    , joinScript);
            }
            //combine parameters
            if (tranResult.Parameters != null)
            {
                var queryParameters = ParseParameters(tranResult.Parameters);
                if (parameters != null)
                {
                    parameters.Union(queryParameters);
                }
                else
                {
                    parameters = queryParameters;
                }
            }
            CommandType commandType = GetCommandType(cmd);
            return new DbExecuteCommand()
            {
                CommandText = cmdText,
                CommandType = commandType,
                ForceReturnValue = cmd.MustReturnValueOnSuccess,
                Parameters = parameters,
                HasPreScript = !string.IsNullOrWhiteSpace(preScript)
            };
        }

        #endregion

        #region query

        /// <summary>
        /// query data list
        /// </summary>
        /// <typeparam name="T">data type</typeparam>
        /// <param name="server">database server</param>
        /// <param name="cmd">command</param>
        /// <returns>data list</returns>
        public IEnumerable<T> Query<T>(ServerInfo server, ICommand cmd)
        {
            return QueryAsync<T>(server, cmd).Result;
        }

        /// <summary>
        /// query data list
        /// </summary>
        /// <typeparam name="T">data type</typeparam>
        /// <param name="server">database server</param>
        /// <param name="cmd">command</param>
        /// <returns>data list</returns>
        public async Task<IEnumerable<T>> QueryAsync<T>(ServerInfo server, ICommand cmd)
        {
            cmd.Query = cmd.Query ?? QueryFactory.Create();

            #region query object translate

            IQueryTranslator translator = QueryTranslator.GetTranslator(server);
            var tranResult = translator.Translate(cmd.Query);
            string preScript = tranResult.PreScript;
            string joinScript = tranResult.AllowJoin ? tranResult.JoinScript : string.Empty;

            #endregion

            #region execute

            StringBuilder cmdText = new StringBuilder();
            switch (cmd.Query.QueryType)
            {
                case QueryCommandType.Text:
                    cmdText.Append(tranResult.ConditionString);
                    break;
                case QueryCommandType.QueryObject:
                default:
                    int size = cmd.Query == null ? 0 : cmd.Query.QuerySize;
                    string objectName = DataManager.GetEntityObjectName(ServerType.SQLServer, cmd.EntityType, cmd.ObjectName);
                    cmdText.AppendFormat("{4}SELECT {0} {1} FROM [{2}] AS {3}{5}"
                        , size > 0 ? "TOP " + size : string.Empty
                        , string.Join(",", FormatQueryFields(translator.ObjectPetName, cmd.Query, cmd.EntityType, out var defaultFieldName))
                        , objectName
                        , translator.ObjectPetName
                        , preScript
                        , joinScript);
                    if (!tranResult.ConditionString.IsNullOrEmpty())
                    {
                        cmdText.AppendFormat(" WHERE {0}", tranResult.ConditionString);
                    }
                    if (!tranResult.OrderString.IsNullOrEmpty())
                    {
                        cmdText.AppendFormat(" ORDER BY {0}", tranResult.OrderString);
                    }
                    break;
            }

            #endregion

            using (var conn = DbServerFactory.GetConnection(server))
            {
                return await conn.QueryAsync<T>(cmdText.ToString(), tranResult.Parameters, commandType: GetCommandType(cmd as RdbCommand)).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// query data with paging
        /// </summary>
        /// <typeparam name="T">data type</typeparam>
        /// <param name="server">databse server</param>
        /// <param name="cmd">command</param>
        /// <returns></returns>
        public IEnumerable<T> QueryPaging<T>(ServerInfo server, ICommand cmd)
        {
            return QueryPagingAsync<T>(server, cmd).Result;
        }

        /// <summary>
        /// query data with paging
        /// </summary>
        /// <typeparam name="T">data type</typeparam>
        /// <param name="server">databse server</param>
        /// <param name="cmd">command</param>
        /// <returns></returns>
        public async Task<IEnumerable<T>> QueryPagingAsync<T>(ServerInfo server, ICommand cmd)
        {
            int beginIndex = 0;
            int pageSize = 1;
            if (cmd.Query != null && cmd.Query.PagingInfo != null)
            {
                beginIndex = cmd.Query.PagingInfo.Page;
                pageSize = cmd.Query.PagingInfo.PageSize;
                beginIndex = (beginIndex - 1) * pageSize;
            }
            return await QueryOffsetAsync<T>(server, cmd, beginIndex, pageSize).ConfigureAwait(false);
        }

        /// <summary>
        /// query data list offset the specified numbers
        /// </summary>
        /// <typeparam name="T">data type</typeparam>
        /// <param name="server">database server</param>
        /// <param name="cmd">command</param>
        /// <param name="offsetNum">offset num</param>
        /// <param name="size">query size</param>
        /// <returns></returns>
        public IEnumerable<T> QueryOffset<T>(ServerInfo server, ICommand cmd, int offsetNum = 0, int size = int.MaxValue)
        {
            return QueryOffsetAsync<T>(server, cmd, offsetNum, size).Result;
        }

        /// <summary>
        /// query data list offset the specified numbers
        /// </summary>
        /// <typeparam name="T">data type</typeparam>
        /// <param name="server">database server</param>
        /// <param name="cmd">command</param>
        /// <param name="offsetNum">offset num</param>
        /// <param name="size">query size</param>
        /// <returns></returns>
        public async Task<IEnumerable<T>> QueryOffsetAsync<T>(ServerInfo server, ICommand cmd, int offsetNum = 0, int size = int.MaxValue)
        {
            cmd.Query = cmd.Query ?? QueryFactory.Create();

            #region query object translate

            IQueryTranslator translator = QueryTranslator.GetTranslator(server);
            var tranResult = translator.Translate(cmd.Query);

            #endregion

            #region execute

            string joinScript = tranResult.AllowJoin ? tranResult.JoinScript : string.Empty;
            StringBuilder cmdText = new StringBuilder();
            switch (cmd.Query.QueryType)
            {
                case QueryCommandType.Text:
                    cmdText.Append(tranResult.ConditionString);
                    break;
                case QueryCommandType.QueryObject:
                default:
                    string objectName = DataManager.GetEntityObjectName(ServerType.SQLServer, cmd.EntityType, cmd.ObjectName);
                    string defaultFieldName = string.Empty;
                    List<string> formatQueryFields = FormatQueryFields(translator.ObjectPetName, cmd.Query, cmd.EntityType, out defaultFieldName);
                    cmdText.AppendFormat("{4}SELECT COUNT({3}.[{0}]) OVER() AS PagingTotalCount,{1} FROM [{2}] AS {3}{5}"
                        , defaultFieldName
                        , string.Join(",", formatQueryFields)
                        , objectName
                        , translator.ObjectPetName
                        , tranResult.PreScript
                        , joinScript);
                    if (!tranResult.ConditionString.IsNullOrEmpty())
                    {
                        cmdText.AppendFormat(" WHERE {0}", tranResult.ConditionString);
                    }
                    if (!tranResult.OrderString.IsNullOrEmpty())
                    {
                        cmdText.AppendFormat(" ORDER BY {0}", tranResult.OrderString);
                    }
                    else
                    {
                        cmdText.AppendFormat(" ORDER BY {0}.[{1}] DESC", translator.ObjectPetName, defaultFieldName);
                    }
                    cmdText.AppendFormat(" OFFSET {0} ROWS FETCH NEXT {1} ROWS ONLY", offsetNum, size);
                    break;
            }

            #endregion

            using (var conn = DbServerFactory.GetConnection(server))
            {
                return await conn.QueryAsync<T>(cmdText.ToString(), tranResult.Parameters, commandType: GetCommandType(cmd as RdbCommand)).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// determine whether data has existed
        /// </summary>
        /// <param name="server">server</param>
        /// <param name="cmd">command</param>
        /// <returns>data has existed</returns>
        public bool Query(ServerInfo server, ICommand cmd)
        {
            return QueryAsync(server, cmd).Result;
        }

        /// <summary>
        /// determine whether data has existed
        /// </summary>
        /// <param name="server">server</param>
        /// <param name="cmd">command</param>
        /// <returns>data has existed</returns>
        public async Task<bool> QueryAsync(ServerInfo server, ICommand cmd)
        {
            var translator = QueryTranslator.GetTranslator(server);

            #region query translate

            var tranResult = translator.Translate(cmd.Query);
            string conditionString = string.Empty;
            if (!tranResult.ConditionString.IsNullOrEmpty())
            {
                conditionString += "WHERE " + tranResult.ConditionString;
            }
            string preScript = tranResult.PreScript;
            string joinScript = tranResult.AllowJoin ? tranResult.JoinScript : string.Empty;

            #endregion

            var field = DataManager.GetDefaultField(ServerType.SQLServer, cmd.EntityType);
            string objectName = DataManager.GetEntityObjectName(ServerType.SQLServer, cmd.EntityType, cmd.ObjectName);
            string cmdText = string.Format("{5}SELECT 1 WHERE EXISTS(SELECT {0}.[{1}] FROM [{2}] AS {0}{4} {3})"
                                , translator.ObjectPetName
                                , field.FieldName
                                , objectName
                                , conditionString
                                , joinScript
                                , preScript);
            using (var conn = DbServerFactory.GetConnection(server))
            {
                int value = await conn.ExecuteScalarAsync<int>(cmdText, tranResult.Parameters).ConfigureAwait(false);
                return value > 0;
            }
        }

        /// <summary>
        /// query single value
        /// </summary>
        /// <typeparam name="T">data type</typeparam>
        /// <param name="server">database server</param>
        /// <param name="cmd">command</param>
        /// <returns>query data</returns>
        public T QuerySingle<T>(ServerInfo server, ICommand cmd)
        {
            return QuerySingleAsync<T>(server, cmd).Result;
        }

        /// <summary>
        /// query single value
        /// </summary>
        /// <typeparam name="T">data type</typeparam>
        /// <param name="server">database server</param>
        /// <param name="cmd">command</param>
        /// <returns>query data</returns>
        public async Task<T> QuerySingleAsync<T>(ServerInfo server, ICommand cmd)
        {
            T result = default(T);
            switch (cmd.Operate)
            {
                case OperateType.Max:
                case OperateType.Min:
                case OperateType.Sum:
                case OperateType.Avg:
                case OperateType.Count:
                    result = await AggregateFunctionAsync<T>(server, cmd).ConfigureAwait(false);
                    break;
                case OperateType.Query:
                    if (cmd.Query == null)
                    {
                        cmd.Query = QueryFactory.Create();
                    }
                    cmd.Query.QuerySize = 1;
                    IEnumerable<T> dataList = await QueryAsync<T>(server, cmd).ConfigureAwait(false);
                    if (dataList != null && dataList.Any())
                    {
                        result = dataList.ElementAt(0);
                    }
                    break;
                default:
                    break;
            }
            return result;
        }

        /// <summary>
        /// Aggregate Function
        /// </summary>
        /// <typeparam name="T">data type</typeparam>
        /// <param name="server">database server</param>
        /// <param name="cmd">command</param>
        /// <returns></returns>
        async Task<T> AggregateFunctionAsync<T>(ServerInfo server, ICommand cmd)
        {
            cmd.Query = cmd.Query ?? QueryFactory.Create();

            #region query object translate

            IQueryTranslator translator = QueryTranslator.GetTranslator(server);
            var tranResult = translator.Translate(cmd.Query);

            #endregion

            #region execute

            StringBuilder cmdText = new StringBuilder();
            string joinScript = tranResult.AllowJoin ? tranResult.JoinScript : string.Empty;
            switch (cmd.Query.QueryType)
            {
                case QueryCommandType.Text:
                    cmdText.Append(tranResult.ConditionString);
                    break;
                case QueryCommandType.QueryObject:
                default:
                    string funcName = GetAggregateFunctionName(cmd.Operate);
                    if (funcName.IsNullOrEmpty() || cmd.Fields.IsNullOrEmpty())
                    {
                        return default(T);
                    }
                    string objectName = DataManager.GetEntityObjectName(ServerType.SQLServer, cmd.EntityType, cmd.ObjectName);
                    if (cmd.Query.QueryFields.IsNullOrEmpty())
                    {
                        throw new EZNEWException("must set one field");
                    }
                    var field = DataManager.GetField(ServerType.SQLServer, cmd.EntityType, cmd.Query.QueryFields[0]);
                    cmdText.AppendFormat("{4}SELECT {0}({3}.[{1}]) FROM [{2}] AS {3}{5}"
                        , funcName
                        , field.FieldName
                        , objectName
                        , translator.ObjectPetName
                        , tranResult.PreScript
                        , joinScript);
                    if (!tranResult.ConditionString.IsNullOrEmpty())
                    {
                        cmdText.AppendFormat(" WHERE {0}", tranResult.ConditionString);
                    }
                    if (!tranResult.OrderString.IsNullOrEmpty())
                    {
                        cmdText.AppendFormat(" ORDER BY {0}", tranResult.OrderString);
                    }
                    break;
            }

            #endregion

            using (var conn = DbServerFactory.GetConnection(server))
            {
                return await conn.ExecuteScalarAsync<T>(cmdText.ToString(), tranResult.Parameters, commandType: GetCommandType(cmd as RdbCommand)).ConfigureAwait(false);
            }
        }

        #endregion

        #region Helpers

        /// <summary>
        /// get command type
        /// </summary>
        /// <param name="cmd">command</param>
        /// <returns></returns>
        CommandType GetCommandType(RdbCommand cmd)
        {
            return cmd.CommandType == RdbCommandTextType.Procedure ? CommandType.StoredProcedure : CommandType.Text;
        }

        /// <summary>
        /// get calculate sign
        /// </summary>
        /// <param name="calculate">calculate operator</param>
        /// <returns></returns>
        string GetCalculateChar(CalculateOperator calculate)
        {
            string opearterChar = "";
            switch (calculate)
            {
                case CalculateOperator.Add:
                    opearterChar = "+";
                    break;
                case CalculateOperator.subtract:
                    opearterChar = "-";
                    break;
                case CalculateOperator.multiply:
                    opearterChar = "*";
                    break;
                case CalculateOperator.divide:
                    opearterChar = "/";
                    break;
            }
            return opearterChar;
        }

        /// <summary>
        /// get aggregate function name
        /// </summary>
        /// <param name="funcType">function type</param>
        /// <returns></returns>
        string GetAggregateFunctionName(OperateType funcType)
        {
            string funcName = string.Empty;
            switch (funcType)
            {
                case OperateType.Max:
                    funcName = "MAX";
                    break;
                case OperateType.Min:
                    funcName = "MIN";
                    break;
                case OperateType.Sum:
                    funcName = "SUM";
                    break;
                case OperateType.Avg:
                    funcName = "AVG";
                    break;
                case OperateType.Count:
                    funcName = "COUNT";
                    break;
            }
            return funcName;
        }

        /// <summary>
        /// format insert fields
        /// </summary>
        /// <param name="fields">fields</param>
        /// <param name="originParameters">origin parameters</param>
        /// <returns>first:fields,second:parameter fields,third:parameters</returns>
        Tuple<List<string>, List<string>, CmdParameters> FormatInsertFields(List<EntityField> fields, object parameters, int parameterSequence)
        {
            if (fields.IsNullOrEmpty())
            {
                return null;
            }
            List<string> formatFields = new List<string>(fields.Count);
            List<string> parameterFields = new List<string>(fields.Count);
            CmdParameters cmdParameters = ParseParameters(parameters);
            string key = ((int)ServerType.SQLServer).ToString();
            foreach (var field in fields)
            {
                //fields
                var formatValue = field.GetEditFormat(key);
                if (formatValue.IsNullOrEmpty())
                {
                    formatValue = string.Format("[{0}]", field.FieldName);
                    field.SetEditFormat(key, formatValue);
                }
                formatFields.Add(formatValue);

                //parameter name
                parameterSequence++;
                string parameterName = field.PropertyName + parameterSequence;
                parameterFields.Add("@" + parameterName);

                //parameter value
                cmdParameters?.Rename(field.PropertyName, parameterName);
            }
            return new Tuple<List<string>, List<string>, CmdParameters>(formatFields, parameterFields, cmdParameters);
        }

        /// <summary>
        /// format fields
        /// </summary>
        /// <param name="fields">fields</param>
        /// <returns></returns>
        List<string> FormatQueryFields(string dbObjectName, IQuery query, Type entityType, out string defaultFieldName)
        {
            defaultFieldName = string.Empty;
            if (query == null || entityType == null)
            {
                return new List<string>(0);
            }
            var queryFields = DataManager.GetQueryFields(ServerType.SQLServer, entityType, query);
            if (queryFields.IsNullOrEmpty())
            {
                return new List<string>(0);
            }
            defaultFieldName = queryFields[0].FieldName;
            List<string> formatFields = new List<string>();
            string key = ((int)ServerType.SQLServer).ToString();
            foreach (var field in queryFields)
            {
                var formatValue = field.GetQueryFormat(key);
                if (formatValue.IsNullOrEmpty())
                {
                    string fieldName = string.Format("{0}.[{1}]", dbObjectName, field.FieldName);
                    if (!field.QueryFormat.IsNullOrEmpty())
                    {
                        formatValue = string.Format(field.QueryFormat + " AS [{1}]", fieldName, field.PropertyName);
                    }
                    else if (field.FieldName != field.PropertyName)
                    {
                        formatValue = string.Format("{0} AS [{1}]", fieldName, field.PropertyName);
                    }
                    else
                    {
                        formatValue = fieldName;
                    }
                    field.SetQueryFormat(key, formatValue);
                }
                formatFields.Add(formatValue);
            }
            return formatFields;
        }

        /// <summary>
        /// get fields
        /// </summary>
        /// <param name="entityType">entity type</param>
        /// <param name="propertyNames">property names</param>
        /// <returns></returns>
        List<EntityField> GetFields(Type entityType, IEnumerable<string> propertyNames)
        {
            return DataManager.GetFields(ServerType.SQLServer, entityType, propertyNames);
        }

        /// <summary>
        /// format parameter name
        /// </summary>
        /// <param name="parameterName">parameter name</param>
        /// <param name="parameterSequence">parameter sequence</param>
        /// <returns></returns>
        static string FormatParameterName(string parameterName, int parameterSequence)
        {
            return parameterName + parameterSequence;
        }

        /// <summary>
        /// parse parameter
        /// </summary>
        /// <param name="originParameters">origin parameter</param>
        /// <returns></returns>
        CmdParameters ParseParameters(object originParameters)
        {
            if (originParameters == null)
            {
                return null;
            }
            CmdParameters parameters = originParameters as CmdParameters;
            if (parameters != null)
            {
                return parameters;
            }
            parameters = new CmdParameters();
            if (originParameters is IEnumerable<KeyValuePair<string, string>>)
            {
                var stringParametersDict = originParameters as IEnumerable<KeyValuePair<string, string>>;
                parameters.Add(stringParametersDict);
            }
            else if (originParameters is IEnumerable<KeyValuePair<string, dynamic>>)
            {
                var dynamicParametersDict = originParameters as IEnumerable<KeyValuePair<string, dynamic>>;
                parameters.Add(dynamicParametersDict);
            }
            else if (originParameters is IEnumerable<KeyValuePair<string, object>>)
            {
                var objectParametersDict = originParameters as IEnumerable<KeyValuePair<string, object>>;
                parameters.Add(objectParametersDict);
            }
            else if (originParameters is IEnumerable<KeyValuePair<string, IModifyValue>>)
            {
                var modifyParametersDict = originParameters as IEnumerable<KeyValuePair<string, IModifyValue>>;
                parameters.Add(modifyParametersDict);
            }
            else
            {
                var objectParametersDict = originParameters.ObjectToDcitionary();
                parameters.Add(objectParametersDict);
            }
            return parameters;
        }

        /// <summary>
        /// convert cmd parameters
        /// </summary>
        /// <param name="cmdParameters">cmd parameters</param>
        /// <returns></returns>
        DynamicParameters ConvertCmdParameters(CmdParameters cmdParameters)
        {
            if (cmdParameters?.Parameters.IsNullOrEmpty() ?? true)
            {
                return null;
            }
            DynamicParameters dynamicParameters = new DynamicParameters();
            foreach (var item in cmdParameters.Parameters)
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
    }
}
