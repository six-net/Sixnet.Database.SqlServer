using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using EZNEW.Development.Entity;
using EZNEW.Development.Query;
using EZNEW.Development.Query.Translator;
using EZNEW.Development.Command;
using EZNEW.Development.Command.Modification;
using EZNEW.Exceptions;
using EZNEW.Data.Configuration;
using EZNEW.Dapper;
using System.Data.SqlClient;

namespace EZNEW.Data.SqlServer
{
    /// <summary>
    /// Imeplements database provider for sqlserver
    /// </summary>
    public class SqlServerProvider : IDatabaseProvider
    {
        #region Execute

        /// <summary>
        /// Execute command
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="executeOptions">Execute options</param>
        /// <param name="commands">Commands</param>
        /// <returns>Return the affected data numbers</returns>
        public int Execute(DatabaseServer server, CommandExecutionOptions executeOptions, IEnumerable<ICommand> commands)
        {
            return ExecuteAsync(server, executeOptions, commands).Result;
        }

        /// <summary>
        /// Execute command
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="executeOption">Execute options</param>
        /// <param name="commands">Commands</param>
        /// <returns>Return the affected data numbers</returns>
        public int Execute(DatabaseServer server, CommandExecutionOptions executeOption, params ICommand[] commands)
        {
            return ExecuteAsync(server, executeOption, commands).Result;
        }

        /// <summary>
        /// Execute command
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="executeOption">Execute options</param>
        /// <param name="commands">Commands</param>
        /// <returns>Return the affected data numbers</returns>
        public async Task<int> ExecuteAsync(DatabaseServer server, CommandExecutionOptions executeOption, IEnumerable<ICommand> commands)
        {
            #region group execute commands

            IQueryTranslator translator = SqlServerFactory.GetQueryTranslator(server);
            List<DatabaseExecutionCommand> executeCommands = new List<DatabaseExecutionCommand>();
            var batchExecuteConfig = DataManager.GetBatchExecuteConfiguration(server.ServerType) ?? BatchExecuteConfiguration.Default;
            var groupStatementsCount = batchExecuteConfig.GroupStatementsCount;
            groupStatementsCount = groupStatementsCount < 0 ? 1 : groupStatementsCount;
            var groupParameterCount = batchExecuteConfig.GroupParametersCount;
            groupParameterCount = groupParameterCount < 0 ? 1 : groupParameterCount;
            StringBuilder commandTextBuilder = new StringBuilder();
            CommandParameters parameters = null;
            int statementsCount = 0;
            bool forceReturnValue = false;
            int cmdCount = 0;

            DatabaseExecutionCommand GetGroupExecuteCommand()
            {
                var executeCommand = new DatabaseExecutionCommand()
                {
                    CommandText = commandTextBuilder.ToString(),
                    CommandType = CommandType.Text,
                    ForceReturnValue = forceReturnValue,
                    Parameters = parameters
                };
                statementsCount = 0;
                translator.ParameterSequence = 0;
                commandTextBuilder.Clear();
                parameters = null;
                forceReturnValue = false;
                return executeCommand;
            }

            foreach (var cmd in commands)
            {
                DatabaseExecutionCommand executeCommand = GetExecuteDbCommand(translator, cmd as DefaultCommand);
                if (executeCommand == null)
                {
                    continue;
                }

                //Trace log
                SqlServerFactory.LogExecutionCommand(executeCommand);

                cmdCount++;
                if (executeCommand.PerformAlone)
                {
                    if (statementsCount > 0)
                    {
                        executeCommands.Add(GetGroupExecuteCommand());
                    }
                    executeCommands.Add(executeCommand);
                    continue;
                }
                commandTextBuilder.AppendLine(executeCommand.CommandText);
                parameters = parameters == null ? executeCommand.Parameters : parameters.Union(executeCommand.Parameters);
                forceReturnValue |= executeCommand.ForceReturnValue;
                statementsCount++;
                if (translator.ParameterSequence >= groupParameterCount || statementsCount >= groupStatementsCount)
                {
                    executeCommands.Add(GetGroupExecuteCommand());
                }
            }
            if (statementsCount > 0)
            {
                executeCommands.Add(GetGroupExecuteCommand());
            }

            #endregion

            return await ExecuteCommandAsync(server, executeOption, executeCommands, executeOption?.ExecuteByTransaction ?? cmdCount > 1).ConfigureAwait(false);
        }

        /// <summary>
        /// Execute command
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="executeOption">Execute options</param>
        /// <param name="commands">Commands</param>
        /// <returns>Return the affected data numbers</returns>
        public async Task<int> ExecuteAsync(DatabaseServer server, CommandExecutionOptions executeOption, params ICommand[] commands)
        {
            IEnumerable<ICommand> cmdCollection = commands;
            return await ExecuteAsync(server, executeOption, cmdCollection).ConfigureAwait(false);
        }

        /// <summary>
        /// Execute commands
        /// </summary>
        /// <param name="server">db server</param>
        /// <param name="executeCommands">execute commands</param>
        /// <param name="useTransaction">use transaction</param>
        /// <returns>Return the affected data numbers</returns>
        async Task<int> ExecuteCommandAsync(DatabaseServer server, CommandExecutionOptions executeOption, IEnumerable<DatabaseExecutionCommand> executeCommands, bool useTransaction)
        {
            int resultValue = 0;
            bool success = true;
            using (var conn = SqlServerFactory.GetConnection(server))
            {
                IDbTransaction transaction = null;
                if (useTransaction)
                {
                    transaction = SqlServerFactory.GetExecuteTransaction(conn, executeOption);
                }
                try
                {
                    foreach (var cmd in executeCommands)
                    {
                        var cmdDefinition = new CommandDefinition(cmd.CommandText, SqlServerFactory.ConvertCmdParameters(cmd.Parameters), transaction: transaction, commandType: cmd.CommandType, cancellationToken: executeOption?.CancellationToken ?? default);
                        var executeResultValue = await conn.ExecuteAsync(cmdDefinition).ConfigureAwait(false);
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
        /// Get database execute command
        /// </summary>
        /// <param name="command">Command</param>
        /// <returns>Return a database execute command</returns>
        DatabaseExecutionCommand GetExecuteDbCommand(IQueryTranslator queryTranslator, DefaultCommand command)
        {
            DatabaseExecutionCommand GetTextCommand()
            {
                return new DatabaseExecutionCommand()
                {
                    CommandText = command.CommandText,
                    Parameters = SqlServerFactory.ParseParameters(command.Parameters),
                    CommandType = SqlServerFactory.GetCommandType(command),
                    ForceReturnValue = command.MustReturnValueOnSuccess,
                    HasPreScript = true
                };
            }
            if (command.ExecutionMode == CommandExecutionMode.CommandText)
            {
                return GetTextCommand();
            }
            DatabaseExecutionCommand executeCommand;
            switch (command.OperateType)
            {
                case CommandOperationType.Insert:
                    executeCommand = GetInsertExecuteDbCommand(queryTranslator, command);
                    break;
                case CommandOperationType.Update:
                    executeCommand = GetUpdateExecuteDbCommand(queryTranslator, command);
                    break;
                case CommandOperationType.Delete:
                    executeCommand = GetDeleteExecuteDbCommand(queryTranslator, command);
                    break;
                default:
                    executeCommand = GetTextCommand();
                    break;
            }
            return executeCommand;
        }

        /// <summary>
        /// Get insert execute DbCommand
        /// </summary>
        /// <param name="translator">Translator</param>
        /// <param name="command">Command</param>
        /// <returns>Return an insert execute command</returns>
        DatabaseExecutionCommand GetInsertExecuteDbCommand(IQueryTranslator translator, DefaultCommand command)
        {
            string objectName = DataManager.GetEntityObjectName(DatabaseServerType.SQLServer, command.EntityType, command.ObjectName);
            var fields = DataManager.GetEditFields(DatabaseServerType.SQLServer, command.EntityType);
            var fieldCount = fields.GetCount();
            var insertFormatResult = SqlServerFactory.FormatInsertFields(fieldCount, fields, command.Parameters, translator.ParameterSequence);
            if (insertFormatResult == null)
            {
                return null;
            }
            string cmdText = $"INSERT INTO {SqlServerFactory.WrapKeyword(objectName)} ({string.Join(",", insertFormatResult.Item1)}) VALUES ({string.Join(",", insertFormatResult.Item2)});";
            CommandParameters parameters = insertFormatResult.Item3;
            translator.ParameterSequence += fieldCount;
            return new DatabaseExecutionCommand()
            {
                CommandText = cmdText,
                CommandType = SqlServerFactory.GetCommandType(command),
                ForceReturnValue = command.MustReturnValueOnSuccess,
                Parameters = parameters
            };
        }

        /// <summary>
        /// Get update execute command
        /// </summary>
        /// <param name="translator">Translator</param>
        /// <param name="command">Command</param>
        /// <returns>Return an update execute command</returns>
        DatabaseExecutionCommand GetUpdateExecuteDbCommand(IQueryTranslator translator, DefaultCommand command)
        {
            #region query translate

            var tranResult = translator.Translate(command.Query);
            string conditionString = string.Empty;
            if (!string.IsNullOrWhiteSpace(tranResult.ConditionString))
            {
                conditionString = $"WHERE {tranResult.ConditionString}";
            }
            string preScript = tranResult.PreScript;
            string joinScript = tranResult.AllowJoin ? tranResult.JoinScript : string.Empty;

            #endregion

            #region script

            CommandParameters parameters = SqlServerFactory.ParseParameters(command.Parameters) ?? new CommandParameters();
            string objectName = DataManager.GetEntityObjectName(DatabaseServerType.SQLServer, command.EntityType, command.ObjectName);
            var fields = SqlServerFactory.GetFields(command.EntityType, command.Fields);
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
                    parameterName = SqlServerFactory.FormatParameterName(parameterName, parameterSequence);
                    parameters.Rename(field.PropertyName, parameterName);
                    if (parameterValue is IModificationValue)
                    {
                        var modifyValue = parameterValue as IModificationValue;
                        parameters.ModifyValue(parameterName, modifyValue.Value);
                        if (parameterValue is CalculationModificationValue)
                        {
                            var calculateModifyValue = parameterValue as CalculationModificationValue;
                            string calChar = SqlServerFactory.GetCalculateChar(calculateModifyValue.Operator);
                            newValueExpression = $"{translator.ObjectPetName}.{SqlServerFactory.WrapKeyword(field.FieldName)}{calChar}{SqlServerFactory.ParameterPrefix}{parameterName}";
                        }
                    }
                }
                if (string.IsNullOrWhiteSpace(newValueExpression))
                {
                    newValueExpression = $"{SqlServerFactory.ParameterPrefix}{parameterName}";
                }
                updateSetArray.Add($"{translator.ObjectPetName}.{SqlServerFactory.WrapKeyword(field.FieldName)}={newValueExpression}");
            }
            string cmdText = $"{preScript}UPDATE {translator.ObjectPetName} SET {string.Join(",", updateSetArray)} FROM {SqlServerFactory.WrapKeyword(objectName)} AS {translator.ObjectPetName} {joinScript} {conditionString};";
            translator.ParameterSequence = parameterSequence;

            #endregion

            #region parameter

            var queryParameters = SqlServerFactory.ParseParameters(tranResult.Parameters);
            parameters.Union(queryParameters);

            #endregion

            return new DatabaseExecutionCommand()
            {
                CommandText = cmdText,
                CommandType = SqlServerFactory.GetCommandType(command),
                ForceReturnValue = command.MustReturnValueOnSuccess,
                Parameters = parameters,
                HasPreScript = !string.IsNullOrWhiteSpace(preScript)
            };
        }

        /// <summary>
        /// Get delete execute command
        /// </summary>
        /// <param name="translator">Translator</param>
        /// <param name="command">Command</param>
        /// <returns>Return a delete execute command</returns>
        DatabaseExecutionCommand GetDeleteExecuteDbCommand(IQueryTranslator translator, DefaultCommand command)
        {
            #region query translate

            var tranResult = translator.Translate(command.Query);
            string conditionString = string.Empty;
            if (!string.IsNullOrWhiteSpace(tranResult.ConditionString))
            {
                conditionString = $"WHERE {tranResult.ConditionString}";
            }
            string preScript = tranResult.PreScript;
            string joinScript = tranResult.AllowJoin ? tranResult.JoinScript : string.Empty;

            #endregion

            #region script

            string objectName = DataManager.GetEntityObjectName(DatabaseServerType.SQLServer, command.EntityType, command.ObjectName);
            string cmdText = $"{preScript}DELETE {translator.ObjectPetName} FROM {SqlServerFactory.WrapKeyword(objectName)} AS {translator.ObjectPetName}{joinScript} {conditionString};";

            #endregion

            #region parameter

            CommandParameters parameters = SqlServerFactory.ParseParameters(command.Parameters) ?? new CommandParameters();
            var queryParameters = SqlServerFactory.ParseParameters(tranResult.Parameters);
            parameters.Union(queryParameters);

            #endregion

            return new DatabaseExecutionCommand()
            {
                CommandText = cmdText,
                CommandType = SqlServerFactory.GetCommandType(command),
                ForceReturnValue = command.MustReturnValueOnSuccess,
                Parameters = parameters,
                HasPreScript = !string.IsNullOrWhiteSpace(preScript)
            };
        }

        #endregion

        #region Query

        /// <summary>
        /// Query datas
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <returns>Return the datas</returns>
        public IEnumerable<T> Query<T>(DatabaseServer server, ICommand command)
        {
            return QueryAsync<T>(server, command).Result;
        }

        /// <summary>
        /// Query datas
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <returns>Return the datas</returns>
        public async Task<IEnumerable<T>> QueryAsync<T>(DatabaseServer server, ICommand command)
        {
            if (command.Query == null)
            {
                throw new EZNEWException("ICommand.Query is null");
            }

            #region query translate

            IQueryTranslator translator = SqlServerFactory.GetQueryTranslator(server);
            var tranResult = translator.Translate(command.Query);
            string preScript = tranResult.PreScript;
            string joinScript = tranResult.AllowJoin ? tranResult.JoinScript : string.Empty;

            #endregion

            #region script

            string cmdText;
            switch (command.Query.QueryType)
            {
                case QueryCommandType.Text:
                    cmdText = tranResult.ConditionString;
                    break;
                case QueryCommandType.QueryObject:
                default:
                    int size = command.Query.QuerySize;
                    string objectName = DataManager.GetEntityObjectName(DatabaseServerType.SQLServer, command.EntityType, command.ObjectName);
                    string orderString = string.IsNullOrWhiteSpace(tranResult.OrderString) ? string.Empty : $"ORDER BY {tranResult.OrderString}";
                    var queryFields = SqlServerFactory.GetQueryFields(command.Query, command.EntityType, true);
                    string outputFormatedField = string.Join(",", SqlServerFactory.FormatQueryFields(translator.ObjectPetName, queryFields, true));
                    if (string.IsNullOrWhiteSpace(tranResult.CombineScript))
                    {
                        cmdText = $"{preScript}SELECT {(size > 0 ? $"TOP {size}" : string.Empty)} {outputFormatedField} FROM {SqlServerFactory.WrapKeyword(objectName)} AS {translator.ObjectPetName} {joinScript} {(string.IsNullOrWhiteSpace(tranResult.ConditionString) ? string.Empty : $"WHERE {tranResult.ConditionString}")} {orderString}";
                    }
                    else
                    {
                        string innerFormatedField = string.Join(",", SqlServerFactory.FormatQueryFields(translator.ObjectPetName, queryFields, false));
                        cmdText = $"{preScript}SELECT {(size > 0 ? $"TOP {size}" : string.Empty)} {outputFormatedField} FROM (SELECT {innerFormatedField} FROM {SqlServerFactory.WrapKeyword(objectName)} AS {translator.ObjectPetName} {joinScript} {(string.IsNullOrWhiteSpace(tranResult.ConditionString) ? string.Empty : $"WHERE {tranResult.ConditionString}")} {tranResult.CombineScript}) AS {translator.ObjectPetName} {orderString}";
                    }
                    break;
            }

            #endregion

            #region parameter

            var parameters = SqlServerFactory.ConvertCmdParameters(SqlServerFactory.ParseParameters(tranResult.Parameters));

            #endregion

            //Trace log
            SqlServerFactory.LogScript(cmdText, tranResult.Parameters);

            using (var conn = SqlServerFactory.GetConnection(server))
            {
                var tran = SqlServerFactory.GetQueryTransaction(conn, command.Query);
                var cmdDefinition = new CommandDefinition(cmdText, parameters, transaction: tran, commandType: SqlServerFactory.GetCommandType(command as DefaultCommand), cancellationToken: command.Query?.GetCancellationToken() ?? default);
                var data = await conn.QueryAsync<T>(cmdDefinition).ConfigureAwait(false);
                return data;
            }
        }

        /// <summary>
        /// Query paging
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="server">Databse server</param>
        /// <param name="command">Command</param>
        /// <returns>Return data paging</returns>
        public IEnumerable<T> QueryPaging<T>(DatabaseServer server, ICommand command)
        {
            return QueryPagingAsync<T>(server, command).Result;
        }

        /// <summary>
        /// Query paging
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="server">Databse server</param>
        /// <param name="command">Command</param>
        /// <returns>Return data paging</returns>
        public async Task<IEnumerable<T>> QueryPagingAsync<T>(DatabaseServer server, ICommand command)
        {
            int beginIndex = 0;
            int pageSize = 1;
            if (command?.Query?.PagingInfo != null)
            {
                beginIndex = command.Query.PagingInfo.Page;
                pageSize = command.Query.PagingInfo.PageSize;
                beginIndex = (beginIndex - 1) * pageSize;
            }
            return await QueryOffsetAsync<T>(server, command, beginIndex, pageSize).ConfigureAwait(false);
        }

        /// <summary>
        /// Query datas offset the specified numbers
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <param name="offsetNum">Offset num</param>
        /// <param name="size">Query size</param>
        /// <returns>Return the datas</returns>
        public IEnumerable<T> QueryOffset<T>(DatabaseServer server, ICommand command, int offsetNum = 0, int size = int.MaxValue)
        {
            return QueryOffsetAsync<T>(server, command, offsetNum, size).Result;
        }

        /// <summary>
        /// Query datas offset the specified numbers
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <param name="offsetNum">Offset num</param>
        /// <param name="size">Query size</param>
        /// <returns>Return the datas</returns>
        public async Task<IEnumerable<T>> QueryOffsetAsync<T>(DatabaseServer server, ICommand command, int offsetNum = 0, int size = int.MaxValue)
        {
            if (command.Query == null)
            {
                throw new EZNEWException("ICommand.Query is null");
            }

            #region query translate

            IQueryTranslator translator = SqlServerFactory.GetQueryTranslator(server);
            var tranResult = translator.Translate(command.Query);
            string joinScript = tranResult.AllowJoin ? tranResult.JoinScript : string.Empty;

            #endregion

            #region script

            string cmdText;
            switch (command.Query.QueryType)
            {
                case QueryCommandType.Text:
                    cmdText = tranResult.ConditionString;
                    break;
                case QueryCommandType.QueryObject:
                default:
                    string objectName = DataManager.GetEntityObjectName(DatabaseServerType.SQLServer, command.EntityType, command.ObjectName);
                    var defaultFieldName = SqlServerFactory.GetDefaultFieldName(command.EntityType);
                    var queryFields = SqlServerFactory.GetQueryFields(command.Query, command.EntityType, true);
                    var outputFormatedField = string.Join(",", SqlServerFactory.FormatQueryFields(translator.ObjectPetName, queryFields, true));
                    if (string.IsNullOrWhiteSpace(tranResult.CombineScript))
                    {
                        cmdText = $"{tranResult.PreScript}SELECT COUNT({translator.ObjectPetName}.{SqlServerFactory.WrapKeyword(defaultFieldName)}) OVER() AS QueryDataTotalCount,{outputFormatedField} FROM {SqlServerFactory.WrapKeyword(objectName)} AS {translator.ObjectPetName} {joinScript} {(string.IsNullOrWhiteSpace(tranResult.ConditionString) ? string.Empty : $"WHERE {tranResult.ConditionString}")} ORDER BY {(string.IsNullOrWhiteSpace(tranResult.OrderString) ? $"{translator.ObjectPetName}.{SqlServerFactory.WrapKeyword(defaultFieldName)} DESC" : tranResult.OrderString)} OFFSET {offsetNum} ROWS FETCH NEXT {size} ROWS ONLY";
                    }
                    else
                    {
                        var innerFormatedField = string.Join(",", SqlServerFactory.FormatQueryFields(translator.ObjectPetName, queryFields, false));
                        cmdText = $"{tranResult.PreScript}SELECT COUNT({translator.ObjectPetName}.{SqlServerFactory.WrapKeyword(defaultFieldName)}) OVER() AS QueryDataTotalCount,{outputFormatedField} FROM (SELECT {innerFormatedField} FROM {SqlServerFactory.WrapKeyword(objectName)} AS {translator.ObjectPetName} {joinScript} {(string.IsNullOrWhiteSpace(tranResult.ConditionString) ? string.Empty : $"WHERE {tranResult.ConditionString}")} {tranResult.CombineScript}) AS {translator.ObjectPetName} ORDER BY {(string.IsNullOrWhiteSpace(tranResult.OrderString) ? $"{translator.ObjectPetName}.{SqlServerFactory.WrapKeyword(defaultFieldName)} DESC" : tranResult.OrderString)} OFFSET {offsetNum} ROWS FETCH NEXT {size} ROWS ONLY";
                    }
                    break;
            }

            #endregion

            #region parameter

            var parameters = SqlServerFactory.ConvertCmdParameters(SqlServerFactory.ParseParameters(tranResult.Parameters));

            #endregion

            //Trace log
            SqlServerFactory.LogScript(cmdText, tranResult.Parameters);

            using (var conn = SqlServerFactory.GetConnection(server))
            {
                var tran = SqlServerFactory.GetQueryTransaction(conn, command.Query);
                var cmdDefinition = new CommandDefinition(cmdText, parameters, transaction: tran, commandType: SqlServerFactory.GetCommandType(command as DefaultCommand), cancellationToken: command.Query?.GetCancellationToken() ?? default);
                return await conn.QueryAsync<T>(cmdDefinition).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Query whether the data exists or not
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <returns>Return whether the data exists or not</returns>
        public bool Query(DatabaseServer server, ICommand command)
        {
            return QueryAsync(server, command).Result;
        }

        /// <summary>
        /// Query whether the data exists or not
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <returns>Return whether the data exists or not</returns>
        public async Task<bool> QueryAsync(DatabaseServer server, ICommand command)
        {
            var translator = SqlServerFactory.GetQueryTranslator(server);

            #region query translate

            command.Query.ClearQueryFields();
            var queryFields = EntityManager.GetPrimaryKeys(command.EntityType).ToArray();
            if (queryFields.IsNullOrEmpty())
            {
                queryFields = EntityManager.GetQueryFields(command.EntityType).ToArray();
            }
            command.Query.AddQueryFields(queryFields);
            var tranResult = translator.Translate(command.Query);
            string conditionString = string.IsNullOrWhiteSpace(tranResult.ConditionString) ? string.Empty : $"WHERE {tranResult.ConditionString}";
            string preScript = tranResult.PreScript;
            string joinScript = tranResult.AllowJoin ? tranResult.JoinScript : string.Empty;

            #endregion

            #region script

            string objectName = DataManager.GetEntityObjectName(DatabaseServerType.SQLServer, command.EntityType, command.ObjectName);
            string formatedField = string.Join(",", SqlServerFactory.FormatQueryFields(translator.ObjectPetName, command.Query, command.EntityType, true, false));
            string cmdText = $"{preScript}SELECT 1 WHERE EXISTS(SELECT {formatedField} FROM {SqlServerFactory.WrapKeyword(objectName)} AS {translator.ObjectPetName} {joinScript} {conditionString} {tranResult.CombineScript})";

            #endregion

            #region parameter

            var parameters = SqlServerFactory.ConvertCmdParameters(SqlServerFactory.ParseParameters(tranResult.Parameters));

            #endregion

            //Trace log
            SqlServerFactory.LogScript(cmdText, tranResult.Parameters);

            using (var conn = SqlServerFactory.GetConnection(server))
            {
                var tran = SqlServerFactory.GetQueryTransaction(conn, command.Query);
                var cmdDefinition = new CommandDefinition(cmdText, parameters, transaction: tran, cancellationToken: command.Query?.GetCancellationToken() ?? default);
                int value = await conn.ExecuteScalarAsync<int>(cmdDefinition).ConfigureAwait(false);
                return value > 0;
            }
        }

        /// <summary>
        /// Query single value
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <returns>Return the data</returns>
        public T AggregateValue<T>(DatabaseServer server, ICommand command)
        {
            return AggregateValueAsync<T>(server, command).Result;
        }

        /// <summary>
        /// Query single value
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <returns>Return the data</returns>
        public async Task<T> AggregateValueAsync<T>(DatabaseServer server, ICommand command)
        {
            if (command.Query == null)
            {
                throw new EZNEWException("ICommand.Query is null");
            }

            #region query translate

            bool queryObject = command.Query.QueryType == QueryCommandType.QueryObject;
            string funcName = SqlServerFactory.GetAggregateFunctionName(command.OperateType);
            EntityField defaultField = null;
            if (queryObject)
            {
                if (string.IsNullOrWhiteSpace(funcName))
                {
                    throw new NotSupportedException($"Not support {command.OperateType}");
                }
                if (SqlServerFactory.AggregateOperateMustNeedField(command.OperateType))
                {
                    if (queryObject && (command.Query?.QueryFields.IsNullOrEmpty() ?? true))
                    {
                        throw new EZNEWException($"You must specify the field to perform for the {funcName} operation");
                    }
                    else
                    {
                        defaultField = DataManager.GetField(DatabaseServerType.SQLServer, command.EntityType, command.Query.QueryFields.First());
                    }
                }
                else
                {
                    defaultField = DataManager.GetDefaultField(DatabaseServerType.SQLServer, command.EntityType);
                }

                //combine fields
                if (!command.Query.CombineItems.IsNullOrEmpty())
                {
                    var combineKeys = EntityManager.GetPrimaryKeys(command.EntityType).Union(new string[1] { defaultField.PropertyName }).ToArray();
                    command.Query.ClearQueryFields();
                    foreach (var combineItem in command.Query.CombineItems)
                    {
                        combineItem.CombineQuery.ClearQueryFields();
                        if (combineKeys.IsNullOrEmpty())
                        {
                            combineItem.CombineQuery.ClearNotQueryFields();
                            command.Query.ClearNotQueryFields();
                        }
                        else
                        {
                            combineItem.CombineQuery.AddQueryFields(combineKeys);
                            command.Query.AddQueryFields(combineKeys);
                        }
                    }
                }
            }
            IQueryTranslator translator = SqlServerFactory.GetQueryTranslator(server);
            var tranResult = translator.Translate(command.Query);

            #endregion

            #region script

            string cmdText;
            string joinScript = tranResult.AllowJoin ? tranResult.JoinScript : string.Empty;
            switch (command.Query.QueryType)
            {
                case QueryCommandType.Text:
                    cmdText = tranResult.ConditionString;
                    break;
                case QueryCommandType.QueryObject:
                default:
                    string objectName = DataManager.GetEntityObjectName(DatabaseServerType.SQLServer, command.EntityType, command.ObjectName);
                    string formatedDefaultField = SqlServerFactory.FormatField(translator.ObjectPetName, defaultField, false);
                    cmdText = string.IsNullOrWhiteSpace(tranResult.CombineScript)
                        ? $"{tranResult.PreScript}SELECT {funcName}({formatedDefaultField}) FROM {SqlServerFactory.WrapKeyword(objectName)} AS {translator.ObjectPetName} {joinScript} {(string.IsNullOrWhiteSpace(tranResult.ConditionString) ? string.Empty : $"WHERE {tranResult.ConditionString}")}"
                        : $"{tranResult.PreScript}SELECT {funcName}({formatedDefaultField}) FROM (SELECT {string.Join(",", SqlServerFactory.FormatQueryFields(translator.ObjectPetName, command.Query, command.EntityType, true, false))} FROM {SqlServerFactory.WrapKeyword(objectName)} AS {translator.ObjectPetName} {joinScript} {(string.IsNullOrWhiteSpace(tranResult.ConditionString) ? string.Empty : $"WHERE {tranResult.ConditionString}")} {tranResult.CombineScript}) AS {translator.ObjectPetName}";
                    break;
            }

            #endregion

            #region parameter

            var parameters = SqlServerFactory.ConvertCmdParameters(SqlServerFactory.ParseParameters(tranResult.Parameters));

            #endregion

            //Trace log
            SqlServerFactory.LogScript(cmdText, tranResult.Parameters);

            using (var conn = SqlServerFactory.GetConnection(server))
            {
                var tran = SqlServerFactory.GetQueryTransaction(conn, command.Query);
                var cmdDefinition = new CommandDefinition(cmdText, parameters, transaction: tran, commandType: SqlServerFactory.GetCommandType(command as DefaultCommand), cancellationToken: command.Query?.GetCancellationToken() ?? default);
                return await conn.ExecuteScalarAsync<T>(cmdDefinition).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Query data set
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <returns>Return the dataset</returns>
        public async Task<DataSet> QueryMultipleAsync(DatabaseServer server, ICommand command)
        {
            //Trace log
            SqlServerFactory.LogScript(command.CommandText, command.Parameters);
            using (var conn = SqlServerFactory.GetConnection(server))
            {
                var tran = SqlServerFactory.GetQueryTransaction(conn, command.Query);
                DynamicParameters parameters = SqlServerFactory.ConvertCmdParameters(SqlServerFactory.ParseParameters(command.Parameters));
                var cmdDefinition = new CommandDefinition(command.CommandText, parameters, transaction: tran, commandType: SqlServerFactory.GetCommandType(command as DefaultCommand), cancellationToken: command.Query?.GetCancellationToken() ?? default);
                using (var reader = await conn.ExecuteReaderAsync(cmdDefinition).ConfigureAwait(false))
                {
                    DataSet dataSet = new DataSet();
                    while (!reader.IsClosed && reader.Read())
                    {
                        DataTable dataTable = new DataTable();
                        dataTable.Load(reader);
                        dataSet.Tables.Add(dataTable);
                    }
                    return dataSet;
                }
            }
        }

        #endregion

        #region Bulk

        /// <summary>
        /// Bulk insert datas
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="dataTable">Data table</param>
        /// <param name="bulkInsertOptions">Insert options</param>
        public void BulkInsert(DatabaseServer server, DataTable dataTable, IBulkInsertOptions bulkInsertOptions = null)
        {
            BulkInsertAsync(server, dataTable).Wait();
        }

        /// <summary>
        /// Bulk insert datas
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="dataTable">Data table</param>
        /// <param name="bulkInsertOptions">Insert options</param>
        public async Task BulkInsertAsync(DatabaseServer server, DataTable dataTable, IBulkInsertOptions bulkInsertOptions = null)
        {
            if (server == null)
            {
                throw new ArgumentNullException(nameof(server));
            }
            if (dataTable == null)
            {
                throw new ArgumentNullException(nameof(dataTable));
            }
            using (SqlConnection sqlConnection = new SqlConnection(server?.ConnectionString))
            {
                try
                {
                    sqlConnection.Open();
                    SqlTransaction sqlTransaction = null;
                    SqlBulkCopy sqlServerBulkCopy = null;
                    if (bulkInsertOptions is SqlServerBulkInsertOptions sqlServerBulkInsertOptions && sqlServerBulkInsertOptions != null)
                    {
                        if (sqlServerBulkInsertOptions.UseTransaction)
                        {
                            sqlTransaction = sqlConnection.BeginTransaction();
                            sqlServerBulkCopy = new SqlBulkCopy(sqlConnection, SqlBulkCopyOptions.Default, sqlTransaction);
                        }
                        else
                        {
                            sqlServerBulkCopy = new SqlBulkCopy(sqlConnection);
                        }
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
                    else
                    {
                        sqlServerBulkCopy = new SqlBulkCopy(sqlConnection);
                    }
                    if (sqlServerBulkCopy.ColumnMappings.Count < 1)
                    {
                        BuildColumnMapping(sqlServerBulkCopy, dataTable);
                    }
                    sqlServerBulkCopy.DestinationTableName = dataTable.TableName;
                    await sqlServerBulkCopy.WriteToServerAsync(dataTable).ConfigureAwait(false);
                    sqlServerBulkCopy.Close();
                }
                catch (Exception)
                {
                    throw;
                }
                finally
                {
                    if (sqlConnection != null && sqlConnection.State != ConnectionState.Closed)
                    {
                        sqlConnection.Close();
                    }
                }
            }
        }

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
