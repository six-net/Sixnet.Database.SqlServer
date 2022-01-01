using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using Dapper;
using EZNEW.Development.Entity;
using EZNEW.Development.Query;
using EZNEW.Development.Query.Translation;
using EZNEW.Development.Command;
using EZNEW.Exceptions;
using EZNEW.Data.Configuration;
using EZNEW.Data.Modification;

namespace EZNEW.Data.SqlServer
{
    /// <summary>
    /// Imeplements database provider for sqlserver
    /// </summary>
    public class SqlServerProvider : IDatabaseProvider
    {
        const DatabaseServerType CurrentDatabaseServerType = SqlServerManager.CurrentDatabaseServerType;

        #region Execute

        /// <summary>
        /// Execute command
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="executionOptions">Execution options</param>
        /// <param name="commands">Commands</param>
        /// <returns>Return affected data number</returns>
        public int Execute(DatabaseServer server, CommandExecutionOptions executionOptions, IEnumerable<ICommand> commands)
        {
            return ExecuteAsync(server, executionOptions, commands).Result;
        }

        /// <summary>
        /// Execute command
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="executionOptions">Execution options</param>
        /// <param name="commands">Commands</param>
        /// <returns>Return affected data number</returns>
        public int Execute(DatabaseServer server, CommandExecutionOptions executionOptions, params ICommand[] commands)
        {
            return ExecuteAsync(server, executionOptions, commands).Result;
        }

        /// <summary>
        /// Execute command
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="executionOptions">Execution options</param>
        /// <param name="commands">Commands</param>
        /// <returns>Return affected data number</returns>
        public async Task<int> ExecuteAsync(DatabaseServer server, CommandExecutionOptions executionOptions, params ICommand[] commands)
        {
            IEnumerable<ICommand> cmdCollection = commands;
            return await ExecuteAsync(server, executionOptions, cmdCollection).ConfigureAwait(false);
        }

        /// <summary>
        /// Execute command
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="executionOptions">Execution options</param>
        /// <param name="commands">Commands</param>
        /// <returns>Return affected data number</returns>
        public async Task<int> ExecuteAsync(DatabaseServer server, CommandExecutionOptions executionOptions, IEnumerable<ICommand> commands)
        {
            #region group execution commands

            IQueryTranslator translator = SqlServerManager.GetQueryTranslator(DataAccessContext.Create(server));
            List<DatabaseExecutionCommand> executeCommands = new List<DatabaseExecutionCommand>();
            var batchExecuteConfig = DataManager.GetBatchExecutionConfiguration(server.ServerType) ?? BatchExecutionConfiguration.Default;
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
                var executionCommand = new DatabaseExecutionCommand()
                {
                    CommandText = commandTextBuilder.ToString(),
                    CommandType = CommandType.Text,
                    MustAffectedData = forceReturnValue,
                    Parameters = parameters
                };
                statementsCount = 0;
                translator.ParameterSequence = 0;
                commandTextBuilder.Clear();
                parameters = null;
                forceReturnValue = false;
                return executionCommand;
            }

            foreach (var cmd in commands)
            {
                DatabaseExecutionCommand databaseExecutionCommand = GetDatabaseExecutionCommand(translator, cmd as DefaultCommand);
                if (databaseExecutionCommand == null)
                {
                    continue;
                }

                //Trace log
                SqlServerManager.LogExecutionCommand(databaseExecutionCommand);

                cmdCount++;
                if (databaseExecutionCommand.PerformAlone)
                {
                    if (statementsCount > 0)
                    {
                        executeCommands.Add(GetGroupExecuteCommand());
                    }
                    executeCommands.Add(databaseExecutionCommand);
                    continue;
                }
                commandTextBuilder.AppendLine(databaseExecutionCommand.CommandText);
                parameters = parameters == null ? databaseExecutionCommand.Parameters : parameters.Union(databaseExecutionCommand.Parameters);
                forceReturnValue |= databaseExecutionCommand.MustAffectedData;
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

            return await ExecuteDatabaseCommandAsync(server, executionOptions, executeCommands, executionOptions?.ExecutionByTransaction ?? cmdCount > 1).ConfigureAwait(false);
        }

        /// <summary>
        /// Execute database command
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="executionOptions">Execution options</param>
        /// <param name="databaseExecutionCommands">Database execution commands</param>
        /// <param name="useTransaction">Whether use transaction</param>
        /// <returns>Return affected data number</returns>
        async Task<int> ExecuteDatabaseCommandAsync(DatabaseServer server, CommandExecutionOptions executionOptions, IEnumerable<DatabaseExecutionCommand> databaseExecutionCommands, bool useTransaction)
        {
            int resultValue = 0;
            bool success = true;
            using (var conn = SqlServerManager.GetConnection(server))
            {
                IDbTransaction transaction = null;
                if (useTransaction)
                {
                    transaction = SqlServerManager.GetExecuteTransaction(conn, executionOptions);
                }
                try
                {
                    foreach (var cmd in databaseExecutionCommands)
                    {
                        var cmdDefinition = new CommandDefinition(cmd.CommandText, SqlServerManager.ConvertCmdParameters(cmd.Parameters), transaction: transaction, commandType: cmd.CommandType, cancellationToken: executionOptions?.CancellationToken ?? default);
                        var executeResultValue = await conn.ExecuteAsync(cmdDefinition).ConfigureAwait(false);
                        success = success && (cmd.MustAffectedData ? executeResultValue > 0 : true);
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
        /// Get database execution command
        /// </summary>
        /// <param name="queryTranslator">Query translator</param>
        /// <param name="command">Command</param>
        /// <returns>Return a database execution command</returns>
        DatabaseExecutionCommand GetDatabaseExecutionCommand(IQueryTranslator queryTranslator, DefaultCommand command)
        {
            DatabaseExecutionCommand GetTextCommand()
            {
                return new DatabaseExecutionCommand()
                {
                    CommandText = command.Text,
                    Parameters = SqlServerManager.ConvertParameter(command.Parameters),
                    CommandType = SqlServerManager.GetCommandType(command),
                    MustAffectedData = command.MustAffectedData,
                    HasPreScript = true
                };
            }
            if (command.ExecutionMode == CommandExecutionMode.CommandText)
            {
                return GetTextCommand();
            }
            DatabaseExecutionCommand executionCommand;
            switch (command.OperationType)
            {
                case CommandOperationType.Insert:
                    executionCommand = GetDatabaseInsertionCommand(queryTranslator, command);
                    break;
                case CommandOperationType.Update:
                    executionCommand = GetDatabaseUpdateCommand(queryTranslator, command);
                    break;
                case CommandOperationType.Delete:
                    executionCommand = GetDatabaseDeletionCommand(queryTranslator, command);
                    break;
                default:
                    executionCommand = GetTextCommand();
                    break;
            }
            return executionCommand;
        }

        /// <summary>
        /// Get database insertion execution command
        /// </summary>
        /// <param name="translator">Query translator</param>
        /// <param name="command">Command</param>
        /// <returns>Return a database insertion command</returns>
        DatabaseExecutionCommand GetDatabaseInsertionCommand(IQueryTranslator translator, DefaultCommand command)
        {
            translator.DataAccessContext.SetCommand(command);
            string objectName = translator.DataAccessContext.GetCommandEntityObjectName(command);
            var fields = DataManager.GetEditFields(CurrentDatabaseServerType, command.EntityType);
            var fieldCount = fields.GetCount();
            var insertFormatResult = SqlServerManager.FormatInsertionFields(command.EntityType, fieldCount, fields, command.Parameters, translator.ParameterSequence);
            if (insertFormatResult == null)
            {
                return null;
            }
            string cmdText = $"INSERT INTO {SqlServerManager.WrapKeyword(objectName)} ({string.Join(",", insertFormatResult.Item1)}) VALUES ({string.Join(",", insertFormatResult.Item2)});";
            CommandParameters parameters = insertFormatResult.Item3;
            translator.ParameterSequence += fieldCount;
            return new DatabaseExecutionCommand()
            {
                CommandText = cmdText,
                CommandType = SqlServerManager.GetCommandType(command),
                MustAffectedData = command.MustAffectedData,
                Parameters = parameters
            };
        }

        /// <summary>
        /// Get database update command
        /// </summary>
        /// <param name="translator">Query translator</param>
        /// <param name="command">Command</param>
        /// <returns>Return a database update command</returns>
        DatabaseExecutionCommand GetDatabaseUpdateCommand(IQueryTranslator translator, DefaultCommand command)
        {
            if (command?.Fields.IsNullOrEmpty() ?? true)
            {
                throw new EZNEWException($"No fields are set to update");
            }

            #region query translation

            translator.DataAccessContext.SetCommand(command);
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

            CommandParameters parameters = SqlServerManager.ConvertParameter(command.Parameters) ?? new CommandParameters();
            string objectName = translator.DataAccessContext.GetCommandEntityObjectName(command);
            var fields = SqlServerManager.GetFields(command.EntityType, command.Fields);
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
                    parameterName = SqlServerManager.FormatParameterName(parameterName, parameterSequence);
                    parameters.Rename(field.PropertyName, parameterName);
                    if (parameterValue is IModificationValue)
                    {
                        var modifyValue = parameterValue as IModificationValue;
                        parameters.ModifyValue(parameterName, modifyValue.Value);
                        if (parameterValue is CalculationModificationValue)
                        {
                            var calculateModifyValue = parameterValue as CalculationModificationValue;
                            string calChar = SqlServerManager.GetSystemCalculationOperator(calculateModifyValue.Operator);
                            newValueExpression = $"{translator.ObjectPetName}.{SqlServerManager.WrapKeyword(field.FieldName)}{calChar}{SqlServerManager.ParameterPrefix}{parameterName}";
                        }
                    }
                }
                if (string.IsNullOrWhiteSpace(newValueExpression))
                {
                    newValueExpression = $"{SqlServerManager.ParameterPrefix}{parameterName}";
                }
                updateSetArray.Add($"{translator.ObjectPetName}.{SqlServerManager.WrapKeyword(field.FieldName)}={newValueExpression}");
            }
            string cmdText = $"{preScript}UPDATE {translator.ObjectPetName} SET {string.Join(",", updateSetArray)} FROM {SqlServerManager.WrapKeyword(objectName)} AS {translator.ObjectPetName} {joinScript} {conditionString};";
            translator.ParameterSequence = parameterSequence;

            #endregion

            #region parameter

            var queryParameters = SqlServerManager.ConvertParameter(tranResult.Parameters);
            parameters.Union(queryParameters);

            #endregion

            return new DatabaseExecutionCommand()
            {
                CommandText = cmdText,
                CommandType = SqlServerManager.GetCommandType(command),
                MustAffectedData = command.MustAffectedData,
                Parameters = parameters,
                HasPreScript = !string.IsNullOrWhiteSpace(preScript)
            };
        }

        /// <summary>
        /// Get database deletion command
        /// </summary>
        /// <param name="translator">Query translator</param>
        /// <param name="command">Command</param>
        /// <returns>Return a database deletion command</returns>
        DatabaseExecutionCommand GetDatabaseDeletionCommand(IQueryTranslator translator, DefaultCommand command)
        {
            translator.DataAccessContext.SetCommand(command);

            #region query translation

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

            string objectName = translator.DataAccessContext.GetCommandEntityObjectName(command);
            string cmdText = $"{preScript}DELETE {translator.ObjectPetName} FROM {SqlServerManager.WrapKeyword(objectName)} AS {translator.ObjectPetName}{joinScript} {conditionString};";

            #endregion

            #region parameter

            CommandParameters parameters = SqlServerManager.ConvertParameter(command.Parameters) ?? new CommandParameters();
            var queryParameters = SqlServerManager.ConvertParameter(tranResult.Parameters);
            parameters.Union(queryParameters);

            #endregion

            return new DatabaseExecutionCommand()
            {
                CommandText = cmdText,
                CommandType = SqlServerManager.GetCommandType(command),
                MustAffectedData = command.MustAffectedData,
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
        /// <returns>Return datas</returns>
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
        /// <returns>Return datas</returns>
        public async Task<IEnumerable<T>> QueryAsync<T>(DatabaseServer server, ICommand command)
        {
            if (command.Query == null)
            {
                throw new EZNEWException($"{nameof(ICommand.Query)} is null");
            }

            #region query translation

            IQueryTranslator translator = SqlServerManager.GetQueryTranslator(DataAccessContext.Create(server, command));
            var tranResult = translator.Translate(command.Query);
            string preScript = tranResult.PreScript;
            string joinScript = tranResult.AllowJoin ? tranResult.JoinScript : string.Empty;

            #endregion

            #region script

            string cmdText;
            switch (command.Query.ExecutionMode)
            {
                case QueryExecutionMode.Text:
                    cmdText = tranResult.ConditionString;
                    break;
                case QueryExecutionMode.QueryObject:
                default:
                    int size = command.Query.QuerySize;
                    string objectName = translator.DataAccessContext.GetCommandEntityObjectName(command);
                    string orderString = string.IsNullOrWhiteSpace(tranResult.SortString) ? string.Empty : $"ORDER BY {tranResult.SortString}";
                    var queryFields = SqlServerManager.GetQueryFields(command.Query, command.EntityType, true);
                    string outputFormatedField = string.Join(",", SqlServerManager.FormatQueryFields(translator.ObjectPetName, queryFields, true));
                    if (string.IsNullOrWhiteSpace(tranResult.CombineScript))
                    {
                        cmdText = $"{preScript}SELECT {(size > 0 ? $"TOP {size}" : string.Empty)} {outputFormatedField} FROM {SqlServerManager.WrapKeyword(objectName)} AS {translator.ObjectPetName} {joinScript} {(string.IsNullOrWhiteSpace(tranResult.ConditionString) ? string.Empty : $"WHERE {tranResult.ConditionString}")} {orderString}";
                    }
                    else
                    {
                        string innerFormatedField = string.Join(",", SqlServerManager.FormatQueryFields(translator.ObjectPetName, queryFields, false));
                        cmdText = $"{preScript}SELECT {(size > 0 ? $"TOP {size}" : string.Empty)} {outputFormatedField} FROM (SELECT {innerFormatedField} FROM {SqlServerManager.WrapKeyword(objectName)} AS {translator.ObjectPetName} {joinScript} {(string.IsNullOrWhiteSpace(tranResult.ConditionString) ? string.Empty : $"WHERE {tranResult.ConditionString}")} {tranResult.CombineScript}) AS {translator.ObjectPetName} {orderString}";
                    }
                    break;
            }

            #endregion

            #region parameter

            var parameters = SqlServerManager.ConvertCmdParameters(SqlServerManager.ConvertParameter(tranResult.Parameters));

            #endregion

            //Trace log
            SqlServerManager.LogScript(cmdText, tranResult.Parameters);

            using (var conn = SqlServerManager.GetConnection(server))
            {
                var tran = SqlServerManager.GetQueryTransaction(conn, command.Query);
                var cmdDefinition = new CommandDefinition(cmdText, parameters, transaction: tran, commandType: SqlServerManager.GetCommandType(command as DefaultCommand), cancellationToken: command.Query?.GetCancellationToken() ?? default);
                var data = await conn.QueryAsync<T>(cmdDefinition).ConfigureAwait(false);
                return data;
            }
        }

        /// <summary>
        /// Query paging data
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="server">Databse server</param>
        /// <param name="command">Command</param>
        /// <returns>Return paging data</returns>
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
        /// <returns>Return datas</returns>
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
        /// <returns>Return datas</returns>
        public async Task<IEnumerable<T>> QueryOffsetAsync<T>(DatabaseServer server, ICommand command, int offsetNum = 0, int size = int.MaxValue)
        {
            if (command.Query == null)
            {
                throw new EZNEWException($"{nameof(ICommand.Query)} is null");
            }

            #region query translation

            IQueryTranslator translator = SqlServerManager.GetQueryTranslator(DataAccessContext.Create(server, command));
            var tranResult = translator.Translate(command.Query);
            string joinScript = tranResult.AllowJoin ? tranResult.JoinScript : string.Empty;

            #endregion

            #region script

            string cmdText;
            switch (command.Query.ExecutionMode)
            {
                case QueryExecutionMode.Text:
                    cmdText = tranResult.ConditionString;
                    break;
                case QueryExecutionMode.QueryObject:
                default:
                    string objectName = translator.DataAccessContext.GetCommandEntityObjectName(command);
                    var defaultFieldName = SqlServerManager.GetDefaultFieldName(command.EntityType);
                    var queryFields = SqlServerManager.GetQueryFields(command.Query, command.EntityType, true);
                    var outputFormatedField = string.Join(",", SqlServerManager.FormatQueryFields(translator.ObjectPetName, queryFields, true));
                    if (string.IsNullOrWhiteSpace(tranResult.CombineScript))
                    {
                        cmdText = $"{tranResult.PreScript}SELECT COUNT({translator.ObjectPetName}.{SqlServerManager.WrapKeyword(defaultFieldName)}) OVER() AS {DataManager.PagingTotalCountFieldName},{outputFormatedField} FROM {SqlServerManager.WrapKeyword(objectName)} AS {translator.ObjectPetName} {joinScript} {(string.IsNullOrWhiteSpace(tranResult.ConditionString) ? string.Empty : $"WHERE {tranResult.ConditionString}")} ORDER BY {(string.IsNullOrWhiteSpace(tranResult.SortString) ? $"{translator.ObjectPetName}.{SqlServerManager.WrapKeyword(defaultFieldName)} DESC" : tranResult.SortString)} OFFSET {offsetNum} ROWS FETCH NEXT {size} ROWS ONLY";
                    }
                    else
                    {
                        var innerFormatedField = string.Join(",", SqlServerManager.FormatQueryFields(translator.ObjectPetName, queryFields, false));
                        cmdText = $"{tranResult.PreScript}SELECT COUNT({translator.ObjectPetName}.{SqlServerManager.WrapKeyword(defaultFieldName)}) OVER() AS {DataManager.PagingTotalCountFieldName},{outputFormatedField} FROM (SELECT {innerFormatedField} FROM {SqlServerManager.WrapKeyword(objectName)} AS {translator.ObjectPetName} {joinScript} {(string.IsNullOrWhiteSpace(tranResult.ConditionString) ? string.Empty : $"WHERE {tranResult.ConditionString}")} {tranResult.CombineScript}) AS {translator.ObjectPetName} ORDER BY {(string.IsNullOrWhiteSpace(tranResult.SortString) ? $"{translator.ObjectPetName}.{SqlServerManager.WrapKeyword(defaultFieldName)} DESC" : tranResult.SortString)} OFFSET {offsetNum} ROWS FETCH NEXT {size} ROWS ONLY";
                    }
                    break;
            }

            #endregion

            #region parameter

            var parameters = SqlServerManager.ConvertCmdParameters(SqlServerManager.ConvertParameter(tranResult.Parameters));

            #endregion

            //Trace log
            SqlServerManager.LogScript(cmdText, tranResult.Parameters);

            using (var conn = SqlServerManager.GetConnection(server))
            {
                var tran = SqlServerManager.GetQueryTransaction(conn, command.Query);
                var cmdDefinition = new CommandDefinition(cmdText, parameters, transaction: tran, commandType: SqlServerManager.GetCommandType(command as DefaultCommand), cancellationToken: command.Query?.GetCancellationToken() ?? default);
                return await conn.QueryAsync<T>(cmdDefinition).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Indecats whether exists data
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <returns>Return whether exists data</returns>
        public bool Exists(DatabaseServer server, ICommand command)
        {
            return ExistsAsync(server, command).Result;
        }

        /// <summary>
        /// Indecats whether exists data
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <returns>Return whether exists data</returns>
        public async Task<bool> ExistsAsync(DatabaseServer server, ICommand command)
        {
            #region query translation

            var translator = SqlServerManager.GetQueryTranslator(DataAccessContext.Create(server, command));
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

            string objectName = translator.DataAccessContext.GetCommandEntityObjectName(command);
            string formatedField = string.Join(",", SqlServerManager.FormatQueryFields(translator.ObjectPetName, command.Query, command.EntityType, true, false));
            string cmdText = $"{preScript}SELECT 1 WHERE EXISTS(SELECT {formatedField} FROM {SqlServerManager.WrapKeyword(objectName)} AS {translator.ObjectPetName} {joinScript} {conditionString} {tranResult.CombineScript})";

            #endregion

            #region parameter

            var parameters = SqlServerManager.ConvertCmdParameters(SqlServerManager.ConvertParameter(tranResult.Parameters));

            #endregion

            //Trace log
            SqlServerManager.LogScript(cmdText, tranResult.Parameters);

            using (var conn = SqlServerManager.GetConnection(server))
            {
                var tran = SqlServerManager.GetQueryTransaction(conn, command.Query);
                var cmdDefinition = new CommandDefinition(cmdText, parameters, transaction: tran, cancellationToken: command.Query?.GetCancellationToken() ?? default);
                int value = await conn.ExecuteScalarAsync<int>(cmdDefinition).ConfigureAwait(false);
                return value > 0;
            }
        }

        /// <summary>
        /// Query aggregation value
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <returns>Return aggregation value</returns>
        public T AggregateValue<T>(DatabaseServer server, ICommand command)
        {
            return AggregateValueAsync<T>(server, command).Result;
        }

        /// <summary>
        /// Query aggregation value
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <returns>Return aggregation value</returns>
        public async Task<T> AggregateValueAsync<T>(DatabaseServer server, ICommand command)
        {
            if (command.Query == null)
            {
                throw new EZNEWException($"{nameof(ICommand.Query)} is null");
            }

            #region query translation

            bool queryObject = command.Query.ExecutionMode == QueryExecutionMode.QueryObject;
            string funcName = SqlServerManager.GetAggregationFunctionName(command.OperationType);
            EntityField defaultField = null;
            if (queryObject)
            {
                if (string.IsNullOrWhiteSpace(funcName))
                {
                    throw new NotSupportedException($"Not support {command.OperationType}");
                }
                if (SqlServerManager.CheckAggregationOperationMustNeedField(command.OperationType))
                {
                    if (queryObject && (command.Query?.QueryFields.IsNullOrEmpty() ?? true))
                    {
                        throw new EZNEWException($"You must specify the field to perform for the {funcName} operation");
                    }
                    else
                    {
                        defaultField = DataManager.GetField(CurrentDatabaseServerType, command.EntityType, command.Query.QueryFields.First());
                    }
                }
                else
                {
                    defaultField = DataManager.GetDefaultField(CurrentDatabaseServerType, command.EntityType);
                }

                //combine fields
                if (!command.Query.Combines.IsNullOrEmpty())
                {
                    var combineKeys = EntityManager.GetPrimaryKeys(command.EntityType).Union(new string[1] { defaultField.PropertyName }).ToArray();
                    command.Query.ClearQueryFields();
                    foreach (var combineEntry in command.Query.Combines)
                    {
                        combineEntry.Query.ClearQueryFields();
                        if (combineKeys.IsNullOrEmpty())
                        {
                            combineEntry.Query.ClearNotQueryFields();
                            command.Query.ClearNotQueryFields();
                        }
                        else
                        {
                            combineEntry.Query.AddQueryFields(combineKeys);
                            command.Query.AddQueryFields(combineKeys);
                        }
                    }
                }
            }
            IQueryTranslator translator = SqlServerManager.GetQueryTranslator(DataAccessContext.Create(server, command));
            var tranResult = translator.Translate(command.Query);

            #endregion

            #region script

            string cmdText;
            string joinScript = tranResult.AllowJoin ? tranResult.JoinScript : string.Empty;
            switch (command.Query.ExecutionMode)
            {
                case QueryExecutionMode.Text:
                    cmdText = tranResult.ConditionString;
                    break;
                case QueryExecutionMode.QueryObject:
                default:
                    string objectName = translator.DataAccessContext.GetCommandEntityObjectName(command);
                    string formatedDefaultField = SqlServerManager.FormatField(translator.ObjectPetName, defaultField, false);
                    cmdText = string.IsNullOrWhiteSpace(tranResult.CombineScript)
                        ? $"{tranResult.PreScript}SELECT {funcName}({formatedDefaultField}) FROM {SqlServerManager.WrapKeyword(objectName)} AS {translator.ObjectPetName} {joinScript} {(string.IsNullOrWhiteSpace(tranResult.ConditionString) ? string.Empty : $"WHERE {tranResult.ConditionString}")}"
                        : $"{tranResult.PreScript}SELECT {funcName}({formatedDefaultField}) FROM (SELECT {string.Join(",", SqlServerManager.FormatQueryFields(translator.ObjectPetName, command.Query, command.EntityType, true, false))} FROM {SqlServerManager.WrapKeyword(objectName)} AS {translator.ObjectPetName} {joinScript} {(string.IsNullOrWhiteSpace(tranResult.ConditionString) ? string.Empty : $"WHERE {tranResult.ConditionString}")} {tranResult.CombineScript}) AS {translator.ObjectPetName}";
                    break;
            }

            #endregion

            #region parameter

            var parameters = SqlServerManager.ConvertCmdParameters(SqlServerManager.ConvertParameter(tranResult.Parameters));

            #endregion

            //Trace log
            SqlServerManager.LogScript(cmdText, tranResult.Parameters);

            using (var conn = SqlServerManager.GetConnection(server))
            {
                var tran = SqlServerManager.GetQueryTransaction(conn, command.Query);
                var cmdDefinition = new CommandDefinition(cmdText, parameters, transaction: tran, commandType: SqlServerManager.GetCommandType(command as DefaultCommand), cancellationToken: command.Query?.GetCancellationToken() ?? default);
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
            SqlServerManager.LogScript(command.Text, command.Parameters);
            using (var conn = SqlServerManager.GetConnection(server))
            {
                var tran = SqlServerManager.GetQueryTransaction(conn, command.Query);
                DynamicParameters parameters = SqlServerManager.ConvertCmdParameters(SqlServerManager.ConvertParameter(command.Parameters));
                var cmdDefinition = new CommandDefinition(command.Text, parameters, transaction: tran, commandType: SqlServerManager.GetCommandType(command as DefaultCommand), cancellationToken: command.Query?.GetCancellationToken() ?? default);
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
        public void BulkInsert(DatabaseServer server, DataTable dataTable, IBulkInsertionOptions bulkInsertOptions = null)
        {
            BulkInsertAsync(server, dataTable).Wait();
        }

        /// <summary>
        /// Bulk insert datas
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="dataTable">Data table</param>
        /// <param name="bulkInsertOptions">Insert options</param>
        public async Task BulkInsertAsync(DatabaseServer server, DataTable dataTable, IBulkInsertionOptions bulkInsertOptions = null)
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
