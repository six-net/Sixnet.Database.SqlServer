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
            var verifyResults = VerifyCommand(cmds);
            if (verifyResults.Item2)
            {
                return await ExecuteByTransactionAsync(server, verifyResults.Item1).ConfigureAwait(false);
            }
            return await ExecuteByNoneTransactionAsync(server, verifyResults.Item1).ConfigureAwait(false);
        }

        /// <summary>
        /// execute by nonee transaction
        /// </summary>
        /// <param name="serverInfo">database server</param>
        /// <param name="cmds">command</param>
        /// <returns></returns>
        async Task<int> ExecuteByNoneTransactionAsync(ServerInfo serverInfo, IEnumerable<RdbCommand> cmds)
        {
            IQueryTranslator queryTranslator = QueryTranslator.GetTranslator(serverInfo);
            using (var conn = DbServerFactory.GetConnection(serverInfo))
            {
                int executeRows = 0;
                foreach (var cmd in cmds)
                {
                    executeRows += await ExecuteSingleCommandAsync(queryTranslator, conn, cmd).ConfigureAwait(false);
                }
                return executeRows;
            }
        }

        /// <summary>
        /// execute by transaction
        /// </summary>
        /// <param name="serverInfo">database server</param>
        /// <param name="cmds">commands</param>
        /// <returns></returns>
        async Task<int> ExecuteByTransactionAsync(ServerInfo serverInfo, IEnumerable<RdbCommand> cmds)
        {
            IQueryTranslator queryTranslator = QueryTranslator.GetTranslator(serverInfo);
            using (var conn = DbServerFactory.GetConnection(serverInfo))
            {
                conn.Open();
                var tran = conn.BeginTransaction();
                bool result = true;
                int resultValue = 0;
                try
                {
                    foreach (var cmd in cmds)
                    {
                        int cmdResult = await ExecuteSingleCommandAsync(queryTranslator, conn, cmd, tran).ConfigureAwait(false);
                        resultValue += cmdResult;
                        if (cmd.VerifyResult != null)
                        {
                            result = result && cmd.VerifyResult(cmdResult);
                        }
                    }
                    if (result)
                    {
                        tran.Commit();
                    }
                    else
                    {
                        tran.Rollback();
                        resultValue = 0;
                    }
                    return resultValue;
                }
                catch (Exception ex)
                {
                    tran.Rollback();
                    throw ex;
                }
            }
        }

        /// <summary>
        /// execute single command
        /// </summary>
        /// <param name="conn">db connection</param>
        /// <param name="cmd">command</param>
        /// <param name="tran">transaction</param>
        /// <returns></returns>
        async Task<int> ExecuteSingleCommandAsync(IQueryTranslator queryTranslator, IDbConnection conn, RdbCommand cmd, IDbTransaction tran = null)
        {
            int resultValue = 0;

            #region query object translate

            IDictionary<string, object> queryParameters = null;
            var tranResult = queryTranslator.Translate(cmd.Query);
            string conditionString = string.Empty;
            if (!tranResult.ConditionString.IsNullOrEmpty())
            {
                conditionString += "WHERE " + tranResult.ConditionString;
            }
            if (tranResult.Parameters != null)
            {
                if (tranResult.Parameters is IDictionary<string, object>)
                {
                    queryParameters = tranResult.Parameters as IDictionary<string, object>;
                }
                else
                {
                    queryParameters = tranResult.Parameters.ObjectToDcitionary();
                }
            }
            string preScript = tranResult.PreScript;

            #endregion

            #region execute

            StringBuilder cmdText = new StringBuilder();
            object newParameters = cmd.Parameters;
            ExecuteCommandResult executeCmdResult = cmd.CommandResultType;
            if (cmd.ExecuteMode == CommandExecuteMode.CommandText)
            {
                cmdText.Append(cmd.CommandText);
            }
            else
            {
                string objectName = DataManager.GetEntityObjectName(ServerType.SQLServer, cmd.EntityType, cmd.ObjectName);
                var fields = GetFields(cmd.EntityType, cmd.Fields);
                switch (cmd.Operate)
                {
                    case OperateType.Insert:
                        cmdText.AppendFormat("INSERT INTO [{0}] ({1}) VALUES ({2})", objectName, string.Join(",", FormatEditFields(fields)), string.Join(",", fields.Select(c => "@" + c)));
                        break;
                    case OperateType.Update:
                        IDictionary<string, dynamic> oldCmdParameterDict = null;
                        IDictionary<string, dynamic> newCmdParameterDict = new Dictionary<string, dynamic>();
                        if (cmd.Parameters is IDictionary<string, dynamic>)
                        {
                            oldCmdParameterDict = cmd.Parameters as IDictionary<string, dynamic>;
                        }
                        else
                        {
                            oldCmdParameterDict = cmd.Parameters.ObjectToDcitionary();
                        }
                        List<string> updateSetArray = new List<string>();
                        foreach (var field in fields)
                        {
                            if (oldCmdParameterDict == null || !oldCmdParameterDict.ContainsKey(field) || !(oldCmdParameterDict[field] is CalculateModifyValue))
                            {
                                updateSetArray.Add(string.Format("{0}.[{1}]=@{2}", queryTranslator.ObjectPetName, field.FieldName, field));
                                if (oldCmdParameterDict != null && oldCmdParameterDict.ContainsKey(field))
                                {
                                    var fieldParameter = oldCmdParameterDict[field];
                                    dynamic parameterValue = fieldParameter;
                                    if (fieldParameter is IModifyValue)
                                    {
                                        parameterValue = (fieldParameter as IModifyValue).Value;
                                    }
                                    newCmdParameterDict.Add(field, parameterValue);
                                }
                            }
                            else if (oldCmdParameterDict[field] is CalculateModifyValue)
                            {
                                CalculateModifyValue calModify = oldCmdParameterDict[field] as CalculateModifyValue;
                                newCmdParameterDict.Add(field, calModify.Value);
                                string calChar = GetCalculateChar(calModify.Operator);
                                updateSetArray.Add(string.Format("{0}.[{1}]={0}.[{1}]{2}@{3}", queryTranslator.ObjectPetName, field.FieldName, calChar, field));
                            }
                        }
                        newParameters = newCmdParameterDict;
                        cmdText.AppendFormat("{4}UPDATE {0} SET {1} FROM [{2}] AS {0} {3}", queryTranslator.ObjectPetName, string.Join(",", updateSetArray.ToArray()), objectName, conditionString, preScript);
                        break;
                    case OperateType.Delete:
                        cmdText.AppendFormat("{3}DELETE {0} FROM [{1}] AS {0} {2}", queryTranslator.ObjectPetName, objectName, conditionString, preScript);
                        break;
                    case OperateType.Exist:
                        var defaultField = GetDefaultField(cmd.EntityType, fields);
                        cmdText.AppendFormat("IF EXISTS(SELECT {0}.[{1}] FROM [{2}] AS {0} {3}) SELECT 1 ELSE SELECT 0", queryTranslator.ObjectPetName, defaultField.FieldName, objectName, conditionString);
                        executeCmdResult = ExecuteCommandResult.ExecuteScalar;
                        break;
                    default:
                        break;
                }
            }
            if (cmdText.Length <= 0)
            {
                return 0;
            }

            #endregion

            #region data operation

            object parameters = queryParameters;
            if (newParameters != null)
            {
                CmdParameters cmdParameters = newParameters as CmdParameters;
                if (cmdParameters != null)
                {
                    DynamicParameters dynamicParameters = new DynamicParameters();
                    foreach (var item in cmdParameters.Parameters)
                    {
                        dynamicParameters.Add(item.Key, item.Value.Value, item.Value.DbType, item.Value.ParameterDirection, item.Value.Size, item.Value.Precision, item.Value.Scale);
                    }
                    if (queryParameters != null)
                    {
                        foreach (var parameter in queryParameters)
                        {
                            dynamicParameters.Add(parameter.Key, parameter.Value);
                        }
                    }
                    parameters = dynamicParameters;
                }
                else if (newParameters is IDictionary<string, dynamic>)
                {
                    IDictionary<string, dynamic> cmdParametersDic = newParameters as IDictionary<string, dynamic>;
                    if (queryParameters != null)
                    {
                        foreach (var queryParaItem in queryParameters)
                        {
                            cmdParametersDic.Add(queryParaItem.Key, queryParaItem.Value);
                        }
                    }
                    parameters = cmdParametersDic;
                }
                else
                {
                    IDictionary<string, object> cmdParametersDic = newParameters.ObjectToDcitionary();
                    if (queryParameters != null)
                    {
                        foreach (var queryParaItem in queryParameters)
                        {
                            cmdParametersDic.Add(queryParaItem.Key, queryParaItem.Value);
                        }
                    }
                    parameters = cmdParametersDic;
                }
            }

            #endregion

            switch (executeCmdResult)
            {
                case ExecuteCommandResult.ExecuteScalar:
                    resultValue = await conn.ExecuteScalarAsync<int>(cmdText.ToString(), parameters, transaction: tran, commandType: GetCommandType(cmd)).ConfigureAwait(false);
                    break;
                case ExecuteCommandResult.ExecuteRows:
                    resultValue = await conn.ExecuteAsync(cmdText.ToString(), parameters, transaction: tran, commandType: GetCommandType(cmd)).ConfigureAwait(false);
                    break;
            }
            return resultValue;
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
            var verifyResults = VerifyCommand(cmd);
            cmd.Query = cmd.Query ?? QueryFactory.Create();

            #region query object translate

            IQueryTranslator queryTranslator = QueryTranslator.GetTranslator(server);
            var tranResult = queryTranslator.Translate(cmd.Query);
            string preScript = tranResult.PreScript;

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
                    //string fieldSplitChar = string.Format(",{0}.", queryTranslator.ObjectPetName);
                    string objectName = DataManager.GetEntityObjectName(ServerType.SQLServer, cmd.EntityType, cmd.ObjectName);
                    var fields = GetFields(cmd.EntityType, cmd.Fields);
                    cmdText.AppendFormat("{4}SELECT {0} {1} FROM [{2}] AS {3}", size > 0 ? "TOP " + size : "", string.Join(",", FormatQueryFields(queryTranslator.ObjectPetName, fields)), objectName, queryTranslator.ObjectPetName, preScript);
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
                return await conn.QueryAsync<T>(cmdText.ToString(), tranResult.Parameters, commandType: GetCommandType(verifyResults.Item1.First())).ConfigureAwait(false);
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
            var verifyResults = VerifyCommand(cmd);
            cmd.Query = cmd.Query ?? QueryFactory.Create();

            #region query object translate

            IQueryTranslator queryTranslator = QueryTranslator.GetTranslator(server);
            var tranResult = queryTranslator.Translate(cmd.Query);

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
                    //string fieldSplitChar = string.Format(",{0}.", queryTranslator.ObjectPetName);
                    string objectName = DataManager.GetEntityObjectName(ServerType.SQLServer, cmd.EntityType, cmd.ObjectName);
                    var fields = GetFields(cmd.EntityType, cmd.Fields);
                    var defaultField = GetDefaultField(cmd.EntityType, fields);
                    cmdText.AppendFormat("{4}SELECT COUNT({3}.[{0}]) OVER() AS PagingTotalCount,{1} FROM [{2}] AS {3}", defaultField.FieldName, string.Join(",", FormatQueryFields(queryTranslator.ObjectPetName, fields)), objectName, queryTranslator.ObjectPetName, tranResult.PreScript);
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
                        cmdText.AppendFormat(" ORDER BY {0}.[{1}] DESC", queryTranslator.ObjectPetName, defaultField.FieldName);
                    }
                    cmdText.AppendFormat(" OFFSET {0} ROWS FETCH NEXT {1} ROWS ONLY", offsetNum, size);
                    break;
            }

            #endregion

            using (var conn = DbServerFactory.GetConnection(server))
            {
                return await conn.QueryAsync<T>(cmdText.ToString(), tranResult.Parameters, commandType: GetCommandType(verifyResults.Item1.First())).ConfigureAwait(false);
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
            int value = await ExecuteAsync(server, cmd).ConfigureAwait(false);
            return value > 0;
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
            var verifyResults = VerifyCommand(cmd);
            cmd.Query = cmd.Query ?? QueryFactory.Create();

            #region query object translate

            IQueryTranslator queryTranslator = QueryTranslator.GetTranslator(server);
            var tranResult = queryTranslator.Translate(cmd.Query);

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
                    string funcName = GetAggregateFunctionName(cmd.Operate);
                    if (funcName.IsNullOrEmpty() || cmd.Fields.IsNullOrEmpty())
                    {
                        return default(T);
                    }
                    string objectName = DataManager.GetEntityObjectName(ServerType.SQLServer, cmd.EntityType, cmd.ObjectName);
                    var fields = DataManager.GetFields(ServerType.SQLServer, cmd.EntityType, cmd.Fields);
                    cmdText.AppendFormat("{4}SELECT {0}({3}.[{1}]) FROM [{2}] AS {3}", funcName, fields[0].FieldName, objectName, queryTranslator.ObjectPetName, tranResult.PreScript);
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
                return await conn.ExecuteScalarAsync<T>(cmdText.ToString(), tranResult.Parameters, commandType: GetCommandType(verifyResults.Item1.First())).ConfigureAwait(false);
            }
        }

        #endregion

        #region Helpers

        /// <summary>
        /// verify command
        /// </summary>
        /// <param name="cmds"></param>
        Tuple<IEnumerable<RdbCommand>, bool> VerifyCommand(params ICommand[] cmds)
        {
            if (cmds == null || !cmds.Any())
            {
                throw new Exception("At Least Special One Execute Command");
            }
            bool transaction = false;
            List<RdbCommand> rdbCommandList = new List<RdbCommand>();
            foreach (var cmd in cmds)
            {
                if (cmd == null)
                {
                    throw new Exception("Null Command");
                }
                RdbCommand rdbCmd = cmd as RdbCommand;
                if (rdbCmd == null)
                {
                    throw new Exception("Command Is Not RDB Command");
                }
                transaction = transaction && rdbCmd.TransactionCommand;
                rdbCommandList.Add(rdbCmd);
            }
            transaction = transaction || rdbCommandList.Count > 1;
            return new Tuple<IEnumerable<RdbCommand>, bool>(rdbCommandList, transaction);
        }

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

        List<string> FormatEditFields(IEnumerable<EntityField> fields)
        {
            if (fields.IsNullOrEmpty())
            {
                return new List<string>(0);
            }
            List<string> formatFields = new List<string>();
            string key = ((int)ServerType.SQLServer).ToString();
            foreach (var field in fields)
            {
                var formatValue = field.GetEditFormat(key);
                if (formatValue.IsNullOrEmpty())
                {
                    formatValue = string.Format("[{0}]", field.FieldName);
                    field.SetEditFormat(key, formatValue);
                }
                formatFields.Add(formatValue);
            }
            return formatFields;
        }

        /// <summary>
        /// format fields
        /// </summary>
        /// <param name="fields">fields</param>
        /// <returns></returns>
        List<string> FormatQueryFields(string dbObjectName, IEnumerable<EntityField> fields)
        {
            if (fields.IsNullOrEmpty())
            {
                return new List<string>(0);
            }
            List<string> formatFields = new List<string>();
            string key = ((int)ServerType.SQLServer).ToString();
            foreach (var field in fields)
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
        /// get default field
        /// </summary>
        /// <param name="fields"></param>
        /// <returns></returns>
        EntityField GetDefaultField(Type entityType, IEnumerable<EntityField> fields)
        {
            if (fields.IsNullOrEmpty())
            {
                return string.Empty;
            }
            if (entityType == null)
            {
                return fields.ElementAt(0);
            }
            var primaryKeys = EntityManager.GetPrimaryKeys(entityType);
            if (primaryKeys.IsNullOrEmpty())
            {
                return fields.ElementAt(0);
            }
            foreach (var pk in primaryKeys)
            {
                var nowField = fields.FirstOrDefault(c => c == pk);
                if (nowField != null)
                {
                    return nowField;
                }
            }
            return fields.ElementAt(0);
        }

        #endregion
    }
}
