using System;
using System.Collections.Generic;
using System.Text;
using Sixnet.Development.Data.Command;
using Sixnet.Development.Data.Database;
using Sixnet.Development.Data.Field;
using Sixnet.Development.Data;
using Sixnet.Development.Entity;
using Sixnet.Development.Queryable;
using Sixnet.Exceptions;
using System.Threading.Tasks;
using System.Linq;

namespace Sixnet.Database.SqlServer
{
    internal partial class SqlServerDataCommandResolver
    {
        #region Get query statement

        /// <summary>
        /// Get query statement
        /// </summary>
        /// <param name="context">Command resolve context</param>
        /// <param name="translationResult">Queryable translation result</param>
        /// <param name="location">Queryable location</param>
        /// <returns></returns>
        protected override async Task<QueryDatabaseStatement> GenerateQueryStatementCoreAsync(DataCommandResolveContext context, QueryableTranslationResult translationResult, QueryableLocation location)
        {
            var queryable = translationResult.GetOriginalQueryable();
            string sqlStatement;
            IEnumerable<ISixnetField> outputFields = null;
            switch (queryable.ExecutionMode)
            {
                case QueryableExecutionMode.Script:
                    sqlStatement = translationResult.GetCondition();
                    break;
                case QueryableExecutionMode.Regular:
                default:
                    // table pet name
                    var tablePetName = context.GetTablePetName(queryable, queryable.GetModelType());
                    //combine
                    var combine = translationResult.GetCombine();
                    var hasCombine = !string.IsNullOrWhiteSpace(combine);
                    //having
                    var having = translationResult.GetHavingCondition();
                    //group
                    var group = translationResult.GetGroup();
                    //pre script output
                    var targetScript = translationResult.GetPreOutputStatement();

                    if (string.IsNullOrWhiteSpace(targetScript))
                    {
                        //target
                        var targetStatement = await GetFromTargetStatementAsync(context, queryable, location, tablePetName).ConfigureAwait(false);
                        outputFields = targetStatement.OutputFields;
                        //condition
                        var condition = translationResult.GetCondition(ConditionStartKeyword);
                        //join
                        var join = translationResult.GetJoin();
                        //target statement
                        targetScript = $"{targetStatement.Script}{join}{condition}{group}{having}";
                    }
                    else
                    {
                        targetScript = $"{targetScript}{group}{having}";
                        outputFields = translationResult.GetPreOutputFields();
                    }

                    // output fields
                    if (outputFields.IsNullOrEmpty() || !queryable.SelectedFields.IsNullOrEmpty())
                    {
                        outputFields = SixnetDataManager.GetQueryableFields(DatabaseType, queryable.GetModelType(), queryable, context.IsRootQueryable(queryable));
                    }
                    var outputFieldString = await FormatFieldsStringAsync(context, queryable, location, FieldLocation.Output, outputFields).ConfigureAwait(false);

                    //sort
                    var hasOffset = queryable.SkipCount > 0;
                    var hasTakeNum = queryable.TakeCount > 0;
                    var sort = translationResult.GetSort();
                    var hasSort = !string.IsNullOrWhiteSpace(sort);
                    if (hasTakeNum && hasOffset && !hasSort)
                    {
                        sort = await GetDefaultSortAsync(context, translationResult, queryable, outputFields, tablePetName).ConfigureAwait(false);
                        hasSort = !string.IsNullOrWhiteSpace(sort);
                    }

                    //limit
                    var limit = GetLimitString(queryable.SkipCount, queryable.TakeCount, hasSort);
                    var hasLimit = !string.IsNullOrWhiteSpace(limit);
                    var useTop = hasLimit && limit.Contains("TOP");

                    //statement
                    sqlStatement = $"SELECT {GetDistinctString(queryable)} {(useTop ? limit : "")}{outputFieldString} FROM {targetScript}{sort}{(!useTop && hasLimit ? limit : "")}";
                    //pre script
                    var preScript = GetPreScript(context, location);
                    switch (queryable.OutputType)
                    {
                        case QueryableOutputType.Count:
                            sqlStatement = hasCombine
                                ? $"{preScript}SELECT COUNT(1) FROM (({sqlStatement}){combine}){TablePetNameKeyword}{tablePetName}"
                                : $"{preScript}SELECT COUNT(1) FROM ({sqlStatement}){TablePetNameKeyword}{tablePetName}";
                            break;
                        case QueryableOutputType.Predicate:
                            sqlStatement = hasCombine
                                ? $"{preScript}SELECT 1 WHERE EXISTS(({sqlStatement}){combine})"
                                : $"{preScript}SELECT 1 WHERE EXISTS({sqlStatement})";
                            break;
                        default:
                            sqlStatement = hasCombine
                            ? $"{preScript}({sqlStatement}){combine}"
                            : $"{preScript}{sqlStatement}";
                            break;
                    }
                    break;
            }

            //parameters
            var parameters = context.GetParameters();

            //log script
            if (location == QueryableLocation.Top)
            {
                LogScript(sqlStatement, parameters);
            }
            return QueryDatabaseStatement.Create(sqlStatement, parameters, outputFields);
        }

        #endregion

        #region Get insert statement

        /// <summary>
        /// Get insert statement
        /// </summary>
        /// <param name="context">Command resolve context</param>
        /// <returns></returns>
        protected override async Task<List<ExecutionDatabaseStatement>> GenerateInsertStatementsAsync(DataCommandResolveContext context)
        {
            var command = context.DataCommandExecutionContext.Command;
            var dataCommandExecutionContext = context.DataCommandExecutionContext;
            var entityType = dataCommandExecutionContext.Command.GetEntityType();
            var fields = SixnetDataManager.GetInsertableFields(DatabaseType, entityType);
            var fieldCount = fields.GetCount();
            var insertFields = new List<string>(fieldCount);
            var insertValues = new List<string>(fieldCount);
            DataField autoIncrementField = null;
            DataField splitField = null;
            dynamic splitValue = default;

            foreach (var field in fields)
            {
                if (field.InRole(FieldRole.Increment))
                {
                    autoIncrementField ??= field;
                    if (!autoIncrementField.InRole(FieldRole.PrimaryKey) && field.InRole(FieldRole.PrimaryKey)) // get first primary key field
                    {
                        autoIncrementField = field;
                    }
                    if (!SixnetDataManager.AllowInsertIncrementField(context.DataCommandExecutionContext.Command?.Options))
                    {
                        continue;
                    }
                }
                // fields
                insertFields.Add(WrapKeywordFunc(field.GetFieldName(DatabaseType)));
                // values
                var insertValue = command.FieldsAssignment.GetNewValue(field.PropertyName);
                insertValues.Add(await FormatInsertValueFieldAsync(context, command.Queryable, insertValue).ConfigureAwait(false));

                // split value
                if (field.InRole(FieldRole.SplitValue))
                {
                    splitValue = insertValue;
                    splitField = field;
                }
            }

            SixnetDirectThrower.ThrowNotSupportIf(autoIncrementField != null && splitField != null, $"Not support auto increment field for split table:{entityType.Name}");

            if (splitField != null)
            {
                dataCommandExecutionContext.SetSplitValues(new List<dynamic>(1) { splitValue });
            }
            var tableNames = await dataCommandExecutionContext.GetTableNamesAsync().ConfigureAwait(false);

            SixnetDirectThrower.ThrowInvalidOperationIf(tableNames.IsNullOrEmpty(), $"Get table name failed for {entityType.Name}");
            SixnetDirectThrower.ThrowInvalidOperationIf(tableNames.Count > 1 && autoIncrementField != null, $"Not support auto increment field for multiple tables");

            var statementBuilder = new StringBuilder();
            var incrScripts = new List<string>();
            var scriptTemplate = $"INSERT INTO {{0}} ({string.Join(",", insertFields)}) VALUES ({string.Join(",", insertValues)});";
            foreach (var tableName in tableNames)
            {
                statementBuilder.AppendLine(string.Format(scriptTemplate, WrapKeywordFunc(tableName)));
            }
            if (autoIncrementField != null)
            {
                var incrField = $"{command.Id}";
                var incrParameter = FormatParameterName(incrField);
                statementBuilder.AppendLine($"DECLARE {incrParameter} BIGINT;SET {incrParameter} = SCOPE_IDENTITY();");
                incrScripts.Add($"{incrParameter} {ColumnPetNameKeyword} {incrField}");
            }
            return new List<ExecutionDatabaseStatement>()
            {
                new ExecutionDatabaseStatement()
                {
                    Script = statementBuilder.ToString(),
                    ScriptType = GetCommandType(command),
                    MustAffectData = command.Options?.MustAffectData ?? false,
                    Parameters = context.GetParameters(),
                    IncrScript = string.Join(",", incrScripts)
                }
            };
        }

        #endregion

        #region Get update statement

        /// <summary>
        /// Get update statement
        /// </summary>
        /// <param name="context">Command resolve context</param>
        /// <returns></returns>
        protected override async Task<List<ExecutionDatabaseStatement>> GenerateUpdateStatementsAsync(DataCommandResolveContext context)
        {
            var command = context.DataCommandExecutionContext.Command;
            SixnetException.ThrowIf(command?.FieldsAssignment?.NewValues.IsNullOrEmpty() ?? true, "No set update field");

            #region translate

            var translationResult = await TranslateAsync(context).ConfigureAwait(false);
            var preScripts = context.GetPreScripts();

            #endregion

            #region script 

            var dataCommandExecutionContext = context.DataCommandExecutionContext;
            var entityType = dataCommandExecutionContext.Command.GetEntityType();

            var tableNames = await dataCommandExecutionContext.GetTableNamesAsync(command).ConfigureAwait(false);
            SixnetDirectThrower.ThrowInvalidOperationIf(tableNames.IsNullOrEmpty(), $"Get table name failed for {entityType.Name}");

            var tablePetName = command.Queryable == null ? context.GetNewTablePetName() : context.GetDefaultTablePetName(command.Queryable);
            var newValues = command.FieldsAssignment.NewValues;
            var updateSetArray = new List<string>();
            foreach (var newValueItem in newValues)
            {
                var newValue = newValueItem.Value;
                var propertyName = newValueItem.Key;
                var updateField = SixnetDataManager.GetField(dataCommandExecutionContext.Server.DatabaseType, command.GetEntityType(), DataField.Create(propertyName)) as DataField;

                SixnetDirectThrower.ThrowSixnetExceptionIf(updateField == null, $"Not found field:{propertyName}");

                var fieldFormattedName = WrapKeywordFunc(updateField.GetFieldName(DatabaseType));
                var newValueExpression = await FormatUpdateValueFieldAsync(context, command, newValue).ConfigureAwait(false);
                updateSetArray.Add($"{tablePetName}.{fieldFormattedName}={newValueExpression}");
            }

            // parameters
            var parameters = ConvertParameter(command.ScriptParameters) ?? new DataCommandParameters();
            parameters.Union(context.GetParameters());

            // statement
            var scriptType = GetCommandType(command);
            string scriptTemplate;
            if (preScripts.IsNullOrEmpty())
            {
                var condition = translationResult?.GetCondition(ConditionStartKeyword);
                var join = translationResult?.GetJoin();
                scriptTemplate = $"UPDATE {tablePetName} SET {string.Join(",", updateSetArray)} FROM {{0}}{TablePetNameKeyword}{tablePetName}{join}{condition};";
                var statementBuilder = new StringBuilder();
                foreach (var tableName in tableNames)
                {
                    statementBuilder.AppendLine(string.Format(scriptTemplate, WrapKeywordFunc(tableName)));
                }
                return new List<ExecutionDatabaseStatement>(1)
                {
                    new ExecutionDatabaseStatement()
                    {
                        Script = statementBuilder.ToString(),
                        ScriptType = scriptType,
                        MustAffectData = command.Options?.MustAffectData ?? false,
                        Parameters = parameters,
                        HasPreScript = false
                    }
                };
            }
            else
            {
                var queryStatement = await GenerateQueryStatementCoreAsync(context, translationResult, QueryableLocation.JoinTarget).ConfigureAwait(false);
                var updateTablePetName = "UTB";
                var joinItems = FormatWrapJoinPrimaryKeys(context, command.Queryable, command.GetEntityType(), tablePetName, tablePetName, updateTablePetName);
                scriptTemplate = $"{FormatPreScript(context)}UPDATE {tablePetName} SET {string.Join(",", updateSetArray)} FROM {{0}}{TablePetNameKeyword}{tablePetName} INNER JOIN ({queryStatement.Script}){TablePetNameKeyword}{updateTablePetName} ON {string.Join(" AND ", joinItems)};";
                var statements = new List<ExecutionDatabaseStatement>(tableNames.Count);
                foreach (var tableName in tableNames)
                {
                    statements.Add(new ExecutionDatabaseStatement()
                    {
                        Script = string.Format(scriptTemplate, WrapKeywordFunc(tableName)),
                        ScriptType = scriptType,
                        MustAffectData = command.Options?.MustAffectData ?? false,
                        Parameters = parameters,
                        HasPreScript = true
                    });
                }
                return statements;
            }


            #endregion
        }

        #endregion

        #region Get delete statement

        /// <summary>
        /// Get delete statement
        /// </summary>
        /// <param name="context">Command resolve context</param>
        /// <returns></returns>
        protected override async Task<List<ExecutionDatabaseStatement>> GenerateDeleteStatementsAsync(DataCommandResolveContext context)
        {
            var dataCommandExecutionContext = context.DataCommandExecutionContext;
            var command = dataCommandExecutionContext.Command;

            #region translate

            var translationResult = await TranslateAsync(context).ConfigureAwait(false);
            var preScripts = context.GetPreScripts();

            #endregion

            #region script

            var tablePetName = command.Queryable == null ? context.GetNewTablePetName() : context.GetDefaultTablePetName(command.Queryable);
            var entityType = dataCommandExecutionContext.Command.GetEntityType();
            var tableNames = await dataCommandExecutionContext.GetTableNamesAsync(command).ConfigureAwait(false);
            SixnetDirectThrower.ThrowInvalidOperationIf(tableNames.IsNullOrEmpty(), $"Get table name failed for {entityType.Name}");

            // parameters
            var parameters = ConvertParameter(command.ScriptParameters) ?? new DataCommandParameters();
            parameters.Union(context.GetParameters());

            // statement
            var scriptType = GetCommandType(command);
            string scriptTemplate;
            if (preScripts.IsNullOrEmpty())
            {
                var condition = translationResult?.GetCondition(ConditionStartKeyword);
                var join = translationResult?.GetJoin();
                scriptTemplate = $"DELETE {tablePetName} FROM {{0}}{TablePetNameKeyword}{tablePetName}{join}{condition};";
                var statementBuilder = new StringBuilder();
                foreach (var tableName in tableNames)
                {
                    statementBuilder.AppendLine(string.Format(scriptTemplate, WrapKeywordFunc(tableName)));
                }
                return new List<ExecutionDatabaseStatement>(1)
                {
                    new ExecutionDatabaseStatement()
                    {
                        Script = statementBuilder.ToString(),
                        ScriptType = scriptType,
                        MustAffectData = command.Options?.MustAffectData ?? false,
                        Parameters = parameters,
                        HasPreScript = false
                    }
                };
            }
            else
            {
                var queryStatement = await GenerateQueryStatementCoreAsync(context, translationResult, QueryableLocation.JoinTarget).ConfigureAwait(false);
                var updateTablePetName = "UTB";
                var joinItems = FormatWrapJoinPrimaryKeys(context, command.Queryable, command.GetEntityType(), tablePetName, tablePetName, updateTablePetName);
                scriptTemplate = $"{FormatPreScript(context)}DELETE {tablePetName} FROM {{0}}{TablePetNameKeyword}{tablePetName} INNER JOIN ({queryStatement.Script}){TablePetNameKeyword}{updateTablePetName} ON {string.Join(" AND ", joinItems)};";
                var statements = new List<ExecutionDatabaseStatement>(tableNames.Count);
                foreach (var tableName in tableNames)
                {
                    statements.Add(new ExecutionDatabaseStatement()
                    {
                        Script = string.Format(scriptTemplate, WrapKeywordFunc(tableName)),
                        ScriptType = scriptType,
                        MustAffectData = command.Options?.MustAffectData ?? false,
                        Parameters = parameters,
                        HasPreScript = true
                    });
                }
                return statements;
            }

            #endregion
        }

        #endregion

        #region Get create table statements

        /// <summary>
        /// Get create table statements
        /// </summary>
        /// <param name="migrationCommand">Migration command</param>
        /// <returns></returns>
        protected override async Task<List<ExecutionDatabaseStatement>> GetCreateTableStatementsAsync(MigrationDatabaseCommand migrationCommand)
        {
            var migrationInfo = migrationCommand.MigrationInfo;
            if (migrationInfo?.NewTables.IsNullOrEmpty() ?? true)
            {
                return new List<ExecutionDatabaseStatement>(0);
            }
            var newTables = migrationInfo.NewTables;
            var statements = new List<ExecutionDatabaseStatement>();
            var options = migrationCommand.MigrationInfo;
            foreach (var newTableInfo in newTables)
            {
                if (newTableInfo?.EntityType == null || (newTableInfo?.TableNames.IsNullOrEmpty() ?? true))
                {
                    continue;
                }
                var entityType = newTableInfo.EntityType;
                var entityConfig = SixnetEntityManager.GetEntityConfig(entityType);
                SixnetDirectThrower.ThrowSixnetExceptionIf(entityConfig == null, $"Get entity config failed for {entityType.Name}");

                var newFieldScripts = new List<string>();
                var primaryKeyNames = new List<string>();
                foreach (var field in entityConfig.AllFields)
                {
                    var dataField = SixnetDataManager.GetField(SqlServerManager.CurrentDatabaseServerType, entityType, field.Value);
                    if (dataField is DataField dataEntityField)
                    {
                        var dataFieldName = SqlServerManager.WrapKeyword(dataEntityField.GetFieldName(DatabaseType));
                        newFieldScripts.Add($"{dataFieldName}{GetSqlDataType(dataEntityField, options)}{GetFieldNullable(dataEntityField, options)}{GetSqlDefaultValue(dataEntityField, options)}");
                        if (dataEntityField.InRole(FieldRole.PrimaryKey))
                        {
                            primaryKeyNames.Add($"{dataFieldName} ASC");
                        }
                    }
                }
                foreach (var tableName in newTableInfo.TableNames)
                {
                    var createTableStatement = new ExecutionDatabaseStatement()
                    {
                        Script = $"IF NOT EXISTS (SELECT * FROM SYS.OBJECTS WHERE OBJECT_ID = OBJECT_ID(N'{tableName}') AND TYPE IN (N'U')){Environment.NewLine}BEGIN{Environment.NewLine}CREATE TABLE {tableName} ({string.Join(",", newFieldScripts)}{(primaryKeyNames.IsNullOrEmpty() ? "" : $", CONSTRAINT PK_{tableName} PRIMARY KEY CLUSTERED ({string.Join(",", primaryKeyNames)})")}){Environment.NewLine}END;"
                    };
                    statements.Add(createTableStatement);

                    // Log script
                    LogExecutionStatement(createTableStatement);
                }
            }
            return await Task.FromResult(statements).ConfigureAwait(false);
        }

        #endregion

        #region Get default sort

        protected override async Task<string> GetDefaultSortAsync(DataCommandResolveContext context, QueryableTranslationResult translationResult, ISixnetQueryable originalQueryable, IEnumerable<ISixnetField> dataFields, string tablePetName)
        {
            var defaultSortField = dataFields?.Where(f => f is DataField)
                                              .OrderByDescending(f => f.InRole(FieldRole.Sequence))
                                              .ThenByDescending(f => f.InRole(FieldRole.PrimaryKey))
                                              .FirstOrDefault();
            if (defaultSortField != null)
            {
                var orderField = DataField.Create(defaultSortField.PropertyName, originalQueryable.GetModelType());
                originalQueryable.OrderBy(orderField);
                await AppendSortAsync(context, originalQueryable, translationResult, true).ConfigureAwait(false);
            }
            return translationResult.GetSort();
        }

        #endregion
    }
}
