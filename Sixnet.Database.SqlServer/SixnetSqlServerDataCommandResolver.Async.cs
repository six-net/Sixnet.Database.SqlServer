using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Sixnet.Development.Data;
using Sixnet.Development.Data.Command;
using Sixnet.Development.Data.Dapper;
using Sixnet.Development.Data.Database;
using Sixnet.Development.Data.Field;
using Sixnet.Development.Entity;
using Sixnet.Development.Queryable;
using Sixnet.Exceptions;

namespace Sixnet.Database.SqlServer
{
    internal partial class SixnetSqlServerDataCommandResolver
    {
        #region Get query statement

        /// <summary>
        /// Get query statement
        /// </summary>
        /// <param name="context">Command resolve context</param>
        /// <param name="translationResult">Queryable translation result</param>
        /// <param name="location">Queryable location</param>
        /// <returns></returns>
        protected override async Task<SixnetQueryDatabaseStatement> GenerateQueryStatementCoreAsync(SixnetDataCommandResolveContext context, SixnetQueryableTranslationResult translationResult, SixnetQueryableLocation location)
        {
            var queryable = translationResult.GetOriginalQueryable();
            string sqlStatement;
            IEnumerable<ISixnetField> outputFields = null;
            switch (queryable.ExecutionMode)
            {
                case SixnetQueryableExecutionMode.Script:
                    sqlStatement = translationResult.GetCondition();
                    break;
                case SixnetQueryableExecutionMode.Regular:
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
                    var outputFieldString = await FormatFieldsStringAsync(context, queryable, location, SixnetFieldLocation.Output, outputFields).ConfigureAwait(false);

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
                        case SixnetQueryableOutputType.Count:
                            sqlStatement = hasCombine
                                ? hasSort
                                    ? $"{preScript}SELECT COUNT(1) FROM ((SELECT {tablePetName}.* FROM ({sqlStatement}){TablePetNameKeyword}{tablePetName}){combine}){TablePetNameKeyword}{tablePetName}"
                                    : $"{preScript}SELECT COUNT(1) FROM (({sqlStatement}){combine}){TablePetNameKeyword}{tablePetName}"
                                : $"{preScript}SELECT COUNT(1) FROM ({sqlStatement}){TablePetNameKeyword}{tablePetName}";
                            break;
                        case SixnetQueryableOutputType.Predicate:
                            sqlStatement = hasCombine
                                ? hasSort
                                    ? $"{preScript}SELECT 1 WHERE EXISTS((SELECT {tablePetName}.* FROM ({sqlStatement}){TablePetNameKeyword}{tablePetName}){combine})"
                                    : $"{preScript}SELECT 1 WHERE EXISTS(({sqlStatement}){combine})"
                                : $"{preScript}SELECT 1 WHERE EXISTS({sqlStatement})";
                            break;
                        default:
                            sqlStatement = hasCombine
                            ? hasSort
                                ? $"{preScript}(SELECT {tablePetName}.* FROM ({sqlStatement}){TablePetNameKeyword}{tablePetName}){combine}"
                                : $"{preScript}({sqlStatement}){combine}"
                            : $"{preScript}{sqlStatement}";
                            break;
                    }
                    break;
            }

            //parameters
            var parameters = context.GetParameters();

            //log script
            if (location == SixnetQueryableLocation.Top)
            {
                LogScript(sqlStatement, parameters);
            }
            return SixnetQueryDatabaseStatement.Create(sqlStatement, parameters, outputFields);
        }

        #endregion

        #region Get insert statement

        /// <summary>
        /// Get insert statement
        /// </summary>
        /// <param name="context">Command resolve context</param>
        /// <returns></returns>
        protected override async Task<List<SixnetExecutionDatabaseStatement>> GenerateInsertStatementsAsync(SixnetDataCommandResolveContext context)
        {
            var command = context.DataCommandExecutionContext.Command;
            var dataCommandExecutionContext = context.DataCommandExecutionContext;
            var entityType = dataCommandExecutionContext.Command.GetEntityType();
            var fields = SixnetDataManager.GetInsertableFields(DatabaseType, entityType);
            var fieldCount = fields.GetCount();
            var insertFields = new List<string>(fieldCount);
            var insertValues = new List<string>(fieldCount);
            SixnetDataField autoIncrementField = null;
            SixnetDataField splitField = null;
            dynamic splitValue = default;

            foreach (var field in fields)
            {
                if (field.InRole(SixnetFieldRole.Increment))
                {
                    autoIncrementField ??= field;
                    if (!autoIncrementField.InRole(SixnetFieldRole.PrimaryKey) && field.InRole(SixnetFieldRole.PrimaryKey)) // get first primary key field
                    {
                        autoIncrementField = field;
                    }
                    if (!SixnetDataManager.AllowInsertIncrementField(context.DataCommandExecutionContext))
                    {
                        continue;
                    }
                }
                // fields
                insertFields.Add(FormatAndWrapObjectName(field.GetFieldName(DatabaseType), SixnetDatabaseObjectType.Column));
                // values
                var insertValue = command.FieldsAssignment.GetNewValue(field.PropertyName);
                insertValues.Add(await FormatInsertValueFieldAsync(context, command.Queryable, insertValue).ConfigureAwait(false));

                // split value
                if (field.InRole(SixnetFieldRole.SplitValue))
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
                statementBuilder.AppendLine(string.Format(scriptTemplate, FormatAndWrapObjectName(tableName)));
            }
            if (autoIncrementField != null)
            {
                var incrField = $"{command.Id}";
                var incrParameter = FormatParameterName(incrField);
                statementBuilder.AppendLine($"DECLARE {incrParameter} BIGINT;SET {incrParameter} = SCOPE_IDENTITY();");
                incrScripts.Add($"{incrParameter} {ColumnPetNameKeyword} {incrField}");
            }
            return new List<SixnetExecutionDatabaseStatement>()
            {
                new SixnetExecutionDatabaseStatement()
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
        protected override async Task<List<SixnetExecutionDatabaseStatement>> GenerateUpdateStatementsAsync(SixnetDataCommandResolveContext context)
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
                var updateField = SixnetDataManager.GetField(dataCommandExecutionContext.Server.DatabaseType, command.GetEntityType(), SixnetDataField.Create(propertyName)) as SixnetDataField;

                SixnetDirectThrower.ThrowSixnetExceptionIf(updateField == null, $"Not found field:{propertyName}");

                var fieldFormattedName = FormatAndWrapObjectName(updateField.GetFieldName(DatabaseType), SixnetDatabaseObjectType.Column);
                var newValueExpression = await FormatUpdateValueFieldAsync(context, command, newValue).ConfigureAwait(false);
                updateSetArray.Add($"{tablePetName}.{fieldFormattedName}={newValueExpression}");
            }

            // parameters
            var parameters = ConvertParameter(command.ScriptParameters) ?? new SixnetDataCommandParameters();
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
                    statementBuilder.AppendLine(string.Format(scriptTemplate, FormatAndWrapObjectName(tableName)));
                }
                return new List<SixnetExecutionDatabaseStatement>(1)
                {
                    new SixnetExecutionDatabaseStatement()
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
                var queryStatement = await GenerateQueryStatementCoreAsync(context, translationResult, SixnetQueryableLocation.JoinTarget).ConfigureAwait(false);
                var updateTablePetName = "UTB";
                var joinItems = FormatWrapJoinPrimaryKeys(context, command.Queryable, command.GetEntityType(), tablePetName, tablePetName, updateTablePetName);
                scriptTemplate = $"{FormatPreScript(context)}UPDATE {tablePetName} SET {string.Join(",", updateSetArray)} FROM {{0}}{TablePetNameKeyword}{tablePetName} INNER JOIN ({queryStatement.Script}){TablePetNameKeyword}{updateTablePetName} ON {string.Join(" AND ", joinItems)};";
                var statements = new List<SixnetExecutionDatabaseStatement>(tableNames.Count);
                foreach (var tableName in tableNames)
                {
                    statements.Add(new SixnetExecutionDatabaseStatement()
                    {
                        Script = string.Format(scriptTemplate, FormatAndWrapObjectName(tableName)),
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
        protected override async Task<List<SixnetExecutionDatabaseStatement>> GenerateDeleteStatementsAsync(SixnetDataCommandResolveContext context)
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
            var parameters = ConvertParameter(command.ScriptParameters) ?? new SixnetDataCommandParameters();
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
                    statementBuilder.AppendLine(string.Format(scriptTemplate, FormatAndWrapObjectName(tableName)));
                }
                return new List<SixnetExecutionDatabaseStatement>(1)
                {
                    new SixnetExecutionDatabaseStatement()
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
                var queryStatement = await GenerateQueryStatementCoreAsync(context, translationResult, SixnetQueryableLocation.JoinTarget).ConfigureAwait(false);
                var updateTablePetName = "UTB";
                var joinItems = FormatWrapJoinPrimaryKeys(context, command.Queryable, command.GetEntityType(), tablePetName, tablePetName, updateTablePetName);
                scriptTemplate = $"{FormatPreScript(context)}DELETE {tablePetName} FROM {{0}}{TablePetNameKeyword}{tablePetName} INNER JOIN ({queryStatement.Script}){TablePetNameKeyword}{updateTablePetName} ON {string.Join(" AND ", joinItems)};";
                var statements = new List<SixnetExecutionDatabaseStatement>(tableNames.Count);
                foreach (var tableName in tableNames)
                {
                    statements.Add(new SixnetExecutionDatabaseStatement()
                    {
                        Script = string.Format(scriptTemplate, FormatAndWrapObjectName(tableName)),
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

        #region Get default sort

        protected override async Task<string> GetDefaultSortAsync(SixnetDataCommandResolveContext context, SixnetQueryableTranslationResult translationResult, ISixnetQueryable originalQueryable, IEnumerable<ISixnetField> dataFields, string tablePetName)
        {
            var defaultSortField = dataFields?.Where(f => f is SixnetDataField)
                                              .OrderByDescending(f => f.InRole(SixnetFieldRole.Sequence))
                                              .ThenByDescending(f => f.InRole(SixnetFieldRole.PrimaryKey))
                                              .FirstOrDefault();
            if (defaultSortField != null)
            {
                var orderField = SixnetDataField.Create(defaultSortField.PropertyName, originalQueryable.GetModelType());
                originalQueryable.OrderBy(orderField);
                await AppendSortAsync(context, originalQueryable, translationResult, true).ConfigureAwait(false);
            }
            return translationResult.GetSort();
        }

        #endregion

        #region Get create table statements

        /// <summary>
        /// Get create table statements
        /// </summary>
        /// <param name="migrationCommand">Migration command</param>
        /// <returns></returns>
        protected override Task<List<SixnetExecutionDatabaseStatement>> GetCreateTableStatementsAsync(SixnetMigrationDatabaseCommand migrationCommand)
        {
            return Task.FromResult(GetCreateTableStatements(migrationCommand));
        }

        #endregion

        #region Add foreign key

        protected override Task<List<SixnetExecutionDatabaseStatement>> GetAddForeignKeyStatementsAsync(SixnetMigrationDatabaseCommand migrationCommand)
        {
            return Task.FromResult(GetAddForeignKeyStatements(migrationCommand));
        }

        #endregion

        #region Delete foreign key

        protected override Task<List<SixnetExecutionDatabaseStatement>> GetDeleteForeignKeyStatementsAsync(SixnetMigrationDatabaseCommand migrationCommand)
        {
            return Task.FromResult(GetDeleteForeignKeyStatements(migrationCommand));
        }

        protected override async Task<List<SixnetExecutionDatabaseStatement>> GetDeleteAllForeignKeyStatementsAsync(SixnetMigrationDatabaseCommand migrationCommand)
        {
            var statements = new List<SixnetExecutionDatabaseStatement>();
            var sql = @"
SELECT
    'ALTER TABLE '
    + QUOTENAME(SCHEMA_NAME(t.schema_id))
    + '.'
    + QUOTENAME(t.name)
    + ' DROP CONSTRAINT '
    + QUOTENAME(fk.name)
    + ';'
FROM sys.foreign_keys fk
JOIN sys.tables t ON fk.parent_object_id = t.object_id
WHERE t.is_ms_shipped = 0;
";
            var deleteScripts = await migrationCommand.Connection.DbConnection.QueryAsync<string>(sql, transaction: migrationCommand.Connection.Transaction.DbTransaction).ConfigureAwait(false);
            foreach (var script in deleteScripts)
            {
                statements.Add(new SixnetExecutionDatabaseStatement()
                {
                    Script = script
                });
            }

            return statements;
        }

        #endregion

        #region Add index

        protected override Task<List<SixnetExecutionDatabaseStatement>> GetAddIndexStatementsAsync(SixnetMigrationDatabaseCommand migrationCommand)
        {
            return Task.FromResult(GetAddIndexStatements(migrationCommand));
        }

        #endregion

        #region Delete index

        /// <summary>
        /// Get delete index statements
        /// </summary>
        /// <param name="migrationCommand"></param>
        /// <returns></returns>
        protected override Task<List<SixnetExecutionDatabaseStatement>> GetDeleteIndexStatementsAsync(SixnetMigrationDatabaseCommand migrationCommand)
        {
            return Task.FromResult(GetDeleteIndexStatements(migrationCommand));
        }

        #endregion

        #region Get add filed statements

        /// <summary>
        /// Get create field statement
        /// </summary>
        /// <param name="migrationCommand"></param>
        /// <returns></returns>
        protected override Task<List<SixnetExecutionDatabaseStatement>> GetAddFieldStatementsAsync(SixnetMigrationDatabaseCommand migrationCommand)
        {
            return Task.FromResult(GetAddFieldStatements(migrationCommand));
        }

        #endregion

        #region Get delete filed statements

        protected override Task<List<SixnetExecutionDatabaseStatement>> GetDeleteFieldStatementsAsync(SixnetMigrationDatabaseCommand migrationCommand)
        {
            return Task.FromResult(GetDeleteFieldStatements(migrationCommand));
        }

        #endregion

        #region Get update field statements 

        protected override Task<List<SixnetExecutionDatabaseStatement>> GetUpdateFieldStatementsAsync(SixnetMigrationDatabaseCommand migrationCommand)
        {
            return Task.FromResult(GetUpdateFieldStatements(migrationCommand));
        }

        #endregion

        #region Get rename table statements

        protected override Task<List<SixnetExecutionDatabaseStatement>> GetRenameTableStatementsAsync(SixnetMigrationDatabaseCommand migrationCommand)
        {
            return Task.FromResult(GetRenameTableStatements(migrationCommand));
        }

        #endregion

        #region Get delete all table statements

        /// <summary>
        /// Get delete all table statements
        /// </summary>
        /// <param name="migrationCommand"></param>
        /// <returns></returns>
        protected override async Task<List<SixnetExecutionDatabaseStatement>> GetDeleteAllTableStatementsAsync(SixnetMigrationDatabaseCommand migrationCommand)
        {
            var statements = new List<SixnetExecutionDatabaseStatement>();
            var sql = @"
SELECT
    N'DROP TABLE '
    + QUOTENAME(s.name)
    + N'.'
    + QUOTENAME(t.name)
    + N';' + CHAR(13)
FROM sys.tables t
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE t.is_ms_shipped = 0;
";
            var deleteScripts = await migrationCommand.Connection.DbConnection.QueryAsync<string>(sql, transaction: migrationCommand.Connection.Transaction.DbTransaction).ConfigureAwait(false);
            foreach (var script in deleteScripts)
            {
                statements.Add(new SixnetExecutionDatabaseStatement()
                {
                    Script = script
                });
            }

            return statements;
        }

        #endregion

        #region Get delete all view statements

        /// <summary>
        /// Get delete all view statements
        /// </summary>
        /// <param name="migrationCommand"></param>
        /// <returns></returns>
        protected override async Task<List<SixnetExecutionDatabaseStatement>> GetDeleteAllViewStatementsAsync(SixnetMigrationDatabaseCommand migrationCommand)
        {
            var statements = new List<SixnetExecutionDatabaseStatement>();
            var sql = @"
SELECT 
    N'DROP VIEW '
    + QUOTENAME(s.name)
    + N'.'
    + QUOTENAME(v.name)
    + N';' + CHAR(13)
FROM sys.views v
JOIN sys.schemas s ON v.schema_id = s.schema_id
WHERE v.is_ms_shipped = 0;
";
            var deleteScripts = await migrationCommand.Connection.DbConnection.QueryAsync<string>(sql, transaction: migrationCommand.Connection.Transaction.DbTransaction).ConfigureAwait(false);
            foreach (var script in deleteScripts)
            {
                statements.Add(new SixnetExecutionDatabaseStatement()
                {
                    Script = script
                });
            }

            return statements;
        }

        #endregion

        #region Get delete all function statements

        /// <summary>
        /// Get delete all function statements
        /// </summary>
        /// <param name="migrationCommand"></param>
        /// <returns></returns>
        protected override async Task<List<SixnetExecutionDatabaseStatement>> GetDeleteAllFunctionStatementsAsync(SixnetMigrationDatabaseCommand migrationCommand)
        {
            var statements = new List<SixnetExecutionDatabaseStatement>();
            var sql = @"
SELECT 
    N'DROP FUNCTION '
    + QUOTENAME(s.name)
    + N'.'
    + QUOTENAME(o.name)
    + N';' + CHAR(13)
FROM sys.objects o
JOIN sys.schemas s ON o.schema_id = s.schema_id
WHERE o.type IN (
    'FN',   -- Scalar Function
    'IF',   -- Inline Table Function
    'TF',   -- Table Function
    'FS',   -- CLR Scalar Function
    'FT'    -- CLR Table Function
)
AND o.is_ms_shipped = 0;
";
            var deleteScripts = await migrationCommand.Connection.DbConnection.QueryAsync<string>(sql, transaction: migrationCommand.Connection.Transaction.DbTransaction).ConfigureAwait(false);
            foreach (var script in deleteScripts)
            {
                statements.Add(new SixnetExecutionDatabaseStatement()
                {
                    Script = script
                });
            }

            return statements;
        }

        #endregion

        #region Get delete all custom type statements

        /// <summary>
        /// Get delete all custom type statements
        /// </summary>
        /// <param name="migrationCommand"></param>
        /// <returns></returns>
        protected override async Task<List<SixnetExecutionDatabaseStatement>> GetDeleteAllCustomTypeStatementsAsync(SixnetMigrationDatabaseCommand migrationCommand)
        {
            var statements = new List<SixnetExecutionDatabaseStatement>();
            var sql = @"
SELECT 
    N'DROP TYPE '
    + QUOTENAME(SCHEMA_NAME(schema_id))
    + N'.'
    + QUOTENAME(name)
    + N';' + CHAR(13)
FROM sys.types
WHERE is_user_defined = 1
  AND is_table_type = 0;
";
            var deleteScripts = await migrationCommand.Connection.DbConnection.QueryAsync<string>(sql, transaction: migrationCommand.Connection.Transaction.DbTransaction).ConfigureAwait(false);
            foreach (var script in deleteScripts)
            {
                statements.Add(new SixnetExecutionDatabaseStatement()
                {
                    Script = script
                });
            }

            return statements;
        }

        #endregion

        #region Get delete all procedure statements

        /// <summary>
        /// Get delete all procedure statements
        /// </summary>
        /// <param name="migrationCommand"></param>
        /// <returns></returns>
        protected override async Task<List<SixnetExecutionDatabaseStatement>> GetDeleteAllProcedureStatementsAsync(SixnetMigrationDatabaseCommand migrationCommand)
        {
            var statements = new List<SixnetExecutionDatabaseStatement>();
            var sql = @"
SELECT 
    N'DROP PROCEDURE '
    + QUOTENAME(s.name)
    + N'.'
    + QUOTENAME(p.name)
    + N';' + CHAR(13)
FROM sys.procedures p
JOIN sys.schemas s ON p.schema_id = s.schema_id
WHERE p.is_ms_shipped = 0;
";
            var deleteScripts = await migrationCommand.Connection.DbConnection.QueryAsync<string>(sql, transaction: migrationCommand.Connection.Transaction.DbTransaction).ConfigureAwait(false);
            foreach (var script in deleteScripts)
            {
                statements.Add(new SixnetExecutionDatabaseStatement()
                {
                    Script = script
                });
            }

            return statements;
        }

        #endregion
    }
}
