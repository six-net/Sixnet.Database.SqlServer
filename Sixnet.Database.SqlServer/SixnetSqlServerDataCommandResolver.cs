using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Net.Http.Headers;
using System.Security.AccessControl;
using System.Text;

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
    /// <summary>
    /// Defines data command resolver for sqlserver
    /// </summary>
    internal partial class SixnetSqlServerDataCommandResolver : SixnetBaseDataCommandResolver
    {
        #region Constructor

        public SixnetSqlServerDataCommandResolver()
        {
            KeywordPrefix = "[";
            KeywordSuffix = "]";
            DatabaseType = SixnetDatabaseType.SQLServer;
            DefaultFieldFormatter = new SixnetSqlServerFieldFormatter();
            RecursiveKeyword = "WITH";
            DbTypeDefaultValues = new Dictionary<DbType, string>()
            {
                { DbType.Byte, "0" },
                { DbType.SByte, "0" },
                { DbType.Int16, "0" },
                { DbType.UInt16, "0" },
                { DbType.Int32, "0" },
                { DbType.UInt32, "0" },
                { DbType.Int64, "0" },
                { DbType.UInt64, "0" },
                { DbType.Single, "0" },
                { DbType.Double, "0" },
                { DbType.Decimal, "0" },
                { DbType.Boolean, "0" },
                { DbType.String, "''" },
                { DbType.StringFixedLength, "''" },
                { DbType.Guid, "NEWID()" },
                { DbType.DateTime, "GETDATE()" },
                { DbType.DateTime2, "SYSDATETIME()" },
                { DbType.DateTimeOffset, "SYSDATETIMEOFFSET()" },
                { DbType.Time, "SYSUTCDATETIME()" }
            };
        }

        #endregion

        #region Get query statement

        /// <summary>
        /// Get query statement
        /// </summary>
        /// <param name="context">Command resolve context</param>
        /// <param name="translationResult">Queryable translation result</param>
        /// <param name="location">Queryable location</param>
        /// <returns></returns>
        protected override SixnetQueryDatabaseStatement GenerateQueryStatementCore(SixnetDataCommandResolveContext context, SixnetQueryableTranslationResult translationResult, SixnetQueryableLocation location)
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
                        var targetStatement = GetFromTargetStatement(context, queryable, location, tablePetName);
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
                    var outputFieldString = FormatFieldsString(context, queryable, location, SixnetFieldLocation.Output, outputFields);

                    //sort
                    var hasOffset = queryable.SkipCount > 0;
                    var hasTakeNum = queryable.TakeCount > 0;
                    var sort = translationResult.GetSort();
                    var hasSort = !string.IsNullOrWhiteSpace(sort);
                    if (hasTakeNum && hasOffset && !hasSort)
                    {
                        sort = GetDefaultSort(context, translationResult, queryable, outputFields, tablePetName);
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
        protected override List<SixnetExecutionDatabaseStatement> GenerateInsertStatements(SixnetDataCommandResolveContext context)
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
                insertValues.Add(FormatInsertValueField(context, command.Queryable, insertValue));

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
            var tableNames = dataCommandExecutionContext.GetTableNames();

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
        protected override List<SixnetExecutionDatabaseStatement> GenerateUpdateStatements(SixnetDataCommandResolveContext context)
        {
            var command = context.DataCommandExecutionContext.Command;
            SixnetException.ThrowIf(command?.FieldsAssignment?.NewValues.IsNullOrEmpty() ?? true, "No set update field");

            #region translate

            var translationResult = Translate(context);
            var preScripts = context.GetPreScripts();

            #endregion

            #region script 

            var dataCommandExecutionContext = context.DataCommandExecutionContext;
            var entityType = dataCommandExecutionContext.Command.GetEntityType();

            var tableNames = dataCommandExecutionContext.GetTableNames(command);
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
                var newValueExpression = FormatUpdateValueField(context, command, newValue);
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
                var queryStatement = GenerateQueryStatementCore(context, translationResult, SixnetQueryableLocation.JoinTarget);
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
        protected override List<SixnetExecutionDatabaseStatement> GenerateDeleteStatements(SixnetDataCommandResolveContext context)
        {
            var dataCommandExecutionContext = context.DataCommandExecutionContext;
            var command = dataCommandExecutionContext.Command;

            #region translate

            var translationResult = Translate(context);
            var preScripts = context.GetPreScripts();

            #endregion

            #region script

            var tablePetName = command.Queryable == null ? context.GetNewTablePetName() : context.GetDefaultTablePetName(command.Queryable);
            var entityType = dataCommandExecutionContext.Command.GetEntityType();
            var tableNames = dataCommandExecutionContext.GetTableNames(command);
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
                var queryStatement = GenerateQueryStatementCore(context, translationResult, SixnetQueryableLocation.JoinTarget);
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

        #region Get create table statements

        /// <summary>
        /// Get create table statements
        /// </summary>
        /// <param name="migrationCommand">Migration command</param>
        /// <returns></returns>
        protected override List<SixnetExecutionDatabaseStatement> GetCreateTableStatements(SixnetMigrationDatabaseCommand migrationCommand)
        {
            var migrationInfo = migrationCommand.MigrationInfo;
            if (migrationInfo?.NewTables.IsNullOrEmpty() ?? true)
            {
                return new List<SixnetExecutionDatabaseStatement>(0);
            }
            var newTables = migrationInfo.NewTables;
            var statements = new List<SixnetExecutionDatabaseStatement>();
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
                    var dataField = SixnetDataManager.GetField(DatabaseType, entityType, field.Value);
                    if (dataField is SixnetDataField dataEntityField)
                    {
                        var dataFieldName = FormatAndWrapObjectName(SixnetDatabaseObjectName.Create(dataEntityField.GetFieldName(DatabaseType), SixnetDatabaseObjectType.Column));
                        newFieldScripts.Add($"{dataFieldName}{GetFieldDefinition(dataEntityField, migrationInfo)}");
                        if (dataEntityField.InRole(SixnetFieldRole.PrimaryKey))
                        {
                            primaryKeyNames.Add($"{dataFieldName} ASC");
                        }
                    }
                }

                foreach (var table in newTableInfo.TableNames)
                {
                    var formattedAndWrapedTableName = FormatAndWrapObjectName(table);
                    var formattedTableName = FormatObjectName(table);
                    var createTableStatement = new SixnetExecutionDatabaseStatement()
                    {
                        Script = $"IF NOT EXISTS (SELECT * FROM SYS.OBJECTS WHERE OBJECT_ID = OBJECT_ID(N'{formattedAndWrapedTableName}') AND TYPE IN (N'U')){Environment.NewLine}BEGIN{Environment.NewLine}CREATE TABLE {formattedAndWrapedTableName} ({string.Join(",", newFieldScripts)}{(primaryKeyNames.IsNullOrEmpty() ? "" : $", CONSTRAINT PK_{table.IdentityName} PRIMARY KEY CLUSTERED ({string.Join(",", primaryKeyNames)})")}){Environment.NewLine}END;"
                    };
                    statements.Add(createTableStatement);

                    // Log script
                    LogExecutionStatement(createTableStatement);

                    // Foreign key
                    if (!entityConfig.RelationFields.IsNullOrEmpty())
                    {
                        var foreignKeyInfos = new List<SixnetEntityForeignKeyInfo>();
                        foreach (var relationTypeItem in entityConfig.RelationFields)
                        {
                            var referenceEntityConfig = SixnetEntityManager.GetEntityConfig(relationTypeItem.Key);
                            foreach (var relationFieldItem in relationTypeItem.Value)
                            {
                                if ((relationFieldItem.Value.Behavior & SixnetRelationBehavior.ForeignKey) != SixnetRelationBehavior.ForeignKey)
                                {
                                    continue;
                                }
                                var sourceTable = table;
                                var sourceField = SixnetDatabaseObjectName.Create(SixnetDataField.Create(relationFieldItem.Key, entityType).GetFieldName(DatabaseType), SixnetDatabaseObjectType.Column);
                                var referenceField = SixnetDatabaseObjectName.Create(SixnetDataField.Create(relationFieldItem.Value.RelationField, relationFieldItem.Value.RelationType).GetFieldName(DatabaseType), SixnetDatabaseObjectType.Column);
                                var referenceCommand = SixnetDataCommand.Create(null);
                                referenceCommand.SetEntityType(referenceEntityConfig.EntityType);
                                var referenceTable = SixnetDataCommandExecutionContext.Create(migrationCommand.Connection, referenceCommand).GetTableNames(null, SixnetQueryableLocation.From).FirstOrDefault();
                                foreignKeyInfos.Add(new SixnetEntityForeignKeyInfo()
                                {
                                    SourceTable = sourceTable,
                                    SourceField = sourceField,
                                    ReferenceTable = referenceTable,
                                    ReferenceField = referenceField
                                });
                            }
                        }
                        var foreignKeyStatements = GetAddForeignKeyStatementsCore(foreignKeyInfos);
                        if (!foreignKeyStatements.IsNullOrEmpty())
                        {
                            statements.AddRange(foreignKeyStatements);
                        }
                    }

                    // index
                    var indexAttributes = entityType.GetCustomAttributes(typeof(SixnetEntityIndexAttribute), false);
                    if (!indexAttributes.IsNullOrEmpty())
                    {
                        var indexInfos = new List<SixnetEntityIndexInfo>();
                        foreach (var indexItem in indexAttributes)
                        {
                            if (indexItem is SixnetEntityIndexAttribute indexAttr && !indexAttr.Fields.IsNullOrEmpty())
                            {
                                var newIndexInfo = new SixnetEntityIndexInfo()
                                {
                                    Table = table,
                                    Fields = new List<SixnetEntityIndexField>(),
                                    Unique = indexAttr.Unique
                                };
                                foreach (var indexItemField in indexAttr.Fields)
                                {
                                    var entityField = entityConfig.AllFields[indexItemField];
                                    var fieldDesc = entityField.HasDbFeature(SixnetFieldDbFeature.IndexDesc);
                                    var entityFieldName = FormatObjectName(SixnetDatabaseObjectName.Create(entityField.GetFieldName(DatabaseType), SixnetDatabaseObjectType.Column));
                                    newIndexInfo.Fields.Add(new SixnetEntityIndexField()
                                    {
                                        Desc = fieldDesc,
                                        Name = entityFieldName,
                                        Sequence = entityField.IndexSequence
                                    });
                                }
                                indexInfos.Add(newIndexInfo);
                            }
                        }
                        var indexStatements = GetAddIndexStatementsCore(indexInfos);
                        if (!indexStatements.IsNullOrEmpty())
                        {
                            statements.AddRange(indexStatements);
                        }
                    }
                }
            }
            return statements;
        }

        #endregion

        #region Get delete all table statements

        /// <summary>
        /// Get delete all table statements
        /// </summary>
        /// <param name="migrationCommand"></param>
        /// <returns></returns>
        protected override List<SixnetExecutionDatabaseStatement> GetDeleteAllTableStatements(SixnetMigrationDatabaseCommand migrationCommand)
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
            var deleteScripts = migrationCommand.Connection.DbConnection.Query<string>(sql, transaction: migrationCommand.Connection.Transaction.DbTransaction);
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

        #region Add foreign key

        List<SixnetExecutionDatabaseStatement> GetAddForeignKeyStatementsCore(List<SixnetEntityForeignKeyInfo> foreignKeyInfos)
        {
            if (foreignKeyInfos.IsNullOrEmpty())
            {
                return new List<SixnetExecutionDatabaseStatement>(0);
            }
            var statements = new List<SixnetExecutionDatabaseStatement>();
            foreach (var foreignKeyInfo in foreignKeyInfos)
            {
                var sourceFieldName = FormatObjectName(foreignKeyInfo.SourceField);
                var formattedSourceTableName = FormatObjectName(foreignKeyInfo.SourceTable);
                var wrapedSourceTableName = FormatAndWrapObjectName(foreignKeyInfo.SourceTable);

                var referenceFieldName = FormatAndWrapObjectName(foreignKeyInfo.ReferenceField);
                var referenceTableName = FormatAndWrapObjectName(foreignKeyInfo.ReferenceTable);

                var constraintName = WrapObjectName(SixnetDatabaseObjectName.Create($"FK_{formattedSourceTableName.Name}_{sourceFieldName}", SixnetDatabaseObjectType.Constraint));
                var foreignKeyStatement = new SixnetExecutionDatabaseStatement()
                {
                    Script = $"IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = '{constraintName}' AND parent_object_id = OBJECT_ID('{wrapedSourceTableName}')) BEGIN ALTER TABLE {wrapedSourceTableName} WITH CHECK ADD CONSTRAINT {constraintName} FOREIGN KEY({WrapObjectName(sourceFieldName).Name}) REFERENCES {referenceTableName} ({referenceFieldName});ALTER TABLE {wrapedSourceTableName} CHECK CONSTRAINT {constraintName}; END"
                };
                statements.Add(foreignKeyStatement);

                // Log script
                LogExecutionStatement(foreignKeyStatement);
            }

            return statements;
        }

        protected override List<SixnetExecutionDatabaseStatement> GetAddForeignKeyStatements(SixnetMigrationDatabaseCommand migrationCommand)
        {
            return GetAddForeignKeyStatementsCore(migrationCommand?.MigrationInfo?.NewForeignKeys);
        }


        #endregion

        #region Delete foreign key

        protected override List<SixnetExecutionDatabaseStatement> GetDeleteForeignKeyStatements(SixnetMigrationDatabaseCommand migrationCommand)
        {
            if (migrationCommand?.MigrationInfo?.DeletedForeignKeys.IsNullOrEmpty() ?? true)
            {
                return new List<SixnetExecutionDatabaseStatement>(0);
            }
            var statements = new List<SixnetExecutionDatabaseStatement>();
            foreach (var foreignKeyInfo in migrationCommand.MigrationInfo.DeletedForeignKeys)
            {
                var sourceFieldName = FormatObjectName(foreignKeyInfo.SourceField);
                var formattedSourceTableName = FormatObjectName(foreignKeyInfo.SourceTable);
                var wrapedSourceTableName = FormatAndWrapObjectName(foreignKeyInfo.SourceTable);
                var constraintName = WrapObjectName(SixnetDatabaseObjectName.Create($"FK_{formattedSourceTableName.Name}_{sourceFieldName}", SixnetDatabaseObjectType.Constraint));
                var foreignKeyStatement = new SixnetExecutionDatabaseStatement()
                {
                    Script = $"ALTER TABLE {wrapedSourceTableName} DROP CONSTRAINT IF EXISTS {constraintName};"
                };
                statements.Add(foreignKeyStatement);

                // Log script
                LogExecutionStatement(foreignKeyStatement);
            }
            return statements;
        }

        protected override List<SixnetExecutionDatabaseStatement> GetDeleteAllForeignKeyStatements(SixnetMigrationDatabaseCommand migrationCommand)
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
            var deleteScripts = migrationCommand.Connection.DbConnection.Query<string>(sql, transaction: migrationCommand.Connection.Transaction.DbTransaction);
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

        List<SixnetExecutionDatabaseStatement> GetAddIndexStatementsCore(List<SixnetEntityIndexInfo> indexInfos)
        {
            if (indexInfos.IsNullOrEmpty())
            {
                return new List<SixnetExecutionDatabaseStatement>(0);
            }
            var statements = new List<SixnetExecutionDatabaseStatement>();
            foreach (var indexInfo in indexInfos)
            {
                var formattedTableName = FormatObjectName(indexInfo.Table);
                var formattedAndWrapedTableName = FormatAndWrapObjectName(indexInfo.Table);
                var indexName = $"INX_{formattedTableName.Name}";
                var indexFieldStrings = new List<string>();
                var indexFields = indexInfo.Fields.OrderBy(c => c.Sequence).ThenBy(c => c.Name);
                foreach (var indexItemField in indexFields)
                {
                    var entityFieldName = FormatObjectName(indexItemField.Name);
                    indexName = $"{indexName}_{entityFieldName.Name}";
                    indexFieldStrings.Add($"{WrapObjectName(entityFieldName).Name} {(indexItemField.Desc ? "DESC" : "ASC")}");
                }
                var indexStatement = new SixnetExecutionDatabaseStatement()
                {
                    Script = $"IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'{indexName}' AND object_id = OBJECT_ID(N'{formattedAndWrapedTableName}')) BEGIN CREATE {(indexInfo.Unique ? "UNIQUE" : "")} NONCLUSTERED INDEX {indexName} ON {formattedAndWrapedTableName} ({string.Join(",", indexFieldStrings)}); END"
                };
                statements.Add(indexStatement);

                // Log script
                LogExecutionStatement(indexStatement);
            }
            return statements;
        }

        protected override List<SixnetExecutionDatabaseStatement> GetAddIndexStatements(SixnetMigrationDatabaseCommand migrationCommand)
        {
            return GetAddIndexStatementsCore(migrationCommand?.MigrationInfo?.NewIndexes);
        }

        #endregion

        #region Delete index

        /// <summary>
        /// Get delete index statements
        /// </summary>
        /// <param name="migrationCommand"></param>
        /// <returns></returns>
        protected override List<SixnetExecutionDatabaseStatement> GetDeleteIndexStatements(SixnetMigrationDatabaseCommand migrationCommand)
        {
            if (migrationCommand?.MigrationInfo?.DeletedIndexes.IsNullOrEmpty() ?? true)
            {
                return new List<SixnetExecutionDatabaseStatement>();
            }
            var statements = new List<SixnetExecutionDatabaseStatement>();
            foreach (var indexInfo in migrationCommand.MigrationInfo.DeletedIndexes)
            {
                var formattedTableName = FormatObjectName(indexInfo.Table);
                var formattedAndWrapedTableName = FormatAndWrapObjectName(indexInfo.Table);
                var indexName = $"INX_{formattedTableName.Name}";
                var indexFields = indexInfo.Fields.OrderBy(c => c.Sequence).ThenBy(c => c.Name);
                foreach (var indexItemField in indexFields)
                {
                    var entityFieldName = FormatObjectName(indexItemField.Name);
                    indexName = $"{indexName}_{entityFieldName.Name}";
                }
                var indexStatement = new SixnetExecutionDatabaseStatement()
                {
                    Script = $"IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'{indexName}' AND object_id = OBJECT_ID(N'{formattedAndWrapedTableName}')) BEGIN DROP INDEX {indexName} ON {formattedAndWrapedTableName}; END"
                };
                statements.Add(indexStatement);

                // Log script
                LogExecutionStatement(indexStatement);
            }
            return statements;

        }

        #endregion

        #region Get add filed statements

        /// <summary>
        /// Get create field statement
        /// </summary>
        /// <param name="migrationCommand"></param>
        /// <returns></returns>
        protected override List<SixnetExecutionDatabaseStatement> GetAddFieldStatements(SixnetMigrationDatabaseCommand migrationCommand)
        {
            if (migrationCommand?.MigrationInfo?.NewFields.IsNullOrEmpty() ?? true)
            {
                return new List<SixnetExecutionDatabaseStatement>(0);
            }

            var statements = new List<SixnetExecutionDatabaseStatement>();
            foreach (var tableItem in migrationCommand.MigrationInfo.NewFields)
            {
                if (!tableItem.Value.IsNullOrEmpty())
                {
                    var formattedTableName = FormatAndWrapObjectName(tableItem.Key);
                    foreach (var field in tableItem.Value)
                    {
                        var dataFieldName = FormatObjectName(SixnetDatabaseObjectName.Create(field.GetFieldName(DatabaseType), SixnetDatabaseObjectType.Column));
                        var newFieldStatement = new SixnetExecutionDatabaseStatement()
                        {
                            Script = $"IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE [object_id]=OBJECT_ID('{formattedTableName}') AND [name]='{dataFieldName.Name}') BEGIN ALTER TABLE {formattedTableName} ADD {WrapObjectName(dataFieldName).Name}{GetFieldDefinition(field, migrationCommand.MigrationInfo)}; END "
                        };
                        statements.Add(newFieldStatement);
                        // Log script
                        LogExecutionStatement(newFieldStatement);
                    }
                }
            }
            return statements;
        }

        #endregion

        #region Get delete filed statements

        protected override List<SixnetExecutionDatabaseStatement> GetDeleteFieldStatements(SixnetMigrationDatabaseCommand migrationCommand)
        {
            if (migrationCommand?.MigrationInfo?.DeletedFields.IsNullOrEmpty() ?? true)
            {
                return new List<SixnetExecutionDatabaseStatement>(0);
            }

            var statements = new List<SixnetExecutionDatabaseStatement>();
            foreach (var tableItem in migrationCommand.MigrationInfo.DeletedFields)
            {
                if (!tableItem.Value.IsNullOrEmpty())
                {
                    var formattedTableName = FormatAndWrapObjectName(tableItem.Key);
                    foreach (var field in tableItem.Value)
                    {
                        var dataFieldName = FormatObjectName(SixnetDatabaseObjectName.Create(field.GetFieldName(DatabaseType), SixnetDatabaseObjectType.Column));
                        var deleteStatement = new SixnetExecutionDatabaseStatement()
                        {
                            Script = $"IF EXISTS (SELECT 1 FROM sys.columns WHERE [object_id]=OBJECT_ID('{formattedTableName}') AND [name]='{dataFieldName.Name}') BEGIN ALTER TABLE {formattedTableName} DROP COLUMN {WrapObjectName(dataFieldName).Name}; END "
                        };
                        statements.Add(deleteStatement);
                        // Log script
                        LogExecutionStatement(deleteStatement);
                    }
                }
            }
            return statements;
        }

        #endregion

        #region Get update field statements 

        protected override List<SixnetExecutionDatabaseStatement> GetUpdateFieldStatements(SixnetMigrationDatabaseCommand migrationCommand)
        {
            if (migrationCommand?.MigrationInfo?.UpdatedFields.IsNullOrEmpty() ?? true)
            {
                return new List<SixnetExecutionDatabaseStatement>(0);
            }

            var statements = new List<SixnetExecutionDatabaseStatement>();
            foreach (var tableItem in migrationCommand.MigrationInfo.UpdatedFields)
            {
                if (tableItem.Value.IsNullOrEmpty())
                {
                    continue;
                }
                var formattedTableName = FormatAndWrapObjectName(tableItem.Key);
                foreach (var fieldItem in tableItem.Value)
                {
                    var field = fieldItem.Value;
                    var nowFieldName = fieldItem.Key;
                    var newFieldName = FormatObjectName(SixnetDatabaseObjectName.Create(field.GetFieldName(DatabaseType), SixnetDatabaseObjectType.Column));
                    var updateStatement = new SixnetExecutionDatabaseStatement()
                    {
                        Script = $"IF EXISTS (SELECT 1 FROM sys.columns WHERE [object_id]=OBJECT_ID('{formattedTableName}') AND [name]='{nowFieldName}') BEGIN ALTER TABLE {formattedTableName} ALTER COLUMN {WrapObjectName(SixnetDatabaseObjectName.Create(nowFieldName, SixnetDatabaseObjectType.Column)).Name}{GetFieldDefinition(field, migrationCommand.MigrationInfo)}; END "
                    };
                    statements.Add(updateStatement);
                    LogExecutionStatement(updateStatement);
                    if (!string.Equals(nowFieldName, newFieldName.Name, StringComparison.OrdinalIgnoreCase))
                    {
                        var renameStatement = new SixnetExecutionDatabaseStatement()
                        {
                            Script = $"IF EXISTS (SELECT 1 FROM sys.columns WHERE [object_id]=OBJECT_ID('{formattedTableName}') AND [name]='{nowFieldName}') BEGIN EXEC sp_rename '{formattedTableName}.{nowFieldName}', {WrapObjectName(newFieldName).Name}, 'COLUMN'; END "
                        };
                        statements.Add(renameStatement);
                        LogExecutionStatement(renameStatement);
                    }
                }
            }
            return statements;
        }

        #endregion

        #region Get rename table statements

        protected override List<SixnetExecutionDatabaseStatement> GetRenameTableStatements(SixnetMigrationDatabaseCommand migrationCommand)
        {
            var migrationInfo = migrationCommand?.MigrationInfo;
            if (migrationInfo?.RenamedTables.IsNullOrEmpty() ?? true)
            {
                return new List<SixnetExecutionDatabaseStatement>(0);
            }
            var renameTables = migrationInfo.RenamedTables;
            var statements = new List<SixnetExecutionDatabaseStatement>();
            foreach (var tableItem in renameTables)
            {
                var oldFormattedTableName = FormatAndWrapObjectName(tableItem.Key);
                var newFormattedTableName = FormatObjectName(tableItem.Value);
                var renameTableStatement = new SixnetExecutionDatabaseStatement()
                {
                    Script = $"IF OBJECT_ID('{oldFormattedTableName}', 'U') IS NOT NULL BEGIN EXEC sp_rename '{oldFormattedTableName}', '{newFormattedTableName.Name}'; END"
                };
                statements.Add(renameTableStatement);

                // Log script
                LogExecutionStatement(renameTableStatement);
            }
            return statements;
        }

        #endregion

        #region Get limit string

        /// <summary>
        /// Get limit string
        /// </summary>
        /// <param name="offsetNum">Offset num</param>
        /// <param name="takeNum">Take num</param>
        /// <param name="hasSort">Has sort</param>
        /// <returns></returns>
        protected override string GetLimitString(int offsetNum, int takeNum, bool hasSort)
        {
            if (takeNum < 1)
            {
                return string.Empty;
            }
            if (offsetNum < 1 && !hasSort)
            {
                return $" TOP({takeNum}) ";
            }
            return $" OFFSET {offsetNum} ROWS FETCH NEXT {takeNum} ROWS ONLY";

        }

        #endregion

        #region Get field sql data type

        /// <summary>
        /// Get sql data type
        /// </summary>
        /// <param name="field">Field</param>
        /// <returns></returns>
        protected override string GetSqlDataType(SixnetDataField field, SixnetMigrationInfo options)
        {
            SixnetDirectThrower.ThrowArgNullIf(field == null, nameof(field));
            var dbTypeName = "";
            if (!string.IsNullOrWhiteSpace(field.DbType))
            {
                dbTypeName = field.DbType;
            }
            else
            {
                var dbType = field.GetDataType().GetDbType();
                var length = field.Length;
                var precision = field.Precision;
                var notFixedLength = options.NotFixedLength || field.HasDbFeature(SixnetFieldDbFeature.NotFixedLength);
                static int getCharLength(int flength, int defLength) => flength < 1 ? defLength : flength;
                switch (dbType)
                {
                    case DbType.AnsiString:
                        dbTypeName = $"VARCHAR({getCharLength(length, DefaultCharLength)})";
                        break;
                    case DbType.AnsiStringFixedLength:
                        dbTypeName = $"CHAR({getCharLength(length, DefaultCharLength)})";
                        break;
                    case DbType.Binary:
                        dbTypeName = $"VARBINARY({getCharLength(length, DefaultCharLength)})";
                        break;
                    case DbType.Boolean:
                        dbTypeName = "BIT";
                        break;
                    case DbType.Byte:
                        dbTypeName = "TINYINT";
                        break;
                    case DbType.Currency:
                        dbTypeName = "MONEY";
                        break;
                    case DbType.Date:
                        dbTypeName = "DATE";
                        break;
                    case DbType.DateTime:
                        dbTypeName = "DATETIME";
                        break;
                    case DbType.DateTime2:
                        dbTypeName = $"DATETIME2({(length < 1 ? 7 : length)})";
                        break;
                    case DbType.DateTimeOffset:
                        dbTypeName = $"DATETIMEOFFSET({(length < 1 ? 7 : length)})";
                        break;
                    case DbType.Decimal:
                        dbTypeName = $"DECIMAL({(length < 1 ? DefaultDecimalLength : length)}, {(precision < 0 ? DefaultDecimalPrecision : precision)})";
                        break;
                    case DbType.Double:
                        dbTypeName = "FLOAT";
                        break;
                    case DbType.Guid:
                        dbTypeName = "UNIQUEIDENTIFIER";
                        break;
                    case DbType.Int16:
                    case DbType.SByte:
                        dbTypeName = "SMALLINT";
                        break;
                    case DbType.Int32:
                    case DbType.UInt16:
                    case DbType.UInt32:
                        dbTypeName = "INT";
                        break;
                    case DbType.Int64:
                    case DbType.UInt64:
                        dbTypeName = "BIGINT";
                        break;
                    case DbType.Object:
                        dbTypeName = "SQL_VARIANT";
                        break;
                    case DbType.Single:
                        dbTypeName = "REAL";
                        break;
                    case DbType.String:
                        length = getCharLength(length, DefaultCharLength);
                        dbTypeName = notFixedLength
                            ? $"VARCHAR({(length > 8000 ? "MAX" : length.ToString())})"
                            : $"NVARCHAR ({(length > 4000 ? "MAX" : length.ToString())})";
                        break;
                    case DbType.StringFixedLength:
                        dbTypeName = $"NCHAR({getCharLength(length, DefaultCharLength)})";
                        break;
                    case DbType.Time:
                        dbTypeName = $"TIME({(length < 1 ? 7 : length)})";
                        break;
                    case DbType.Xml:
                        dbTypeName = "XML";
                        break;
                    default:
                        throw new NotSupportedException(dbType.ToString());
                }
            }
            return $" {dbTypeName}";
        }

        #endregion

        #region Get field identity

        /// <summary>
        /// Get field identity
        /// </summary>
        /// <param name="field">Field</param>
        /// <param name="options">Options</param>
        /// <returns></returns>
        protected override string GetFieldIdentity(SixnetDataField field, SixnetMigrationInfo options)
        {
            SixnetDirectThrower.ThrowArgNullIf(field == null, nameof(field));
            if (!field.InRole(SixnetFieldRole.Increment))
            {
                return string.Empty;
            }
            var startValue = field.StartValue;
            if (startValue == 0)
            {
                startValue = 1;
            }
            var incrementValue = field.IncrementValue;
            if (incrementValue == 0)
            {
                incrementValue = 1;
            }

            return $" IDENTITY({startValue}, {incrementValue})";
        }

        #endregion

        #region Get delete all view statements

        /// <summary>
        /// Get delete all view statements
        /// </summary>
        /// <param name="migrationCommand"></param>
        /// <returns></returns>
        protected override List<SixnetExecutionDatabaseStatement> GetDeleteAllViewStatements(SixnetMigrationDatabaseCommand migrationCommand)
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
            var deleteScripts = migrationCommand.Connection.DbConnection.Query<string>(sql, transaction: migrationCommand.Connection.Transaction.DbTransaction);
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
        protected override List<SixnetExecutionDatabaseStatement> GetDeleteAllFunctionStatements(SixnetMigrationDatabaseCommand migrationCommand)
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
            var deleteScripts = migrationCommand.Connection.DbConnection.Query<string>(sql, transaction: migrationCommand.Connection.Transaction.DbTransaction);
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
        protected override List<SixnetExecutionDatabaseStatement> GetDeleteAllCustomTypeStatements(SixnetMigrationDatabaseCommand migrationCommand)
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
            var deleteScripts = migrationCommand.Connection.DbConnection.Query<string>(sql, transaction: migrationCommand.Connection.Transaction.DbTransaction);
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
        protected override List<SixnetExecutionDatabaseStatement> GetDeleteAllProcedureStatements(SixnetMigrationDatabaseCommand migrationCommand)
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
            var deleteScripts = migrationCommand.Connection.DbConnection.Query<string>(sql, transaction: migrationCommand.Connection.Transaction.DbTransaction);
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
