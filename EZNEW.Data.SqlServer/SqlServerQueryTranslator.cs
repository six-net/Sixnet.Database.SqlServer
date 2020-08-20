using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using EZNEW.Develop.Command;
using EZNEW.Develop.CQuery;
using EZNEW.Develop.CQuery.CriteriaConverter;
using EZNEW.Develop.CQuery.Translator;
using EZNEW.Develop.Entity;
using EZNEW.Fault;

namespace EZNEW.Data.SqlServer
{
    /// <summary>
    /// Query translator implement for SqlServer
    /// </summary>
    internal class SqlServerQueryTranslator : IQueryTranslator
    {
        #region Fields

        const string EqualOperator = "=";
        const string GreaterThanOperator = ">";
        const string GreaterThanOrEqualOperator = ">=";
        const string NotEqualOperator = "<>";
        const string LessThanOperator = "<";
        const string LessThanOrEqualOperator = "<=";
        const string InOperator = "IN";
        const string NotInOperator = "NOT IN";
        const string LikeOperator = "LIKE";
        const string NotLikeOperator = "NOT LIKE";
        const string IsNullOperator = "IS NULL";
        const string NotNullOperator = "IS NOT NULL";
        const string DescKeyWord = "DESC";
        const string AscKeyWord = "ASC";
        public const string ObjPetName = "TB";
        const string parameterPrefix = "@";
        const string TreeTableName = "RecurveTable";
        const string TreeTablePetName = "RTT";
        static readonly Dictionary<JoinType, string> joinOperatorDict = new Dictionary<JoinType, string>()
        {
            { JoinType.InnerJoin,"INNER JOIN" },
            { JoinType.CrossJoin,"CROSS JOIN" },
            { JoinType.LeftJoin,"LEFT JOIN" },
            { JoinType.RightJoin,"RIGHT JOIN" },
            { JoinType.FullJoin,"FULL JOIN" }
        };

        int subObjectSequence = 0;
        int recurveObjectSequence = 0;

        #endregion

        #region Properties

        /// <summary>
        /// Get the query object pet name
        /// </summary>
        public string ObjectPetName
        {
            get
            {
                return ObjPetName;
            }
        }

        /// <summary>
        /// Gets or sets the parameter sequence
        /// </summary>
        public int ParameterSequence { get; set; } = 0;

        #endregion

        #region Functions

        /// <summary>
        /// Translate Query Object
        /// </summary>
        /// <param name="query">Query object</param>
        /// <returns>Return translate result</returns>
        public TranslateResult Translate(IQuery query)
        {
            Init();
            var result = ExecuteTranslate(query);
            if (!result.WithScripts.IsNullOrEmpty())
            {
                result.PreScript = FormatWithScript(result.WithScripts);
            }
            return result;
        }

        /// <summary>
        /// Execute Translate
        /// </summary>
        /// <param name="query">Query object</param>
        /// <param name="paras">Parameters</param>
        /// <param name="objectName">Query object name</param>
        /// <returns>Return translate result</returns>
        public TranslateResult ExecuteTranslate(IQuery query, CommandParameters paras = null, string objectName = "", bool subQuery = false, bool useOrder = true)
        {
            if (query == null)
            {
                return TranslateResult.Empty;
            }
            StringBuilder conditionBuilder = new StringBuilder();
            if (query.QueryType == QueryCommandType.QueryObject)
            {
                StringBuilder orderBuilder = new StringBuilder();
                CommandParameters parameters = paras ?? new CommandParameters();
                objectName = string.IsNullOrWhiteSpace(objectName) ? ObjPetName : objectName;
                List<string> withScripts = new List<string>();
                string recurveTableName = string.Empty;
                string recurveTablePetName = string.Empty;

                #region query condition

                if (!query.Criterias.IsNullOrEmpty())
                {
                    int index = 0;
                    foreach (var queryItem in query.Criterias)
                    {
                        var queryItemCondition = TranslateCondition(query, queryItem, parameters, objectName);
                        if (!queryItemCondition.WithScripts.IsNullOrEmpty())
                        {
                            withScripts.AddRange(queryItemCondition.WithScripts);
                            recurveTableName = queryItemCondition.RecurveObjectName;
                            recurveTablePetName = queryItemCondition.RecurvePetName;
                        }
                        conditionBuilder.Append($" {(index > 0 ? queryItem.Item1.ToString() : string.Empty)} {queryItemCondition.ConditionString}");
                        index++;
                    }
                }

                #endregion

                #region sort

                if (useOrder && !query.Orders.IsNullOrEmpty())
                {
                    foreach (var orderItem in query.Orders)
                    {
                        orderBuilder.Append($"{ConvertOrderCriteriaName(query, objectName, orderItem)} {(orderItem.Desc ? DescKeyWord : AscKeyWord)},");
                    }
                }

                #endregion

                #region combine

                StringBuilder combineBuilder = new StringBuilder();
                if (!query.CombineItems.IsNullOrEmpty())
                {
                    foreach (var combine in query.CombineItems)
                    {
                        if (combine?.CombineQuery == null)
                        {
                            continue;
                        }
                        var combineObjectPetName = GetNewSubObjectPetName();
                        string combineObjectName = DataManager.GetQueryRelationObjectName(DatabaseServerType.SQLServer, combine.CombineQuery);
                        var combineQueryResult = ExecuteTranslate(combine.CombineQuery, parameters, combineObjectPetName, true, true);
                        string combineConditionString = string.IsNullOrWhiteSpace(combineQueryResult.ConditionString) ? string.Empty : $"WHERE {combineQueryResult.ConditionString}";
                        combineBuilder.Append($" {GetCombineOperator(combine.CombineType)} SELECT {string.Join(",", SqlServerFactory.FormatQueryFields(combineObjectPetName, query, query.GetEntityType(), true, false))} FROM [{combineObjectName}] AS {combineObjectPetName} {(combineQueryResult.AllowJoin ? combineQueryResult.JoinScript : string.Empty)} {combineConditionString}");
                        if (!combineQueryResult.WithScripts.IsNullOrEmpty())
                        {
                            withScripts.AddRange(combineQueryResult.WithScripts);
                            recurveTableName = combineQueryResult.RecurveObjectName;
                            recurveTablePetName = combineQueryResult.RecurvePetName;
                        }
                    }

                }

                #endregion

                #region join

                bool allowJoin = true;
                StringBuilder joinBuilder = new StringBuilder();
                if (!query.JoinItems.IsNullOrEmpty())
                {
                    foreach (var joinItem in query.JoinItems)
                    {
                        if (joinItem == null || joinItem.JoinQuery == null)
                        {
                            continue;
                        }
                        if (joinItem.JoinQuery.GetEntityType() == null)
                        {
                            throw new EZNEWException("IQuery object must set entity type if use in join operation");
                        }
                        string joinObjName = GetNewSubObjectPetName();
                        var joinQueryResult = ExecuteTranslate(joinItem.JoinQuery, parameters, joinObjName, true, true);
                        if (string.IsNullOrWhiteSpace(joinQueryResult.CombineScript))
                        {
                            var joinConnection = GetJoinCondition(query, joinItem, objectName, joinObjName);
                            if (!string.IsNullOrWhiteSpace(joinQueryResult.ConditionString))
                            {
                                if (joinQueryResult.AllowJoin && PositionJoinConditionToConnection(joinItem.JoinType))
                                {
                                    joinConnection += $"{(string.IsNullOrWhiteSpace(joinConnection) ? " ON" : " AND ")}{joinQueryResult.ConditionString}";
                                }
                                else
                                {
                                    conditionBuilder.Append($"{(conditionBuilder.Length == 0 ? string.Empty : " AND ")}{joinQueryResult.ConditionString}");
                                }
                            }

                            joinBuilder.Append($" {GetJoinOperator(joinItem.JoinType)} [{DataManager.GetQueryRelationObjectName(DatabaseServerType.SQLServer, joinItem.JoinQuery)}] AS {joinObjName}{joinConnection}");
                            if (joinQueryResult.AllowJoin && !string.IsNullOrWhiteSpace(joinQueryResult.JoinScript))
                            {
                                joinBuilder.Append($" {joinQueryResult.JoinScript}");
                            }
                        }
                        else
                        {
                            var combineJoinObjName = GetNewSubObjectPetName();
                            var joinConnection = GetJoinCondition(query, joinItem, objectName, combineJoinObjName);
                            joinBuilder.Append($" {GetJoinOperator(joinItem.JoinType)} (SELECT {string.Join(",", SqlServerFactory.FormatQueryFields(joinObjName, joinItem.JoinQuery, joinItem.JoinQuery.GetEntityType(), true, false))} FROM [{DataManager.GetQueryRelationObjectName(DatabaseServerType.SQLServer, joinItem.JoinQuery)}] AS {joinObjName} {(joinQueryResult.AllowJoin ? joinQueryResult.JoinScript : string.Empty)} {(string.IsNullOrWhiteSpace(joinQueryResult.ConditionString) ? string.Empty : "WHERE " + joinQueryResult.ConditionString)} {joinQueryResult.CombineScript}) AS {combineJoinObjName}{joinConnection}");
                        }
                        if (!joinQueryResult.WithScripts.IsNullOrEmpty())
                        {
                            withScripts.AddRange(joinQueryResult.WithScripts);
                            recurveTableName = joinQueryResult.RecurveObjectName;
                            recurveTablePetName = joinQueryResult.RecurvePetName;
                        }
                    }
                }
                string joinScript = joinBuilder.ToString();

                #endregion

                #region recurve script

                string conditionString = conditionBuilder.ToString();
                if (query.RecurveCriteria != null)
                {
                    allowJoin = false;
                    string nowConditionString = conditionString;
                    EntityField recurveField = DataManager.GetField(DatabaseServerType.SQLServer, query, query.RecurveCriteria.Key);
                    EntityField recurveRelationField = DataManager.GetField(DatabaseServerType.SQLServer, query, query.RecurveCriteria.RelationKey);
                    var recurveTable = GetNewRecurveTableName();
                    recurveTablePetName = recurveTable.Item1;
                    recurveTableName = recurveTable.Item2;
                    conditionString = $"{objectName}.[{recurveField.FieldName}] IN (SELECT {recurveTablePetName}.[{recurveField.FieldName}] FROM [{recurveTableName}] AS {recurveTablePetName})";
                    string queryObjectName = DataManager.GetQueryRelationObjectName(DatabaseServerType.SQLServer, query);
                    string withScript =
                        $"{recurveTableName} AS (SELECT {objectName}.[{recurveField.FieldName}],{objectName}.[{recurveRelationField.FieldName}] FROM [{queryObjectName}] AS {objectName} {joinScript} {(string.IsNullOrWhiteSpace(nowConditionString) ? string.Empty : $"WHERE {nowConditionString}")} " +
                        $"UNION ALL SELECT {objectName}.[{recurveField.FieldName}],{objectName}.[{recurveRelationField.FieldName}] FROM [{queryObjectName}] AS {objectName},{recurveTableName} AS {recurveTablePetName} " +
                        $"WHERE {(query.RecurveCriteria.Direction == RecurveDirection.Up ? $"{objectName}.[{recurveField.FieldName}]={recurveTablePetName}.[{recurveRelationField.FieldName}]" : $"{objectName}.[{recurveRelationField.FieldName}]={recurveTablePetName}.[{recurveField.FieldName}]")})";
                    withScripts.Add(withScript);
                }

                #endregion

                var result = TranslateResult.CreateNewResult(conditionString, orderBuilder.ToString().Trim(','), parameters);
                result.JoinScript = joinScript;
                result.AllowJoin = allowJoin;
                result.WithScripts = withScripts;
                result.RecurveObjectName = recurveTableName;
                result.RecurvePetName = recurveTablePetName;
                result.CombineScript = combineBuilder.ToString();
                return result;
            }
            else
            {
                conditionBuilder.Append(query.QueryText);
                return TranslateResult.CreateNewResult(conditionBuilder.ToString(), string.Empty, query.QueryTextParameters);
            }
        }

        /// <summary>
        /// translate query condition
        /// </summary>
        /// <param name="queryItem">Query condition</param>
        /// <returns>Return translate result</returns>
        TranslateResult TranslateCondition(IQuery query, Tuple<QueryOperator, IQueryItem> queryItem, CommandParameters parameters, string objectName)
        {
            if (queryItem == null)
            {
                return TranslateResult.Empty;
            }
            Criteria criteria = queryItem.Item2 as Criteria;
            if (criteria != null)
            {
                return TranslateCriteria(query, criteria, parameters, objectName);
            }
            IQuery groupQuery = queryItem.Item2 as IQuery;
            if (groupQuery != null && !groupQuery.Criterias.IsNullOrEmpty())
            {
                groupQuery.SetEntityType(query.GetEntityType());
                var criteriasCount = groupQuery.Criterias.Count();
                if (criteriasCount == 1)
                {
                    var firstCriterias = groupQuery.Criterias.First();
                    if (firstCriterias.Item2 is Criteria)
                    {
                        return TranslateCriteria(groupQuery, firstCriterias.Item2 as Criteria, parameters, objectName);
                    }
                    return TranslateCondition(groupQuery, firstCriterias, parameters, objectName);
                }
                StringBuilder subCondition = new StringBuilder("(");
                List<string> groupWithScripts = new List<string>();
                string recurveTableName = string.Empty;
                string recurveTablePetName = string.Empty;
                int index = 0;
                foreach (var subQueryItem in groupQuery.Criterias)
                {
                    var subGroupResult = TranslateCondition(groupQuery, subQueryItem, parameters, objectName);
                    if (!subGroupResult.WithScripts.IsNullOrEmpty())
                    {
                        recurveTableName = subGroupResult.RecurveObjectName;
                        recurveTablePetName = subGroupResult.RecurvePetName;
                        groupWithScripts.AddRange(subGroupResult.WithScripts);
                    }
                    subCondition.Append($" {(index > 0 ? subQueryItem.Item1.ToString() : string.Empty)} {subGroupResult.ConditionString}");
                    index++;
                }
                var groupResult = TranslateResult.CreateNewResult(subCondition.Append(")").ToString());
                groupResult.RecurveObjectName = recurveTableName;
                groupResult.RecurvePetName = recurveTablePetName;
                groupResult.WithScripts = groupWithScripts;
                return groupResult;
            }
            return TranslateResult.Empty;
        }

        /// <summary>
        /// Translate Single Criteria
        /// </summary>
        /// <param name="criteria">Criteria</param>
        /// <param name="parameters">Parameters</param>
        /// <returns>Return translate result</returns>
        TranslateResult TranslateCriteria(IQuery query, Criteria criteria, CommandParameters parameters, string objectName)
        {
            if (criteria == null)
            {
                return TranslateResult.Empty;
            }
            string sqlOperator = GetOperator(criteria.Operator);
            bool needParameter = OperatorNeedParameter(criteria.Operator);
            string criteriaFieldName = ConvertCriteriaName(query, objectName, criteria);
            if (!needParameter)
            {
                return TranslateResult.CreateNewResult($"{criteriaFieldName} {sqlOperator}");
            }
            IQuery valueQuery = criteria.Value as IQuery;
            string parameterName = GetNewParameterName(criteria.Name);
            if (valueQuery != null)
            {
                string valueQueryObjectName = DataManager.GetQueryRelationObjectName(DatabaseServerType.SQLServer, valueQuery);
                if (valueQuery.QueryFields.IsNullOrEmpty())
                {
                    throw new EZNEWException($"The {valueQueryObjectName} query object that is a subquery must have at least one query field set");
                }
                var valueQueryField = DataManager.GetField(DatabaseServerType.SQLServer, valueQuery, valueQuery.QueryFields.First());
                string subObjName = GetNewSubObjectPetName();
                string topString = GetSubqueryLimitCondition(sqlOperator, valueQuery.QuerySize);
                var useOrder = !string.IsNullOrWhiteSpace(topString);
                var subQueryResult = ExecuteTranslate(valueQuery, parameters, subObjName, true, useOrder);
                string conditionString = string.IsNullOrWhiteSpace(subQueryResult.ConditionString) ? string.Empty : $"WHERE {subQueryResult.ConditionString}";
                string orderString = string.IsNullOrWhiteSpace(subQueryResult.OrderString) ? string.Empty : $"ORDER BY {subQueryResult.OrderString}";
                string valueQueryCondition = string.IsNullOrWhiteSpace(subQueryResult.CombineScript)
                    ? $"{criteriaFieldName} {sqlOperator} (SELECT {topString} {subObjName}.[{valueQueryField.FieldName}] FROM [{valueQueryObjectName}] AS {subObjName} {(subQueryResult.AllowJoin ? subQueryResult.JoinScript : string.Empty)} {conditionString} {orderString})"
                    : $"{criteriaFieldName} {sqlOperator} (SELECT {topString} {subObjName}.[{valueQueryField.FieldName}] FROM (SELECT {string.Join(",", SqlServerFactory.FormatQueryFields(subObjName, valueQuery, valueQuery.GetEntityType(), true, false))} FROM [{valueQueryObjectName}] AS {subObjName} {(subQueryResult.AllowJoin ? subQueryResult.JoinScript : string.Empty)} {conditionString} {subQueryResult.CombineScript}) AS {subObjName} {orderString})";
                var valueQueryResult = TranslateResult.CreateNewResult(valueQueryCondition);
                if (!subQueryResult.WithScripts.IsNullOrEmpty())
                {
                    valueQueryResult.WithScripts = new List<string>(subQueryResult.WithScripts);
                    valueQueryResult.RecurveObjectName = subQueryResult.RecurveObjectName;
                    valueQueryResult.RecurvePetName = subQueryResult.RecurvePetName;
                }
                return valueQueryResult;
            }
            parameters.Add(parameterName, FormatCriteriaValue(criteria.Operator, criteria.GetCriteriaRealValue()));
            var criteriaCondition = $"{criteriaFieldName} {sqlOperator} {parameterPrefix}{parameterName}";
            return TranslateResult.CreateNewResult(criteriaCondition);
        }

        /// <summary>
        /// Get sql operator by condition operator
        /// </summary>
        /// <param name="criteriaOperator">Criteeria operation</param>
        /// <returns></returns>
        string GetOperator(CriteriaOperator criteriaOperator)
        {
            string sqlOperator = string.Empty;
            switch (criteriaOperator)
            {
                case CriteriaOperator.Equal:
                    sqlOperator = EqualOperator;
                    break;
                case CriteriaOperator.GreaterThan:
                    sqlOperator = GreaterThanOperator;
                    break;
                case CriteriaOperator.GreaterThanOrEqual:
                    sqlOperator = GreaterThanOrEqualOperator;
                    break;
                case CriteriaOperator.NotEqual:
                    sqlOperator = NotEqualOperator;
                    break;
                case CriteriaOperator.LessThan:
                    sqlOperator = LessThanOperator;
                    break;
                case CriteriaOperator.LessThanOrEqual:
                    sqlOperator = LessThanOrEqualOperator;
                    break;
                case CriteriaOperator.In:
                    sqlOperator = InOperator;
                    break;
                case CriteriaOperator.NotIn:
                    sqlOperator = NotInOperator;
                    break;
                case CriteriaOperator.Like:
                case CriteriaOperator.BeginLike:
                case CriteriaOperator.EndLike:
                    sqlOperator = LikeOperator;
                    break;
                case CriteriaOperator.NotLike:
                case CriteriaOperator.NotBeginLike:
                case CriteriaOperator.NotEndLike:
                    sqlOperator = NotLikeOperator;
                    break;
                case CriteriaOperator.IsNull:
                    sqlOperator = IsNullOperator;
                    break;
                case CriteriaOperator.NotNull:
                    sqlOperator = NotNullOperator;
                    break;
            }
            return sqlOperator;
        }

        /// <summary>
        /// Operator need parameter
        /// </summary>
        /// <param name="criteriaOperator">Criteria operator</param>
        /// <returns></returns>
        bool OperatorNeedParameter(CriteriaOperator criteriaOperator)
        {
            bool needParameter = true;
            switch (criteriaOperator)
            {
                case CriteriaOperator.NotNull:
                case CriteriaOperator.IsNull:
                    needParameter = false;
                    break;
            }
            return needParameter;
        }

        /// <summary>
        /// Format Value
        /// </summary>
        /// <param name="criteriaOperator">Condition operator</param>
        /// <param name="value">value</param>
        /// <returns></returns>
        dynamic FormatCriteriaValue(CriteriaOperator criteriaOperator, dynamic value)
        {
            dynamic realValue = value;
            switch (criteriaOperator)
            {
                case CriteriaOperator.Like:
                case CriteriaOperator.NotLike:
                    realValue = $"%{value}%";
                    break;
                case CriteriaOperator.BeginLike:
                case CriteriaOperator.NotBeginLike:
                    realValue = $"{value}%";
                    break;
                case CriteriaOperator.EndLike:
                case CriteriaOperator.NotEndLike:
                    realValue = $"%{value}";
                    break;
            }
            return realValue;
        }

        /// <summary>
        /// Convert criteria
        /// </summary>
        /// <param name="objectName">Object name</param>
        /// <param name="criteria">Criteria</param>
        /// <returns></returns>
        string ConvertCriteriaName(IQuery query, string objectName, Criteria criteria)
        {
            return FormatCriteriaName(query, objectName, criteria.Name, criteria.Converter);
        }

        /// <summary>
        /// Convert order criteria name
        /// </summary>
        /// <param name="objectName">Object name</param>
        /// <param name="orderCriteria">Order criteria</param>
        /// <returns></returns>
        string ConvertOrderCriteriaName(IQuery query, string objectName, SortCriteria orderCriteria)
        {
            return FormatCriteriaName(query, objectName, orderCriteria.Name, orderCriteria.Converter);
        }

        /// <summary>
        /// Format criteria name
        /// </summary>
        /// <param name="objectName">Object name</param>
        /// <param name="fieldName">Field name</param>
        /// <param name="convert">Criteria convert</param>
        /// <returns></returns>
        string FormatCriteriaName(IQuery query, string objectName, string fieldName, ICriteriaConverter convert)
        {
            var field = DataManager.GetField(DatabaseServerType.SQLServer, query, fieldName);
            fieldName = field.FieldName;
            if (convert == null)
            {
                return $"{objectName}.[{fieldName}]";
            }
            return SqlServerFactory.ParseCriteriaConverter(convert, objectName, fieldName);
        }

        /// <summary>
        /// Get join operator
        /// </summary>
        /// <param name="joinType">Join type</param>
        /// <returns></returns>
        string GetJoinOperator(JoinType joinType)
        {
            return joinOperatorDict[joinType];
        }

        /// <summary>
        /// Determines whether position join condition to connection
        /// </summary>
        /// <param name="joinType">Join type</param>
        /// <returns></returns>
        bool PositionJoinConditionToConnection(JoinType joinType)
        {
            switch (joinType)
            {
                case JoinType.CrossJoin:
                    return false;
                case JoinType.InnerJoin:
                case JoinType.LeftJoin:
                case JoinType.RightJoin:
                case JoinType.FullJoin:
                default:
                    return true;
            }
        }

        /// <summary>
        /// Get join condition
        /// </summary>
        /// <param name="sourceQuery">source query</param>
        /// <param name="joinItem">join item</param>
        /// <returns></returns>
        string GetJoinCondition(IQuery sourceQuery, JoinItem joinItem, string sourceObjShortName, string targetObjShortName)
        {
            if (joinItem.JoinType == JoinType.CrossJoin)
            {
                return string.Empty;
            }
            var joinFields = joinItem?.JoinFields.Where(r => !string.IsNullOrWhiteSpace(r.Key) && !string.IsNullOrWhiteSpace(r.Value));
            var sourceEntityType = sourceQuery.GetEntityType();
            var targetEntityType = joinItem.JoinQuery.GetEntityType();
            bool useValueAsSource = false;
            if (joinFields.IsNullOrEmpty())
            {
                if (sourceEntityType == targetEntityType)
                {
                    var primaryKeys = EntityManager.GetPrimaryKeys(sourceEntityType);
                    if (primaryKeys.IsNullOrEmpty())
                    {
                        return string.Empty;
                    }
                    joinFields = primaryKeys.ToDictionary(c => c, c => c);
                }
                else
                {
                    joinFields = EntityManager.GetRelationFields(sourceEntityType, targetEntityType);
                    if (joinFields.IsNullOrEmpty())
                    {
                        useValueAsSource = true;
                        joinFields = EntityManager.GetRelationFields(targetEntityType, sourceEntityType);
                    }
                }
                if (joinFields.IsNullOrEmpty())
                {
                    return string.Empty;
                }
            }
            List<string> joinList = new List<string>();
            foreach (var joinField in joinFields)
            {
                if (string.IsNullOrWhiteSpace(joinField.Key) || string.IsNullOrWhiteSpace(joinField.Value))
                {
                    continue;
                }
                var sourceField = DataManager.GetField(DatabaseServerType.SQLServer, sourceEntityType, joinField.Key);
                var targetField = DataManager.GetField(DatabaseServerType.SQLServer, targetEntityType, joinField.Value);
                joinList.Add($" {sourceObjShortName}.[{(useValueAsSource ? targetField.FieldName : sourceField.FieldName)}]{GetJoinOperator(joinItem.Operator)}{targetObjShortName}.[{(useValueAsSource ? sourceField.FieldName : targetField.FieldName)}]");
            }
            return joinList.IsNullOrEmpty() ? string.Empty : " ON" + string.Join(" AND", joinList);
        }

        /// <summary>
        /// Get sql operator by condition operator
        /// </summary>
        /// <param name="joinOperator"></param>
        /// <returns></returns>
        string GetJoinOperator(JoinOperator joinOperator)
        {
            string sqlOperator = string.Empty;
            switch (joinOperator)
            {
                case JoinOperator.Equal:
                    sqlOperator = EqualOperator;
                    break;
                case JoinOperator.GreaterThan:
                    sqlOperator = GreaterThanOperator;
                    break;
                case JoinOperator.GreaterThanOrEqual:
                    sqlOperator = GreaterThanOrEqualOperator;
                    break;
                case JoinOperator.NotEqual:
                    sqlOperator = NotEqualOperator;
                    break;
                case JoinOperator.LessThan:
                    sqlOperator = LessThanOperator;
                    break;
                case JoinOperator.LessThanOrEqual:
                    sqlOperator = LessThanOrEqualOperator;
                    break;
            }
            return sqlOperator;
        }

        /// <summary>
        /// Format with script
        /// </summary>
        /// <returns></returns>
        string FormatWithScript(List<string> withScripts)
        {
            if (withScripts.IsNullOrEmpty())
            {
                return string.Empty;
            }
            return $"WITH {string.Join(",", withScripts)}";
        }

        /// <summary>
        /// Get new recurve table name
        /// item1:petname,item2:fullname
        /// </summary>
        /// <returns></returns>
        Tuple<string, string> GetNewRecurveTableName()
        {
            var recurveIndex = (recurveObjectSequence++).ToString();
            return new Tuple<string, string>
                (
                    $"{TreeTablePetName}{recurveIndex}",
                    $"{TreeTableName}{recurveIndex}"
                );
        }

        /// <summary>
        /// Get new sub object pet name
        /// </summary>
        /// <returns></returns>
        string GetNewSubObjectPetName()
        {
            return $"TSB{subObjectSequence++}";
        }

        /// <summary>
        /// Get new parameter name
        /// </summary>
        /// <returns></returns>
        string GetNewParameterName(string originParameterName)
        {
            return $"{originParameterName}{ParameterSequence++}";
        }

        /// <summary>
        /// Init
        /// </summary>
        void Init()
        {
            recurveObjectSequence = subObjectSequence = 0;
        }

        /// <summary>
        /// Get sub query limit condition
        /// </summary>
        /// <param name="sqlOperator"></param>
        /// <param name="querySize"></param>
        string GetSubqueryLimitCondition(string sqlOperator, int querySize)
        {
            var limitString = string.Empty;
            switch (sqlOperator)
            {
                case InOperator:
                case NotInOperator:
                    if (querySize > 0)
                    {
                        limitString = $"TOP {querySize}";
                    }
                    break;
                default:
                    limitString = $"TOP 1";
                    break;
            }
            return limitString;
        }

        /// <summary>
        /// Get combine operator
        /// </summary>
        /// <param name="combineType">Combine type</param>
        /// <returns>Return combine operator</returns>
        string GetCombineOperator(CombineType combineType)
        {
            switch (combineType)
            {
                case CombineType.UnionAll:
                default:
                    return "UNION ALL";
                case CombineType.Union:
                    return "UNION";
                case CombineType.Except:
                    return "EXCEPT";
                case CombineType.Intersect:
                    return "INTERSECT";
            }
        }

        #endregion
    }
}
