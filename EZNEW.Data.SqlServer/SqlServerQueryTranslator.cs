using EZNEW.Develop.Command;
using EZNEW.Develop.CQuery;
using EZNEW.Develop.CQuery.CriteriaConvert;
using EZNEW.Develop.CQuery.Translator;
using EZNEW.Develop.Entity;
using EZNEW.Framework.Extension;
using EZNEW.Framework.Fault;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace EZNEW.Data.SqlServer
{
    /// <summary>
    /// Query Translator Implement For SqlServer DataBase
    /// </summary>
    internal class SqlServerQueryTranslator : IQueryTranslator
    {
        #region fields

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

        #region propertys

        /// <summary>
        /// Query Object Pet Name
        /// </summary>
        public string ObjectPetName
        {
            get
            {
                return ObjPetName;
            }
        }

        /// <summary>
        /// parameter sequence
        /// </summary>
        public int ParameterSequence { get; set; } = 0;

        #endregion

        #region functions

        /// <summary>
        /// Translate Query Object
        /// </summary>
        /// <param name="query">query object</param>
        /// <returns>translate result</returns>
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
        /// <param name="query">query object</param>
        /// <param name="paras">parameters</param>
        /// <param name="objectName">query object name</param>
        /// <returns></returns>
        public TranslateResult ExecuteTranslate(IQuery query, CmdParameters paras = null, string objectName = "", bool subQuery = false)
        {
            if (query == null)
            {
                return TranslateResult.Empty;
            }
            StringBuilder conditionBuilder = new StringBuilder();
            if (query.QueryType == QueryCommandType.QueryObject)
            {
                StringBuilder orderBuilder = new StringBuilder();
                CmdParameters parameters = paras ?? new CmdParameters();
                objectName = string.IsNullOrWhiteSpace(objectName) ? ObjPetName : objectName;
                List<string> withScripts = new List<string>();
                string recurveTableName = string.Empty;
                string recurveTablePetName = string.Empty;

                #region query condition

                if (query.Criterias != null && query.Criterias.Count > 0)
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

                if (!subQuery && query.Orders != null && query.Orders.Count > 0)
                {
                    foreach (var orderItem in query.Orders)
                    {
                        orderBuilder.Append($"{ConvertOrderCriteriaName(query, objectName, orderItem)} {(orderItem.Desc ? DescKeyWord : AscKeyWord)},");
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
                        var joinQueryResult = ExecuteTranslate(joinItem.JoinQuery, parameters, joinObjName, true);
                        if (!joinQueryResult.ConditionString.IsNullOrEmpty())
                        {
                            conditionBuilder.Append($"{(conditionBuilder.Length == 0 ? string.Empty : " AND")}{joinQueryResult.ConditionString}");
                        }
                        joinBuilder.Append($"{GetJoinOperator(joinItem.JoinType)} [{DataManager.GetQueryRelationObjectName(ServerType.SQLServer, joinItem.JoinQuery)}] AS {joinObjName}{GetJoinCondition(query, joinItem, objectName, joinObjName)}");
                        if (!joinQueryResult.JoinScript.IsNullOrEmpty())
                        {
                            joinBuilder.Append($" {joinQueryResult.JoinScript}");
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
                    EntityField recurveField = DataManager.GetField(ServerType.SQLServer, query, query.RecurveCriteria.Key);
                    EntityField recurveRelationField = DataManager.GetField(ServerType.SQLServer, query, query.RecurveCriteria.RelationKey);
                    var recurveTable = GetNewRecurveTableName();
                    recurveTablePetName = recurveTable.Item1;
                    recurveTableName = recurveTable.Item2;
                    conditionString = $"{objectName}.[{recurveField.FieldName}] IN (SELECT {recurveTablePetName}.[{recurveField.FieldName}] FROM [{recurveTableName}] AS {recurveTablePetName})";
                    string queryObjectName = DataManager.GetQueryRelationObjectName(ServerType.SQLServer, query);
                    string withScript =
                        $"{recurveTableName} AS (SELECT {objectName}.[{recurveField.FieldName}],{objectName}.[{recurveRelationField.FieldName}] FROM [{queryObjectName}] AS {objectName} {joinScript} {(string.IsNullOrWhiteSpace(nowConditionString) ? string.Empty : $"WHERE {nowConditionString}")} " +
                        $"UNION ALL SELECT {objectName}.[{recurveField.FieldName}],{objectName}.[{recurveRelationField.FieldName}] FROM [{queryObjectName}] AS {objectName},{recurveTableName} AS {recurveTablePetName} " +
                        $"WHERE {(query.RecurveCriteria.Direction == RecurveDirection.Up ? $"{objectName}.[{recurveField.FieldName}]={recurveTablePetName}.[{recurveRelationField.FieldName}]" : $"{objectName}.[{recurveRelationField.FieldName}]={recurveTablePetName}.[{recurveField.FieldName}]")})";
                    withScripts.Add(withScript);
                }
                var result = TranslateResult.CreateNewResult(conditionString, orderBuilder.ToString().Trim(','), parameters);
                result.JoinScript = joinScript;
                result.AllowJoin = allowJoin;
                result.WithScripts = withScripts;
                result.RecurveObjectName = recurveTableName;
                result.RecurvePetName = recurveTablePetName;

                #endregion

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
        /// <param name="queryItem">query condition</param>
        /// <returns></returns>
        TranslateResult TranslateCondition(IQuery query, Tuple<QueryOperator, IQueryItem> queryItem, CmdParameters parameters, string objectName)
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
            if (groupQuery != null && groupQuery.Criterias != null && groupQuery.Criterias.Count > 0)
            {
                groupQuery.SetEntityType(query.GetEntityType());
                if (groupQuery.Criterias.Count == 1)
                {
                    var firstCriterias = groupQuery.Criterias[0];
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
        /// <param name="criteria">criteria</param>
        /// <param name="parameters">parameters</param>
        /// <returns></returns>
        TranslateResult TranslateCriteria(IQuery query, Criteria criteria, CmdParameters parameters, string objectName)
        {
            if (criteria == null)
            {
                return TranslateResult.Empty;
            }
            string sqlOperator = GetOperator(criteria.Operator);
            bool needParameter = OperatorNeedParameter(criteria.Operator);
            if (!needParameter)
            {
                return TranslateResult.CreateNewResult($"{ConvertCriteriaName(query, objectName, criteria)} {sqlOperator}");
            }
            IQuery valueQuery = criteria.Value as IQuery;
            string parameterName = GetNewParameterName(criteria.Name);
            if (valueQuery != null)
            {
                string valueQueryObjectName = DataManager.GetQueryRelationObjectName(ServerType.SQLServer, valueQuery);
                var valueQueryField = DataManager.GetField(ServerType.SQLServer, valueQuery, valueQuery.QueryFields[0]);
                string subObjName = GetNewSubObjectPetName();
                var subQueryResult = ExecuteTranslate(valueQuery, parameters, subObjName, true);
                string topString = string.Empty;
                if (sqlOperator != InOperator && sqlOperator != NotInOperator)
                {
                    topString = "TOP 1";
                }
                string conditionString = subQueryResult.ConditionString;
                if (!string.IsNullOrWhiteSpace(conditionString))
                {
                    conditionString = "WHERE " + conditionString;
                }
                var valueQueryCondition = $"{ConvertCriteriaName(valueQuery, objectName, criteria)} {sqlOperator} (SELECT {topString} {subObjName}.[{valueQueryField.FieldName}] FROM [{valueQueryObjectName}] AS {subObjName} {(subQueryResult.AllowJoin ? subQueryResult.JoinScript : string.Empty)} {conditionString} {subQueryResult.OrderString})";
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
            var criteriaCondition = $"{ConvertCriteriaName(query, objectName, criteria)} {sqlOperator} {parameterPrefix}{parameterName}";
            return TranslateResult.CreateNewResult(criteriaCondition);
        }

        /// <summary>
        /// get sql operator by condition operator
        /// </summary>
        /// <param name="criteriaOperator"></param>
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
        /// operator need parameter
        /// </summary>
        /// <param name="criteriaOperator">criteria operator</param>
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
        /// <param name="criteriaOperator">condition operator</param>
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
        /// convert criteria
        /// </summary>
        /// <param name="objectName">object name</param>
        /// <param name="criteria">criteria</param>
        /// <returns></returns>
        string ConvertCriteriaName(IQuery query, string objectName, Criteria criteria)
        {
            return FormatCriteriaName(query, objectName, criteria.Name, criteria.Convert);
        }

        /// <summary>
        /// convert order criteria name
        /// </summary>
        /// <param name="objectName">object name</param>
        /// <param name="orderCriteria">order criteria</param>
        /// <returns></returns>
        string ConvertOrderCriteriaName(IQuery query, string objectName, OrderCriteria orderCriteria)
        {
            return FormatCriteriaName(query, objectName, orderCriteria.Name, orderCriteria.Convert);
        }

        /// <summary>
        /// format criteria name
        /// </summary>
        /// <param name="objectName">object name</param>
        /// <param name="fieldName">field name</param>
        /// <param name="convert">convert</param>
        /// <returns></returns>
        string FormatCriteriaName(IQuery query, string objectName, string fieldName, ICriteriaConvert convert)
        {
            var field = DataManager.GetField(ServerType.SQLServer, query, fieldName);
            fieldName = field.FieldName;
            if (convert == null)
            {
                return $"{objectName}.[{fieldName}]";
            }
            return SqlServerFactory.ParseCriteriaConvert(convert, objectName, fieldName);
        }

        /// <summary>
        /// get join operator
        /// </summary>
        /// <param name="joinType">join type</param>
        /// <returns></returns>
        string GetJoinOperator(JoinType joinType)
        {
            return joinOperatorDict[joinType];
        }

        /// <summary>
        /// get join condition
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
            var joinFields = joinItem?.JoinFields.Where(r => !r.Key.IsNullOrEmpty() && !r.Value.IsNullOrEmpty());
            var sourceEntityType = sourceQuery.GetEntityType();
            var targetEntityType = joinItem.JoinQuery.GetEntityType();
            bool useValueAsSource = false;
            if (joinFields.IsNullOrEmpty())
            {
                joinFields = EntityManager.GetRelationFields(sourceEntityType, targetEntityType);
            }
            if (joinFields.IsNullOrEmpty())
            {
                useValueAsSource = true;
                joinFields = EntityManager.GetRelationFields(targetEntityType, sourceEntityType);
            }
            if (joinFields.IsNullOrEmpty())
            {
                return string.Empty;
            }

            List<string> joinList = new List<string>();
            foreach (var joinField in joinFields)
            {
                if (joinField.Key.IsNullOrEmpty() || joinField.Value.IsNullOrEmpty())
                {
                    continue;
                }
                var sourceField = DataManager.GetField(ServerType.SQLServer, sourceEntityType, joinField.Key);
                var targetField = DataManager.GetField(ServerType.SQLServer, targetEntityType, joinField.Value);
                joinList.Add($" {sourceObjShortName}.[{(useValueAsSource ? targetField.FieldName : sourceField.FieldName)}]{GetJoinOperator(joinItem.Operator)}{targetObjShortName}.[{(useValueAsSource ? sourceField.FieldName : targetField.FieldName)}]");
            }
            return joinList.IsNullOrEmpty() ? string.Empty : " ON" + string.Join(" AND", joinList);
        }

        /// <summary>
        /// get sql operator by condition operator
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
        /// format with script
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
        /// get new recurve table name
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
        /// get new sub object pet name
        /// </summary>
        /// <returns></returns>
        string GetNewSubObjectPetName()
        {
            return $"TSB{subObjectSequence++}";
        }

        /// <summary>
        /// get new parameter name
        /// </summary>
        /// <returns></returns>
        string GetNewParameterName(string originParameterName)
        {
            return $"{originParameterName}{ParameterSequence++}";
        }

        /// <summary>
        /// init
        /// </summary>
        void Init()
        {
            recurveObjectSequence = subObjectSequence = 0;
        }

        #endregion
    }
}
