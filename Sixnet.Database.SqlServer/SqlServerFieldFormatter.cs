using System;
using System.Collections.Generic;
using Sixnet.Development.Data.Field.Formatting;
using Sixnet.Exceptions;

namespace Sixnet.Database.SqlServer
{
    /// <summary>
    /// Default field formatter for sqlserver
    /// </summary>
    public class SqlServerFieldFormatter : ISixnetFieldFormatter
    {
        static List<StringComparison> StringIgnoreCaseValues = new List<StringComparison>()
        {
            StringComparison.OrdinalIgnoreCase,
            StringComparison.InvariantCultureIgnoreCase,
            StringComparison.CurrentCultureIgnoreCase
        };

        public string Format(FormatFieldContext context)
        {
            var formatOption = context.FormatSetting;
            var formatedFieldName = context.FieldName;
            var parameterString = formatOption.Parameter?.ToString();
            formatedFieldName = formatOption.Name switch
            {
                FieldFormatterNames.TO_STRING => $"CAST({formatedFieldName} AS NVARCHAR({(string.IsNullOrWhiteSpace(parameterString) ? "MAX" : parameterString)}))",
                FieldFormatterNames.DISTINCT => $"DISTINCT {formatedFieldName}",
                FieldFormatterNames.IS_NULL => $"{formatedFieldName} IS NULL",
                FieldFormatterNames.NOT_NULL => $"{formatedFieldName} IS NOT NULL",
                FieldFormatterNames.CHARLENGTH => $"LEN({formatedFieldName})",
                FieldFormatterNames.COUNT => $"COUNT({formatedFieldName})",
                FieldFormatterNames.SUM => $"SUM({formatedFieldName})",
                FieldFormatterNames.MAX => $"MAX({formatedFieldName})",
                FieldFormatterNames.MIN => $"MIN({formatedFieldName})",
                FieldFormatterNames.AVG => $"AVG({formatedFieldName})",
                FieldFormatterNames.JSON_VALUE => $"JSON_VALUE({formatedFieldName},{parameterString})",
                FieldFormatterNames.JSON_OBJECT => $"JSON_QUERY({formatedFieldName},{parameterString})",
                FieldFormatterNames.AND => $"({formatedFieldName}&{parameterString})",
                FieldFormatterNames.OR => $"({formatedFieldName}|{parameterString})",
                FieldFormatterNames.XOR => $"({formatedFieldName}^{parameterString})",
                FieldFormatterNames.NOT => $"(~{formatedFieldName})",
                FieldFormatterNames.ADD => $"({formatedFieldName}+{parameterString})",
                FieldFormatterNames.SUBTRACT => $"({formatedFieldName}-{parameterString})",
                FieldFormatterNames.MULTIPLY => $"({formatedFieldName}*{parameterString})",
                FieldFormatterNames.DIVIDE => $"({formatedFieldName}/{parameterString})",
                FieldFormatterNames.MODULO => $"({formatedFieldName}%{parameterString})",
                FieldFormatterNames.LEFT_SHIFT => $"({formatedFieldName}<<{parameterString})",
                FieldFormatterNames.RIGHT_SHIFT => $"({formatedFieldName}>>{parameterString})",
                FieldFormatterNames.TRIM => StringTrim(formatedFieldName, formatOption.Parameter),
                FieldFormatterNames.TRIM_START => StringTrimStart(formatedFieldName, formatOption.Parameter),
                FieldFormatterNames.TRIM_END => StringTrimEnd(formatedFieldName, formatOption.Parameter),
                FieldFormatterNames.STRING_CONCAT => $"({formatedFieldName}+{parameterString})",
                FieldFormatterNames.DATE_TIME_DATE => $"CAST({formatedFieldName} AS DATE)",
                FieldFormatterNames.DATE_TIME_YEAR => $"DATEPART(YEAR,{formatedFieldName})",
                FieldFormatterNames.DATE_TIME_MONTH => $"DATEPART(MONTH,{formatedFieldName})",
                FieldFormatterNames.DATE_TIME_DAY => $"DATEPART(DAY,{formatedFieldName})",
                FieldFormatterNames.DATE_TIME_DAY_OF_YEAR => $"DATEPART(DAYOFYEAR,{formatedFieldName})",
                FieldFormatterNames.DATE_TIME_DAY_OF_WEEK => $"(DATEPART(WEEKDAY,{formatedFieldName}) - 1)",
                FieldFormatterNames.DATE_TIME_HOUR => $"DATEPART(HOUR,{formatedFieldName})",
                FieldFormatterNames.DATE_TIME_MINUTE => $"DATEPART(MINUTE,{formatedFieldName})",
                FieldFormatterNames.DATE_TIME_SECOND => $"DATEPART(SECOND,{formatedFieldName})",
                FieldFormatterNames.DATE_TIME_MILLISECOND => $"DATEPART(MILLISECOND, {formatedFieldName})",
                FieldFormatterNames.DATE_TIME_TIME_OF_DAY => $"CAST({formatedFieldName} AS TIME)",
                FieldFormatterNames.DATE_TIME_UTC => $"SWITCHOFFSET({formatedFieldName}, '+00:00')",
                FieldFormatterNames.DATE_TIME_FORMAT_STRING => $"FORMAT({formatedFieldName}, '{parameterString}')",
                FieldFormatterNames.DATE_TIME_STRING => $"CONVERT(VARCHAR(20), {formatedFieldName}, 120)",
                FieldFormatterNames.DATE_TIME_WITH_MILLISECOND_STRING => $"CONVERT(VARCHAR(25), {formatedFieldName}, 121)",
                FieldFormatterNames.DATE_STRING => $"CONVERT(VARCHAR(15), {formatedFieldName}, 23)",
                FieldFormatterNames.US_DATE_STRING => $"CONVERT(VARCHAR(15), {formatedFieldName}, 101)",
                FieldFormatterNames.JAPAN_DATE_STRING => $"CONVERT(VARCHAR(15), {formatedFieldName}, 111)",
                FieldFormatterNames.TIME_SPAN_DAYS => $"DATEDIFF(DAY, {formatedFieldName?.Trim('(').Replace("-", ",")}",
                FieldFormatterNames.TIME_SPAN_TOTAL_DAYS => $"(DATEDIFF_BIG(SECOND, {formatedFieldName?.Trim('(').Replace("-", ",")} / 86400.0)",
                FieldFormatterNames.TIME_SPAN_HOURS => $"DATEDIFF(HOUR, {formatedFieldName?.Trim('(').Replace("-", ",")}",
                FieldFormatterNames.TIME_SPAN_TOTAL_HOURS => $"(DATEDIFF_BIG(SECOND, {formatedFieldName?.Trim('(').Replace("-", ",")} / 3600.0)",
                FieldFormatterNames.TIME_SPAN_MINUTES => $"DATEDIFF(MINUTE, {formatedFieldName?.Trim('(').Replace("-", ",")}",
                FieldFormatterNames.TIME_SPAN_TOTAL_MINUTES => $"(DATEDIFF_BIG(SECOND, {formatedFieldName?.Trim('(').Replace("-", ",")} / 60.0)",
                FieldFormatterNames.TIME_SPAN_SECONDS => $"DATEDIFF(SECOND, {formatedFieldName?.Trim('(').Replace("-", ",")}",
                FieldFormatterNames.TIME_SPAN_TOTAL_SECONDS => $"(DATEDIFF(MILLISECOND, {formatedFieldName?.Trim('(').Replace("-", ",")} / 1000.0)",
                FieldFormatterNames.TIME_SPAN_MILLISECONDS => $"DATEDIFF(MILLISECOND, {formatedFieldName?.Trim('(').Replace("-", ",")}",
                FieldFormatterNames.TIME_SPAN_TOTAL_MILLISECONDS => $"(DATEDIFF(MICROSECOND, {formatedFieldName?.Trim('(').Replace("-", ",")} / 1000.0)",
                FieldFormatterNames.TO_LOWER => $"LOWER({formatedFieldName})",
                FieldFormatterNames.TO_UPPER => $"UPPER({formatedFieldName})",
                FieldFormatterNames.SUB_STRING => Substring(formatedFieldName, formatOption.Parameter),
                FieldFormatterNames.STRING_REPLACE => ReplaceString(formatedFieldName, formatOption.Parameter),
                FieldFormatterNames.DATE_TIME_ADD_DAY => $"DATEADD(DAY, {parameterString}, {formatedFieldName})",
                FieldFormatterNames.DATE_TIME_ADD_MONTH => $"DATEADD(MONTH, {parameterString}, {formatedFieldName})",
                FieldFormatterNames.DATE_TIME_ADD_YEAR => $"DATEADD(YEAR, {parameterString}, {formatedFieldName})",
                FieldFormatterNames.DATE_TIME_ADD_HOUR => $"DATEADD(HOUR, {parameterString}, {formatedFieldName})",
                FieldFormatterNames.DATE_TIME_ADD_MINUTE => $"DATEADD(MINUTE, {parameterString}, {formatedFieldName})",
                FieldFormatterNames.DATE_TIME_ADD_SECOND => $"DATEADD(SECOND, {parameterString}, {formatedFieldName})",
                FieldFormatterNames.DATE_TIME_ADD_MILLISECOND => $"DATEADD(MILLISECOND, {parameterString}, {formatedFieldName})",
                FieldFormatterNames.CONVERT_TO_INT => $"CAST({formatedFieldName} AS INT)",
                FieldFormatterNames.CONVERT_TO_BOOLEAN => $"CAST({formatedFieldName} AS BIT)",
                FieldFormatterNames.CONVERT_TO_BYTE => $"CAST({formatedFieldName} AS TINYINT)",
                FieldFormatterNames.CONVERT_TO_CHAR => $"CAST({formatedFieldName} AS NCHAR(4000))",
                FieldFormatterNames.CONVERT_TO_DATE_TIME => $"CAST({formatedFieldName} AS DATETIME)",
                FieldFormatterNames.CONVERT_TO_DECIMAL => $"CAST({formatedFieldName} AS DECIMAL(20, 4))",
                FieldFormatterNames.CONVERT_TO_DOUBLE => $"CAST({formatedFieldName} AS FLOAT)",
                FieldFormatterNames.CONVERT_TO_INT_16 => $"CAST({formatedFieldName} AS SMALLINT)",
                FieldFormatterNames.CONVERT_TO_INT_64 => $"CAST({formatedFieldName} AS BIGINT)",
                FieldFormatterNames.CONVERT_TO_SBYTE => $"CAST({formatedFieldName} AS SMALLINT)",
                FieldFormatterNames.CONVERT_TO_SINGLE => $"CAST({formatedFieldName} AS REAL)",
                FieldFormatterNames.CONVERT_TO_UINT_16 => $"CAST({formatedFieldName} AS INT)",
                FieldFormatterNames.CONVERT_TO_UINT_32 => $"CAST({formatedFieldName} AS INT)",
                FieldFormatterNames.CONVERT_TO_UINT_64 => $"CAST({formatedFieldName} AS BIGINT)",
                FieldFormatterNames.MATH_ROUND => $"ROUND({formatedFieldName}, parameterString)",
                FieldFormatterNames.MATH_ABS => $"ABS({formatedFieldName})",
                FieldFormatterNames.MATH_CEILING => $"CEILING({formatedFieldName})",
                FieldFormatterNames.MATH_FLOOR => $"FLOOR({formatedFieldName})",
                FieldFormatterNames.MATH_TRUNCATE => $"ROUND({formatedFieldName}, 0, 1)",
                FieldFormatterNames.MATH_SIGN => $"SIGN({formatedFieldName})",
                FieldFormatterNames.MATH_POW => $"POWER({formatedFieldName}, {parameterString})",
                FieldFormatterNames.MATH_SQRT => $"SQRT({formatedFieldName})",
                FieldFormatterNames.MATH_EXP => $"EXP({formatedFieldName})",
                FieldFormatterNames.MATH_LOG => $"LOG({formatedFieldName}, {parameterString})",
                FieldFormatterNames.MATH_COS => $"COS({formatedFieldName})",
                FieldFormatterNames.MATH_SIN => $"SIN({formatedFieldName})",
                FieldFormatterNames.MATH_TAN => $"TAN({formatedFieldName})",
                FieldFormatterNames.MATH_ACOS => $"ACOS({formatedFieldName})",
                FieldFormatterNames.MATH_ASIN => $"ASIN({formatedFieldName})",
                FieldFormatterNames.MATH_ATAN => $"ATAN({formatedFieldName})",
                FieldFormatterNames.MATH_ATAN2 => $"ATN2({formatedFieldName}, {parameterString})",
                FieldFormatterNames.STRING_PAD_LEFT => StringPadLeft(formatedFieldName, formatOption.Parameter),
                FieldFormatterNames.STRING_PAD_RIGHT => StringPadRight(formatedFieldName, formatOption.Parameter),
                FieldFormatterNames.STRING_INDEX_OF => StringIndexOf(formatedFieldName, formatOption.Parameter),
                FieldFormatterNames.STRING_INDEX_OF_ANY => StringIndexOfAny(formatedFieldName, formatOption.Parameter),
                FieldFormatterNames.STRING_LAST_INDEX_OF => StringLastIndexOf(formatedFieldName, formatOption.Parameter),
                FieldFormatterNames.STRING_LAST_INDEX_OF_ANY => StringLastIndexOfAny(formatedFieldName, formatOption.Parameter),
                _ => throw new SixnetException($"{SqlServerManager.CurrentDatabaseServerType} does not support field formatter: {formatOption.Name}"),
            };
            return formatedFieldName;
        }

        #region Substring

        string Substring(string formatedFieldName, dynamic parameter)
        {
            if (parameter is Tuple<dynamic, dynamic> tupeParameter)
            {
                return $"SUBSTRING({formatedFieldName},{tupeParameter.Item1 + 1}, {tupeParameter.Item2})";
            }
            else
            {
                return $"SUBSTRING({formatedFieldName},{parameter + 1}, LEN({formatedFieldName}))";
            }
        }

        #endregion

        #region Replace

        string ReplaceString(string formatedFieldName, object parameter)
        {
            if (parameter is Tuple<dynamic, dynamic, dynamic> tupeThreeParameter)
            {
                if (StringIgnoreCaseValues.Contains(tupeThreeParameter.Item3))
                {
                    return $"REPLACE({formatedFieldName},'{tupeThreeParameter.Item1}', '{tupeThreeParameter.Item2}')";
                }
                else
                {
                    return $"REPLACE({formatedFieldName} COLLATE Latin1_General_CS_AS,'{tupeThreeParameter.Item1}' COLLATE Latin1_General_CS_AS, '{tupeThreeParameter.Item2}')";
                }
            }
            else if (parameter is Tuple<dynamic, dynamic> tupeTwoParameter)
            {
                return $"REPLACE({formatedFieldName} COLLATE Latin1_General_CS_AS,'{tupeTwoParameter.Item1}' COLLATE Latin1_General_CS_AS, '{tupeTwoParameter.Item2}')";
            }
            SixnetDirectThrower.ThrowAppException(true, $"Error field formatter: {formatedFieldName}");
            return string.Empty;
        }

        #endregion

        #region String

        string StringPadLeft(string formatedFieldName, object parameter)
        {
            if (parameter is Tuple<dynamic, dynamic> tupeParameter)
            {
                return $"RIGHT(REPLICATE('{tupeParameter.Item2}', {tupeParameter.Item1}) + {formatedFieldName}, {tupeParameter.Item1})";
            }
            SixnetDirectThrower.ThrowAppException(true, $"Error field formatter: {formatedFieldName}");
            return string.Empty;
        }

        string StringPadRight(string formatedFieldName, object parameter)
        {
            if (parameter is Tuple<dynamic, dynamic> tupeParameter)
            {
                return $"LEFT({formatedFieldName} + REPLICATE('{tupeParameter.Item2}', {tupeParameter.Item1}), {tupeParameter.Item1})";
            }
            SixnetDirectThrower.ThrowAppException(true, $"Error field formatter: {formatedFieldName}");
            return string.Empty;
        }

        string StringIndexOf(string formatedFieldName, object parameter)
        {
            if (parameter is Tuple<dynamic, dynamic, dynamic> tupeThreeParameter)
            {
                var charValue = tupeThreeParameter.Item1;
                var startIndex = tupeThreeParameter.Item2;
                var count = tupeThreeParameter.Item3;
                return $"(CHARINDEX('{charValue}', SUBSTRING({formatedFieldName},{startIndex + 1}, {count})) + {startIndex - 1})";
            }
            else if (parameter is Tuple<dynamic, dynamic> tupeTwoParameter)
            {
                var charValue = tupeTwoParameter.Item1;
                var startIndex = tupeTwoParameter.Item2;
                var count = $"LEN({formatedFieldName})";
                return $"(CHARINDEX('{charValue}', SUBSTRING({formatedFieldName},{startIndex + 1}, {count})) + {startIndex - 1})";
            }
            else
            {
                return $"(CHARINDEX('{parameter}', {formatedFieldName}) -1)";
            }
        }

        string StringIndexOfAny(string formatedFieldName, dynamic parameter)
        {
            if (parameter is Tuple<dynamic, dynamic, dynamic> tupeThreeParameter)
            {
                var charValue = new string(tupeThreeParameter.Item1);
                var startIndex = tupeThreeParameter.Item2;
                var count = tupeThreeParameter.Item3;
                return $"(PATINDEX('%[{charValue}]%', SUBSTRING({formatedFieldName},{startIndex + 1}, {count})) + {startIndex - 1})";
            }
            else if (parameter is Tuple<dynamic, dynamic> tupeTwoParameter)
            {
                var charValue = new string(tupeTwoParameter.Item1);
                var startIndex = tupeTwoParameter.Item2;
                var count = $"LEN({formatedFieldName})";
                return $"(PATINDEX('%[{charValue}]%', SUBSTRING({formatedFieldName},{startIndex + 1}, {count})) + {startIndex - 1} )";
            }
            else
            {
                var charValue = new string(parameter);
                return $"(PATINDEX('%[{charValue}]%', {formatedFieldName}) -1)";
            }
        }

        string StringLastIndexOf(string formatedFieldName, object parameter)
        {
            if (parameter is Tuple<dynamic, dynamic, dynamic> tupeThreeParameter)
            {
                var charValue = tupeThreeParameter.Item1;
                var startIndex = tupeThreeParameter.Item2;
                var count = tupeThreeParameter.Item3;
                return $"({startIndex + 1} - CHARINDEX('{charValue}', REVERSE(SUBSTRING({formatedFieldName}, {startIndex + 2} - {count}))))";
            }
            else if (parameter is Tuple<dynamic, dynamic> tupeTwoParameter)
            {
                var charValue = tupeTwoParameter.Item1;
                var startIndex = tupeTwoParameter.Item2;
                var count = $"LEN({formatedFieldName})";
                return $"({startIndex + 1} - CHARINDEX('{charValue}', REVERSE(SUBSTRING({formatedFieldName}, {startIndex + 2} - {count}))))";
            }
            else
            {
                return $"(LEN({formatedFieldName})-CHARINDEX('{parameter}', REVERSE({formatedFieldName})))";
            }
        }

        string StringLastIndexOfAny(string formatedFieldName, dynamic parameter)
        {
            if (parameter is Tuple<dynamic, dynamic, dynamic> tupeThreeParameter)
            {
                var charValue = new string(tupeThreeParameter.Item1);
                var startIndex = tupeThreeParameter.Item2;
                var count = tupeThreeParameter.Item3;
                return $"({startIndex + 1} - PATINDEX('%[{charValue}]%', REVERSE(SUBSTRING({formatedFieldName}, {startIndex + 2} - {count}))))";
            }
            else if (parameter is Tuple<dynamic, dynamic> tupeTwoParameter)
            {
                var charValue = new string(tupeTwoParameter.Item1);
                var startIndex = tupeTwoParameter.Item2;
                var count = $"LEN({formatedFieldName})";
                return $"({startIndex + 1} - PATINDEX('%[{charValue}]%', REVERSE(SUBSTRING({formatedFieldName}, {startIndex + 2} - {count}))))";
            }
            else
            {
                var charValue = new string(parameter);
                return $"(LEN({formatedFieldName})-PATINDEX('%[{charValue}]%', REVERSE({formatedFieldName})))";
            }
        }

        string StringTrim(string formatedFieldName, dynamic parameter)
        {
            if (parameter == null)
            {
                return $"TRIM({formatedFieldName})";
            }
            else
            {
                var charValue = new string(parameter);
                return $"SUBSTRING({formatedFieldName}, PATINDEX('%[^{charValue}]%', {formatedFieldName}), LEN({formatedFieldName}) - PATINDEX('%[^{charValue}]%', REVERSE({formatedFieldName})) - PATINDEX('%[^{charValue}]%', {formatedFieldName}) + 2)";
            }
        }

        string StringTrimStart(string formatedFieldName, dynamic parameter)
        {
            if (parameter == null)
            {
                return $"LTRIM({formatedFieldName})";
            }
            else
            {
                var charValue = new string(parameter);
                return $"SUBSTRING({formatedFieldName}, PATINDEX('%[^{charValue}]%', {formatedFieldName}), LEN({formatedFieldName}) - PATINDEX('%[^{charValue}]%', {formatedFieldName}) + 1)";
            }
        }

        string StringTrimEnd(string formatedFieldName, dynamic parameter)
        {
            if (parameter == null)
            {
                return $"RTRIM({formatedFieldName})";
            }
            else
            {
                var charValue = new string(parameter);
                return $"LEFT({formatedFieldName}, LEN({formatedFieldName}) - PATINDEX('%[^{charValue}]%', REVERSE({formatedFieldName})) + 1)";
            }
        }

        #endregion
    }
}
