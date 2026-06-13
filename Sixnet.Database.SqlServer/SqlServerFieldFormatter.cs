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

        public string Format(SixnetFormatFieldContext context)
        {
            var formatOption = context.FormatSetting;
            var formatedFieldName = context.FieldName;
            var parameterString = formatOption.Parameter?.ToString();
            formatedFieldName = formatOption.Name switch
            {
                SixnetFieldFormatterNames.TO_STRING => $"CAST({formatedFieldName} AS NVARCHAR({(string.IsNullOrWhiteSpace(parameterString) ? "MAX" : parameterString)}))",
                SixnetFieldFormatterNames.DISTINCT => $"DISTINCT {formatedFieldName}",
                SixnetFieldFormatterNames.IS_NULL => $"{formatedFieldName} IS NULL",
                SixnetFieldFormatterNames.NOT_NULL => $"{formatedFieldName} IS NOT NULL",
                SixnetFieldFormatterNames.CHARLENGTH => $"LEN({formatedFieldName})",
                SixnetFieldFormatterNames.COUNT => $"COUNT({formatedFieldName})",
                SixnetFieldFormatterNames.SUM => $"SUM({formatedFieldName})",
                SixnetFieldFormatterNames.MAX => $"MAX({formatedFieldName})",
                SixnetFieldFormatterNames.MIN => $"MIN({formatedFieldName})",
                SixnetFieldFormatterNames.AVG => $"AVG({formatedFieldName})",
                SixnetFieldFormatterNames.JSON_VALUE => $"JSON_VALUE({formatedFieldName},{parameterString})",
                SixnetFieldFormatterNames.JSON_OBJECT => $"JSON_QUERY({formatedFieldName},{parameterString})",
                SixnetFieldFormatterNames.AND => $"({formatedFieldName}&{parameterString})",
                SixnetFieldFormatterNames.OR => $"({formatedFieldName}|{parameterString})",
                SixnetFieldFormatterNames.XOR => $"({formatedFieldName}^{parameterString})",
                SixnetFieldFormatterNames.NOT => $"(~{formatedFieldName})",
                SixnetFieldFormatterNames.ADD => $"({formatedFieldName}+{parameterString})",
                SixnetFieldFormatterNames.SUBTRACT => $"({formatedFieldName}-{parameterString})",
                SixnetFieldFormatterNames.MULTIPLY => $"({formatedFieldName}*{parameterString})",
                SixnetFieldFormatterNames.DIVIDE => $"({formatedFieldName}/{parameterString})",
                SixnetFieldFormatterNames.MODULO => $"({formatedFieldName}%{parameterString})",
                SixnetFieldFormatterNames.LEFT_SHIFT => $"({formatedFieldName}<<{parameterString})",
                SixnetFieldFormatterNames.RIGHT_SHIFT => $"({formatedFieldName}>>{parameterString})",
                SixnetFieldFormatterNames.TRIM => StringTrim(formatedFieldName, formatOption.Parameter),
                SixnetFieldFormatterNames.TRIM_START => StringTrimStart(formatedFieldName, formatOption.Parameter),
                SixnetFieldFormatterNames.TRIM_END => StringTrimEnd(formatedFieldName, formatOption.Parameter),
                SixnetFieldFormatterNames.STRING_CONCAT => $"({formatedFieldName}+{parameterString})",
                SixnetFieldFormatterNames.DATE_TIME_DATE => $"CAST({formatedFieldName} AS DATE)",
                SixnetFieldFormatterNames.DATE_TIME_YEAR => $"DATEPART(YEAR,{formatedFieldName})",
                SixnetFieldFormatterNames.DATE_TIME_MONTH => $"DATEPART(MONTH,{formatedFieldName})",
                SixnetFieldFormatterNames.DATE_TIME_DAY => $"DATEPART(DAY,{formatedFieldName})",
                SixnetFieldFormatterNames.DATE_TIME_DAY_OF_YEAR => $"DATEPART(DAYOFYEAR,{formatedFieldName})",
                SixnetFieldFormatterNames.DATE_TIME_DAY_OF_WEEK => $"(DATEPART(WEEKDAY,{formatedFieldName}) - 1)",
                SixnetFieldFormatterNames.DATE_TIME_HOUR => $"DATEPART(HOUR,{formatedFieldName})",
                SixnetFieldFormatterNames.DATE_TIME_MINUTE => $"DATEPART(MINUTE,{formatedFieldName})",
                SixnetFieldFormatterNames.DATE_TIME_SECOND => $"DATEPART(SECOND,{formatedFieldName})",
                SixnetFieldFormatterNames.DATE_TIME_MILLISECOND => $"DATEPART(MILLISECOND, {formatedFieldName})",
                SixnetFieldFormatterNames.DATE_TIME_TIME_OF_DAY => $"CAST({formatedFieldName} AS TIME)",
                SixnetFieldFormatterNames.DATE_TIME_UTC => $"SWITCHOFFSET({formatedFieldName}, '+00:00')",
                SixnetFieldFormatterNames.DATE_TIME_FORMAT_STRING => $"FORMAT({formatedFieldName}, '{parameterString}')",
                SixnetFieldFormatterNames.DATE_TIME_STRING => $"CONVERT(VARCHAR(20), {formatedFieldName}, 120)",
                SixnetFieldFormatterNames.DATE_TIME_WITH_MILLISECOND_STRING => $"CONVERT(VARCHAR(25), {formatedFieldName}, 121)",
                SixnetFieldFormatterNames.DATE_STRING => $"CONVERT(VARCHAR(15), {formatedFieldName}, 23)",
                SixnetFieldFormatterNames.US_DATE_STRING => $"CONVERT(VARCHAR(15), {formatedFieldName}, 101)",
                SixnetFieldFormatterNames.JAPAN_DATE_STRING => $"CONVERT(VARCHAR(15), {formatedFieldName}, 111)",
                SixnetFieldFormatterNames.TIME_SPAN_DAYS => $"DATEDIFF(DAY, {formatedFieldName?.Trim('(').Replace("-", ",")}",
                SixnetFieldFormatterNames.TIME_SPAN_TOTAL_DAYS => $"(DATEDIFF_BIG(SECOND, {formatedFieldName?.Trim('(').Replace("-", ",")} / 86400.0)",
                SixnetFieldFormatterNames.TIME_SPAN_HOURS => $"DATEDIFF(HOUR, {formatedFieldName?.Trim('(').Replace("-", ",")}",
                SixnetFieldFormatterNames.TIME_SPAN_TOTAL_HOURS => $"(DATEDIFF_BIG(SECOND, {formatedFieldName?.Trim('(').Replace("-", ",")} / 3600.0)",
                SixnetFieldFormatterNames.TIME_SPAN_MINUTES => $"DATEDIFF(MINUTE, {formatedFieldName?.Trim('(').Replace("-", ",")}",
                SixnetFieldFormatterNames.TIME_SPAN_TOTAL_MINUTES => $"(DATEDIFF_BIG(SECOND, {formatedFieldName?.Trim('(').Replace("-", ",")} / 60.0)",
                SixnetFieldFormatterNames.TIME_SPAN_SECONDS => $"DATEDIFF(SECOND, {formatedFieldName?.Trim('(').Replace("-", ",")}",
                SixnetFieldFormatterNames.TIME_SPAN_TOTAL_SECONDS => $"(DATEDIFF(MILLISECOND, {formatedFieldName?.Trim('(').Replace("-", ",")} / 1000.0)",
                SixnetFieldFormatterNames.TIME_SPAN_MILLISECONDS => $"DATEDIFF(MILLISECOND, {formatedFieldName?.Trim('(').Replace("-", ",")}",
                SixnetFieldFormatterNames.TIME_SPAN_TOTAL_MILLISECONDS => $"(DATEDIFF(MICROSECOND, {formatedFieldName?.Trim('(').Replace("-", ",")} / 1000.0)",
                SixnetFieldFormatterNames.TO_LOWER => $"LOWER({formatedFieldName})",
                SixnetFieldFormatterNames.TO_UPPER => $"UPPER({formatedFieldName})",
                SixnetFieldFormatterNames.SUB_STRING => Substring(formatedFieldName, formatOption.Parameter),
                SixnetFieldFormatterNames.STRING_REPLACE => ReplaceString(formatedFieldName, formatOption.Parameter),
                SixnetFieldFormatterNames.DATE_TIME_ADD_DAY => $"DATEADD(DAY, {parameterString}, {formatedFieldName})",
                SixnetFieldFormatterNames.DATE_TIME_ADD_MONTH => $"DATEADD(MONTH, {parameterString}, {formatedFieldName})",
                SixnetFieldFormatterNames.DATE_TIME_ADD_YEAR => $"DATEADD(YEAR, {parameterString}, {formatedFieldName})",
                SixnetFieldFormatterNames.DATE_TIME_ADD_HOUR => $"DATEADD(HOUR, {parameterString}, {formatedFieldName})",
                SixnetFieldFormatterNames.DATE_TIME_ADD_MINUTE => $"DATEADD(MINUTE, {parameterString}, {formatedFieldName})",
                SixnetFieldFormatterNames.DATE_TIME_ADD_SECOND => $"DATEADD(SECOND, {parameterString}, {formatedFieldName})",
                SixnetFieldFormatterNames.DATE_TIME_ADD_MILLISECOND => $"DATEADD(MILLISECOND, {parameterString}, {formatedFieldName})",
                SixnetFieldFormatterNames.CONVERT_TO_INT => $"CAST({formatedFieldName} AS INT)",
                SixnetFieldFormatterNames.CONVERT_TO_BOOLEAN => $"CAST({formatedFieldName} AS BIT)",
                SixnetFieldFormatterNames.CONVERT_TO_BYTE => $"CAST({formatedFieldName} AS TINYINT)",
                SixnetFieldFormatterNames.CONVERT_TO_CHAR => $"CAST({formatedFieldName} AS NCHAR(4000))",
                SixnetFieldFormatterNames.CONVERT_TO_DATE_TIME => $"CAST({formatedFieldName} AS DATETIME)",
                SixnetFieldFormatterNames.CONVERT_TO_DECIMAL => $"CAST({formatedFieldName} AS DECIMAL(20, 4))",
                SixnetFieldFormatterNames.CONVERT_TO_DOUBLE => $"CAST({formatedFieldName} AS FLOAT)",
                SixnetFieldFormatterNames.CONVERT_TO_INT_16 => $"CAST({formatedFieldName} AS SMALLINT)",
                SixnetFieldFormatterNames.CONVERT_TO_INT_64 => $"CAST({formatedFieldName} AS BIGINT)",
                SixnetFieldFormatterNames.CONVERT_TO_SBYTE => $"CAST({formatedFieldName} AS SMALLINT)",
                SixnetFieldFormatterNames.CONVERT_TO_SINGLE => $"CAST({formatedFieldName} AS REAL)",
                SixnetFieldFormatterNames.CONVERT_TO_UINT_16 => $"CAST({formatedFieldName} AS INT)",
                SixnetFieldFormatterNames.CONVERT_TO_UINT_32 => $"CAST({formatedFieldName} AS INT)",
                SixnetFieldFormatterNames.CONVERT_TO_UINT_64 => $"CAST({formatedFieldName} AS BIGINT)",
                SixnetFieldFormatterNames.MATH_ROUND => $"ROUND({formatedFieldName}, parameterString)",
                SixnetFieldFormatterNames.MATH_ABS => $"ABS({formatedFieldName})",
                SixnetFieldFormatterNames.MATH_CEILING => $"CEILING({formatedFieldName})",
                SixnetFieldFormatterNames.MATH_FLOOR => $"FLOOR({formatedFieldName})",
                SixnetFieldFormatterNames.MATH_TRUNCATE => $"ROUND({formatedFieldName}, 0, 1)",
                SixnetFieldFormatterNames.MATH_SIGN => $"SIGN({formatedFieldName})",
                SixnetFieldFormatterNames.MATH_POW => $"POWER({formatedFieldName}, {parameterString})",
                SixnetFieldFormatterNames.MATH_SQRT => $"SQRT({formatedFieldName})",
                SixnetFieldFormatterNames.MATH_EXP => $"EXP({formatedFieldName})",
                SixnetFieldFormatterNames.MATH_LOG => $"LOG({formatedFieldName}, {parameterString})",
                SixnetFieldFormatterNames.MATH_COS => $"COS({formatedFieldName})",
                SixnetFieldFormatterNames.MATH_SIN => $"SIN({formatedFieldName})",
                SixnetFieldFormatterNames.MATH_TAN => $"TAN({formatedFieldName})",
                SixnetFieldFormatterNames.MATH_ACOS => $"ACOS({formatedFieldName})",
                SixnetFieldFormatterNames.MATH_ASIN => $"ASIN({formatedFieldName})",
                SixnetFieldFormatterNames.MATH_ATAN => $"ATAN({formatedFieldName})",
                SixnetFieldFormatterNames.MATH_ATAN2 => $"ATN2({formatedFieldName}, {parameterString})",
                SixnetFieldFormatterNames.STRING_PAD_LEFT => StringPadLeft(formatedFieldName, formatOption.Parameter),
                SixnetFieldFormatterNames.STRING_PAD_RIGHT => StringPadRight(formatedFieldName, formatOption.Parameter),
                SixnetFieldFormatterNames.STRING_INDEX_OF => StringIndexOf(formatedFieldName, formatOption.Parameter),
                SixnetFieldFormatterNames.STRING_INDEX_OF_ANY => StringIndexOfAny(formatedFieldName, formatOption.Parameter),
                SixnetFieldFormatterNames.STRING_LAST_INDEX_OF => StringLastIndexOf(formatedFieldName, formatOption.Parameter),
                SixnetFieldFormatterNames.STRING_LAST_INDEX_OF_ANY => StringLastIndexOfAny(formatedFieldName, formatOption.Parameter),
                _ => throw new SixnetException($"{context.Server.DatabaseType} does not support field formatter: {formatOption.Name}"),
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
