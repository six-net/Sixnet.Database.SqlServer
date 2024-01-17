using Sixnet.Development.Data.Field.Formatting;
using Sixnet.Exceptions;

namespace Sixnet.Database.SqlServer
{
    /// <summary>
    /// Default field formatter for sqlserver
    /// </summary>
    public class SqlServerFieldFormatter : IFieldFormatter
    {
        public string Format(FieldFormatContext context)
        {
            var formatOption = context.FormatOption;
            var formatedFieldName = context.FieldName;
            formatedFieldName = formatOption.Name switch
            {
                FieldFormatterNames.CHARLENGTH => $"LEN({formatedFieldName})",
                FieldFormatterNames.COUNT => $"COUNT({formatedFieldName})",
                FieldFormatterNames.SUM => $"SUM({formatedFieldName})",
                FieldFormatterNames.MAX => $"MAX({formatedFieldName})",
                FieldFormatterNames.MIN => $"MIN({formatedFieldName})",
                FieldFormatterNames.AVG => $"AVG({formatedFieldName})",
                FieldFormatterNames.JSON_VALUE => $"JSON_VALUE({formatedFieldName},{formatOption.Parameter})",
                FieldFormatterNames.JSON_OBJECT => $"JSON_QUERY({formatedFieldName},{formatOption.Parameter})",
                FieldFormatterNames.AND => $"({formatedFieldName}&{formatOption.Parameter})",
                FieldFormatterNames.OR => $"({formatedFieldName}|{formatOption.Parameter})",
                FieldFormatterNames.XOR => $"({formatedFieldName}^{formatOption.Parameter})",
                FieldFormatterNames.NOT => $"(~{formatedFieldName})",
                FieldFormatterNames.ADD => $"({formatedFieldName}+{formatOption.Parameter})",
                FieldFormatterNames.SUBTRACT => $"({formatedFieldName}-{formatOption.Parameter})",
                FieldFormatterNames.MULTIPLY => $"({formatedFieldName}*{formatOption.Parameter})",
                FieldFormatterNames.DIVIDE => $"({formatedFieldName}/{formatOption.Parameter})",
                FieldFormatterNames.MODULO => $"({formatedFieldName}%{formatOption.Parameter})",
                FieldFormatterNames.LEFT_SHIFT => $"({formatedFieldName}<<{formatOption.Parameter})",
                FieldFormatterNames.RIGHT_SHIFT => $"({formatedFieldName}>>{formatOption.Parameter})",
                FieldFormatterNames.TRIM => $"TRIM({formatedFieldName})",
                FieldFormatterNames.STRING_CONCAT => $"({formatedFieldName}+{formatOption.Parameter})",
                _ => throw new SixnetException($"{SqlServerManager.CurrentDatabaseServerType} does not support field formatter: {formatOption.Name}"),
            };
            return formatedFieldName;
        }
    }
}
