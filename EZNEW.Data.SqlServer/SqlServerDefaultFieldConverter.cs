using System;
using System.Collections.Generic;
using System.Text;
using EZNEW.Data.Conversion;
using EZNEW.Exceptions;

namespace EZNEW.Data.SqlServer
{
    /// <summary>
    /// Default field converter for sqlserver
    /// </summary>
    public class SqlServerDefaultFieldConverter : IFieldConverter
    {
        public FieldConversionResult Convert(FieldConversionContext fieldConversionContext)
        {
            if (string.IsNullOrWhiteSpace(fieldConversionContext?.ConversionName))
            {
                return null;
            }
            string formatedFieldName = null;
            switch (fieldConversionContext.ConversionName)
            {
                case FieldConversionNames.StringLength:
                    formatedFieldName = string.IsNullOrWhiteSpace(fieldConversionContext.ObjectName)
                        ? $"LEN({fieldConversionContext.ObjectName}.{SqlServerManager.WrapKeyword(fieldConversionContext.FieldName)})"
                        : $"LEN({SqlServerManager.WrapKeyword(fieldConversionContext.FieldName)})";
                    break;
                default:
                    throw new EZNEWException($"{SqlServerManager.CurrentDatabaseServerType} does not support field conversion: {fieldConversionContext.ConversionName}");
            }

            return new FieldConversionResult()
            {
                NewFieldName = formatedFieldName
            };
        }
    }
}
