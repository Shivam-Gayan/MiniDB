using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DB.Engine.Execution.Ast.Where
{
    /// <summary>
    /// Represents a simple WHERE condition like:
    /// column = value
    /// column BETWEEN value AND value
    /// </summary>
    public sealed class BinaryExpression : WhereNode
    {
        public string Column { get; }
        public ComparisonOperator Operator { get; }
        public object Value { get; }
        public object? SecondValue { get; }

        public BinaryExpression(
            string column,
            ComparisonOperator op,
            object value,
            object? secondValue = null)
        {
            Column = column;
            Operator = op;
            Value = value;
            SecondValue = secondValue;
        }
    }
}
