using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DB.Engine.Execution.Parsing
{
    public sealed class Token
    {
        public TokenType Type { get; }
        public string Text { get; }
        public object? Value { get; }

        public Token(TokenType type, string text)
        {
            Type = type;
            Text = text;
            Value = null;
        }

        public Token(TokenType type, object value)
        {
            Type = type;
            Text = value.ToString()!;
            Value = value;
        }
    }

}
