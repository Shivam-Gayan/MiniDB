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
        public string Lexeme { get; }

        public Token(TokenType type, string lexeme)
        {
            Type = type;
            Lexeme = lexeme;
        }

        public override string ToString()
        {
            return $"{Type}('{Lexeme}')";
        }
    }
}
