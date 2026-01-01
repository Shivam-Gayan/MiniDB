using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DB.Engine.Execution.Parsing
{
    public sealed class Lexer
    {
        private readonly string _input;
        private int _pos;

        public Lexer(string input)
        {
            _input = input;
            _pos = 0;
        }

        public IEnumerable<Token> Tokenize()
        {
            while (!IsAtEnd())
            {
                char c = Peek();

                // Skip whitespace
                if (char.IsWhiteSpace(c))
                {
                    Advance();
                    continue;
                }

                // Identifiers or keywords
                if (char.IsLetter(c) || c == '_')
                {
                    yield return ReadIdentifierOrKeyword();
                    continue;
                }

                // Numbers
                if (char.IsDigit(c))
                {
                    yield return ReadNumber();
                    continue;
                }

                // Strings
                if (c == '\'')
                {
                    yield return ReadString();
                    continue;
                }

                // Symbols & operators
                switch (c)
                {
                    case '*':
                        Advance();
                        yield return new Token(TokenType.Star, "*");
                        break;

                    case ',':
                        Advance();
                        yield return new Token(TokenType.Comma, ",");
                        break;

                    case ';':
                        Advance();
                        yield return new Token(TokenType.Semicolon, ";");
                        break;

                    case '(':
                        Advance();
                        yield return new Token(TokenType.OpenParen, "(");
                        break;

                    case ')':
                        Advance();
                        yield return new Token(TokenType.CloseParen, ")");
                        break;

                    case '=':
                        Advance();
                        yield return new Token(TokenType.Equal, "=");
                        break;

                    case '<':
                        Advance();
                        if (Match('='))
                            yield return new Token(TokenType.LessThanOrEqual, "<=");
                        else
                            yield return new Token(TokenType.LessThan, "<");
                        break;

                    case '>':
                        Advance();
                        if (Match('='))
                            yield return new Token(TokenType.GreaterThanOrEqual, ">=");
                        else
                            yield return new Token(TokenType.GreaterThan, ">");
                        break;

                    default:
                        throw new InvalidOperationException(
                            $"Unexpected character '{c}' at position {_pos}");
                }
            }

            yield return new Token(TokenType.EOF, "");
        }

        // ------------------- Helpers -------------------

        private Token ReadIdentifierOrKeyword()
        {
            var sb = new StringBuilder();

            while (!IsAtEnd() && (char.IsLetterOrDigit(Peek()) || Peek() == '_'))
            {
                sb.Append(Advance());
            }

            string text = sb.ToString();
            string upper = text.ToUpperInvariant();

            return upper switch
            {
                "SELECT" => new Token(TokenType.Select, text),
                "INSERT" => new Token(TokenType.Insert, text),
                "CREATE" => new Token(TokenType.Create, text),
                "TABLE" => new Token(TokenType.Table, text),
                "INDEX" => new Token(TokenType.Index, text),
                "INTO" => new Token(TokenType.Into, text),
                "VALUES" => new Token(TokenType.Values, text),
                "FROM" => new Token(TokenType.From, text),
                "WHERE" => new Token(TokenType.Where, text),
                "BETWEEN" => new Token(TokenType.Between, text),
                "AND" => new Token(TokenType.And, text),
                "DROP" => new Token(TokenType.Drop, text),
                "TRUE" => new Token(TokenType.True, text),
                "FALSE" => new Token(TokenType.False, text),
                "ON" => new Token(TokenType.On, text),
                "DELETE" => new Token(TokenType.Delete, text),
                "UPDATE" => new Token(TokenType.Update, text),
                "SET" => new Token(TokenType.Set, text),
                _ => new Token(TokenType.Identifier, text)
            };
        }

        private Token ReadNumber()
        {
            var sb = new StringBuilder();

            while (!IsAtEnd() && char.IsDigit(Peek()))
            {
                sb.Append(Advance());
            }

            // Optional decimal part
            if (!IsAtEnd() && Peek() == '.')
            {
                sb.Append(Advance());
                while (!IsAtEnd() && char.IsDigit(Peek()))
                {
                    sb.Append(Advance());
                }
            }

            return new Token(TokenType.Number, sb.ToString());
        }

        private Token ReadString()
        {
            Advance(); // consume opening quote
            var sb = new StringBuilder();

            while (!IsAtEnd() && Peek() != '\'')
            {
                sb.Append(Advance());
            }

            if (IsAtEnd())
                throw new InvalidOperationException("Unterminated string literal");

            Advance(); // closing quote
            return new Token(TokenType.String, sb.ToString());
        }

        private char Advance() => _input[_pos++];

        private char Peek() => _input[_pos];

        private bool Match(char expected)
        {
            if (IsAtEnd() || _input[_pos] != expected)
                return false;

            _pos++;
            return true;
        }

        private bool IsAtEnd() => _pos >= _input.Length;
    }
}
