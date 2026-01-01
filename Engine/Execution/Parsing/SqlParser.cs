using DB.Engine.Execution.Ast;
using DB.Engine.Execution.Ast.Where;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DB.Engine.Execution.Parsing
{
    public sealed class SqlParser
    {
        private readonly List<Token> _tokens;
        private int _pos;

        public SqlParser(IEnumerable<Token> tokens)
        {
            _tokens = tokens.ToList();
            _pos = 0;
        }

        // ------------------- Entry Point -------------------

        public StatementNode ParseStatement()
        {
            if (Match(TokenType.Create))
            {
                if (Match(TokenType.Table))
                {
                    return ParseCreateTable();
                }
                if (Match(TokenType.Index))
                {
                    return ParseCreateIndex();
                }

                throw Error("Expected TABLE or INDEX after CREATE");
            }

            if (Match(TokenType.Delete)) return ParseDelete();

            if (Match(TokenType.Drop))
            {
                return ParseDropTable();
            }

            if (Match(TokenType.Insert))
                return ParseInsert();

            if (Match(TokenType.Select))
                return ParseSelect();

            throw Error($"Unexpected token '{Peek().Lexeme}'");
        }

        // ------------------- Delete Record -------------------
        private StatementNode ParseDelete()
        {
            Consume(TokenType.From, "Expected FROM after DELETE");
            string table = Consume(TokenType.Identifier, "Expected table name").Lexeme;

            WhereNode? where = null;
            if (Match(TokenType.Where))
            {
                where = ParseWhere();
            }

            ConsumeOptional(TokenType.Semicolon);

            return new DeleteNode(table, where);
        }
        // ------------------- DROP TABLE -------------------

        private StatementNode ParseDropTable()
        {
            Consume(TokenType.Table, "Expected TABLE after DROP");
            string table = Consume(TokenType.Identifier, "Expected table name").Lexeme;
            ConsumeOptional(TokenType.Semicolon);
            return new DropTableNode(table);
        }

        // ------------------- CREATE INDEX -------------------

        private StatementNode ParseCreateIndex()
        {

            string indexName = Consume(TokenType.Identifier, "Expected index name").Lexeme;

            Consume(TokenType.On, "Expected ON after index name");

            string table = Consume(TokenType.Identifier, "Expected table name").Lexeme;

            Consume(TokenType.OpenParen, "Expected '('");

            string column = Consume(TokenType.Identifier, "Expected column name").Lexeme;

            Consume(TokenType.CloseParen, "Expected ')'");
            ConsumeOptional(TokenType.Semicolon);

            return new CreateIndexNode(indexName, table, column);
        }

        // ------------------- INSERT -------------------

        private StatementNode ParseInsert()
        {
            Consume(TokenType.Into, "Expected INTO after INSERT");

            string table = Consume(TokenType.Identifier, "Expected table name").Lexeme;

            Consume(TokenType.Values, "Expected VALUES");

            Consume(TokenType.OpenParen, "Expected '('");

            var values = new List<object?>();

            do
            {
                values.Add(ParseLiteral());
            }
            while (Match(TokenType.Comma));

            Consume(TokenType.CloseParen, "Expected ')'");
            ConsumeOptional(TokenType.Semicolon);

            return new InsertNode(table, values);
        }

        // ------------------- SELECT -------------------

        private StatementNode ParseSelect()
        {
            Consume(TokenType.Star, "Only SELECT * is supported");

            Consume(TokenType.From, "Expected FROM");

            string table = Consume(TokenType.Identifier, "Expected table name").Lexeme;

            WhereNode? where = null;

            if (Match(TokenType.Where))
            {
                where = ParseWhere();
            }

            ConsumeOptional(TokenType.Semicolon);

            return new SelectNode(table, where);
        }
        // ------------------- CREATE TABLE -------------------
        private StatementNode ParseCreateTable()
        {
            string tableName = Consume(TokenType.Identifier, "Expected table name").Lexeme;

            Consume(TokenType.OpenParen, "Expected '(' after table name");

            var columns = new List<ColumnDefinition>();

            do
            {
                string colName = Consume(TokenType.Identifier, "Expected column name").Lexeme;
                string colType = Consume(TokenType.Identifier, "Expected column type").Lexeme;

                columns.Add(new ColumnDefinition(colName, colType));
            }
            while (Match(TokenType.Comma));

            Consume(TokenType.CloseParen, "Expected ')'");
            ConsumeOptional(TokenType.Semicolon);

            return new CreateTableNode(tableName, columns);
        }


        // ------------------- WHERE -------------------

        private WhereNode ParseWhere()
        {
            string column = Consume(TokenType.Identifier, "Expected column name").Lexeme;

            if (Match(TokenType.Equal))
                return new BinaryExpression(column, ComparisonOperator.Equal, ParseLiteral());

            if (Match(TokenType.LessThan))
                return new BinaryExpression(column, ComparisonOperator.LessThan, ParseLiteral());

            if (Match(TokenType.LessThanOrEqual))
                return new BinaryExpression(column, ComparisonOperator.LessThanOrEqual, ParseLiteral());

            if (Match(TokenType.GreaterThan))
                return new BinaryExpression(column, ComparisonOperator.GreaterThan, ParseLiteral());

            if (Match(TokenType.GreaterThanOrEqual))
                return new BinaryExpression(column, ComparisonOperator.GreaterThanOrEqual, ParseLiteral());

            if (Match(TokenType.Between))
            {
                var start = ParseLiteral();
                Consume(TokenType.And, "Expected AND in BETWEEN");
                var end = ParseLiteral();
                return new BinaryExpression(column, ComparisonOperator.Between, start, end);
            }

            throw Error("Invalid WHERE clause");
        }

        // ------------------- Literals -------------------

        private object ParseLiteral()
        {
            if (Match(TokenType.Number))
            {
                var text = Previous().Lexeme;
                return text.Contains('.')
                    ? double.Parse(text)
                    : int.Parse(text);
            }

            if (Match(TokenType.String))
            {
                return Previous().Lexeme;
            }
            if (Match(TokenType.True))
            {
                return true;
            }

            if (Match(TokenType.False))
            {
                return false;
            }

            throw Error("Expected literal value");
        }

        // ------------------- Helpers -------------------

        private bool Match(TokenType type)
        {
            if (Check(type))
            {
                Advance();
                return true;
            }
            return false;
        }

        private Token Consume(TokenType type, string message)
        {
            if (Check(type))
                return Advance();

            throw Error(message);
        }

        private void ConsumeOptional(TokenType type)
        {
            if (Check(type))
                Advance();
        }

        private bool Check(TokenType type)
        {
            return !IsAtEnd() && Peek().Type == type;
        }

        private Token Advance()
        {
            if (!IsAtEnd())
                _pos++;
            return Previous();
        }

        private bool IsAtEnd()
        {
            return Peek().Type == TokenType.EOF;
        }

        private Token Peek()
        {
            return _tokens[_pos];
        }

        private Token Previous()
        {
            return _tokens[_pos - 1];
        }

        private Exception Error(string message)
        {
            return new InvalidOperationException(
                $"{message} at token '{Peek().Lexeme}'");
        }
    }
}
