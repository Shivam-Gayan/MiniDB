using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DB.Engine.Execution.Parsing
{
    public enum TokenType
    {
        // Keywords
        Select,
        Insert,
        Create,
        Table,
        Index,
        Into,
        Values,
        From,
        Where,
        Between,
        And,

        // Symbols
        Star,          // *
        Comma,         // ,
        Semicolon,     // ;
        OpenParen,     // (
        CloseParen,    // )

        // Operators
        Equal,         // =
        LessThan,      // <
        GreaterThan,   // >
        LessThanOrEqual,    // <=
        GreaterThanOrEqual, // >=

        // Literals & identifiers
        Identifier,
        Number,
        String,

        // End
        EOF
    }
}
