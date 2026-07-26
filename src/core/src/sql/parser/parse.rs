// Copyright 2026 MonoTS Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! MonoTS SQL parsing (`parse_sql`).
//!
//! Mirrors FunctionStream's [`parse_sql`](https://github.com/FunctionStream/function-stream/blob/robot/src/streaming_planner/src/parse.rs):
//! tokenize with a custom dialect, parse into AST, then classify/execute elsewhere.

use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::{Parser, ParserError};
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer, Word};

use super::ast::{
    CreateStreamStmt, DropStreamStmt, MonotsStatement, ShowStreamStatusStmt, ShowStreamStmt,
};
use super::dialect::MonotsDialect;
use super::options::sql_options_to_map;

macro_rules! parser_err {
    ($MSG:expr) => {
        Err(ParserError::ParserError($MSG.to_string()))
    };
}

/// Parse one or more MonoTS stream DDL statements.
pub fn parse_sql(query: &str) -> Result<Vec<MonotsStatement>, ParserError> {
    let trimmed = query.trim().trim_end_matches(';');
    if trimmed.is_empty() {
        return parser_err!("Query is empty");
    }

    let dialect = MonotsDialect;
    let mut tokenizer = Tokenizer::new(&dialect, trimmed);
    let tokens = tokenizer.tokenize()?;

    let mut parser = Parser::new(&dialect).with_tokens(tokens);
    let mut stmts = Vec::new();
    let mut expecting_delimiter = false;

    loop {
        while parser.consume_token(&Token::SemiColon) {
            expecting_delimiter = false;
        }
        if parser.peek_token() == Token::EOF {
            break;
        }
        if expecting_delimiter {
            return parser_err!("expected end of statement");
        }
        stmts.push(parse_statement(&mut parser)?);
        expecting_delimiter = true;
    }

    if stmts.is_empty() {
        return parser_err!("No SQL statements found");
    }
    Ok(stmts)
}

/// Parse exactly one stream DDL statement.
pub fn parse_one(sql: &str) -> Result<MonotsStatement, ParserError> {
    let mut stmts = parse_sql(sql)?;
    if stmts.len() != 1 {
        return parser_err!("expected exactly one stream DDL statement");
    }
    Ok(stmts.remove(0))
}

fn parse_statement(parser: &mut Parser) -> Result<MonotsStatement, ParserError> {
    match parser.peek_token().token {
        Token::Word(w) => match w.value.to_ascii_uppercase().as_str() {
            "CREATE" => {
                parser.next_token();
                parse_create_stream(parser)
            }
            "DROP" => {
                parser.next_token();
                parse_drop_stream(parser)
            }
            "SHOW" => {
                parser.next_token();
                parse_show(parser)
            }
            _ => parser_err!("unsupported MonoTS stream DDL statement"),
        },
        _ => parser_err!("unsupported MonoTS stream DDL statement"),
    }
}

fn parse_create_stream(parser: &mut Parser) -> Result<MonotsStatement, ParserError> {
    expect_word(parser, "STREAM")?;
    let if_not_exists = parser.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
    let name = parser.parse_object_name(false)?.to_string();
    let with_options = parser.parse_options(Keyword::WITH)?;
    if with_options.is_empty() {
        return parser_err!("CREATE STREAM requires WITH ('key' = 'value', ...)");
    }
    Ok(MonotsStatement::CreateStream(CreateStreamStmt {
        name,
        if_not_exists,
        options: sql_options_to_map(&with_options),
    }))
}

fn parse_drop_stream(parser: &mut Parser) -> Result<MonotsStatement, ParserError> {
    expect_word(parser, "STREAM")?;
    let name = parser.parse_object_name(false)?.to_string();
    let delete_checkpoint = word_matches(parser, "WITH")
        && {
            parser.next_token();
            word_matches(parser, "CHECKPOINT")
        }
        && {
            parser.next_token();
            true
        };
    Ok(MonotsStatement::DropStream(DropStreamStmt {
        name,
        delete_checkpoint,
    }))
}

fn parse_show(parser: &mut Parser) -> Result<MonotsStatement, ParserError> {
    if word_matches(parser, "STREAMS") {
        parser.next_token();
        return Ok(MonotsStatement::ShowStreams);
    }
    if word_matches(parser, "STREAM") {
        parser.next_token();
        if word_matches(parser, "STATUS") {
            parser.next_token();
            expect_word(parser, "FOR")?;
            let stream_id = parser.parse_object_name(false)?.to_string();
            return Ok(MonotsStatement::ShowStreamStatus(ShowStreamStatusStmt {
                stream_id,
            }));
        }
        let name = parser.parse_object_name(false)?.to_string();
        return Ok(MonotsStatement::ShowStream(ShowStreamStmt { name }));
    }
    parser_err!("expected STREAM or STREAMS")
}

fn word_matches(parser: &Parser, word: &str) -> bool {
    matches!(
        parser.peek_token().token,
        Token::Word(Word { ref value, .. }) if value.eq_ignore_ascii_case(word)
    )
}

fn expect_word(parser: &mut Parser, word: &str) -> Result<(), ParserError> {
    match parser.next_token().token {
        Token::Word(Word { value, .. }) if value.eq_ignore_ascii_case(word) => Ok(()),
        _ => parser_err!("expected '{word}'"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parse_create_stream_with_options() {
        let sql = "CREATE STREAM s1 WITH ('sink.type' = 'delta', 'sink.delta.path' = '/tmp/x', 'source.table' = 'metrics')";
        let stmts = parse_sql(sql).unwrap();
        assert_eq!(stmts.len(), 1);
        match &stmts[0] {
            MonotsStatement::CreateStream(s) => {
                assert_eq!(s.name, "s1");
                assert_eq!(s.options.len(), 3);
                assert_eq!(
                    s.options.get("sink.type").map(String::as_str),
                    Some("delta")
                );
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn parse_multiple_statements() {
        let sql =
            "CREATE STREAM s1 WITH ('sink.type'='delta','sink.delta.path'='/tmp/x'); SHOW STREAMS";
        let stmts = parse_sql(sql).unwrap();
        assert_eq!(stmts.len(), 2);
        assert!(matches!(stmts[0], MonotsStatement::CreateStream(_)));
        assert!(matches!(stmts[1], MonotsStatement::ShowStreams));
    }

    #[test]
    fn parse_if_not_exists() {
        let sql =
            "CREATE STREAM IF NOT EXISTS s1 WITH ('sink.type'='kafka','sink.kafka.brokers'='b','sink.kafka.topic'='t')";
        match parse_one(sql).unwrap() {
            MonotsStatement::CreateStream(s) => assert!(s.if_not_exists),
            _ => panic!("expected CreateStream"),
        }
    }

    #[test]
    fn rejects_missing_with() {
        assert!(parse_one("CREATE STREAM s1").is_err());
    }
}
