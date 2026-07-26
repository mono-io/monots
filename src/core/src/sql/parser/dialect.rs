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

//! [`MonotsDialect`] — MonoTS SQL dialect (based on FunctionStream's Postgres-style dialect).

use datafusion::sql::sqlparser::dialect::{Dialect, Precedence};
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::{Parser, ParserError};
use datafusion::sql::sqlparser::tokenizer::Token;

const DOUBLE_COLON_PREC: u8 = 140;
const BRACKET_PREC: u8 = 130;
const COLLATE_PREC: u8 = 120;
const AT_TZ_PREC: u8 = 110;
const CARET_PREC: u8 = 100;
const MUL_DIV_MOD_OP_PREC: u8 = 90;
const PLUS_MINUS_PREC: u8 = 80;
const XOR_PREC: u8 = 75;
const PG_OTHER_PREC: u8 = 70;
const BETWEEN_LIKE_PREC: u8 = 60;
const EQ_PREC: u8 = 50;
const IS_PREC: u8 = 40;
const NOT_PREC: u8 = 30;
const AND_PREC: u8 = 20;
const OR_PREC: u8 = 10;

/// MonoTS SQL dialect for stream DDL tokenization (FunctionStream-style).
#[derive(Debug, Default, Clone, Copy)]
pub struct MonotsDialect;

impl Dialect for MonotsDialect {
    fn identifier_quote_style(&self, _identifier: &str) -> Option<char> {
        Some('"')
    }

    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        ch == '"'
    }

    fn is_identifier_start(&self, ch: char) -> bool {
        ch.is_alphabetic() || ch == '_'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_alphabetic() || ch.is_ascii_digit() || ch == '$' || ch == '_'
    }

    fn supports_unicode_string_literal(&self) -> bool {
        true
    }

    fn is_custom_operator_part(&self, ch: char) -> bool {
        matches!(
            ch,
            '+' | '-'
                | '*'
                | '/'
                | '<'
                | '>'
                | '='
                | '~'
                | '!'
                | '@'
                | '#'
                | '%'
                | '^'
                | '&'
                | '|'
                | '`'
                | '?'
        )
    }

    fn get_next_precedence(&self, parser: &Parser) -> Option<Result<u8, ParserError>> {
        let token = parser.peek_token();
        match token.token {
            Token::Word(w) if w.keyword == Keyword::COLLATE => Some(Ok(COLLATE_PREC)),
            Token::LBracket => Some(Ok(BRACKET_PREC)),
            Token::Arrow
            | Token::LongArrow
            | Token::HashArrow
            | Token::HashLongArrow
            | Token::AtArrow
            | Token::ArrowAt
            | Token::HashMinus
            | Token::AtQuestion
            | Token::AtAt
            | Token::Question
            | Token::QuestionAnd
            | Token::QuestionPipe
            | Token::ExclamationMark
            | Token::Overlap
            | Token::CaretAt
            | Token::StringConcat
            | Token::Sharp
            | Token::ShiftRight
            | Token::ShiftLeft
            | Token::CustomBinaryOperator(_) => Some(Ok(PG_OTHER_PREC)),
            _ => None,
        }
    }

    fn supports_filter_during_aggregation(&self) -> bool {
        true
    }

    fn supports_group_by_expr(&self) -> bool {
        true
    }

    fn prec_value(&self, prec: Precedence) -> u8 {
        match prec {
            Precedence::DoubleColon => DOUBLE_COLON_PREC,
            Precedence::AtTz => AT_TZ_PREC,
            Precedence::MulDivModOp => MUL_DIV_MOD_OP_PREC,
            Precedence::PlusMinus => PLUS_MINUS_PREC,
            Precedence::Xor => XOR_PREC,
            Precedence::Ampersand => PG_OTHER_PREC,
            Precedence::Caret => CARET_PREC,
            Precedence::Pipe => PG_OTHER_PREC,
            Precedence::Between => BETWEEN_LIKE_PREC,
            Precedence::Eq => EQ_PREC,
            Precedence::Like => BETWEEN_LIKE_PREC,
            Precedence::Is => IS_PREC,
            Precedence::PgOther => PG_OTHER_PREC,
            Precedence::UnaryNot => NOT_PREC,
            Precedence::And => AND_PREC,
            Precedence::Or => OR_PREC,
        }
    }

    fn allow_extract_custom(&self) -> bool {
        true
    }

    fn allow_extract_single_quotes(&self) -> bool {
        true
    }
}
