use anyhow::{anyhow, bail, Result};
use bigdecimal::{BigDecimal, ToPrimitive};
use sqlparser::ast::{BinaryOperator, DataType, Expr, Value as AstValue};

pub fn evaluate_expression(
    row: &[String],
    data_columns: &[(String, String)],
    expression: &Expr,
) -> Result<Value> {
    Ok(match expression {
        Expr::Identifier(identifier) => {
            let Some(pos) = data_columns
                .iter()
                .position(|(_, column)| *column == identifier.value)
            else {
                bail!("Failed to find column {}", identifier.value)
            };
            let value = row.get(pos).ok_or(anyhow!("Failed to find data"))?;
            Value::String(value.clone())
        }
        Expr::CompoundIdentifier(identifiers) => {
            if identifiers.len() != 2 {
                bail!("Can only process compound indentifiers with two fields");
            }
            let Some(pos) = data_columns.iter().position(|(table, column)| {
                *table == identifiers[0].value && *column == identifiers[1].value
            }) else {
                bail!("Failed to find column {:?}", identifiers)
            };
            let value = row.get(pos).ok_or(anyhow!("Failed to find data"))?;
            Value::String(value.clone())
        }
        Expr::Value(value) => match value {
            AstValue::Boolean(value) => (*value).into(),
            AstValue::DoubleQuotedString(string) | AstValue::SingleQuotedString(string) => {
                Value::String(string.clone())
            }
            AstValue::Null => Value::Null,
            AstValue::Number(decimal, _) => {
                if decimal.is_integer() {
                    Value::Integer(
                        decimal
                            .to_i64()
                            .ok_or(anyhow!("Number is not a valid integer"))?,
                    )
                } else {
                    Value::Decimal(decimal.clone())
                }
            }
            _ => bail!("Not implemented value {value}"),
        },
        Expr::IsFalse(expr) => (!evaluate_expression(row, data_columns, expr)?.to_bool()).into(),
        Expr::IsNotFalse(expr) => evaluate_expression(row, data_columns, expr)?
            .to_bool()
            .into(),
        Expr::IsTrue(expr) => evaluate_expression(row, data_columns, expr)?
            .to_bool()
            .into(),
        Expr::IsNotTrue(expr) => (!evaluate_expression(row, data_columns, expr)?.to_bool()).into(),
        Expr::IsNull(expr) => {
            matches!(evaluate_expression(row, data_columns, expr)?, Value::Null).into()
        }
        Expr::IsNotNull(expr) => {
            (!matches!(evaluate_expression(row, data_columns, expr)?, Value::Null)).into()
        }
        Expr::BinaryOp { left, op, right } => {
            let left = evaluate_expression(row, data_columns, left)?;
            match op {
                BinaryOperator::And => (left.to_bool()
                    && evaluate_expression(row, data_columns, right)?.to_bool())
                .into(),
                BinaryOperator::Or => (left.to_bool()
                    || evaluate_expression(row, data_columns, right)?.to_bool())
                .into(),
                BinaryOperator::Eq => {
                    (left == evaluate_expression(row, data_columns, right)?).into()
                }
                _ => bail!("Not implemented operator {op}"),
            }
        }
        Expr::Cast {
            expr,
            data_type,
            format,
        } => {
            if format.is_some() {
                bail!("Cannot format in casts");
            }
            let value = evaluate_expression(row, data_columns, expr)?;
            match (&value, data_type) {
                (Value::Integer(value), DataType::Text) => Value::String(value.to_string()),
                (Value::String(value), DataType::Integer(_)) => Value::Integer(value.parse()?),
                _ => bail!("Cannot cast {value:?} into {data_type}"),
            }
        }
        _ => bail!("Not implemented expression {expression:?}"),
    })
}

#[derive(Debug)]
pub enum Value {
    Bool(bool),
    Integer(i64),
    Decimal(BigDecimal),
    String(String),
    Null,
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Bool(l), Self::Bool(r)) => l == r,
            (Self::Integer(l), Self::Integer(r)) => l == r,
            (Self::String(l), Self::String(r)) => l == r,
            (Self::Decimal(l), Self::Decimal(r)) => l == r,
            (Self::Null, _) | (_, Self::Null) => false,
            _ => false,
        }
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Value::Bool(value)
    }
}

impl Value {
    pub fn to_bool(&self) -> bool {
        match self {
            Value::Bool(value) => *value,
            Value::Integer(integer) => *integer != 0,
            Value::String(string) => !string.is_empty(),
            Value::Decimal(decimal) => decimal.to_i64().map(|n| n != 0).unwrap_or(true),
            Value::Null => false,
        }
    }
}
