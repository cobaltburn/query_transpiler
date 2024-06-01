use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::surql::Transpile;

mod surql {

    use std::{
        borrow::Borrow,
        collections::{HashMap, HashSet},
    };

    use anyhow::{anyhow, Result};
    use sqlparser::ast;

    pub trait Transpile {
        fn transpile(&self) -> String;
    }

    pub trait QueryFormat {
        fn query_format(&self) -> String;
    }

    #[derive(Debug)]
    pub struct Select {
        pub items: Vec<Item>,
        pub from: Vec<Table>,
        pub filter: Option<Expr>,
    }
    impl Transpile for Select {
        fn transpile(&self) -> String {
            let mut query = vec![String::from("SELECT")];
            let mut items = vec![];
            self.items.iter().for_each(|item| {
                items.push(format!("{},", item.query_format()));
            });
            items.sort();
            let last = items.last_mut().unwrap();
            *last = last.trim_end_matches(',').to_owned();
            query.append(&mut items);
            query.push(String::from("\nFROM"));

            self.from.iter().for_each(|table| {
                query.push(table.query_format());
            });

            if let Some(selection) = &self.filter {
                query.push(String::from("\nWHERE"));
                query.push(selection.query_format());
            }

            query.join(" ")
        }
    }

    impl Select {
        pub fn parse(statement: ast::Statement) -> Result<Select> {
            let ast::Statement::Query(query) = statement else {
                return Err(anyhow!("Statement give was not a select type"));
            };

            let ast::SetExpr::Select(select) = *query.body else {
                return Err(anyhow!("Statement give was not a select type"));
            };
            // TODO check for cases where expr would be part of an edge and it should check for id
            let mut filter = select.selection.map(|e| Expr::from(e));

            let mut from = vec![];
            let mut edges = HashMap::new();
            for ast::TableWithJoins { relation, joins } in select.from {
                let ast::TableFactor::Table {
                    name: ast::ObjectName(idents),
                    alias,
                    ..
                } = relation
                else {
                    return Err(anyhow!("No tables were found"));
                };

                let alias = alias.map(|e| e.name.value);
                let name = idents.last().unwrap().value.to_owned();

                let new_edges = parse_joins(joins);
                for mut edge in new_edges {
                    filter = edge.bind_filter(filter);

                    if edge.alias.is_some() {
                        let edge = edge.clone();
                        edges.insert(edge.alias.clone().unwrap(), edge);
                    }
                    edges.insert(edge.name.clone(), edge);
                }

                from.push(Table { name, alias });
            }
            let mut items = vec![];
            for item in select.projection {
                let item = match item {
                    //TODO work on edge check here
                    ast::SelectItem::UnnamedExpr(expr) => Item::Expr(Expr::from(expr)),
                    ast::SelectItem::ExprWithAlias { expr, alias } => Item::ExprWithAias {
                        expr: Expr::from(expr),
                        alias: alias.value,
                    },
                    ast::SelectItem::QualifiedWildcard(_, _) => todo!(),
                    ast::SelectItem::Wildcard(_) => Item::WildCard,
                };
                items.push(item);
            }
            let items = Item::bind_edges(items, edges);

            Ok(Select {
                items,
                from,
                filter,
            })
        }
    }

    // TODO need to add ability to detect many bridge tables
    fn parse_joins(joins: Vec<ast::Join>) -> Vec<Edge> {
        let mut edges = vec![];
        for join in joins {
            let node = match join.relation {
                ast::TableFactor::Table { name, alias, .. } => {
                    let root = name.0.last().unwrap().value.to_owned();
                    let alias = alias.map(|e| e.name.value);
                    Edge::new(root, alias)
                }
                _ => todo!(),
            };

            edges.push(node);
        }
        edges
    }

    #[derive(Debug, Clone, Hash, PartialEq, Eq)]
    pub enum Expr {
        Identifier(String),
        CompoundIdentifier(Vec<String>),
        Edge(Edge),
        CompoundEdge(Edge, String),
        IsFalse(Box<Expr>),
        IsNotFalse(Box<Expr>),
        IsTrue(Box<Expr>),
        IsNotTrue(Box<Expr>),
        IsNull(Box<Expr>),
        IsNotNull(Box<Expr>),
        Value(Value),
        BinaryOp {
            left: Box<Expr>,
            op: BinaryOperator,
            right: Box<Expr>,
        },
        UnaryOp {
            op: UnaryOperator,
            expr: Box<Expr>,
        },
        Nested(Box<Expr>),
    }

    impl Expr {
        fn idents(&self) -> Option<&Vec<String>> {
            match self {
                Expr::CompoundIdentifier(idents) => Some(idents),
                _ => None,
            }
        }
        fn is_value(&self) -> bool {
            match self {
                Expr::Value(_) => true,
                _ => false,
            }
        }
        fn is_compound_id(&self) -> bool {
            match self {
                Expr::CompoundIdentifier(_) => true,
                _ => false,
            }
        }
    }

    impl QueryFormat for Expr {
        fn query_format(&self) -> String {
            match self {
                Expr::Identifier(ident) => ident.clone(),
                Expr::CompoundIdentifier(idents) => idents.last().unwrap().clone(), // TODO will have to get match id against table
                Expr::Edge(edge) => edge.query_format(),
                Expr::CompoundEdge(edge, id) => {
                    let mut edge = edge.query_format().trim_end_matches("*").to_string();
                    edge.push_str(&id);
                    edge
                }
                Expr::IsFalse(e) => format!("{} IS false", e.query_format()),
                Expr::IsNotFalse(e) => format!("{} IS NOT false", e.query_format()),
                Expr::IsTrue(e) => format!("{} IS true", e.query_format()),
                Expr::IsNotTrue(e) => format!("{} IS NOT true", e.query_format()),
                Expr::IsNull(e) => format!("{} IS null", e.query_format()),
                Expr::IsNotNull(e) => format!("{} IS NOT null", e.query_format()),
                Expr::Value(e) => e.query_format(),
                Expr::BinaryOp { left, op, right } => format!(
                    "{} {} {}",
                    left.query_format(),
                    op.query_format(),
                    right.query_format()
                ),
                Expr::UnaryOp { op, expr } => {
                    format!("{}{}", op.query_format(), expr.query_format())
                }
                Expr::Nested(expr) => format!("({})", expr.query_format()),
            }
        }
    }

    impl From<ast::Expr> for Expr {
        fn from(value: ast::Expr) -> Self {
            match value {
                ast::Expr::Identifier(ident) => {
                    if ident.quote_style.is_some() {
                        Expr::Value(Value::String(ident.value))
                    } else {
                        Expr::Identifier(ident.value)
                    }
                }
                ast::Expr::CompoundIdentifier(idents) => {
                    Expr::CompoundIdentifier(idents.iter().map(|e| e.value.to_owned()).collect())
                }
                ast::Expr::IsFalse(expr) => Expr::IsFalse(Box::new(Expr::from(*expr))),
                ast::Expr::IsNotFalse(expr) => Expr::IsNotFalse(Box::new(Expr::from(*expr))),
                ast::Expr::IsTrue(expr) => Expr::IsTrue(Box::new(Expr::from(*expr))),
                ast::Expr::IsNotTrue(expr) => Expr::IsNotTrue(Box::new(Expr::from(*expr))),
                ast::Expr::IsNull(expr) => Expr::IsNull(Box::new(Expr::from(*expr))),
                ast::Expr::IsNotNull(expr) => Expr::IsNotNull(Box::new(Expr::from(*expr))),
                ast::Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
                    left: Box::new(Expr::from(*left)),
                    op: BinaryOperator::from(op),
                    right: Box::new(Expr::from(*right)),
                },
                ast::Expr::UnaryOp { op, expr } => Expr::UnaryOp {
                    op: UnaryOperator::from(op),
                    expr: Box::new(Expr::from(*expr)),
                },
                ast::Expr::Value(e) => Expr::Value(Value::from(e)),
                ast::Expr::Nested(expr) => Expr::Nested(Box::new(Expr::from(*expr))),
                _ => todo!(),
            }
        }
    }

    #[derive(Debug, Clone, Hash, PartialEq, Eq)]
    pub enum Value {
        Number(String),
        Boolean(bool),
        String(String),
        Null,
        Varable(String),
    }

    impl QueryFormat for Value {
        fn query_format(&self) -> String {
            match self {
                Value::Number(e) => e.to_owned(),
                Value::Boolean(e) => e.to_string(),
                Value::String(e) => format!("\"{}\"", e),
                Value::Null => String::from("Null"),
                Value::Varable(e) => e.to_owned(),
            }
        }
    }

    impl From<ast::Value> for Value {
        fn from(value: ast::Value) -> Self {
            match value {
                ast::Value::Number(e, _) => Value::Number(e),
                ast::Value::Boolean(e) => Value::Boolean(e),
                ast::Value::Null => Value::Null,
                ast::Value::SingleQuotedString(e) => Value::String(e),
                ast::Value::DollarQuotedString(e) => Value::String(e.value),
                ast::Value::EscapedStringLiteral(e) => Value::String(e),
                ast::Value::SingleQuotedByteStringLiteral(e) => Value::String(e),
                ast::Value::DoubleQuotedByteStringLiteral(e) => Value::String(e),
                ast::Value::RawStringLiteral(e) => Value::String(e),
                ast::Value::NationalStringLiteral(e) => Value::String(e),
                ast::Value::HexStringLiteral(e) => Value::String(e),
                ast::Value::DoubleQuotedString(e) => Value::String(e),
                ast::Value::Placeholder(e) => Value::Varable(e),
            }
        }
    }

    #[allow(dead_code)]
    #[derive(Debug, Clone, Hash, PartialEq, Eq)]
    pub enum BinaryOperator {
        Plus,
        Minus,
        Multiply,
        Divide,
        Modulo,
        StringConcat,
        Gt,
        Lt,
        GtEq,
        LtEq,
        Eq,
        NotEq,
        And,
        Or,
    }

    impl QueryFormat for BinaryOperator {
        fn query_format(&self) -> String {
            match self {
                BinaryOperator::Plus => String::from("+"),
                BinaryOperator::Minus => String::from("-"),
                BinaryOperator::Multiply => String::from("*"),
                BinaryOperator::Divide => String::from("/"),
                BinaryOperator::Modulo => String::from("%"),
                BinaryOperator::StringConcat => String::from("+"),
                BinaryOperator::Gt => String::from(">"),
                BinaryOperator::Lt => String::from("<"),
                BinaryOperator::GtEq => String::from(">="),
                BinaryOperator::LtEq => String::from("<="),
                BinaryOperator::Eq => String::from("="),
                BinaryOperator::NotEq => String::from("!="),
                BinaryOperator::And => String::from("&&"),
                BinaryOperator::Or => String::from("||"),
            }
        }
    }

    impl From<ast::BinaryOperator> for BinaryOperator {
        fn from(op: ast::BinaryOperator) -> Self {
            match op {
                ast::BinaryOperator::Plus => BinaryOperator::Plus,
                ast::BinaryOperator::Minus => BinaryOperator::Minus,
                ast::BinaryOperator::Multiply => BinaryOperator::Multiply,
                ast::BinaryOperator::Divide => BinaryOperator::Divide,
                ast::BinaryOperator::Modulo => BinaryOperator::Modulo,
                ast::BinaryOperator::Gt => BinaryOperator::Gt,
                ast::BinaryOperator::Lt => BinaryOperator::Lt,
                ast::BinaryOperator::GtEq => BinaryOperator::GtEq,
                ast::BinaryOperator::LtEq => BinaryOperator::LtEq,
                ast::BinaryOperator::Eq => BinaryOperator::Eq,
                ast::BinaryOperator::NotEq => BinaryOperator::NotEq,
                ast::BinaryOperator::And => BinaryOperator::And,
                ast::BinaryOperator::Or => BinaryOperator::Or,
                _ => todo!(),
            }
        }
    }

    #[derive(Debug, Clone, Hash, PartialEq, Eq)]
    pub enum UnaryOperator {
        Plus,
        Minus,
        Not,
    }

    impl QueryFormat for UnaryOperator {
        fn query_format(&self) -> String {
            match self {
                UnaryOperator::Plus => String::from("+"),
                UnaryOperator::Minus => String::from("-"),
                UnaryOperator::Not => String::from("NOT "),
            }
        }
    }

    impl From<ast::UnaryOperator> for UnaryOperator {
        fn from(op: ast::UnaryOperator) -> Self {
            match op {
                ast::UnaryOperator::Plus => UnaryOperator::Plus,
                ast::UnaryOperator::Minus => UnaryOperator::Minus,
                ast::UnaryOperator::Not => UnaryOperator::Not,
                _ => todo!(),
            }
        }
    }

    #[derive(Debug)]
    pub enum Item {
        WildCard,
        Expr(Expr),
        ExprWithAias { expr: Expr, alias: String },
    }

    impl Item {
        fn bind_edges(items: Vec<Item>, edges: HashMap<String, Edge>) -> Vec<Self> {
            let mut used = HashSet::new();
            let mut items: Vec<Item> = items
                .into_iter()
                .map(|item| match item {
                    Item::Expr(Expr::CompoundIdentifier(ids)) => {
                        let table = ids.first().unwrap();
                        if let Some(edge) = edges.get(table) {
                            used.insert(edge.name.clone());
                            Item::Expr(Expr::CompoundEdge(
                                edge.clone(),
                                ids.last().unwrap().clone(),
                            ))
                        } else {
                            Item::Expr(Expr::CompoundIdentifier(ids))
                        }
                    }
                    Item::ExprWithAias {
                        expr: Expr::CompoundIdentifier(ids),
                        alias,
                    } => {
                        let table = ids.first().unwrap();
                        if let Some(edge) = edges.get(table) {
                            used.insert(edge.name.clone());
                            Item::Expr(Expr::CompoundEdge(
                                edge.clone(),
                                ids.last().unwrap().clone(),
                            ))
                        } else {
                            Item::ExprWithAias {
                                expr: Expr::CompoundIdentifier(ids),
                                alias,
                            }
                        }
                    }
                    e => e,
                })
                .collect();
            let mut edges = edges
                .into_values()
                .collect::<HashSet<Edge>>()
                .iter()
                .filter_map(|edge| {
                    if !used.contains(&edge.name) {
                        Some(Item::Expr(Expr::Edge(edge.clone())))
                    } else {
                        None
                    }
                })
                .collect();
            items.append(&mut edges);
            items
        }
    }

    impl QueryFormat for Item {
        fn query_format(&self) -> String {
            match self {
                Item::WildCard => String::from("*"),
                Item::Expr(expr) => expr.query_format(),
                Item::ExprWithAias { expr, alias } => {
                    format!("{} AS {}", expr.query_format(), alias)
                }
            }
        }
    }

    #[derive(Debug)]
    pub struct Table {
        pub name: String,
        pub alias: Option<String>,
    }

    impl QueryFormat for Table {
        fn query_format(&self) -> String {
            self.name.clone()
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    pub struct Edge {
        pub name: String,
        pub alias: Option<String>,
        pub edge: Option<String>,
        pub leaf: Option<Box<Edge>>,
        pub filter: Option<Box<Expr>>,
    }

    impl QueryFormat for Edge {
        fn query_format(&self) -> String {
            let mut edges: Vec<String> = vec![];
            if let Some(filter) = &self.filter {
                // TODO need to complete this string
                edges.push(format!("({} WHERE {})", self.name, filter.query_format()));
            } else {
                edges.push(self.name.to_owned());
            }
            let edge = self
                .edge
                .as_ref()
                .map_or(String::from("?"), |e| e.to_owned());
            edges.push(edge);

            let mut leaf = self;

            while let Some(node) = &leaf.leaf {
                if node.filter.is_some() {
                } else {
                    let edge = node
                        .edge
                        .as_ref()
                        .map_or(String::from("?"), |e| e.to_owned());
                    edges.push(node.name.to_owned());
                    edges.push(edge);
                }
                leaf = node;
            }
            edges.push(String::from(""));
            edges.reverse();
            let mut edge = edges.join("->");
            edge.push_str(".*");
            edge
        }
    }

    impl Edge {
        fn new(name: String, alias: Option<String>) -> Self {
            Edge {
                name,
                alias,
                edge: None,
                leaf: None,
                filter: None,
            }
        }

        #[allow(dead_code)]
        fn append_leaf(self, mut node: Edge) -> Edge {
            node.leaf = Some(Box::new(self));
            node
        }

        fn bind_filter(&mut self, filter: Option<Expr>) -> Option<Expr> {
            if let Some(filter) = filter {
                self.walk_tree(&filter)
            } else {
                filter
            }
        }

        fn is_table(&self, idents: &Vec<String>) -> bool {
            idents.contains(&self.name) || self.alias.as_ref().is_some_and(|e| idents.contains(e))
        }

        fn set_filter(&mut self, left: &Expr, op: &BinaryOperator, right: &Expr) {
            self.filter = Some(Box::new(Expr::BinaryOp {
                left: Box::new(left.clone()),
                op: op.clone(),
                right: Box::new(right.clone()),
            }));
        }

        fn walk_tree(&mut self, filter: &Expr) -> Option<Expr> {
            match &filter {
                Expr::CompoundIdentifier(idents) if self.is_table(idents) => Some(filter.clone()),
                Expr::Value(_) => Some(filter.clone()),
                Expr::IsFalse(expr) => self.walk_tree(expr.borrow()),
                Expr::IsNotFalse(expr) => self.walk_tree(expr.borrow()),
                Expr::IsTrue(expr) => self.walk_tree(expr.borrow()),
                Expr::IsNotTrue(expr) => self.walk_tree(expr.borrow()),
                Expr::IsNull(expr) => self.walk_tree(expr.borrow()),
                Expr::IsNotNull(expr) => self.walk_tree(expr.borrow()),
                Expr::BinaryOp { left, op, right } => {
                    let left_expr = self.walk_tree(left.borrow());
                    let right_expr = self.walk_tree(right.borrow());
                    //TODO make add ability to perform multiple checks for same edge
                    if is_edge_filter_left(&left_expr, &right_expr) {
                        if self.is_table(left_expr.unwrap().idents().unwrap()) {
                            self.set_filter(left, op, right);
                            None
                        } else {
                            Some(filter.clone())
                        }
                    } else if is_edge_filter_right(&left_expr, &right_expr) {
                        if self.is_table(right_expr.unwrap().idents().unwrap()) {
                            self.set_filter(left, op, right);
                            None
                        } else {
                            Some(filter.clone())
                        }
                    } else if left_expr.is_none() {
                        right_expr
                    } else if right_expr.is_none() {
                        left_expr
                    } else {
                        Some(Expr::BinaryOp {
                            left: Box::new(left_expr.unwrap()),
                            op: op.clone(),
                            right: Box::new(right_expr.unwrap()),
                        })
                    }
                }
                Expr::UnaryOp {
                    op: UnaryOperator::Not,
                    expr,
                } => self.walk_tree(expr.borrow()),
                Expr::Nested(expr) => self
                    .walk_tree(expr.borrow())
                    .map(|e| Expr::Nested(Box::new(e))),
                _ => Some(filter.clone()),
            }
        }
    }

    fn is_edge_filter_left(left: &Option<Expr>, right: &Option<Expr>) -> bool {
        right.as_ref().is_some_and(|e| e.is_value())
            && left.as_ref().is_some_and(|e| e.is_compound_id())
    }

    fn is_edge_filter_right(left: &Option<Expr>, right: &Option<Expr>) -> bool {
        right.as_ref().is_some_and(|e| e.is_compound_id())
            && left.as_ref().is_some_and(|e| e.is_value())
    }
}

fn main() {
    let dialect = GenericDialect {}; // or AnsiDialect

    let sql = r#"
            SELECT *
            FROM table_1 AS tb1
            JOIN table_2 AS tb2 ON tb2.id = tb1.id
            JOIN table_3
            WHERE tb1.id = 2 OR table_2.name = 12 AND (tb1.ad = 3 AND tb1.at = "try this")
            "#;

    let ast = Parser::parse_sql(&dialect, sql)
        .unwrap()
        .first()
        .unwrap()
        .to_owned();

    println!("AST: {:#?}", ast);

    let surql = surql::Select::parse(ast).unwrap();

    println!("surql: {:#?}", surql);

    let surql = surql.transpile();
    println!("surql: \n{}", surql);
}

#[cfg(test)]
mod test {
    use crate::surql::Transpile;

    use super::*;

    #[test]
    fn transplie_select_simple() {
        let dialect = GenericDialect {}; // or AnsiDialect
        let sql = r#"
                        SELECT *
                        FROM table_1
                        "#;
        let ast = Parser::parse_sql(&dialect, sql)
            .unwrap()
            .first()
            .unwrap()
            .to_owned();
        let surql = surql::Select::parse(ast).unwrap();

        let surql = surql::Select::transpile(&surql);

        let sql = "SELECT * \nFROM table_1";
        assert_eq!(sql, surql);
    }

    #[test]
    fn transplie_select_alias() {
        let dialect = GenericDialect {}; // or AnsiDialect
        let sql = r#"
                        SELECT *, row_1 AS r1
                        FROM table_1 AS t
                        "#;
        let ast = Parser::parse_sql(&dialect, sql)
            .unwrap()
            .first()
            .unwrap()
            .to_owned();
        let surql = surql::Select::parse(ast).unwrap();

        let surql = surql::Select::transpile(&surql);

        let sql = "SELECT *, row_1 AS r1 \nFROM table_1";
        assert_eq!(sql, surql);
    }

    #[test]
    fn transplie_select_compound() {
        let dialect = GenericDialect {}; // or AnsiDialect
        let sql = r#"
                        SELECT *, t.row_1 AS r1
                        FROM table_1 AS t
                        "#;
        let ast = Parser::parse_sql(&dialect, sql)
            .unwrap()
            .first()
            .unwrap()
            .to_owned();
        let surql = surql::Select::parse(ast).unwrap();

        let surql = surql::Select::transpile(&surql);

        let sql = "SELECT *, row_1 AS r1 \nFROM table_1";
        assert_eq!(sql, surql);
    }

    #[test]
    fn transpile_select_edge() {
        let dialect = GenericDialect {}; // or AnsiDialect
        let sql = r#"
                        SELECT *
                        FROM table_1
                        LEFT JOIN table_2
                        JOIN table_3
                        "#;
        let ast = Parser::parse_sql(&dialect, sql)
            .unwrap()
            .first()
            .unwrap()
            .to_owned();
        let surql = surql::Select::parse(ast).unwrap();

        let surql = surql::Select::transpile(&surql);
        let sql = "SELECT *, ->?->table_2.*, ->?->table_3.* \nFROM table_1";
        assert_eq!(sql, surql);
    }

    #[test]
    fn transpile_select_edge_alias() {
        let dialect = GenericDialect {}; // or AnsiDialect

        let sql = r#"
        SELECT t1.a, t2.b, table_3.c
        FROM table_1 AS t1
        JOIN table_2 AS t2 ON t1.id = t2.id
        JOIN table_3 AS t3 ON t3.id = t2.id
        "#;

        let ast = Parser::parse_sql(&dialect, sql)
            .unwrap()
            .first()
            .unwrap()
            .to_owned();

        let surql = surql::Select::parse(ast).unwrap();

        let sql = "SELECT ->?->table_2.b, ->?->table_3.c, a \nFROM table_1";
        let surql = surql.transpile();
        assert_eq!(sql, surql);
    }
    #[test]
    fn transpile_select_where_parse() {
        let dialect = GenericDialect {}; // or AnsiDialect

        let sql = r#"
            SELECT *
            FROM table_1 AS tb1
            JOIN table_2 AS tb2 ON tb2.id = tb1.id
            JOIN table_3 AS tb3
            WHERE tb1.id = 2 OR table_2.name = 12 AND tb3.ad = 3 AND tb1.at = "a"
        "#;

        let ast = Parser::parse_sql(&dialect, sql)
            .unwrap()
            .first()
            .unwrap()
            .to_owned();

        let surql = surql::Select::parse(ast).unwrap();

        let sql = "SELECT *, ->?->(table_2 WHERE name = 12).*, ->?->(table_3 WHERE ad = 3).* \nFROM table_1 \nWHERE id = 2 || at = \"a\"";
        let surql = surql.transpile();
        assert_eq!(sql, surql);
    }
}
