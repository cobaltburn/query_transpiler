use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::surql::Transpile;

mod surql {

    use std::borrow::{Borrow, BorrowMut};

    use anyhow::{anyhow, Result};
    use sqlparser::ast;

    #[allow(dead_code)]
    pub trait Transpile {
        fn transpile(&self) -> String;
    }

    #[allow(dead_code)]
    pub trait QueryFormat {
        fn query_format(&self) -> String;
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    pub struct Select {
        pub items: Vec<Item>,
        pub from: Vec<Table>,
    }
    impl Transpile for Select {
        fn transpile(&self) -> String {
            let mut query = vec![String::from("SELECT")];
            self.items.iter().for_each(|item| {
                query.push(format!("{},", item.query_format()));
            });
            let last = query.last_mut().unwrap();
            *last = last.trim_end_matches(',').to_owned();
            query.push("\nFROM".to_owned());

            self.from.iter().for_each(|table| {
                query.push(table.query_format());
            });

            query.join(" ")
        }
    }

    #[allow(dead_code)]
    impl Select {
        pub fn parse(statement: ast::Statement) -> Result<Select> {
            let ast::Statement::Query(query) = statement else {
                return Err(anyhow!("Statement give was not a select type"));
            };

            let ast::SetExpr::Select(select) = *query.body else {
                return Err(anyhow!("Statement give was not a select type"));
            };
            let mut from = vec![];
            let mut edges = vec![];
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

                let mut new_edges = parse_joins(joins, &name, &alias);
                edges.append(&mut new_edges);

                from.push(Table { name, alias });
            }
            let mut items = vec![];
            for item in select.projection {
                let item = match item {
                    //TODO worko on edge check here
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

            Ok(Select { items, from })
        }
    }

    // TODO need to add ability to detect many bridge tables
    fn parse_joins(joins: Vec<ast::Join>, root: &String, alias: &Option<String>) -> Vec<Edge> {
        let mut edges = vec![];
        for join in joins {
            let edge = Edge::new(root.clone(), alias.clone());
            let node = match join.relation {
                ast::TableFactor::Table { name, alias, .. } => {
                    let root = name.0.last().unwrap().value.to_owned();
                    let alias = alias.map(|e| e.name.value);
                    Edge::new(root, alias)
                }
                _ => todo!(),
            };
            let edge = edge.append_leaf(node);

            edges.push(edge);
        }
        edges
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    pub enum Expr {
        Identifier(String),
        CompoundIdentifier(Vec<String>),
        Edge(Edge),
        CompoundEdge(Edge, Vec<String>),
    }

    impl QueryFormat for Expr {
        fn query_format(&self) -> String {
            match self {
                Expr::Identifier(ident) => ident.clone(),
                Expr::CompoundIdentifier(idents) => idents.join("."),
                Expr::Edge(edge) => edge.query_format(),
                Expr::CompoundEdge(_, _) => todo!(),
            }
        }
    }

    impl Expr {
        fn append_edges(mut exprs: Vec<Expr>, edges: Vec<Edge>) -> Vec<Self> {
            for edge in edges {
                let x = edge.name;
                exprs.fi
            }
            todo!()
        }
    }

    impl From<ast::Expr> for Expr {
        fn from(value: ast::Expr) -> Self {
            match value {
                ast::Expr::Identifier(ident) => Expr::Identifier(ident.value),
                ast::Expr::CompoundIdentifier(idents) => {
                    Expr::CompoundIdentifier(idents.iter().map(|e| e.value.to_owned()).collect())
                }
                _ => todo!(),
            }
        }
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    pub enum Item {
        WildCard,
        Expr(Expr),
        ExprWithAias { expr: Expr, alias: String },
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

    #[allow(dead_code)]
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

    #[allow(dead_code)]
    #[derive(Debug)]
    pub struct Edge {
        pub name: String,
        pub alias: Option<String>,
        pub edge: Option<String>,
        pub leaf: Option<Box<Edge>>,
    }

    impl QueryFormat for Edge {
        fn query_format(&self) -> String {
            let mut edges: Vec<&str> = vec![&self.name];
            let edge = self.edge.as_ref().map_or("?", |e| e.as_str());
            edges.push(edge);

            let mut leaf = self.leaf.borrow();
            while let Some(node) = &leaf {
                if node.leaf.is_some() {
                    let edge = node.edge.as_ref().map_or("?", |e| e.as_str());
                    edges.push(&node.name);
                    edges.push(edge);
                }
                leaf = node.leaf.borrow();
            }
            edges.push("");
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
            }
        }

        fn append_leaf(self, mut node: Edge) -> Edge {
            node.leaf = Some(Box::new(self));
            node
        }
    }
}

fn main() {
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
    fn transpil_select_edge() {
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
}
