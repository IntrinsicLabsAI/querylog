// Create a wire protocol listener

use std::error::Error;
use std::fmt::Debug;
use std::ops::Deref;
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream;
use futures_sink::Sink;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::auth::{LoginInfo, StartupHandler};
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler, StatementOrPortal};
use pgwire::api::results::{DescribeResponse, FieldFormat, FieldInfo, QueryResponse, Response};
use pgwire::api::stmt::NoopQueryParser;
use pgwire::api::store::MemPortalStore;
use pgwire::api::{ClientInfo, MakeHandler, StatelessMakeHandler, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use pgwire::tokio::process_socket;
use querylog::macros::row_vec;
use querylog::{make_query_response, numeric_field, text_field, FieldValue, SimpleRow};
use sqlparser::ast::{Expr, Ident, Query, Select, SelectItem, SetExpr};
use sqlparser::parser::Parser;
use tracing::instrument;
use tracing_subscriber::fmt::format::FmtSpan;

/// Top-level connection data type that all handling logic is attached on.
struct PgConnectionHandler {
    query_parser: Arc<NoopQueryParser>,
    portal_store: Arc<MemPortalStore<String>>,
}

impl PgConnectionHandler {
    #[instrument(skip(self))]
    fn extract_schema(&self, select: &Select) -> Vec<FieldInfo> {
        let default_schema = vec![text_field("a"), numeric_field("b")];

        // Create a schema for each of these things
        let mut schema: Vec<FieldInfo> = Vec::new();

        for proj in &select.projection {
            match proj {
                SelectItem::Wildcard(_) => schema.append(&mut default_schema.clone()),
                SelectItem::UnnamedExpr(expr) => match expr {
                    Expr::Identifier(id) => schema.push(FieldInfo::new(
                        id.to_string(),
                        None,
                        None,
                        Type::TEXT,
                        FieldFormat::Text,
                    )),
                    Expr::Value(value) => match &value {
                        sqlparser::ast::Value::Number(_, _) => schema.push(numeric_field("value")),
                        sqlparser::ast::Value::SingleQuotedString(_) => {
                            schema.push(text_field("value"))
                        }
                        sqlparser::ast::Value::DollarQuotedString(_) => {
                            schema.push(text_field("value"))
                        }
                        sqlparser::ast::Value::EscapedStringLiteral(_) => {
                            schema.push(text_field("value"))
                        }
                        sqlparser::ast::Value::SingleQuotedByteStringLiteral(_) => {
                            schema.push(text_field("value"))
                        }
                        sqlparser::ast::Value::DoubleQuotedByteStringLiteral(_) => {
                            schema.push(text_field("value"))
                        }
                        sqlparser::ast::Value::RawStringLiteral(_) => {
                            schema.push(text_field("value"))
                        }
                        sqlparser::ast::Value::NationalStringLiteral(_) => {
                            schema.push(text_field("value"))
                        }
                        sqlparser::ast::Value::HexStringLiteral(_) => {
                            schema.push(text_field("value"))
                        }
                        sqlparser::ast::Value::DoubleQuotedString(_) => {
                            schema.push(text_field("value"))
                        }
                        sqlparser::ast::Value::Boolean(_) => schema.push(text_field("value")),
                        sqlparser::ast::Value::Null => schema.push(text_field("value")),
                        sqlparser::ast::Value::Placeholder(_) => todo!(),
                        sqlparser::ast::Value::UnQuotedString(_) => todo!(),
                    },
                    _ => todo!("bad stuff"),
                },
                _ => todo!("whoops"),
            }
        }

        schema
    }

    #[instrument(skip(self))]
    fn handle_select<'a>(&self, select: &Query) -> PgWireResult<Vec<Response<'a>>> {
        let mut responses = Vec::new();
        let schema = Arc::new(match select.body.deref() {
            SetExpr::Select(selection) => self.extract_schema(selection),
            _ => todo!("bad stuff happened"),
        });

        // Fake data strings here
        for _ in 0..10 {
            let fields = schema
                .iter()
                .map(|field| FieldValue::String(format!("Hello {}", field.name().as_str())))
                .collect::<Vec<_>>();
            let row = SimpleRow::new(schema.clone(), fields).try_into();
            responses.push(row);
        }

        Ok(vec![Response::Query(QueryResponse::new(
            Arc::clone(&schema),
            stream::iter(responses),
        ))])
    }

    #[instrument(skip(self))]
    fn empty_result<'a>(&self) -> PgWireResult<Vec<Response<'a>>> {
        Ok(vec![Response::EmptyQuery])
    }

    #[instrument(skip(self))]
    fn handle_show<'a>(&self, variables: &[Ident]) -> PgWireResult<Vec<Response<'a>>> {
        let text = variables
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<String>>()
            .join(" ");

        let transaction_isolation_level_schema =
            Arc::new(vec![text_field("transaction_isolation")]);

        let responses = match text.to_lowercase().as_str() {
            "transaction isolation level" => {
                let row: DataRow = SimpleRow::new(
                    transaction_isolation_level_schema.clone(),
                    row_vec!["read committed"],
                )
                .try_into()?;

                vec![make_query_response(
                    transaction_isolation_level_schema.clone(),
                    &[row],
                )]
            }
            _ => todo!("unhandled variable {:?}", text),
        };

        Ok(responses)
    }

    #[instrument(skip(self))]
    fn handle_stmt<'a>(&self, query: &str) -> PgWireResult<Vec<Response<'a>>> {
        let dialect = sqlparser::dialect::GenericDialect;

        if let Ok(ast) = Parser::parse_sql(&dialect, query) {
            if let Some(stmt) = ast.get(0) {
                match stmt {
                    sqlparser::ast::Statement::Query(ref query) => {
                        return self.handle_select(query)
                    }
                    sqlparser::ast::Statement::ShowVariable { variable } => {
                        return self.handle_show(variable)
                    }
                    _ => {
                        tracing::error!("unmatched statement: {:?}", stmt);
                        return self.empty_result();
                    }
                }
            } else {
                PgWireResult::Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "error".to_string(),
                    "CODE".to_string(),
                    "failed to parse statement".to_string(),
                ))))
            }
        } else {
            PgWireResult::Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "bad".to_string(),
                "CODE".to_string(),
                "failed to parse statement".to_string(),
            ))))
        }
    }
}

#[async_trait]
impl SimpleQueryHandler for PgConnectionHandler {
    #[instrument(skip(self, _client))]
    async fn do_query<'a, C>(&self, _client: &C, query: &'a str) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        self.handle_stmt(query)
    }
}

#[async_trait]
impl ExtendedQueryHandler for PgConnectionHandler {
    type Statement = String;
    type QueryParser = NoopQueryParser;
    type PortalStore = MemPortalStore<Self::Statement>;

    fn portal_store(&self) -> Arc<Self::PortalStore> {
        self.portal_store.clone()
    }

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.query_parser.clone()
    }

    #[instrument(skip(self, _client))]
    async fn do_describe<C>(
        &self,
        _client: &mut C,
        stmt: StatementOrPortal<'_, Self::Statement>,
    ) -> PgWireResult<DescribeResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let stmt = match stmt {
            StatementOrPortal::Statement(stmt) => stmt.statement().clone(),
            StatementOrPortal::Portal(portal) => portal.statement().statement().clone(),
        };

        // Figure out how we're going to get everything selected as well
        if &stmt.to_lowercase() == "select 1" {
            Ok(DescribeResponse::new(None, vec![numeric_field("value")]))
        } else {
            // Describe for anything other than a SELECT
            Ok(DescribeResponse::no_data())
        }
    }

    async fn do_query<'a, 'b: 'a, C>(
        &'b self,
        client: &mut C,
        portal: &'a Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response<'a>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let login_info = LoginInfo::from_client_info(client);
        let query = portal.statement().statement();
        tracing::info!(
            "EXTENDED_QUERY user: {:?}     query: {:?}",
            &login_info,
            query.as_str()
        );

        match self.handle_stmt(query.as_str()) {
            Ok(responses) => {
                if let Some(response) = responses.into_iter().next() {
                    return Ok(response);
                } else {
                    return Ok(Response::EmptyQuery);
                }
            }
            Err(err) => return Err(err),
        }
    }
}

struct LoggingNoopAuthenticator(NoopStartupHandler);

#[async_trait]
impl StartupHandler for LoggingNoopAuthenticator {
    #[instrument(skip(self, client))]
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        self.0.on_startup(client, message).await
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Setup logging
    tracing_subscriber::fmt()
        .with_span_events(FmtSpan::CLOSE)
        .init();

    let connection = Arc::new(PgConnectionHandler {
        portal_store: Arc::new(MemPortalStore::new()),
        query_parser: Arc::new(NoopQueryParser::new()),
    });

    let authenticator_factory = Arc::new(StatelessMakeHandler::new(Arc::new(
        LoggingNoopAuthenticator(NoopStartupHandler),
    )));

    let listener = tokio::net::TcpListener::bind(&"0.0.0.0:5555").await?;

    tracing::info!("serving Postgres protocol (insecure) on 0.0.0.0:5555");

    loop {
        let (conn, addr) = listener.accept().await?;

        let authenticator_factory = Arc::clone(&authenticator_factory);
        let query_handler = Arc::clone(&connection);
        let extended_query_handler = Arc::clone(&query_handler);
        tokio::spawn(async move {
            tracing::info!("connection from {:?}", &addr);

            let authenticator = authenticator_factory.make();

            process_socket(
                conn,
                None,
                authenticator,
                query_handler,
                extended_query_handler,
            )
            .await
            .unwrap();
        });
    }
}
