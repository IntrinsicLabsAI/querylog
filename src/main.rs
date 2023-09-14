// Create a wire protocol listener

use std::error::Error;
use std::fmt::{Debug, Pointer};
use std::ops::Deref;
use std::sync::Arc;

use anyhow::bail;
use async_trait::async_trait;
use futures::stream::{self, StreamExt};
use futures_sink::Sink;
use futures_util::Stream;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::auth::{LoginInfo, StartupHandler};
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler, StatementOrPortal};
use pgwire::api::results::{
    DataRowEncoder, DescribeResponse, FieldFormat, FieldInfo, QueryResponse, Response,
};
use pgwire::api::stmt::NoopQueryParser;
use pgwire::api::store::MemPortalStore;
use pgwire::api::{ClientInfo, MakeHandler, PgWireConnectionState, StatelessMakeHandler, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::{response, PgWireBackendMessage, PgWireFrontendMessage};
use pgwire::tokio::process_socket;
use sqlparser::ast::{Expr, Query, Select, SelectItem, SetExpr};
use sqlparser::parser::Parser;

/// Simple handler for all things
struct PgConnectionHandler {
    query_parser: Arc<NoopQueryParser>,
    portal_store: Arc<MemPortalStore<String>>,
}

// Make a new handler for the connection process
#[async_trait]
impl StartupHandler for PgConnectionHandler {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: Debug,
    {
        println!("startup executing");
        match client.state() {
            PgWireConnectionState::AwaitingStartup => println!("AwaitingStartup"),
            PgWireConnectionState::AuthenticationInProgress => println!("AuthenticationInProgress"),
            PgWireConnectionState::ReadyForQuery => println!("ReadyForQuery"),
            PgWireConnectionState::QueryInProgress => println!("QueryInProgress"),
        };

        match message {
            PgWireFrontendMessage::Startup(ref startup) => println!("Startup msg: {:?}", startup),
            PgWireFrontendMessage::PasswordMessageFamily(_) => {}
            PgWireFrontendMessage::Query(ref query) => println!("query: {:?}", query),
            PgWireFrontendMessage::Parse(_) => {}
            PgWireFrontendMessage::Close(_) => {}
            PgWireFrontendMessage::Bind(_) => {}
            PgWireFrontendMessage::Describe(_) => {}
            PgWireFrontendMessage::Execute(_) => {}
            PgWireFrontendMessage::Flush(_) => {}
            PgWireFrontendMessage::Sync(_) => {}
            PgWireFrontendMessage::Terminate(_) => {}
            PgWireFrontendMessage::CopyData(_) => {}
            PgWireFrontendMessage::CopyFail(_) => {}
            PgWireFrontendMessage::CopyDone(_) => {}
        };

        Ok(())
    }
}

// Return a silly in-memory schema that we can use for validation here instead
// Convert types from one framework to this new framework, e.g. Strings, numbers

// Extract output schema from the projection clause

impl PgConnectionHandler {
    fn extract_schema(&self, select: &Select) -> Vec<FieldInfo> {
        let default_schema = vec![
            FieldInfo::new(
                "a".to_string(),
                None,
                None,
                pgwire::api::Type::VARCHAR,
                FieldFormat::Text,
            ),
            FieldInfo::new("b".to_string(), None, None, Type::INT4, FieldFormat::Text),
        ];

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
                    _ => todo!("bad stuff"),
                },
                _ => todo!("whoops"),
            }
        }

        schema
    }

    fn handle_select<'a>(&self, select: &Query) -> PgWireResult<Vec<Response<'a>>> {
        let mut responses = Vec::new();
        let schema = Arc::new(match select.body.deref() {
            SetExpr::Select(selection) => self.extract_schema(&selection),
            _ => todo!("bad stuff happened"),
        });

        // Fake data strings here
        for _ in 0..10 {
            let row = {
                let mut enc = DataRowEncoder::new(Arc::clone(&schema));
                // Create some random data for every field
                for field in schema.iter() {
                    // Depending on the data type, we do one or the other here
                    enc.encode_field(&format!("Hello world {}", field.name()))?;
                }
                enc.finish()
            };

            responses.push(row);
        }

        Ok(vec![Response::Query(QueryResponse::new(
            Arc::clone(&schema),
            stream::iter(responses),
        ))])
    }

    fn empty_result<'a>(&self) -> PgWireResult<Vec<Response<'a>>> {
        Ok(vec![Response::EmptyQuery])
    }
}

#[async_trait]
impl SimpleQueryHandler for PgConnectionHandler {
    async fn do_query<'a, C>(&self, client: &C, query: &'a str) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let login_info = LoginInfo::from_client_info(client);

        println!("user={:?} initial query: {:?}", &login_info, query);

        // Extract the query, make sure that we have proper query handling here.
        let dialect = sqlparser::dialect::GenericDialect::default();
        if let Ok(ast) = Parser::parse_sql(&dialect, query) {
            if let Some(stmt) = ast.get(0) {
                match stmt {
                    sqlparser::ast::Statement::Query(ref query) => {
                        return self.handle_select(query)
                    }
                    _ => return self.empty_result(),
                }
            } else {
                return PgWireResult::Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "error".to_string(),
                    "CODE".to_string(),
                    "failed to parse statement".to_string(),
                ))));
            }
        } else {
            return PgWireResult::Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "bad".to_string(),
                "CODE".to_string(),
                "failed to parse statement".to_string(),
            ))));
        }
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

    async fn do_describe<C>(
        &self,
        client: &mut C,
        target: StatementOrPortal<'_, Self::Statement>,
    ) -> PgWireResult<DescribeResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        Ok(DescribeResponse::no_data())
    }

    async fn do_query<'a, 'b: 'a, C>(
        &'b self,
        client: &mut C,
        portal: &'a Portal<Self::Statement>,
        max_rows: usize,
    ) -> PgWireResult<Response<'a>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let login_info = LoginInfo::from_client_info(client);
        println!(
            "user: {:?}     query: {:?}",
            &login_info,
            portal.statement().statement()
        );
        Ok(Response::EmptyQuery)
    }
}

struct LoggingNoopAuthenticator(NoopStartupHandler);

#[async_trait]
impl StartupHandler for LoggingNoopAuthenticator {
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
        let login_info = LoginInfo::from_client_info(client);
        println!("STARTUP MESSAGE: user={:?} {:?}", &login_info, &message);

        // Ensure that the DB is setup properly.
        return self.0.on_startup(client, message).await;
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("Hello, world!");

    let startup_handler = Arc::new(PgConnectionHandler {
        portal_store: Arc::new(MemPortalStore::new()),
        query_parser: Arc::new(NoopQueryParser::new()),
    });

    let authenticator_factory = Arc::new(StatelessMakeHandler::new(Arc::new(
        LoggingNoopAuthenticator(NoopStartupHandler),
    )));

    let listener = tokio::net::TcpListener::bind(&"0.0.0.0:5555").await?;

    loop {
        let (conn, addr) = listener.accept().await?;

        let authenticator_factory = Arc::clone(&authenticator_factory);
        let query_handler = Arc::clone(&startup_handler);
        let extended_query_handler = Arc::clone(&query_handler);
        tokio::spawn(async move {
            println!("starting socket process loop with {:?}", &addr);

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
