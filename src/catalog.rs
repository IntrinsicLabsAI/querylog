#[cfg(test)]
mod test {
    use arrow_schema::{DataType, Field};
    use datafusion::{
        arrow::datatypes::SchemaBuilder,
        catalog::{
            schema::{MemorySchemaProvider, SchemaProvider},
            CatalogProvider, MemoryCatalogProvider,
        },
        datasource::MemTable,
        error::DataFusionError,
        logical_expr::{ScalarUDF, Signature},
        prelude::{SessionConfig, SessionContext},
        scalar::ScalarValue,
    };
    use datafusion_expr::{ColumnarValue, ReturnTypeFunction, ScalarFunctionImplementation};
    use std::{error::Error, sync::Arc};

    /// pg_fake_schema
    ///
    /// Test to ensure we can meet Metabase's requirement for "schema ripping" from PG. We're going to verify
    /// that all queries issued by Metabase will execute with the right statement internally.
    #[tokio::test]
    async fn pg_fake_schema() -> Result<(), Box<dyn Error>> {
        //
        // Defining a custom schema that encapsulates the following Postgres schema for the pg_namespace table:
        //
        //      Column  |   Type    | Collation | Nullable | Default | Storage  | Compression | Stats target | Description
        //      ----------+-----------+-----------+----------+---------+----------+-------------+--------------+-------------
        //       oid      | oid       |           | not null |         | plain    |             |              |
        //       nspname  | name      |           | not null |         | plain    |             |              |
        //       nspowner | oid       |           | not null |         | plain    |             |              |
        //       nspacl   | aclitem[] |           |          |         | extended |             |              |
        //
        let mut pg_namespace_schema = SchemaBuilder::new();
        pg_namespace_schema.push(Arc::new(arrow_schema::Field::new(
            "oid",
            arrow_schema::DataType::UInt32,
            false,
        )));
        pg_namespace_schema.push(Arc::new(arrow_schema::Field::new(
            "nspname",
            arrow_schema::DataType::Utf8,
            false,
        )));
        pg_namespace_schema.push(Arc::new(arrow_schema::Field::new(
            "nspowner",
            arrow_schema::DataType::Utf8,
            false,
        )));
        let pg_namespace_schema = pg_namespace_schema.finish();
        // pg_namespace_schema.push(Arc::new(arrow_schema::Field::new_list(
        //     "nspacl",
        //     Arc::new(arrow_schema::Field::new()),
        //     true,
        // )));
        // How to define a custom type

        // Create a bunch of new schemas this way

        let pg_namespace = Arc::new(MemTable::try_new(
            Arc::new(pg_namespace_schema),
            // Create some dumb records
            vec![vec![]],
        )?);

        let pg_catalog = Arc::new(MemorySchemaProvider::new());
        pg_catalog.register_table("pg_namespace".to_string(), pg_namespace)?;

        let catalog = Arc::new(MemoryCatalogProvider::new());
        catalog.register_schema("pg_catalog", pg_catalog)?;

        // Validate that we can query from there
        let ctx = SessionContext::with_config(
            SessionConfig::default().with_default_catalog_and_schema("fake_postgres", "pg_catalog"),
        );

        fn current_schemas(true_or_false: bool) -> Vec<String> {
            if true_or_false {
                vec!["pg_catalog".to_string(), "public".to_string()]
            } else {
                vec!["public".to_string()]
            }
        }

        // Figure out how we're going to execute against the provided catalog
        // String list
        ctx.register_catalog("fake_postgres", catalog);

        let rt_func: ReturnTypeFunction = Arc::new(
            |_dts: &[DataType]| -> Result<Arc<DataType>, DataFusionError> {
                Ok(Arc::new(DataType::new_list(DataType::Utf8, false)))
            },
        );
        let func_impl: ScalarFunctionImplementation = Arc::new(
            move |cvs: &[ColumnarValue]| -> Result<ColumnarValue, DataFusionError> {
                if let Some(cv) = cvs.iter().next() {
                    let schemas = match cv {
                        ColumnarValue::Scalar(val) => match val {
                            datafusion::scalar::ScalarValue::Boolean(Some(val)) => {
                                current_schemas(val.clone())
                            }
                            _ => todo!("invalid value type"),
                        },
                        _ => todo!("get f*cked"),
                    };
                    return Ok(ColumnarValue::Scalar(ScalarValue::List(
                        Some(
                            schemas
                                .into_iter()
                                .map(|sch| ScalarValue::Utf8(Some(sch)))
                                .collect(),
                        ),
                        Arc::new(Field::new("item", DataType::Utf8, false)),
                    )));
                } else {
                    return Err(DataFusionError::Internal(
                        "Failed to extract bool function arg".to_string(),
                    ));
                }
            },
        );
        ctx.register_udf(ScalarUDF::new(
            "pg_catalog.current_schemas",
            &Signature::new(
                datafusion::logical_expr::TypeSignature::Exact(vec![DataType::Boolean]),
                datafusion::logical_expr::Volatility::Immutable,
            ),
            &rt_func,
            &func_impl,
        ));

        let results = ctx.sql(r#"
            SELECT nspname AS TABLE_SCHEM, NULL AS TABLE_CATALOG
            FROM pg_catalog.pg_namespace  
            WHERE       nspname <> 'pg_toast'
                    AND (nspname !~ '^pg_temp_'  OR nspname = (pg_catalog.current_schemas(true))[1])
                    AND (nspname !~ '^pg_toast_temp_'  OR nspname = replace((pg_catalog.current_schemas(true))[1], 'pg_temp_', 'pg_toast_temp_'))
            ORDER BY TABLE_SCHEM
    "#).await?;

        results.show().await?;

        Ok(())
    }
}
