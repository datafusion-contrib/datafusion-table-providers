use tokio_postgres::{NoTls, Error};

mod common;

#[tokio::test]
async fn test_test() {
    println!("100");
    let port = crate::get_random_port();
    let container = common::start_postgres_docker_container(port)
        .await
        .expect("Postgres container to start");

    println!("200");

    let params = common::get_pg_params(port); // or whatever port you want

    let conn_str = format!(
        "host={} port={} user={} password={} dbname={} sslmode={}",
        params["pg_host"],
        params["pg_port"],
        params["pg_user"],
        params["pg_pass"],
        params["pg_db"],
        params["pg_sslmode"]
    );

    println!("conn_str: {}", conn_str);

    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await.expect("Connection to database");

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    println!("300");
    client
        .execute(
            "CREATE TABLE IF NOT EXISTS demo_table (id SERIAL PRIMARY KEY, name TEXT)",
            &[],
        )
        .await.expect("Table to be created");

    println!("400");

    // Insert a row
    client
        .execute("INSERT INTO demo_table (name) VALUES ($1)", &[&"Hello, world!"])
        .await.expect("Row to be inserted");

    println!("500");

    // Read it back
    let rows = client
        .query("SELECT id, name FROM demo_table", &[])
        .await.expect("Row to be read");

    println!("600");

    for row in rows {
        let id: i32 = row.get(0);
        let name: &str = row.get(1);
        println!("Read row: id = {}, name = {}", id, name);
        assert!(id > 0);
        assert_eq!(name, "Hello, world!");
    }
}
