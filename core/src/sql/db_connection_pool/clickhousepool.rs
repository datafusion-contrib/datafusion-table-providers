use clickhouse::Client;

use super::{dbconnection::DbConnection, DbConnectionPool, JoinPushDown};

#[async_trait::async_trait]
impl DbConnectionPool<Client, ()> for Client {
    async fn connect(&self) -> super::Result<Box<dyn DbConnection<Client, ()>>> {
        Ok(Box::new(self.clone()))
    }

    fn join_push_down(&self) -> JoinPushDown {
        JoinPushDown::Disallow
    }
}
