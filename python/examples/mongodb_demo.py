# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Example demonstrating MongoDB table provider usage.

Prerequisites:
Start a MongoDB server using Docker:

```bash
docker run --name mongodb \
  -e MONGO_INITDB_ROOT_USERNAME=root \
  -e MONGO_INITDB_ROOT_PASSWORD=password \
  -e MONGO_INITDB_DATABASE=mongo_db \
  -p 27017:27017 \
  -d mongo:7.0

# Wait for the MongoDB server to start
sleep 30

# Create a table in the MongoDB server and insert some data
docker exec -i mongodb mongosh -u root -p password --authenticationDatabase admin <<EOF
use mongo_db;

db.companies.insertOne({
  id: 1,
  name: "Acme Corporation"
});
EOF
```
"""

import datafusion
from datafusion_table_providers.mongodb import MongoDBTableFactory


def main():
    # Create MongoDB connection parameters
    mongodb_params = {
        "connection_string": "mongodb://root:password@localhost:27017/mongo_db?authSource=admin&tls=false"
    }

    # Create MongoDB table factory
    factory = MongoDBTableFactory(mongodb_params)

    # List all tables
    tables = factory.tables()
    print(f"Tables: {tables}")

    # Get table provider for 'companies' table
    table = factory.get_table("companies")

    # Create DataFusion context and register the table
    ctx = datafusion.SessionContext()
    ctx.register_table_provider("companies", table)

    # Query the table
    df = ctx.sql("SELECT * FROM companies")
    df.show()


if __name__ == "__main__":
    main()
