import unittest
import os
from datafusion import SessionContext
from datafusion_table_providers import sqlite

class TestsqliteIntegration(unittest.TestCase):
    def setUp(self):
        """Set up the test environment"""
        self.ctx = SessionContext()
        self.db_path = os.path.join(os.path.dirname(__file__), "..", "..", "..", "core", "examples", "sqlite_example.db")
        self.pool = sqlite.SqliteTableFactory(self.db_path, "file", 3.0, None)

    def test_get_tables(self):
        """Test retrieving tables from the database"""
        tables = self.pool.tables()
        self.assertIsInstance(tables, list)
        self.assertTrue(len(tables) == 2)
        self.assertEqual(tables, ["companies", "projects"])
        
    def test_query_companies(self):
        """Test querying companies table with SQL"""
        self.ctx.register_table_provider("companies", self.pool.get_table("companies"))
        
        # Run SQL query to select Microsoft row
        df = self.ctx.sql("SELECT name FROM companies WHERE ticker = 'MSFT'")
        result = df.collect()
        
        # Verify single row returned with name = Microsoft
        self.assertEqual(len(result), 1)
        self.assertEqual(str(result[0]['name'][0]), "Microsoft")
        
    def test_complex_query(self):
        """Test querying companies table with SQL"""
        self.ctx.register_table_provider("companies", self.pool.get_table("companies"))
        self.ctx.register_table_provider("projects", self.pool.get_table("projects"))
        
        # Run SQL query to select Microsoft row
        df = self.ctx.sql(
            """SELECT companies.id, companies.name as company_name, projects.name as project_name
            FROM companies, projects
            WHERE companies.id = projects.id"""
        )
        result = df.collect()
    
        self.assertEqual(len(result), 1)
        self.assertEqual(str(result[0]['company_name'][0]), "Microsoft")
        self.assertEqual(str(result[0]['project_name'][0]), "DataFusion")
        
    def test_write_fails(self):
        """Test that writing fails when database is opened read-only"""
        table_name = "companies"
        self.ctx.register_table_provider(table_name, self.pool.get_table("companies"))
        
        with self.assertRaises(Exception):
            tmp = self.ctx.sql("INSERT INTO companies VALUES (3, 'Test Corp', 'TEST')")
            tmp.collect() # this will trigger the execution of the query


if __name__ == '__main__':
    unittest.main() 