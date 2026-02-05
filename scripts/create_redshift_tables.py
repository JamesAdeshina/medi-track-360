"""
Create Redshift tables for MediTrack360 Gold layer
Creates star schema tables in Redshift based on S3 Gold layer
"""

import psycopg2
import sys

# Add project root to Python path
sys.path.append(".")

try:
    from config.aws_config import aws_config
except ImportError:
    print("Warning: Could not import aws_config")
    aws_config = None


class RedshiftTableCreator:
    """Creates tables and views in Redshift"""

    def __init__(self, connection_params=None):
        self.connection_params = connection_params or self.load_connection_params()
        self.s3_bucket = getattr(aws_config, "bucket_name", "medi-track-360-data-lake")
        self.region = getattr(aws_config, "region", "eu-north-1")

    def load_connection_params(self):
        """
        Load connection parameters from file or defaults.
        File (optional): redshift_connection.txt
        """
        try:
            with open("redshift_connection.txt") as f:
                lines = f.readlines()
                params = {
                    line.split(":")[0].strip(): line.split(":")[1].strip()
                    for line in lines
                    if ":" in line
                }
                return {
                    "host": params.get("Host"),
                    "port": int(params.get("Port", 5439)),
                    "database": params.get("Database"),
                    "user": params.get("Username"),
                    "password": params.get("Password"),
                }
        except Exception:
            # Fallback: WORKING TEST CLUSTER
            return {
                "host": "test-redshift-cluster.cpou9pfugity.eu-north-1.redshift.amazonaws.com",
                "port": 5439,
                "database": "testdb",
                "user": "admin",
                "password": "YourStrongPassword123!",
            }

    def test_connection(self):
        """Test connection to Redshift"""
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT current_database(), current_user, version();"
                    )
                    db, user, version = cur.fetchone()
                    print(f"Connected to Redshift: {db} ({user})")
                    print(version)
            return True
        except Exception as e:
            print(f"Connection failed: {e}")
            return False

    def execute_statements(self, statements: dict, object_type="table"):
        """Generic executor for tables or views"""
        success_count = 0
        fail_count = 0

        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cur:
                    for name, sql in statements.items():
                        try:
                            cur.execute(sql)
                            print(f"Created {object_type}: {name}")
                            success_count += 1
                        except Exception as e:
                            print(f"Failed {object_type} {name}: {e}")
                            conn.rollback()
                            fail_count += 1

            print(
                f"\nCreated {success_count} {object_type}(s), Failed: {fail_count}"
            )
            return success_count > 0
        except Exception as e:
            print(f"Error executing {object_type} statements: {e}")
            return False

    def create_dimension_tables(self):
        """Create dimension tables"""
        dimension_sql = {
            "dim_date": """
                CREATE TABLE IF NOT EXISTS dim_date (
                    date_id INTEGER PRIMARY KEY,
                    full_date DATE NOT NULL,
                    year INTEGER NOT NULL,
                    quarter INTEGER NOT NULL,
                    month INTEGER NOT NULL,
                    month_name VARCHAR(20) NOT NULL,
                    day INTEGER NOT NULL,
                    day_of_week VARCHAR(20) NOT NULL,
                    day_of_week_num INTEGER NOT NULL,
                    is_weekend BOOLEAN NOT NULL,
                    is_holiday BOOLEAN DEFAULT FALSE,
                    financial_year INTEGER NOT NULL,
                    financial_quarter INTEGER NOT NULL,
                    created_at TIMESTAMP DEFAULT GETDATE()
                );
            """,
            "dim_patient": """
                CREATE TABLE IF NOT EXISTS dim_patient (
                    patient_id INTEGER PRIMARY KEY,
                    first_name VARCHAR(100),
                    last_name VARCHAR(100),
                    date_of_birth DATE,
                    gender VARCHAR(20),
                    age INTEGER,
                    age_group VARCHAR(20),
                    is_active BOOLEAN DEFAULT TRUE,
                    dim_loaded_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT GETDATE()
                );
            """,
            "dim_ward": """
                CREATE TABLE IF NOT EXISTS dim_ward (
                    ward_id INTEGER PRIMARY KEY,
                    ward_name VARCHAR(100),
                    ward_type VARCHAR(50),
                    capacity INTEGER,
                    capacity_category VARCHAR(20),
                    is_active BOOLEAN DEFAULT TRUE,
                    dim_loaded_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT GETDATE()
                );
            """,
            "dim_drug": """
                CREATE TABLE IF NOT EXISTS dim_drug (
                    drug_id INTEGER PRIMARY KEY,
                    drug_name VARCHAR(200),
                    drug_category VARCHAR(50),
                    reorder_level INTEGER,
                    is_active BOOLEAN DEFAULT TRUE,
                    dim_loaded_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT GETDATE()
                );
            """,
        }
        return self.execute_statements(dimension_sql, "dimension table")

    def create_fact_tables(self):
        """Create fact tables"""
        fact_sql = {
            "fact_admissions": """
                CREATE TABLE IF NOT EXISTS fact_admissions (
                    admission_id INTEGER PRIMARY KEY,
                    patient_id INTEGER REFERENCES dim_patient(patient_id),
                    ward_id INTEGER REFERENCES dim_ward(ward_id),
                    admission_date DATE,
                    admission_date_id INTEGER REFERENCES dim_date(date_id),
                    discharge_date DATE,
                    discharge_date_id INTEGER REFERENCES dim_date(date_id),
                    admission_time TIMESTAMP,
                    discharge_time TIMESTAMP,
                    triage_level VARCHAR(20),
                    length_of_stay_hours DECIMAL(10,2),
                    length_of_stay_days DECIMAL(10,2),
                    stay_category VARCHAR(50),
                    is_active BOOLEAN,
                    fact_loaded_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT GETDATE()
                );
            """,
            "fact_lab_turnaround": """
                CREATE TABLE IF NOT EXISTS fact_lab_turnaround (
                    test_id INTEGER PRIMARY KEY,
                    patient_id INTEGER REFERENCES dim_patient(patient_id),
                    test_name VARCHAR(200),
                    test_type VARCHAR(50),
                    result VARCHAR(100),
                    result_category VARCHAR(50),
                    sample_date DATE,
                    sample_date_id INTEGER REFERENCES dim_date(date_id),
                    completed_date DATE,
                    completed_date_id INTEGER REFERENCES dim_date(date_id),
                    sample_time TIMESTAMP,
                    completed_time TIMESTAMP,
                    turnaround_hours DECIMAL(10,2),
                    turnaround_category VARCHAR(50),
                    is_pending BOOLEAN,
                    fact_loaded_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT GETDATE()
                );
            """,
            "fact_pharmacy_stock": """
                CREATE TABLE IF NOT EXISTS fact_pharmacy_stock (
                    stock_id INTEGER IDENTITY(1,1) PRIMARY KEY,
                    drug_id INTEGER REFERENCES dim_drug(drug_id),
                    current_stock INTEGER,
                    threshold INTEGER,
                    expiry_date DATE,
                    expiry_date_id INTEGER REFERENCES dim_date(date_id),
                    last_updated TIMESTAMP,
                    last_updated_date_id INTEGER REFERENCES dim_date(date_id),
                    stockout_risk BOOLEAN,
                    stock_percentage DECIMAL(10,2),
                    days_until_expiry INTEGER,
                    expiry_risk BOOLEAN,
                    days_of_supply DECIMAL(10,2),
                    stock_level_category VARCHAR(50),
                    fact_loaded_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT GETDATE()
                );
            """,
        }
        return self.execute_statements(fact_sql, "fact table")

    def create_views(self):
        """Create analytical views"""
        view_sql = {
            "v_daily_admissions": """
                CREATE OR REPLACE VIEW v_daily_admissions AS
                SELECT
                    d.full_date AS admission_date,
                    COUNT(fa.admission_id) AS admission_count,
                    AVG(fa.length_of_stay_days) AS avg_length_of_stay,
                    SUM(CASE WHEN fa.is_active THEN 1 ELSE 0 END) AS active_admissions
                FROM fact_admissions fa
                JOIN dim_date d
                    ON fa.admission_date_id = d.date_id
                GROUP BY d.full_date
                ORDER BY d.full_date DESC;
            """
        }
        return self.execute_statements(view_sql, "view")


def main():
    print("=" * 60)
    print("MediTrack360 - Redshift Tables Setup")
    print("=" * 60)

    creator = RedshiftTableCreator()

    if not creator.test_connection():
        print("Cannot proceed without Redshift connection")
        return

    print("\n1. Creating dimension tables...")
    creator.create_dimension_tables()

    print("\n2. Creating fact tables...")
    creator.create_fact_tables()

    print("\n3. Creating views...")
    creator.create_views()

    print("\nRedshift setup complete!")


if __name__ == "__main__":
    main()
