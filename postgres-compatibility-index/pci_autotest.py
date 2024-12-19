import psycopg2
from psycopg2 import sql
import json
import os
from tabulate import tabulate

# PostgreSQL connection parameters from environment variables or defaults
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", 5432)
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "password")
PG_DBNAME = os.getenv("PG_DBNAME", "testdb")

def get_connection():
    """Establish and return a PostgreSQL connection."""
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=PG_DBNAME
    )

# Define the features to test
FEATURES = {
    "data_types": ["Primitive Types", "Complex Types", "JSONB", "Geospatial Types", "Custom Types", "Full-Text Search", "Vector"],
    "ddl_features": ["Schemas", "Sequences", "Views", "Materialized Views"],
    "sql_features": ["CTEs", "Upsert", "Window Functions", "Subqueries"],
    "procedural_features": ["Stored Procedures", "Functions", "Triggers"],
    "transaction_features": ["ACID Compliance", "Isolation Levels", "Nested Transactions", "Row-Level Locking"],
    "extensions": ["Extension Support", "Foreign Data Wrappers", "Custom Plugins"],
    "performance": ["Index Types", "Partitioning", "Parallel Query Execution"],
    "constraints": ["Foreign Key", "Check", "Not Null", "Unique", "Exclusion"],
    "security": ["Role Management", "GRANT/REVOKE Privileges", "Row-Level Security"],
    "replication": ["Streaming Replication", "Logical Replication"],
    "notifications": ["LISTEN/NOTIFY", "Event Triggers"],
    "miscellaneous": ["Temporary Tables", "Monitoring and Statistics"],
    "utilities": ["pg_dump", "pg_stat_statements", "pg_walinspect", "amcheck"]
}

SUPPORT_SCORES = {"full": 1.0, "partial": 0.5, "no": 0.0}
FEATURE_WEIGHTS = {
    "data_types": 5,
    "ddl_features": 5,
    "sql_features": 5,
    "procedural_features": 15,
    "transaction_features": 15,
    "extensions": 15,
    "performance": 5,
    "constraints": 10,
    "security": 5,
    "replication": 5,
    "notifications": 5,
    "miscellaneous": 5,
    "utilities": 5
}

PENALTY_PER_FAILURE = 5  # Negative points per failure


def test_feature(cursor, feature_category, feature_name):
    try:
        # Test each feature based on its category and subfeature
        if feature_category == "data_types":
            if feature_name == "Primitive Types":
                cursor.execute("CREATE TABLE test_primitive (id INT, name TEXT);")
            elif feature_name == "Complex Types":
                cursor.execute("CREATE TYPE test_complex AS (x INT, y TEXT);")
            elif feature_name == "JSONB":
                cursor.execute("CREATE TABLE test_jsonb (data JSONB);")
            elif feature_name == "Geospatial Types":
                cursor.execute("CREATE EXTENSION postgis; CREATE TABLE test_geo (geom GEOMETRY);")
            elif feature_name == "Custom Types":
                cursor.execute("CREATE TYPE mood AS ENUM ('happy', 'sad', 'neutral');")
            elif feature_name == "Full-Text Search":
                cursor.execute("CREATE TABLE test_fts (content TSVECTOR);")
            elif feature_name == "Vector":
                cursor.execute("CREATE EXTENSION vector; CREATE TABLE test_vector (embedding VECTOR(3));")

        elif feature_category == "ddl_features":
            if feature_name == "Schemas":
                cursor.execute("DROP SCHEMA IF EXISTS test_schema CASCADE; CREATE SCHEMA test_schema;")
            elif feature_name == "Sequences":
                cursor.execute("CREATE SEQUENCE test_seq START 1;")
            elif feature_name == "Views":
                cursor.execute("CREATE VIEW test_view AS SELECT 1 AS col;")
            elif feature_name == "Materialized Views":
                cursor.execute("CREATE MATERIALIZED VIEW test_matview AS SELECT 1 AS col;")

        elif feature_category == "sql_features":
            if feature_name == "CTEs":
                cursor.execute("WITH cte AS (SELECT 1 AS val) SELECT * FROM cte;")
            elif feature_name == "Upsert":
                cursor.execute("CREATE TABLE test_upsert (id INT PRIMARY KEY, data TEXT); INSERT INTO test_upsert VALUES (1, 'test') ON CONFLICT (id) DO UPDATE SET data = 'updated';")
            elif feature_name == "Window Functions":
                cursor.execute("SELECT ROW_NUMBER() OVER (PARTITION BY 1 ORDER BY 1);")
            elif feature_name == "Subqueries":
                cursor.execute("SELECT * FROM (SELECT 1) AS sub WHERE 1 = (SELECT 1);")

        elif feature_category == "procedural_features":
            if feature_name == "Stored Procedures":
                cursor.execute("CREATE PROCEDURE test_proc() LANGUAGE SQL AS $$ SELECT 1; $$; CALL test_proc();")
            elif feature_name == "Functions":
                cursor.execute("CREATE FUNCTION test_func() RETURNS INT LANGUAGE SQL AS $$ SELECT 1; $$; SELECT test_func();")
            elif feature_name == "Triggers":
                cursor.execute("CREATE TABLE test_trig (id INT); CREATE FUNCTION test_trigger() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$; CREATE TRIGGER trg BEFORE INSERT ON test_trig FOR EACH ROW EXECUTE FUNCTION test_trigger();")

        elif feature_category == "transaction_features":
            if feature_name == "ACID Compliance":
                cursor.execute("BEGIN; INSERT INTO test_primitive VALUES (1, 'test'); ROLLBACK;")
            elif feature_name == "Isolation Levels":
                cursor.execute("BEGIN; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; SET TRANSACTION ISOLATION LEVEL READ COMMITTED;")
            elif feature_name == "Nested Transactions":
                cursor.execute("BEGIN; SAVEPOINT sp; RELEASE SAVEPOINT sp;")
            elif feature_name == "Row-Level Locking":
                cursor.execute("SELECT * FROM test_primitive FOR UPDATE;")

        elif feature_category == "extensions":
            if feature_name == "Extension Support":
                cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
            elif feature_name == "Foreign Data Wrappers":
                cursor.execute("CREATE EXTENSION IF NOT EXISTS postgres_fdw;")
            elif feature_name == "Custom Plugins":
                # Assuming a sample plugin; usually requires admin setup
                pass

        elif feature_category == "performance":
            if feature_name == "Index Types":
                cursor.execute("CREATE INDEX test_btree ON test_primitive USING btree (id);")
                cursor.execute("CREATE INDEX test_gin ON test_jsonb USING gin (data);")
                cursor.execute("CREATE INDEX test_gist ON test_fts USING gist (content);")
                cursor.execute("CREATE INDEX test_hash ON test_primitive USING hash (id);")
            elif feature_name == "Partitioning":
                cursor.execute("CREATE TABLE test_part (id INT) PARTITION BY RANGE (id);")
                cursor.execute("CREATE TABLE test_part1 PARTITION OF test_part FOR VALUES FROM (1) TO (100);")
                cursor.execute("CREATE TABLE test_part2 PARTITION OF test_part FOR VALUES FROM (101) TO (200);")
                cursor.execute("ANALYZE test_part;")
                cursor.execute("EXPLAIN (FORMAT JSON) SELECT * FROM test_part WHERE id = 150;")
                result = cursor.fetchone()
                explain_output = result[0]
                partitions_in_plan = json.dumps(explain_output)
                if "test_part2" in partitions_in_plan and "test_part1" not in partitions_in_plan:
                    return "full"
                else:
                    raise Exception("Partition Pruning test failed: Incorrect partitions included in the plan.")
            elif feature_name == "Parallel Query Execution":
                cursor.execute("SET max_parallel_workers = 4; SET max_parallel_workers_per_gather=4; SELECT COUNT(*) FROM generate_series(1, 1000000) t(id);")

        elif feature_category == "constraints":
            if feature_name == "Foreign Key":
                cursor.execute("CREATE TABLE parent (id INT PRIMARY KEY); CREATE TABLE child (parent_id INT );")
            elif feature_name == "Check":
                cursor.execute("CREATE TABLE test_check (id INT CHECK (id > 0));")
            elif feature_name == "Not Null":
                cursor.execute("CREATE TABLE test_notnull (id INT NOT NULL);")
            elif feature_name == "Unique":
                cursor.execute("CREATE TABLE test_unique (id INT UNIQUE);")
            elif feature_name == "Exclusion":
                cursor.execute("CREATE EXTENSION IF NOT EXISTS btree_gist; CREATE TABLE test_exclusion (id int, t text, ts tstzrange, exclude using gist ((case when t ='A' THEN true end) with =,ts with && ));")

        elif feature_category == "security":
            if feature_name == "Role Management":
                cursor.execute("CREATE ROLE test_role; DROP ROLE test_role;")
            elif feature_name == "GRANT/REVOKE Privileges":
                cursor.execute("GRANT SELECT ON test_primitive TO PUBLIC;")
            elif feature_name == "Row-Level Security":
                cursor.execute("ALTER TABLE test_primitive ENABLE ROW LEVEL SECURITY;")

        # Add similar blocks for other categories...

        return "full"
    except Exception as e:
        print(f"Feature {feature_name} failed in {feature_category}: {e}")
        return "no"

# Continue with schema creation, PCI calculations, and detailed reporting (same structure as earlier).
def calculate_pci(features):
    """
    Calculate the PCI score based on feature test results.
    Failures introduce penalties, and the score is capped at 100%.
    """
    total_score = 0
    total_weight = sum(FEATURE_WEIGHTS.values())
    penalty = 0
    failed_tests = []

    for category, subfeatures in FEATURES.items():
        category_score = 0
        for subfeature in subfeatures:
            result = features[category][subfeature]
            category_score += SUPPORT_SCORES[result]
            if result == "no":
                penalty += PENALTY_PER_FAILURE
                failed_tests.append((category, subfeature))

        weighted_score = (category_score / len(subfeatures)) * FEATURE_WEIGHTS[category]
        total_score += weighted_score

    # Apply penalties and cap the score
    total_score = max(0, total_score - penalty)
    total_score = min(100, total_score)
    return round(total_score, 2), failed_tests

def print_summary(pci_score, failed_tests):
    """Print a detailed summary of the PCI results."""
    print("\n==================== PCI SUMMARY REPORT ====================")
    print(f"Overall PCI Score: {pci_score}%\n")

    if failed_tests:
        print("Failed Features:\n")
        print(tabulate(failed_tests, headers=["Category", "Feature"], tablefmt="grid"))
    else:
        print("All features passed successfully!\n")
    print("==========================================================\n")

def main():

    connection = get_connection()
    connection.autocommit = True
    cursor = connection.cursor()

    # Create a test schema
    cursor.execute("DROP SCHEMA IF EXISTS pci_test CASCADE;")
    cursor.execute("CREATE SCHEMA pci_test;")
    cursor.execute("SET search_path TO pci_test;")

    # Run tests
    pci_results = {category: {} for category in FEATURES.keys()}
    for category, subfeatures in FEATURES.items():
        for subfeature in subfeatures:
            pci_results[category][subfeature] = test_feature(cursor, category, subfeature)

    # Calculate PCI score
    pci_score, failed_tests = calculate_pci(pci_results)
    print_summary(pci_score, failed_tests)

    # Save results
    with open("pci_report.json", "w") as report_file:
        json.dump({"pci_score": pci_score, "details": pci_results}, report_file, indent=4)

    print("PCI testing completed. Report saved as 'pci_report.json'.")

    cursor.close()
    connection.close()

if __name__ == "__main__":
    main()
