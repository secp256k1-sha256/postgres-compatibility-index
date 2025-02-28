import psycopg2
from psycopg2 import sql
from psycopg2 import errors
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
        dbname=PG_DBNAME,
	    sslmode='require'
        #sslrootcert='/home/dcgcore/postgres-compatibility-index/postgres-compatibility-index/root.crt'

    )

# Define the features to test
FEATURES = {
    "data_types": ["Primitive Types", "Complex Types", "JSONB", "Geospatial Types", "Custom Types", "Full-Text Search", "Vector"],
    "DDL_features": ["Schemas", "Sequences", "Views", "Materialized Views"],
    "SQL_features": ["CTEs", "Upsert", "Window Functions", "Subqueries"],
    "procedural_features": ["Stored Procedures", "Functions", "Triggers"],
    "performance": ["Index Types", "Partitioning", "Parallel Query Execution","Unlogged Table"],
    "constraints": ["Foreign Key", "Check", "Not Null", "Unique", "Exclusion"],    
    "extensions": ["Extension Support", "Foreign Data Wrappers"],
    "security": ["Role Management", "GRANT/REVOKE Privileges", "Row-Level Security"],
    "replication": ["Streaming Replication", "Logical Replication"],
    "transaction_features": ["ACID Compliance", "Isolation Levels", "Nested Transactions", "Row-Level Locking"],	
# Removed till addition of tests
#    "notifications": ["LISTEN/NOTIFY", "Event Triggers"],
    "miscellaneous": ["pg_stat_statements", "pg_walinspect","External Programming Language"],
# Removed till addition of tests
#    "utilities": ["pg_dump", , "amcheck"]
}

SUPPORT_SCORES = {"full": 1.0, "partial": 0.5, "no": 0.0}
FEATURE_WEIGHTS = {
    "data_types": 7,
    "DDL_features": 5,
    "SQL_features": 6,
    "procedural_features": 15,
    "transaction_features": 15,
    "extensions": 15,
    "performance": 10,
    "constraints": 10,
    "security": 5,
    "replication": 10,
    "miscellaneous": 2,	
#removing feature weight for tests not added yet.	
    "notifications": 0,
    "utilities": 0
}

PENALTY_PER_FAILURE = 1.5  # Negative points per failure


def test_feature(cursor, feature_category, feature_name):
    support ="no"
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

        elif feature_category == "DDL_features":
            if feature_name == "Schemas":
                cursor.execute("DROP SCHEMA IF EXISTS test_schema CASCADE; CREATE SCHEMA test_schema;")
            elif feature_name == "Sequences":
                cursor.execute("CREATE SEQUENCE test_seq START 1;")
            elif feature_name == "Views":
                cursor.execute("CREATE VIEW test_view AS SELECT 1 AS col;")
            elif feature_name == "Materialized Views":
                cursor.execute("CREATE MATERIALIZED VIEW test_matview AS SELECT 1 AS col;")

        elif feature_category == "SQL_features":
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
                cursor.execute("DROP FUNCTION IF EXISTS test_func();CREATE FUNCTION test_func() RETURNS INT LANGUAGE SQL AS $$ SELECT 1; $$; SELECT test_func();")
                support = "partial"
                cursor.execute("DROP FUNCTION IF EXISTS test_func_plpgsql();CREATE FUNCTION test_func_plpgsql() RETURNS void LANGUAGE plpgsql AS $$ begin null; end; $$; SELECT test_func_plpgsql();")
            elif feature_name == "Triggers":
                cursor.execute("CREATE TABLE test_trig (id INT); CREATE FUNCTION test_trigger() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$; CREATE TRIGGER trg BEFORE INSERT ON test_trig FOR EACH ROW EXECUTE FUNCTION test_trigger();")

        elif feature_category == "transaction_features":
            if feature_name == "ACID Compliance":
                cursor.execute("BEGIN; INSERT INTO test_primitive VALUES (1, 'test'); ROLLBACK;")
            elif feature_name == "Isolation Levels":
                cursor.execute("BEGIN; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; SET TRANSACTION ISOLATION LEVEL READ COMMITTED; ROLLBACK;")
            elif feature_name == "Nested Transactions":
                cursor.execute("BEGIN; SAVEPOINT sp; RELEASE SAVEPOINT sp;")
            elif feature_name == "Row-Level Locking":
                cursor.execute("SELECT * FROM test_primitive FOR UPDATE;")

        elif feature_category == "extensions":
            if feature_name == "Extension Support":
                #cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
                cursor.execute("select coalesce((select 1 from pg_available_extensions where name ='pg_trgm'),0)")
                extensions = cursor.fetchone()[0]
                if extensions != 0:
                    support ="full"
                else:
                    support ="no"
                return support
            elif feature_name == "Foreign Data Wrappers":
                #cursor.execute("CREATE EXTENSION IF NOT EXISTS postgres_fdw;")
                cursor.execute("select coalesce((select 1 from pg_available_extensions where name ='postgres_fdw'),0)")
                extensions = cursor.fetchone()[0]
                if extensions !=  0:
                    support ="full"
                else:
                    support ="no"
                return support
            

        elif feature_category == "performance":
            if feature_name == "Index Types":
                cursor.execute("CREATE INDEX test_btree ON test_primitive USING btree (id);")
                support = "partial"
                cursor.execute("CREATE INDEX test_gin ON test_jsonb USING gin (data);")
                support = "partial"
                cursor.execute("CREATE INDEX test_gist ON test_fts USING gist (content);")
                support = "partial"
                cursor.execute("CREATE INDEX test_hash ON test_primitive USING hash (id);")
            elif feature_name == "Partitioning":
                cursor.execute("""CREATE TABLE test_part (id INT) PARTITION BY RANGE (id);
                                CREATE TABLE test_part1 PARTITION OF test_part FOR VALUES FROM (1) TO (100);
                                CREATE TABLE test_part2 PARTITION OF test_part FOR VALUES FROM (101) TO (200);
                                ANALYZE test_part;
                                """)
                cursor.execute("EXPLAIN (FORMAT JSON) SELECT * FROM test_part WHERE id = 150;")
                result = cursor.fetchone()
                explain_output = result[0]
                partitions_in_plan = json.dumps(explain_output)
                if "test_part2" in partitions_in_plan and "test_part1" not in partitions_in_plan:
                    return "full"
                else:
                    raise Exception("Partition Pruning test failed: Incorrect partitions included in the plan.")
            elif feature_name == "Parallel Query Execution":
                cursor.execute("SET max_parallel_workers = 4; SET max_parallel_workers_per_gather=4; SELECT COUNT(*) FROM generate_series(1, 50000) t(id);")
            elif feature_name == "Unlogged Table":
                cursor.execute("""drop table if exists unlogged_pci_demo;
                                create unlogged table unlogged_pci_demo(n int primary key,flag char,text text);"""
                )
                cursor.execute("select pg_current_wal_lsn() from pg_stat_database where datname=current_database();")
                wal_lsn_before = cursor.fetchone()[0]
                cursor.execute("insert into unlogged_pci_demo select generate_series, 'N',lpad('x',generate_series,'x') from generate_series(1,10000);")
                cursor.execute(f"select (pg_wal_lsn_diff(pg_current_wal_lsn(),'{wal_lsn_before}')) from pg_stat_database where datname=current_database();")
                diff_after = cursor.fetchone()[0]
                if diff_after < 50000:
                    return "full"
                else:
                    raise Exception("Unlogged Table test failed: Excessive WAL Generated.")

        elif feature_category == "constraints":
            if feature_name == "Foreign Key":
                cursor.execute("CREATE TABLE parent (id INT PRIMARY KEY); CREATE TABLE child (parent_id INT , CONSTRAINT fk_customer FOREIGN KEY(parent_id) REFERENCES parent(id));")
            if feature_name == "Check":
                cursor.execute("CREATE TABLE test_check (id INT CHECK (id > 0));")
            if feature_name == "Not Null":
                cursor.execute("CREATE TABLE test_notnull (id INT NOT NULL);")
            if feature_name == "Unique":
                cursor.execute("CREATE TABLE test_unique (id INT UNIQUE);")
            #if feature_name == "DisableConstraint":
                #cursor.execute("alter table child disable trigger all;")
            if feature_name == "Exclusion":
                cursor.execute("CREATE EXTENSION IF NOT EXISTS btree_gist; CREATE TABLE test_exclusion (id int, t text, ts tstzrange, exclude using gist ((case when t ='A' THEN true end) with =,ts with && ));")

        elif feature_category == "security":
            if feature_name == "Role Management":
                cursor.execute("CREATE ROLE test_role; DROP ROLE test_role;")
            elif feature_name == "GRANT/REVOKE Privileges":
                cursor.execute("GRANT SELECT ON test_primitive TO PUBLIC;")
                support = "partial"
            elif feature_name == "Row-Level Security":
                cursor.execute("ALTER TABLE test_primitive ENABLE ROW LEVEL SECURITY;")

        elif feature_category == "replication":
            if feature_name == "Logical Replication":
                cursor.execute("CREATE TABLE test_replication (id INT PRIMARY KEY, value TEXT);")
                cursor.execute("CREATE PUBLICATION test_pub FOR TABLE test_replication WHERE (id > 10 and value <>'UNKNOWN');")
                cursor.execute("SELECT pubname, puballtables, pubinsert, pubupdate, pubdelete FROM pg_publication WHERE pubname = 'test_pub';")
                result = cursor.fetchone()
                if result is None or result[0] != 'test_pub':
                    raise Exception("Logical Replication Publication not found or misconfigured.")
                else:
                    cursor.execute("DROP PUBLICATION test_pub;")
                    return "full"
        
        elif feature_category == "miscellaneous":
            if feature_name == "External Programming Language":
                cursor.execute("SET search_path TO public, pg_catalog;")
                cursor.execute("CREATE EXTENSION IF NOT EXISTS unaccent;")
                cursor.execute(
                     "CREATE OR REPLACE FUNCTION public.immutable_unaccent(regdictionary, text) "
                     "RETURNS text LANGUAGE c IMMUTABLE PARALLEL SAFE STRICT AS "
                     "'$libdir/unaccent', 'unaccent_dict';"
                   )
                cursor.execute(
                     "CREATE OR REPLACE FUNCTION public.f_unaccent(text) "
                     "RETURNS text LANGUAGE sql IMMUTABLE PARALLEL SAFE STRICT AS "
                     "$func$ SELECT public.immutable_unaccent(regdictionary 'public.unaccent', $1) $func$;"
                   )
                cursor.execute("SELECT public.f_unaccent('Crème Brûlée');")
                
                result = cursor.fetchone()[0]
                support = "full" if result.strip() == "Creme Brulee" else "no"
                   # Clean up by dropping the created functions
                cursor.execute("DROP FUNCTION IF EXISTS public.f_unaccent(text);")
                cursor.execute("DROP FUNCTION IF EXISTS public.immutable_unaccent(regdictionary, text);")
                cursor.execute("DROP EXTENSION IF EXISTS unaccent;")
            elif feature_name == "pg_stat_statements":
                cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements;")
                cursor.execute("SELECT count(*) FROM pg_stat_statements;")
            elif feature_name == "pg_walinspect":
                cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_walinspect;")
            

        # Add similar blocks for other categories...

        return "full"
    except errors.SyntaxError as e:
        print(f"Feature {feature_name} failed in {feature_category}: {e}")
        return support
    except errors.UndefinedFunction  as e:
        print(f"Feature {feature_name} failed in {feature_category}: {e}")
        return support
    except errors.FeatureNotSupported as e:
        print(f"Feature {feature_name} failed in {feature_category}: {e}")
        return support
    except Exception as e:
        print(f"Feature {feature_name} failed in {feature_category}: {e}")
        #cursor.execute('rollback;')
        return support


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

    print(pci_results)
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
