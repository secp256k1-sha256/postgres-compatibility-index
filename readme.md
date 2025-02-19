# PostgreSQL Compatibility Index (PCI)

This repository provides a standardized method to evaluate the compatibility of a database system with PostgreSQL using fixed feature categories and scoring rules.

PostgreSQL 17 is used for baseline.

A visualization of the up-to-date results can be found in this dashboard: [postgres.is](https://postgres.is/)

## Methodology

The PCI evaluates a fixed set of PostgreSQL features across 12 categories. Each feature is scored as:
- `full` (1.0)
- `partial` (0.5)
- `no` (0.0)

The final PCI score is a weighted average of the scores for each category.


### Prerequisites
- Python 3.5+, psycopg2, postgresql client and working connection to the database to be tested. 
- pip install tabulate

## Automated scoring
- Set environment variables or provide inline username, connection details of the database where tests are supposed to run.
- You will lose points for extensions that you do not install. 
- python3 pci_autotest.py

### Example Output in Tabular Format

**PostgreSQL Compatibility Report**

**Overall Compatibility Score:** `85%`

#### Failed Features:

| **Category**          | **Feature**         |
|------------------------|---------------------|
| `data_types`          | `Vector`            |
| `procedural_features` | `Triggers`          |


## Manual mode example

### CockroachDB
/pci/postgres-compatibility-index/postgres-compatibility-index$ python3 pci_calculator.py example_inputs/cockroach.json outputs/cockroachdb_report.txt
- PCI Score: 53.66%
- Detailed report saved to outputs/cockroachdb_report.txt

### DSQL
/pci/postgres-compatibility-index/postgres-compatibility-index$ python3 pci_calculator.py example_inputs/dsql.json outputs/dsql_report.txt
- PCI Score: 18.39%
- Detailed report saved to outputs/dsql_report.txt

### Yugabyte
/pci/postgres-compatibility-index/postgres-compatibility-index$ python3 pci_calculator.py example_inputs/yugabyte.json outputs/yugabytedb_report.txt
- PCI Score: 85.08%
- Detailed report saved to outputs/yugabytedb_report.txt

### AlloyDB
/pci/postgres-compatibility-index/postgres-compatibility-index$ python3 pci_calculator.py example_inputs/alloydb.json outputs/alloydb_report.txt
- PCI Score: 100%
- Detailed report saved to outputs/alloydb_report.txt
