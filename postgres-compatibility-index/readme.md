# PostgreSQL Compatibility Index (PCI)

This repository provides a standardized method to evaluate the compatibility of a database system with PostgreSQL using fixed feature categories and scoring rules.

PostgreSQL 17 is used for baseline.

## Methodology

The PCI evaluates a fixed set of PostgreSQL features across 12 categories. Each feature is scored as:
- `full` (1.0)
- `partial` (0.5)
- `no` (0.0)

The final PCI score is a weighted average of the scores for each category.


### Prerequisites
- Python 3.5+, psycopg2, postgresql client and working connection to the database to be tested. 

## Automated scoring
- Add username and connection details of the database where tests are supposed to run in pci_autotest.py
- You will lose points for extensions that you do not install.
- python3 pci_autotest.py   


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
/pci/postgres-compatibility-index/postgres-compatibility-index$ python3 pci_calculator.py example_inputs/yugabyte.json outputs/ydb_report.txt
- PCI Score: 85.08%
- Detailed report saved to outputs/ydb_report.txt

### AlloyDB
/pci/postgres-compatibility-index/postgres-compatibility-index$ python3 pci_calculator.py example_inputs/alloydb.json outputs/alloydb_report.txt
- PCI Score: 100%
- Detailed report saved to outputs/alloydb_report.txt