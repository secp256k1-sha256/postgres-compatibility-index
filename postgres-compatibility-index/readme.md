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
- Python 3.5+

### Example 
1. CockroachDB
/pci/postgres-compatibility-index/postgres-compatibility-index$ python3 pci_calculator.py example_inputs/cockroach.json outputs/cockroachdb_report.txt
PCI Score: 53.66%
Detailed report saved to outputs/cockroachdb_report.txt

2. DSQL
/pci/postgres-compatibility-index/postgres-compatibility-index$ python3 pci_calculator.py example_inputs/dsql.json outputs/dsql_report.txt
PCI Score: 18.39%
Detailed report saved to outputs/dsql_report.txt

3. Yugabyte
/pci/postgres-compatibility-index/postgres-compatibility-index$ python3 pci_calculator.py example_inputs/yugabyte.json outputs/ydb_report.txt
PCI Score: 85.08%
Detailed report saved to outputs/ydb_report.txt

