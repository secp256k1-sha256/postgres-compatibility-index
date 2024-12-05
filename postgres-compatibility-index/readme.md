# PostgreSQL Compatibility Index (PCI)

This repository provides a standardized method to evaluate the compatibility of a database system with PostgreSQL 17 using fixed feature categories and scoring rules.

## Methodology

The PCI evaluates a fixed set of PostgreSQL features across 12 categories. Each feature is scored as:
- `full` (1.0)
- `partial` (0.5)
- `no` (0.0)

The final PCI score is a weighted average of the scores for each category.


### Prerequisites
- Python 3.6+

