import json
import argparse
import sys

# Standardized PostgreSQL feature set with categories and sub-features
STANDARD_FEATURES = {
    "data_types": ["Primitive Types", "Complex Types", "JSONB", "Geospatial Types", "Custom Types", "Full-Text Search","Vector"],
    "ddl_features": ["Schemas", "Sequences", "Views", "Materialized Views"],
    "sql_features": ["CTEs", "Upsert", "Window Functions", "Subqueries"],
    "procedural_features": ["Stored Procedures", "Functions", "Triggers"],
    "transaction_features": ["ACID Compliance", "Isolation Levels", "Nested Transactions", "Row-Level Locking"],
    "extensions": ["Extension Support", "Foreign Data Wrappers", "Custom Plugins"],
    "performance": ["Index Types", "Partitioning", "Parallel Query Execution"],
    "constraints":["Foriegn Key", "Check", "Not Null","Unique","Exclusion"],
    "security": ["Role Management", "GRANT/REVOKE Privileges", "Row-Level Security"],
    "replication": ["Streaming Replication", "Logical Replication"],
    "notifications": ["LISTEN/NOTIFY", "Event Triggers"],
    "miscellaneous": ["Temporary Tables", "Monitoring and Statistics"],
    "utilities": ["pg_dump","pg_stat_statements","pg_walinspect","amcheck"]
}

# Weights for each category
FEATURE_WEIGHTS = {
    "data_types": 5,
    "ddl_features": 5,
    "sql_features": 5,
    "procedural_features": 15,
    "transaction_features": 15,
    "extensions": 15,
    "performance": 5,
    "constraints":10,
    "security": 5,
    "replication": 5,
    "notifications": 5,
    "miscellaneous": 5,
    "utilities": 5
}

# Scoring system
SUPPORT_SCORES = {"full": 1.0, "partial": 0.5, "no": 0.0}

def validate_input(features):
    """
    Validate the input JSON file against the standardized feature set.
    """
    for category, subfeatures in STANDARD_FEATURES.items():
        if category not in features:
            raise ValueError(f"Missing category: {category}")
        for subfeature in subfeatures:
            if subfeature not in features[category]:
                raise ValueError(f"Missing sub-feature: {subfeature} in category: {category}")
            if features[category][subfeature] not in SUPPORT_SCORES:
                raise ValueError(f"Invalid support level '{features[category][subfeature]}' for {subfeature}. Use 'full', 'partial', or 'no'.")

def calculate_pci(features):
    """
    Calculate the PostgreSQL Compatibility Index (PCI) score.
    """
    total_score = 0
    total_weight = sum(FEATURE_WEIGHTS.values())

    for category, subfeatures in STANDARD_FEATURES.items():
        category_score = 0
        for subfeature in subfeatures:
            support_level = features[category][subfeature]
            category_score += SUPPORT_SCORES[support_level]
        # Average category score and apply weight
        category_percentage = category_score / len(subfeatures)
        weighted_score = category_percentage * FEATURE_WEIGHTS[category]
        total_score += weighted_score

    return round((total_score / total_weight) * 100, 2)

def main():
    parser = argparse.ArgumentParser(description="Calculate PostgreSQL Compatibility Index (PCI).")
    parser.add_argument("input_file", help="Path to JSON file describing database features.")
    parser.add_argument("output_file", help="Path to save the PCI score report.")
    args = parser.parse_args()

    try:
        # Load and validate input
        with open(args.input_file, "r") as file:
            features = json.load(file)
        validate_input(features)

        # Calculate PCI score
        pci_score = calculate_pci(features)

        # Save the report
        with open(args.output_file, "w") as file:
            file.write(f"PostgreSQL Compatibility Index (PCI) Score: {pci_score}%\n")
            file.write(json.dumps(features, indent=4))
        
        print(f"PCI Score: {pci_score}%")
        print(f"Detailed report saved to {args.output_file}")

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
