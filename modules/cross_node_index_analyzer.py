def ensure_list(obj, context=""):
    """Utility to ensure obj is a list. If not, return empty list and warn."""
    if not isinstance(obj, list):
        print(f"[WARN] {context}: Expected list, got {type(obj)}. Using empty list instead.")
        return []
    return obj

class CrossNodeIndexAnalyzer:
    # ... existing code ...
    def identify_unused_indexes(self):
        """Identify indexes that are truly unused across all nodes."""
        unused_indexes = []
        try:
            # Get all unique indexes from primary
            primary_indexes = {row[1] for row in self.index_data['primary']['index_usage']}
        except Exception as e:
            print(f"[WARN] Could not get primary indexes: {e}")
            return []
        for index_name in primary_indexes:
            # Check if this index is used on any node
            used_on_any_node = False
            usage_summary = {}
            for node_name, node_data in self.index_data.items():
                for row in node_data.get('index_usage', []):
                    if row[1] == index_name:  # index_name matches
                        idx_scan = row[2]
                        usage_summary[node_name] = {
                            'idx_scan': idx_scan,
                            'idx_tup_read': row[3],
                            'idx_tup_fetch': row[4],
                            'index_size': row[5],
                            'table_name': row[0]
                        }
                        if idx_scan > 0:
                            used_on_any_node = True
                        break
            # If not used on any node, check if it supports constraints
            if not used_on_any_node:
                # Check if this index supports any constraints
                supports_constraints = self.check_index_constraints(index_name)
                if not supports_constraints:
                    unused_indexes.append({
                        'index_name': index_name,
                        'usage_summary': usage_summary,
                        'supports_constraints': False
                    })
        unused_indexes = ensure_list(unused_indexes, context="identify_unused_indexes")
        return unused_indexes
    # ... existing code ...
    def generate_report(self, unused_indexes, ...):
        """Generate a comprehensive AsciiDoc report."""
        report_content = []
        unused_indexes = ensure_list(unused_indexes, context="generate_report")
        # ... existing code ...

# ... existing code ...

if __name__ == "__main__":
    # ... existing code ...
    unused_indexes = analyzer.identify_unused_indexes()
    unused_indexes = ensure_list(unused_indexes, context="main")
    # ... existing code ...
# ... existing code ...