Overview
This is a MySQL database copier application that efficiently transfers data between MySQL databases with proper dependency handling and progress tracking.

Key Features
1. Dependency-Aware Table Copying
  Analyzes table relationships using foreign key constraints
  Orders tables topologically to handle dependencies (child tables copied after parent tables)
  Prevents errors from copying tables before their referenced tables exist
2. Performance Optimizations
  Batch Processing: Copies data in chunks of 10,000 rows at a time
  Buffering: Uses an insert buffer to accumulate rows before bulk insertion
  Parallel Processing: Copies multiple tables concurrently (max 4 at once)
  Retry Logic: Automatically retries failed operations with exponential backoff
3. User Interface
  Interactive prompts for database connection details
  Table selection (all or specific tables)
  Progress bars showing:
  Overall progress for all tables
  Per-table progress with estimated time
  Real-time statistics (speed, time, size)
4. Error Handling & Robustness
  Handles datetime conversion issues
  Automatic cleanup of existing tables (drop or truncate)
  Comprehensive error reporting
  Circular dependency detection
