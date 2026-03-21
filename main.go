package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"mit.edu/dsg/godb/catalog"
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/execution"
	"mit.edu/dsg/godb/indexing"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/recovery"
	"mit.edu/dsg/godb/storage"
	"mit.edu/dsg/godb/transaction"
)

// GoDB is the top-level container for the database system.
type GoDB struct {
	Catalog            *catalog.Catalog
	BufferPool         *storage.BufferPool
	TableManager       *execution.TableManager
	LogManager         storage.LogManager
	TransactionManager *transaction.TransactionManager
	LockManager        *transaction.LockManager
	IndexManager       *indexing.IndexManager
	Planner            *planner.SQLPlanner
}

func NewGoDB(catalog *catalog.Catalog, storageDir, logDir string, bufferPoolSize int, truncate bool) (*GoDB, error) {
	if truncate {
		_ = os.RemoveAll(storageDir)
		_ = os.RemoveAll(logDir)
	}
	if err := os.MkdirAll(storageDir, 0755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil, err
	}

	// TODO: Replace with your actual log manager implementation from lab 4
	logManager := &storage.NoopLogManager{}
	bufferPool := storage.NewBufferPool(bufferPoolSize, storage.NewDiskStorageManager(storageDir), logManager)
	lockManager := transaction.NewLockManager()
	txnManager := transaction.NewTransactionManager(logManager, bufferPool, lockManager)
	tableManager, err := execution.NewTableManager(catalog, bufferPool, logManager, lockManager)
	if err != nil {
		return nil, err
	}
	indexManager, err := indexing.NewIndexManager(catalog)
	if err != nil {
		return nil, err
	}

	// TODO: Use an actual recovery manager once recovery is implemented in lab 4
	recoveryManager := recovery.NewNoLogRecoveryManager(bufferPool, txnManager, catalog, tableManager, indexManager)
	if err := recoveryManager.Recover(); err != nil {
		return nil, err
	}

	logicalRules := []planner.LogicalRule{
		&planner.PredicatePushDownRule{},
		//&planner.ProjectionPushDownRule{},
	}
	// TODO: Activate rules as you implement the relevant executors in Lab 2
	physicalRules := []planner.PhysicalConversionRule{
		&planner.SeqScanRule{},
		&planner.IndexScanRule{},
		&planner.IndexLookupRule{},
		&planner.IndexNestedLoopJoinRule{},
		// &planner.SortMergeJoinRule{},
		&planner.HashJoinRule{},
		&planner.BlockNestedLoopJoinRule{},
		&planner.LimitRule{},
		// You can activate this rule once you implement Materialize in lab2
		// &planner.Subquery{},
		&planner.AggregationRule{},
		&planner.ProjectionRule{},
		&planner.FilterRule{},
		&planner.SortRule{},
		&planner.InsertRule{},
		&planner.DeleteRule{},
		&planner.UpdateRule{},
		&planner.ValuesRule{},
	}
	return &GoDB{
		Catalog:            catalog,
		BufferPool:         bufferPool,
		TableManager:       tableManager,
		LogManager:         logManager,
		TransactionManager: txnManager,
		LockManager:        lockManager,
		IndexManager:       indexManager,
		Planner:            planner.NewSQLPlanner(catalog, logicalRules, physicalRules),
	}, nil
}

// Global config flags
var (
	catalogPath string
	dbDir       string
	logDir      string
	bufferSize  int
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	cmd := os.Args[1]

	switch cmd {
	case "load":
		runLoad(os.Args[2:])
	case "shell":
		runShell(os.Args[2:])
	default:
		fmt.Printf("Unknown command: %s\n", cmd)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("GoDB - A Go-based Database System")
	fmt.Println("\nUsage:")
	fmt.Println("  godb <command> [flags] [arguments]")
	fmt.Println("\nCommands:")
	fmt.Println("  load    Batch load data from CSV files.")
	fmt.Println("          Usage: godb load [flags] <file1.csv> <file2.csv> ...")
	fmt.Println("  shell   Start the interactive SQL terminal.")
	fmt.Println("          Usage: godb shell [flags]")
	fmt.Println("\nCommon Flags:")
	fmt.Println("  -catalog <path>  Path to the catalog file (default: catalog.json)")
	fmt.Println("  -db <dir>        Directory for database heap files (default: godb_data)")
	fmt.Println("  -log <dir>       Directory for write-ahead logs (default: godb_log)")
	fmt.Println("  -buffer <int>    Buffer pool size in pages (default: 1000)")
	fmt.Println("\nShell Commands:")
	fmt.Println("  help             Show this usage information")
	fmt.Println("  exit, \\q         Exit the shell")
	fmt.Println("\nNotes:")
	fmt.Println("  - The CSV loader is a basic implementation. It does not escape characters.")
	fmt.Println("    Strings containing quotes (e.g. O'Reilly) or ; may cause SQL syntax errors. You need to escape them.")
	fmt.Println("  - Transactions are not supported in the shell (shell is inherently single-threaded).")
	fmt.Println("  - In Go, flags must strictly come before arguments.")

}

func setupCommonFlags(fs *flag.FlagSet) {
	fs.StringVar(&catalogPath, "catalog", "catalog.json", "Path to the catalog file")
	fs.StringVar(&dbDir, "db", "godb_data", "Directory for database heap files")
	fs.StringVar(&logDir, "log", "godb_log", "Directory for write-ahead logs")
	fs.IntVar(&bufferSize, "buffer", 1000, "Buffer pool size in pages")
}

// ----------------------------------------------------------------------------
// Command: Load
// ----------------------------------------------------------------------------

func runLoad(args []string) {
	fs := flag.NewFlagSet("load", flag.ExitOnError)
	setupCommonFlags(fs)
	truncate := fs.Bool("truncate", false, "Truncate heap files and logs before loading")
	fs.Parse(args)

	csvFiles := fs.Args()
	if len(csvFiles) == 0 {
		fmt.Println("Error: No CSV files provided.")
		fmt.Println("Usage: godb load [flags] <file1.csv> <file2.csv> ...")
		os.Exit(1)
	}

	// 1. Load Catalog
	catProvider := catalog.NewDiskCatalogManager(catalogPath)
	cat, err := catalog.NewCatalog(catProvider)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal: Could not load catalog from '%s'.\nError: %v\n", catalogPath, err)
		os.Exit(1)
	}

	// 2. Initialize Engine
	fmt.Println("Initializing Engine...")
	db, err := NewGoDB(cat, dbDir, logDir, bufferSize, *truncate)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal: Failed to initialize database.\nError: %v\n", err)
		os.Exit(1)
	}

	// 3. Load CSVs provided in arguments
	fmt.Println("Loading Data...")
	start := time.Now()
	totalRows := 0

	for _, csvPath := range csvFiles {
		baseName := filepath.Base(csvPath)
		ext := filepath.Ext(baseName)
		tableName := strings.TrimSuffix(baseName, ext)

		// Verify table exists in catalog
		if _, err := cat.GetTableMetadata(tableName); err != nil {
			fmt.Printf("  [SKIP] '%s' (Table '%s' not found in catalog)\n", csvPath, tableName)
			continue
		}

		fmt.Printf("  [LOAD] Loading '%s' into table '%s'...\n", csvPath, tableName)
		rows, err := loadTableFromCSV(db, tableName, csvPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Fatal: Failed to load table %s: %v\n", tableName, err)
			os.Exit(1)
		}
		fmt.Printf("         -> %d rows inserted.\n", rows)
		totalRows += rows
	}

	// 4. Flush Buffer Pool to ensure durability of loaded data
	fmt.Println("Flushing pages to disk...")
	if err := db.BufferPool.FlushAllPages(); err != nil {
		fmt.Fprintf(os.Stderr, "Fatal: Failed to flush buffer pool: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Success. Loaded %d rows in %v.\n", totalRows, time.Since(start))
}

func loadTableFromCSV(db *GoDB, tableName string, fileName string) (int, error) {
	table, err := db.Catalog.GetTableMetadata(tableName)
	if err != nil {
		return 0, err
	}

	f, err := os.Open(fileName)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	r := csv.NewReader(f)
	records, err := r.ReadAll()
	if err != nil {
		return 0, err
	}

	const batchSize = 500
	rowCount := 0

	for i := 0; i < len(records); i += batchSize {
		end := i + batchSize
		if end > len(records) {
			end = len(records)
		}

		var sb strings.Builder
		sb.WriteString(fmt.Sprintf("INSERT INTO %s VALUES ", tableName))

		for j := i; j < end; j++ {
			if j > i {
				sb.WriteString(", ")
			}
			sb.WriteString("(")
			for k, val := range records[j] {
				colType := table.Columns[k].Type
				if k > 0 {
					sb.WriteString(", ")
				}
				switch colType {
				case common.IntType:
					// Validate it is a number, but write exactly what is in CSV if possible,
					// or parse/clean it.
					if _, err := strconv.Atoi(val); err != nil {
						return 0, fmt.Errorf("column %s expects int, got '%s'", table.Columns[i].Name, val)
					}
					sb.WriteString(val)
				case common.StringType:
					sb.WriteString(fmt.Sprintf("'%s'", val))
				}
			}
			sb.WriteString(")")
		}

		// pass false for explain (loading doesn't need plan printing)
		if err := executeStatement(db, sb.String(), true, false); err != nil {
			return rowCount, fmt.Errorf("batch insert error at row %d: %w", i, err)
		}
		rowCount += (end - i)
	}

	return rowCount, nil
}

// ----------------------------------------------------------------------------
// Command: Shell
// ----------------------------------------------------------------------------

func runShell(args []string) {
	fs := flag.NewFlagSet("shell", flag.ExitOnError)
	setupCommonFlags(fs)
	// Add explain flag
	explain := fs.Bool("explain", false, "Print the physical execution plan before executing")
	fs.Parse(args)

	fmt.Println("Initializing Engine...")
	catProvider := catalog.NewDiskCatalogManager(catalogPath)
	cat, err := catalog.NewCatalog(catProvider)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal: Could not load catalog from '%s'.\nError: %v\n", catalogPath, err)
		os.Exit(1)
	}

	db, err := NewGoDB(cat, dbDir, logDir, bufferSize, false)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	fmt.Println("GoDB SQL Shell")
	fmt.Println("Type 'help' for usage, '\\q' to exit.")
	fmt.Println("------------------------------------------------")

	scanner := bufio.NewScanner(os.Stdin)
	var queryBuffer bytes.Buffer

	for {
		if queryBuffer.Len() == 0 {
			fmt.Print("GoDB> ")
		} else {
			fmt.Print("   -> ")
		}

		if !scanner.Scan() {
			break
		}

		line := strings.TrimSpace(scanner.Text())
		if len(line) == 0 {
			continue
		}

		if queryBuffer.Len() == 0 {
			if line == "exit" || line == "\\q" || line == "quit" {
				fmt.Println("Bye!")
				break
			}
			if line == "help" {
				printUsage()
				continue
			}
		}

		queryBuffer.WriteString(line)
		queryBuffer.WriteString(" ")

		if strings.HasSuffix(line, ";") {
			fullQuery := queryBuffer.String()
			err := executeStatement(db, fullQuery, false, *explain)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			}
			queryBuffer.Reset()
		}
	}
}

func executeStatement(db *GoDB, sql string, silent bool, explain bool) error {
	plan, err := db.Planner.Plan(sql, (!explain) || silent)
	if err != nil {
		return err
	}

	if explain {
		fmt.Println("Physical Plan:")
		fmt.Println(planner.PrettyPrint(plan))
		fmt.Println("")
	}

	executor, err := execution.BuildExecutorTree(plan, db.Catalog, db.TableManager, db.IndexManager)
	if err != nil {
		return err
	}

	err = executor.Init(execution.NewExecutorContext(nil))
	if err != nil {
		return err
	}
	defer executor.Close()

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	count := 0
	start := time.Now()
	if !silent {
		if proj, ok := plan.(*planner.ProjectionNode); ok {
			headers := projectionHeaders(proj)
			if len(headers) > 0 {
				fmt.Fprintln(w, strings.Join(headers, "\t"))
			}
		}
	}

	for executor.Next() {
		tuple := executor.Current()
		count++

		if !silent {
			var fields []string
			for i := 0; i < tuple.NumColumns(); i++ {
				val := tuple.GetValue(i)
				fields = append(fields, val.String())
			}
			fmt.Fprintln(w, strings.Join(fields, "\t"))
		}
	}
	_ = w.Flush()

	if err := executor.Error(); err != nil {
		return err
	}

	if !silent {
		duration := time.Since(start)
		if count > 0 || duration > time.Millisecond*10 {
			fmt.Printf("(%d rows) [%v]\n", count, duration)
		}
	}
	return nil
}

func projectionHeaders(proj *planner.ProjectionNode) []string {
	headers := make([]string, len(proj.Expressions))
	for i, expr := range proj.Expressions {
		switch v := expr.(type) {
		case interface{ Name() string }:
			name := v.Name()
			if name != "" {
				headers[i] = name
				continue
			}
		}
		headers[i] = expr.String()
	}
	return headers
}
