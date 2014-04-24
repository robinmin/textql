package main

import (
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"io/ioutil"
	"regexp"

	_ "github.com/mattn/go-sqlite3"

	"bytes"
	"encoding/hex"
	"io"
	"log"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	NUM_PER_DOT = 10000
)

type FieldDefinition struct {
	cid        int
	field_name string
	field_type string
	notnull    int
	dflt_value string
	pk         int
}

type TableSchema struct {
	Fields []FieldDefinition
}

type FieldSummary struct {
	var_name   string
	min_length int
	max_length int
	distinct   int
	min_val    string
	max_var    string
}

func main() {
	// Parse command line opts
	commands := flag.String("sql", "", "SQL Command(s) to run on the data")
	source_text := flag.String("source", "stdin", "Source file to load, or defaults to stdin")
	delimiter := flag.String("dlm", ",", "Delimiter between fields -dlm=tab for tab, -dlm=0x## to specify a character code in hex")
	pks := flag.String("pk", "", "Primary key(s) for imported table, seperated by the seperator witch specified by dlm option")
	ori_op := flag.String("dup", "", "How to deal with the duplicated records. This option will work only with pk valid. Must be one of (replace/rollback/abord/fail/ignore/)")
	showvar := flag.Bool("showvar", false, "Show variable summary after imported the data")
	header := flag.Bool("header", false, "Treat file as having the first row as a header row")
	tableName := flag.String("table-name", "tbl", "Override the default table name (tbl)")
	save_to := flag.String("save-to", "", "If set, sqlite3 db is left on disk at this path")
	console := flag.Bool("console", false, "After all commands are run, open sqlite3 console with this data")
	verbose := flag.Bool("verbose", false, "Enable verbose logging")
	flag.Parse()

	if *console && (*source_text == "stdin") {
		log.Fatalln("Can not open console with pipe input, read a file instead")
	}

	seperator := determineSeperator(delimiter)
	primarykeys := determinePKs(pks, ",")
	vld_op := determineDupOp(ori_op)

	// Open db, in memory if possible
	db, openPath := openDB(save_to, console)

	// Open the input source
	var fp *os.File
	fp = openFileOrStdin(source_text)
	defer fp.Close()

	// Init a structured text reader
	reader := csv.NewReader(fp)
	reader.FieldsPerRecord = 0
	reader.Comma = seperator

	// Read the first row
	first_row, read_err := reader.Read()

	if read_err != nil {
		log.Fatalln(read_err)
	}

	var headerRow []string

	if *header {
		headerRow = first_row
		first_row, read_err = reader.Read()

		if read_err != nil {
			log.Fatalln(read_err)
		}
	} else {
		headerRow = make([]string, len(first_row))
		for i := 0; i < len(first_row); i++ {
			headerRow[i] = "c" + strconv.Itoa(i)
		}
	}

	// Create the table to load to
	createTable(tableName, &headerRow, db, primarykeys, verbose)

	// Start the clock for importing
	t0 := time.Now()
	nBefore := countTable(db, *tableName, "")
	// Create transaction
	tx, tx_err := db.Begin()

	if tx_err != nil {
		log.Fatalln(tx_err)
	}

	// check whethere this table has PK or not
	blWithPK := false
	schema := FetchTableSchema(db, *tableName)
	for _, column := range schema.Fields {
		if column.pk > 0 {
			blWithPK = true
		}
	}
	///////////////////////////////////////////////////////////////////////////////
	// TODO :  there is a bug in current version given the result of `pragma table_info('table_name')` can not
	//         fetch the pk information correctly, so add an ad-hoc detection here
	if len(primarykeys) > 0 {
		blWithPK = true
	}
	///////////////////////////////////////////////////////////////////////////////

	// Load first row
	stmt := createLoadStmt(tableName, &headerRow, vld_op, blWithPK, tx, verbose)
	loadRow(tableName, &first_row, tx, stmt, verbose)

	// Read the data
	nLines := 1
	for {
		row, file_err := reader.Read()
		if file_err == io.EOF {
			break
		} else if file_err != nil {
			log.Println(file_err)
		} else {
			nLines++
			loadRow(tableName, &row, tx, stmt, verbose)
			if *verbose && nLines%NUM_PER_DOT == 0 {
				fmt.Fprintf(os.Stderr, ".")
			}
		}
	}
	stmt.Close()
	tx.Commit()
	nAfter := countTable(db, *tableName, "")
	t1 := time.Now()

	if *verbose {
		fmt.Fprintf(os.Stderr, "\n#of row : %d + %d ==> %d (%v)\n", nBefore, nLines, nAfter, t1.Sub(t0))
	}
	if *showvar {
		fmt.Fprintf(os.Stderr, "\nField\tMin.Length\tMax.Length\tDistinct\tMin.Value\tMax.Value\n")
		var tmpFldSum *FieldSummary
		for _, column := range schema.Fields {
			tmpFldSum = SummaryTableByField(db, *tableName, column.field_name)
			fmt.Fprintf(
				os.Stderr, "%v\t%d\t%d\t%d\t%v\t%v\n",
				tmpFldSum.var_name,
				tmpFldSum.min_length,
				tmpFldSum.max_length,
				tmpFldSum.distinct,
				tmpFldSum.min_val,
				tmpFldSum.max_var,
			)
		}
		fmt.Fprintf(os.Stderr, "\n")
	}

	// Determine what sql to execute
	sqls_to_execute := strings.Split(*commands, ";")

	t0 = time.Now()

	// Execute given SQL
	for _, sql_cmd := range sqls_to_execute {
		if strings.Trim(sql_cmd, " ") != "" {
			result, err := db.Query(sql_cmd)
			if err != nil {
				log.Fatalln(err)
			}
			displayResult(result)
		}
	}

	t1 = time.Now()

	if *verbose {
		fmt.Fprintf(os.Stderr, "Queries run in: %v\n", t1.Sub(t0))
	}

	// Open console
	if *console {
		db.Close()

		cmd := exec.Command("sqlite3", *openPath)
		cmd.Stdin = os.Stdin
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		cmd_err := cmd.Run()
		if cmd.Process != nil {
			cmd.Process.Release()
		}

		if len(*save_to) == 0 {
			os.RemoveAll(filepath.Dir(*openPath))
		}

		if cmd_err != nil {
			log.Fatalln(cmd_err)
		}
	} else if len(*save_to) == 0 {
		db.Close()
		os.Remove(*openPath)
	} else {
		db.Close()
	}
}

func createTable(tableName *string, columnNames *[]string, db *sql.DB, primarykeys []string, verbose *bool) error {
	var buffer bytes.Buffer

	buffer.WriteString("CREATE TABLE IF NOT EXISTS " + (*tableName) + " (")

	for i, col := range *columnNames {
		var col_name string

		reg := regexp.MustCompile(`[^a-zA-Z0-9]`)

		col_name = reg.ReplaceAllString(col, "_")
		if *verbose && col_name != col {
			fmt.Fprintf(os.Stderr, "Column %x renamed to %s\n", col, col_name)
		}

		buffer.WriteString(col_name + " TEXT")

		if i != len(*columnNames)-1 {
			buffer.WriteString(", ")
		} else {
			if len(primarykeys) > 0 {
				buffer.WriteString(", primary key(")
				for idx, fld := range primarykeys {
					buffer.WriteString(fld)
					if idx != len(primarykeys)-1 {
						buffer.WriteString(", ")
					}
				}
				buffer.WriteString(")")
			}
		}
	}

	buffer.WriteString(");")
	if *verbose {
		fmt.Println(buffer.String())
	}

	_, err := db.Exec(buffer.String())
	if err != nil {
		log.Fatalln(err)
	}

	return err
}

func createLoadStmt(tableName *string, values *[]string, vld_op string, blWithPK bool, db *sql.Tx, verbose *bool) *sql.Stmt {
	if len(*values) == 0 {
		log.Fatalln("Nothing to build insert with!")
	}
	var buffer bytes.Buffer

	buffer.WriteString("INSERT ")

	if len(vld_op) > 0 && blWithPK {
		buffer.WriteString(" OR " + vld_op + " ")
	}
	buffer.WriteString(" INTO " + (*tableName) + " VALUES (")
	for i := range *values {
		buffer.WriteString("?")
		if i != len(*values)-1 {
			buffer.WriteString(", ")
		}
	}
	buffer.WriteString(");")
	if *verbose {
		fmt.Println(buffer.String())
	}

	stmt, err := db.Prepare(buffer.String())
	if err != nil {
		log.Fatalln(err)
	}
	return stmt
}

func loadRow(tableName *string, values *[]string, db *sql.Tx, stmt *sql.Stmt, verbose *bool) error {
	if len(*values) == 0 {
		return nil
	}
	vals := make([]interface{}, 0)
	for _, val := range *values {
		vals = append(vals, val)
	}
	_, err := stmt.Exec(vals...)
	if err != nil && *verbose {
		fmt.Fprintln(os.Stderr, "Bad row: ", err)
	}
	return err
}

func displayResult(rows *sql.Rows) {
	cols, cols_err := rows.Columns()

	if cols_err != nil {
		log.Fatalln(cols_err)
	}

	rawResult := make([][]byte, len(cols))
	result := make([]string, len(cols))

	dest := make([]interface{}, len(cols))
	for i, _ := range cols {
		dest[i] = &rawResult[i]
	}

	for rows.Next() {
		rows.Scan(dest...)

		for i, raw := range rawResult {
			result[i] = string(raw)
		}

		for j, v := range result {
			fmt.Printf("%s", v)
			if j != len(result)-1 {
				fmt.Printf(", ")
			}
		}
		fmt.Printf("\n")
	}
}

func openFileOrStdin(path *string) *os.File {
	var fp *os.File
	var err error
	if (*path) == "stdin" {
		fp = os.Stdin
		err = nil
	} else {
		fp, err = os.Open(*cleanPath(path))
	}

	if err != nil {
		log.Fatalln(err)
	}

	return fp
}

func cleanPath(path *string) *string {
	var result string
	usr, err := user.Current()
	if err != nil {
		log.Fatalln(err)
	}

	if (*path)[:2] == "~/" {
		dir := usr.HomeDir + "/"
		result = strings.Replace(*path, "~/", dir, 1)
	} else {
		result = (*path)
	}

	abs_result, abs_err := filepath.Abs(result)
	if abs_err != nil {
		log.Fatalln(err)
	}

	clean_result := filepath.Clean(abs_result)

	return &clean_result
}

func openDB(path *string, no_memory *bool) (*sql.DB, *string) {
	openPath := ":memory:"
	if len(*path) != 0 {
		openPath = *cleanPath(path)
	} else if *no_memory {
		outDir, err := ioutil.TempDir(os.TempDir(), "textql")
		if err != nil {
			log.Fatalln(err)
		}
		openPath = filepath.Join(outDir, "textql.db")
	}

	db, err := sql.Open("sqlite3", openPath)

	if err != nil {
		log.Fatalln(err)
	}
	return db, &openPath
}

func determineSeperator(delimiter *string) rune {
	var seperator rune

	if (*delimiter) == "tab" {
		seperator = '\t'
	} else if strings.Index((*delimiter), "0x") == 0 {
		dlm, hex_err := hex.DecodeString((*delimiter)[2:])

		if hex_err != nil {
			log.Fatalln(hex_err)
		}

		seperator, _ = utf8.DecodeRuneInString(string(dlm))
	} else {
		seperator, _ = utf8.DecodeRuneInString(*delimiter)
	}
	return seperator
}

func determinePKs(pks *string, seperator string) []string {
	pk := []string{}
	tmp := strings.Split(*pks, seperator)
	for _, fld := range tmp {
		if len(strings.TrimSpace(fld)) > 0 {
			pk = append(pk, strings.TrimSpace(fld))
		}
	}
	return pk
}

func determineDupOp(ori_op *string) string {
	// all valiable duplication-operator
	all_ops := []string{
		"replace",
		"rollback",
		"abord",
		"fail",
		"ignore",
	}
	rtn := ""
	tmp := strings.ToLower(strings.TrimSpace(*ori_op))
	for _, item := range all_ops {
		if item == tmp {
			rtn = tmp
			break
		}
	}
	return rtn
}

func countTable(db *sql.DB, strTable string, strCondition string) int {
	strSQL := "select count(*) as N_COUNT from " + strTable
	if len(strCondition) > 0 {
		strSQL += " where " + strCondition
	}
	rows, err := db.Query(strSQL)
	if err != nil {
		log.Fatalln("Failed to count records on table("+strCondition+"):", err)
	}
	defer rows.Close()

	rows.Next()

	result := 0
	rows.Scan(&result)
	return result
}

func FetchTableSchema(db *sql.DB, table string) *TableSchema {
	rows, err := db.Query("pragma table_info('" + table + "')")
	if err != nil {
		log.Fatalln("Failed to fetch table schema from database : ", table)
		log.Fatalln(err.Error())
		return nil
	}
	defer rows.Close()

	schema := new(TableSchema)
	var tmpFld FieldDefinition
	counter := 0
	for rows.Next() {
		rows.Scan(&tmpFld.cid, &tmpFld.field_name, &tmpFld.field_type, &tmpFld.notnull, &tmpFld.dflt_value, &tmpFld.pk)
		schema.Fields = append(schema.Fields, tmpFld)
		counter++
	}
	return schema
}

func SummaryTableByField(db *sql.DB, table string, field string) *FieldSummary {
	strSQL := `select
		min(length(` + field + `)) as len_min,
		max(length(` + field + `)) as len_max,
		count(distinct ` + field + `) as var_dist,
		min(` + field + `) as var_min,
		max(` + field + `) as var_max
	from ` + table
	rows, err := db.Query(strSQL)
	if err != nil {
		log.Fatalln("Failed to summary table(", table, ") on : ", field)
		log.Fatalln(err.Error())
		return nil
	}
	defer rows.Close()

	result := new(FieldSummary)
	result.var_name = field
	rows.Next()
	rows.Scan(
		&result.min_length,
		&result.max_length,
		&result.distinct,
		&result.min_val,
		&result.max_var,
	)
	return result
}
