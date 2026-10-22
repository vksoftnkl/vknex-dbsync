package pg

import (
	"context"
	"fmt"
	"io"

	"github.com/jackc/pgx/v5/pgconn"
)

// CopyStream pipes `COPY (selectSQL) TO STDOUT` on the source straight into
// `COPY into (cols) FROM STDIN` on the target. Rows never sit in Go memory, so
// table size does not matter. Binary format is used unless a column type is not
// portable between databases (arrays of enums, composites), then text.
func CopyStream(ctx context.Context, src, dst *pgconn.PgConn, selectSQL, into string, cols []string, text bool) (int64, error) {
	format := "binary"
	if text {
		format = "text"
	}
	outSQL := fmt.Sprintf("COPY (%s) TO STDOUT (FORMAT %s)", selectSQL, format)
	inSQL := fmt.Sprintf("COPY %s (%s) FROM STDIN (FORMAT %s)", into, IdentList(cols, ""), format)

	pr, pw := io.Pipe()
	srcErr := make(chan error, 1)
	go func() {
		_, err := src.CopyTo(ctx, pw, outSQL)
		pw.CloseWithError(err) // nil => clean EOF for the reader
		srcErr <- err
	}()

	tag, dstErr := dst.CopyFrom(ctx, pr, inSQL)
	if dstErr != nil {
		pr.CloseWithError(dstErr) // unblock the source writer
	}
	sErr := <-srcErr

	switch {
	case dstErr != nil && sErr != nil && sErr != dstErr:
		return 0, fmt.Errorf("target copy: %w (source: %v)", dstErr, sErr)
	case dstErr != nil:
		return 0, fmt.Errorf("target copy: %w", dstErr)
	case sErr != nil:
		return 0, fmt.Errorf("source copy: %w", sErr)
	}
	return tag.RowsAffected(), nil
}
