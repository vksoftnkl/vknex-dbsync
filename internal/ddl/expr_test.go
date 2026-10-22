package ddl

import "testing"

func TestNormExpr(t *testing.T) {
	src := `CHECK (((abj_adj_type)::text = ANY ((ARRAY['ALLOCATION'::character varying, 'A,B''x'::character varying])::text[])))`
	dst := `CHECK (((abj_adj_type)::text = ANY (ARRAY[('ALLOCATION'::character varying)::text, ('A,B''x'::character varying)::text])))`
	if !sameExpr(src, dst) {
		t.Fatalf("not equal:\n%s\n%s", normExpr(src), normExpr(dst))
	}
	if sameExpr(src, `CHECK (((abj_adj_type)::text = ANY (ARRAY[('ALLOCATION'::character varying)::text])))`) {
		t.Fatal("different lists compare equal")
	}
	if got := normExpr(`x = ANY ((ARRAY[1, 2])::numeric(10,2)[])`); got != `x = ANY (ARRAY[(1)::numeric(10,2), (2)::numeric(10,2)])` {
		t.Fatalf("modifier: %s", got)
	}
	if s := `ARRAY['a'::text] || (ARRAY['b'])::foo`; normExpr(s) != s {
		t.Fatalf("non-array cast changed: %s", normExpr(s))
	}
}
