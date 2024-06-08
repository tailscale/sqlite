// Package jsonb handles SQLite's JSONB format.
//
// See https://sqlite.org/draft/jsonb.html.
package jsonb

//go:generate go run golang.org/x/tools/cmd/stringer -type=Type

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
)

// Value is a JSONB value.
//
// The methods on Value report whether it's valid, its type, length,
// and so on.
type Value []byte

func (v Value) HeaderLen() int {
	if len(v) == 0 {
		return 0
	}
	switch v[0] >> 4 {
	default:
		return 1
	case 0xc:
		return 2
	case 0xd:
		return 3
	case 0xe:
		return 5
	case 0xf:
		return 9
	}
}

func (v Value) Type() Type {
	if len(v) == 0 {
		panic("Type called on invalid Value")
	}
	return Type(v[0] & 0xf)
}

func (v Value) PayloadLen() int {
	switch v.HeaderLen() {
	default:
		return 0
	case 1:
		return int(v[0] >> 4)
	case 2:
		return int(v[1])
	case 3:
		return int(binary.BigEndian.Uint16(v[1:]))
	case 5:
		n := binary.BigEndian.Uint32(v[1:])
		if int64(n) > math.MaxInt {
			return 0
		}
		return int(n)
	case 9:
		n := binary.BigEndian.Uint64(v[1:])
		if n > math.MaxInt {
			return 0
		}
		return int(n)
	}
}

// Payload returns the payload of the element.
//
// Depending on v's element type, the payload may be a series of zero+
// concatenated valid Value elements.
func (v Value) Payload() []byte {
	return v[v.HeaderLen():][:v.PayloadLen()]
}

// RangeArray calls f for each element in v, which must be an array. It returns
// an error if v is not a valid array, or if f returns an error.
func (v Value) RangeArray(f func(Value) error) error {
	if !v.Valid() {
		return fmt.Errorf("not valid")
	}
	if v.Type() != Array {
		return fmt.Errorf("got type %v; not an array", v.Type())
	}
	pay := v.Payload()
	for len(pay) > 0 {
		v, rest, ok := Cut(pay)
		pay = rest
		if !ok {
			return errors.New("malformed array payload")
		}
		if err := f(v); err != nil {
			return err
		}
	}
	return nil
}

// RangeObject calls f for each pair in v, which must be an object. It returns
// an error if v is not a valid object, or if f returns an error.
func (v Value) RangeObject(f func(k, v Value) error) error {
	if !v.Valid() {
		return fmt.Errorf("not valid")
	}
	if v.Type() != Object {
		return fmt.Errorf("got type %v; not an object", v.Type())
	}
	pay := v.Payload()
	for len(pay) > 0 {
		key, rest, ok := Cut(pay)
		pay = rest
		if !ok {
			return errors.New("malformed array payload")
		}
		val, rest, ok := Cut(pay)
		pay = rest
		if !ok {
			return errors.New("malformed array payload")
		}
		if !key.Type().CanText() {
			return errors.New("object key is not text")
		}
		if err := f(key, val); err != nil {
			return err
		}
	}
	return nil
}

// Cut returns the first valid JSONB element in v, the rest of v, and whether
// the cut was successful. When ok is true, v is Valid.
func Cut(b []byte) (v Value, rest []byte, ok bool) {
	if len(b) == 0 {
		return nil, nil, false
	}
	v = Value(b)
	hlen := v.HeaderLen()
	if hlen == 0 {
		return nil, nil, false
	}
	plen := v.PayloadLen()
	if len(v) < hlen+plen {
		return nil, nil, false
	}
	return v[:hlen+plen], b[hlen+plen:], true
}

// Valid reports whether v contains a single valid JSONB value.
func (v Value) Valid() bool {
	h := v.HeaderLen()
	p := v.PayloadLen()
	return h > 0 && len(v) == h+p
}

// Text returns the unescaped text of v, which must be a text element.
func (v Value) Text() string {
	t := v.Type()
	if !t.CanText() {
		panic("Text called on non-text Value")
	}
	switch t {
	case Text:
		return string(v.Payload())
	case TextJ:
		got, err := appendUnquote(nil, v.Payload())
		if err != nil {
			// TODO: add TextErr variant?
			panic(err)
		}
		return string(got)
	case TextRaw:
		return string(v.Payload()) // TODO: escape stuff?
	case Text5:
		got, err := appendUnquote(nil, v.Payload())
		if err != nil {
			// TODO: add TextErr variant?
			panic(err)
		}
		return string(got)
	}
	panic("unreachable")
}

// Int returns the integer value of v.
// It panics if v is not an integer type or can't fit in an int64.
// TODO(bradfitz): add IntOk for a non-panicking out-of-bounds version?
func (v Value) Int() int64 {
	t := v.Type()
	if !t.CanInt() {
		panic("Int called on non-int Value")
	}
	switch t {
	case Int:
		n, err := strconv.ParseInt(string(v.Payload()), 10, 64)
		if err != nil {
			panic(err)
		}
		return n
	default:
		panic(fmt.Sprintf("TODO: handle %v", t))
	}
}

// Float returns the float64 value of v.
// It panics if v is not an integer type or can't fit in an float64.
// TODO(bradfitz): add IntOk for a non-panicking out-of-bounds version?
func (v Value) Float() float64 {
	t := v.Type()
	if !t.CanFloat() {
		panic("Float called on non-float Value")
	}
	switch t {
	case Float:
		n, err := strconv.ParseFloat(string(v.Payload()), 64)
		if err != nil {
			panic(err)
		}
		return n
	default:
		panic(fmt.Sprintf("TODO: handle %v", t))
	}
}

// Type is a JSONB element type.
type Type byte

const (
	Null   Type = 0x0
	True   Type = 0x1
	False  Type = 0x2
	Int    Type = 0x3
	Int5   Type = 0x4
	Float  Type = 0x5
	Float5 Type = 0x6

	// Text is a JSON string value that does not contain any escapes nor any
	// characters that need to be escaped for either SQL or JSON
	Text Type = 0x7
	// TextJ is a JSON string value that contains RFC 8259 character escapes
	// (such as "\n" or "\u0020"). Those escapes will need to be translated into
	// actual UTF8 if this element is extracted into SQL. The payload is the
	// UTF8 text representation of the escaped string value.
	TextJ   Type = 0x8
	Text5   Type = 0x9
	TextRaw Type = 0xa

	Array  Type = 0xb
	Object Type = 0xc // pairs of key/value

	Reserved13 Type = 0xd
	Reserved14 Type = 0xe
	Reserved15 Type = 0xf
)

func (t Type) CanText() bool {
	return t >= Text && t <= TextRaw
}

func (t Type) CanInt() bool {
	return t == Int || t == Int5
}

func (t Type) CanBool() bool {
	return t == True || t == False
}

func (t Type) CanFloat() bool {
	return t == Float || t == Float5
}
